use std::collections::HashMap;
use std::convert::Infallible;
use std::io::Write as _;
use std::net::SocketAddr;
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use bytes::Bytes;
use http_body_util::{BodyExt, StreamBody};
use hyper::body::{Frame, Incoming};
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;
use tokio::sync::{Mutex, Notify, RwLock};
use tokio::time::Duration;
use tokio_stream::wrappers::ReceiverStream;

use crate::error::{BoxBody, PipeError, ok_response};

const READ_BUF_SIZE: usize = 64 * 1024;

struct PipeInner {
    written: u64,
    done: bool,
    content_length: Option<u64>,
    reader_count: u32,
    upload_started_at: Instant,
    upload_ended_at: Option<Instant>,
    first_get_at: Option<Instant>,
    last_get_at: Option<Instant>,
}

struct PipeEntry {
    inner: Mutex<PipeInner>,
    /// Shared file handle. Writer appends under the mutex.
    /// Readers use `read_at()` (pread) with their own offset — no seek, no conflict.
    file: std::fs::File,
    path: PathBuf,
    notify: Notify,
}

pub struct AppState {
    pipes: RwLock<HashMap<String, Arc<PipeEntry>>>,
    key_added: Notify,
    draining: AtomicBool,
    data_dir: PathBuf,
}

async fn handle(
    req: Request<Incoming>,
    state: Arc<AppState>,
) -> Result<Response<BoxBody>, Infallible> {
    let key = req.uri().path().trim_start_matches('/').to_string();

    if key.is_empty() {
        return Ok(PipeError::EmptyKey.into_response());
    }

    match *req.method() {
        Method::PUT => Ok(handle_put(key, req, state).await),
        Method::GET => Ok(handle_get(key, state).await),
        _ => Ok(PipeError::MethodNotAllowed.into_response()),
    }
}

async fn handle_put(
    key: String,
    req: Request<Incoming>,
    state: Arc<AppState>,
) -> Response<BoxBody> {
    if state.draining.load(Ordering::Relaxed) {
        return PipeError::Draining.into_response();
    }

    let content_length = req
        .headers()
        .get(hyper::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok());

    // Reject if key already exists
    {
        let map = state.pipes.read().await;

        if map.contains_key(&key) {
            return PipeError::KeyAlreadyExists.into_response();
        }
    }

    // Create temp file for buffering
    let file_path = state.data_dir.join(format!(
        "pipe-{}-{}",
        std::process::id(),
        key.replace('/', "_")
    ));

    let file = match std::fs::File::options()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&file_path)
    {
        Ok(f) => f,
        Err(e) => {
            return PipeError::from_io(e).into_response();
        }
    };

    let entry = Arc::new(PipeEntry {
        inner: Mutex::new(PipeInner {
            written: 0,
            done: false,
            content_length,
            reader_count: 0,
            upload_started_at: Instant::now(),
            upload_ended_at: None,
            first_get_at: None,
            last_get_at: None,
        }),
        file,
        path: file_path,
        notify: Notify::new(),
    });

    {
        let mut map = state.pipes.write().await;
        map.insert(key.clone(), entry.clone());
    }

    // Notify GETs waiting for this key
    state.key_added.notify_waiters();

    eprintln!("[PUT] key={key} upload started");

    // Stream the request body to disk
    let mut body = req.into_body();

    loop {
        match body.frame().await {
            Some(Ok(frame)) => {
                if let Ok(data) = frame.into_data() {
                    if !data.is_empty() {
                        // Write to disk (writer is the only one calling write)
                        if let Err(e) = (&entry.file).write_all(&data) {
                            let mut inner = entry.inner.lock().await;
                            inner.done = true;
                            drop(inner);
                            entry.notify.notify_waiters();
                            return PipeError::from_io(e).into_response();
                        }

                        // Update written counter under the lock
                        let mut inner = entry.inner.lock().await;
                        inner.written += data.len() as u64;
                        drop(inner);
                        entry.notify.notify_waiters();
                    }
                }
            }
            Some(Err(e)) => {
                let mut inner = entry.inner.lock().await;
                inner.done = true;
                drop(inner);
                entry.notify.notify_waiters();
                return PipeError::UploadError(e).into_response();
            }
            None => {
                let mut inner = entry.inner.lock().await;
                inner.done = true;
                inner.upload_ended_at = Some(Instant::now());
                let total_bytes = inner.written;
                drop(inner);
                entry.notify.notify_waiters();
                eprintln!("[PUT] key={key} upload complete: {total_bytes} bytes");
                break;
            }
        }
    }

    // Cleanup 30s after upload ends
    let state_clone = state.clone();
    let key_clone = key.clone();

    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(30)).await;
        cleanup_key(&state_clone, &key_clone).await;
    });

    ok_response()
}

async fn handle_get(key: String, state: Arc<AppState>) -> Response<BoxBody> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);

    // Wait for the key to appear (up to 5s)
    let entry = loop {
        let notified = state.key_added.notified();

        {
            let map = state.pipes.read().await;

            if let Some(entry) = map.get(&key) {
                break entry.clone();
            }
        }

        tokio::select! {
            _ = notified => continue,
            _ = tokio::time::sleep_until(deadline) => {
                return PipeError::KeyNotFound.into_response();
            }
        }
    };

    // Update metadata
    let content_length = {
        let mut inner = entry.inner.lock().await;
        inner.reader_count += 1;
        let now = Instant::now();

        if inner.first_get_at.is_none() {
            inner.first_get_at = Some(now);

            // Schedule cleanup 5s after first GET
            let state_clone = state.clone();
            let key_clone = key.clone();

            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(5)).await;
                cleanup_key(&state_clone, &key_clone).await;
            });
        }

        inner.last_get_at = Some(now);
        let reader_num = inner.reader_count;
        eprintln!("[GET] key={key} reader #{reader_num}");
        inner.content_length
    };

    // Stream the response using pread (read_at) on the shared file descriptor
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Frame<Bytes>, Infallible>>(2);

    tokio::spawn(async move {
        let mut pos: u64 = 0;
        let mut buf = vec![0u8; READ_BUF_SIZE];

        loop {
            // Register interest BEFORE checking to avoid missed notifications
            let notified = entry.notify.notified();

            let (written, is_done) = {
                let inner = entry.inner.lock().await;
                (inner.written, inner.done)
            };

            // Read all available data from disk using pread (no seek needed)
            while pos < written {
                let to_read = std::cmp::min(READ_BUF_SIZE as u64, written - pos) as usize;

                match entry.file.read_at(&mut buf[..to_read], pos) {
                    Ok(n) if n > 0 => {
                        pos += n as u64;

                        if tx
                            .send(Ok(Frame::data(Bytes::copy_from_slice(&buf[..n]))))
                            .await
                            .is_err()
                        {
                            return; // Client disconnected
                        }
                    }
                    Ok(_) => break, // Short read, wait for more
                    Err(e) => {
                        eprintln!("[GET] read error: {e}");
                        return;
                    }
                }
            }

            if is_done {
                return;
            }

            notified.await;
        }
    });

    let stream = ReceiverStream::new(rx);
    let body = StreamBody::new(stream).boxed();

    let mut response = Response::builder().status(StatusCode::OK);

    if let Some(len) = content_length {
        response = response.header(hyper::header::CONTENT_LENGTH, len);
    }

    response.body(body).unwrap()
}

async fn cleanup_key(state: &AppState, key: &str) {
    let entry = state.pipes.write().await.remove(key);

    if let Some(entry) = entry {
        if let Err(e) = std::fs::remove_file(&entry.path) {
            if e.kind() != std::io::ErrorKind::NotFound {
                eprintln!("[CLEANUP] key={key} failed to remove file: {e}");
            }
        }

        eprintln!("[CLEANUP] key={key} removed");
    }
}

pub struct ServerHandle {
    pub addr: SocketAddr,
    state: Arc<AppState>,
}

impl ServerHandle {
    /// Start draining: reject new PUTs with 503, but let existing transfers finish.
    pub fn drain(&self) {
        self.state.draining.store(true, Ordering::Relaxed);
    }

    /// Remove all temp files for active pipes.
    pub async fn cleanup(&self) {
        let mut map = self.state.pipes.write().await;
        let keys: Vec<String> = map.keys().cloned().collect();

        for key in &keys {
            if let Some(entry) = map.remove(key) {
                if let Err(e) = std::fs::remove_file(&entry.path) {
                    if e.kind() != std::io::ErrorKind::NotFound {
                        eprintln!("[CLEANUP] key={key} failed to remove file: {e}");
                    }
                }

                eprintln!("[CLEANUP] key={key} removed");
            }
        }
    }
}

/// Start the server on the given address.
pub async fn start_server(addr: &str) -> ServerHandle {
    start_server_with_data_dir(addr, std::env::temp_dir()).await
}

/// Start the server with a custom data directory for temp files.
pub async fn start_server_with_data_dir(addr: &str, data_dir: impl Into<PathBuf>) -> ServerHandle {
    let data_dir = data_dir.into();
    std::fs::create_dir_all(&data_dir).expect("failed to create data directory");

    let state = Arc::new(AppState {
        pipes: RwLock::new(HashMap::new()),
        key_added: Notify::new(),
        draining: AtomicBool::new(false),
        data_dir,
    });

    let listener = TcpListener::bind(addr).await.unwrap();
    let local_addr = listener.local_addr().unwrap();

    let state_clone = state.clone();

    tokio::spawn(async move {
        loop {
            let (stream, remote) = match listener.accept().await {
                Ok(conn) => conn,
                Err(e) => {
                    eprintln!("[ERROR] accept failed: {e}");
                    continue;
                }
            };
            let state = state_clone.clone();

            tokio::spawn(async move {
                let io = TokioIo::new(stream);

                let service = service_fn(move |req| {
                    let state = state.clone();
                    handle(req, state)
                });

                if let Err(e) = hyper::server::conn::http1::Builder::new()
                    .serve_connection(io, service)
                    .await
                {
                    eprintln!("[ERROR] {remote}: {e}");
                }
            });
        }
    });

    ServerHandle {
        addr: local_addr,
        state,
    }
}
