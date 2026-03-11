use std::collections::HashMap;
use std::convert::Infallible;
use std::net::SocketAddr;
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

struct PipeInner {
    chunks: Vec<Bytes>,
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
    notify: Notify,
}

pub struct AppState {
    pipes: RwLock<HashMap<String, Arc<PipeEntry>>>,
    key_added: Notify,
    draining: AtomicBool,
}

type BoxBody = http_body_util::combinators::BoxBody<Bytes, Infallible>;

fn empty_response(status: StatusCode) -> Response<BoxBody> {
    Response::builder()
        .status(status)
        .body(
            http_body_util::Empty::new()
                .map_err(|never| match never {})
                .boxed(),
        )
        .unwrap()
}

async fn handle(
    req: Request<Incoming>,
    state: Arc<AppState>,
) -> Result<Response<BoxBody>, Infallible> {
    let key = req.uri().path().trim_start_matches('/').to_string();

    if key.is_empty() {
        return Ok(empty_response(StatusCode::BAD_REQUEST));
    }

    match *req.method() {
        Method::PUT => Ok(handle_put(key, req, state).await),
        Method::GET => Ok(handle_get(key, state).await),
        _ => Ok(empty_response(StatusCode::METHOD_NOT_ALLOWED)),
    }
}

async fn handle_put(
    key: String,
    req: Request<Incoming>,
    state: Arc<AppState>,
) -> Response<BoxBody> {
    if state.draining.load(Ordering::Relaxed) {
        return empty_response(StatusCode::SERVICE_UNAVAILABLE);
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
            return empty_response(StatusCode::CONFLICT);
        }
    }

    let entry = Arc::new(PipeEntry {
        inner: Mutex::new(PipeInner {
            chunks: Vec::new(),
            done: false,
            content_length,
            reader_count: 0,
            upload_started_at: Instant::now(),
            upload_ended_at: None,
            first_get_at: None,
            last_get_at: None,
        }),
        notify: Notify::new(),
    });

    {
        let mut map = state.pipes.write().await;
        map.insert(key.clone(), entry.clone());
    }

    // Notify GETs waiting for this key
    state.key_added.notify_waiters();

    eprintln!("[PUT] key={key} upload started");

    // Stream the request body into the pipe
    let mut body = req.into_body();

    loop {
        match body.frame().await {
            Some(Ok(frame)) => {
                if let Ok(data) = frame.into_data() {
                    if !data.is_empty() {
                        let mut inner = entry.inner.lock().await;
                        inner.chunks.push(data);
                        drop(inner);
                        entry.notify.notify_waiters();
                    }
                }
            }
            Some(Err(e)) => {
                eprintln!("[PUT] key={key} upload error: {e}");
                let mut inner = entry.inner.lock().await;
                inner.done = true;
                drop(inner);
                entry.notify.notify_waiters();
                return empty_response(StatusCode::INTERNAL_SERVER_ERROR);
            }
            None => {
                let mut inner = entry.inner.lock().await;
                inner.done = true;
                inner.upload_ended_at = Some(Instant::now());
                let chunks_count = inner.chunks.len();
                let total_bytes: usize = inner.chunks.iter().map(|c| c.len()).sum();
                drop(inner);
                entry.notify.notify_waiters();
                eprintln!(
                    "[PUT] key={key} upload complete: {total_bytes} bytes in {chunks_count} chunks"
                );
                break;
            }
        }
    }

    // Cleanup 30s after upload ends
    let state_clone = state.clone();
    let key_clone = key.clone();

    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(30)).await;

        if state_clone.pipes.write().await.remove(&key_clone).is_some() {
            eprintln!("[CLEANUP] key={key_clone} removed (30s after upload)");
        }
    });

    empty_response(StatusCode::OK)
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
                eprintln!("[GET] key={key} not found (timeout)");
                return empty_response(StatusCode::NOT_FOUND);
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

                if state_clone.pipes.write().await.remove(&key_clone).is_some() {
                    eprintln!("[CLEANUP] key={key_clone} removed (5s after first GET)");
                }
            });
        }

        inner.last_get_at = Some(now);
        let reader_num = inner.reader_count;
        eprintln!("[GET] key={key} reader #{reader_num}");
        inner.content_length
    };

    // Stream the response from the pipe
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Frame<Bytes>, Infallible>>(32);

    tokio::spawn(async move {
        let mut pos = 0;

        loop {
            // Register interest BEFORE checking to avoid missed notifications
            let notified = entry.notify.notified();

            let (new_chunks, is_done) = {
                let inner = entry.inner.lock().await;
                let chunks: Vec<Bytes> = inner.chunks[pos..].to_vec();
                pos = inner.chunks.len();
                (chunks, inner.done)
            };

            for chunk in new_chunks {
                if tx.send(Ok(Frame::data(chunk))).await.is_err() {
                    return; // Client disconnected
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

pub struct ServerHandle {
    pub addr: SocketAddr,
    state: Arc<AppState>,
}

impl ServerHandle {
    /// Start draining: reject new PUTs with 503, but let existing transfers finish.
    pub fn drain(&self) {
        self.state.draining.store(true, Ordering::Relaxed);
    }
}

/// Start the server on the given address.
pub async fn start_server(addr: &str) -> ServerHandle {
    let state = Arc::new(AppState {
        pipes: RwLock::new(HashMap::new()),
        key_added: Notify::new(),
        draining: AtomicBool::new(false),
    });

    let listener = TcpListener::bind(addr).await.unwrap();
    let local_addr = listener.local_addr().unwrap();

    let state_clone = state.clone();

    tokio::spawn(async move {
        loop {
            let (stream, remote) = listener.accept().await.unwrap();
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
