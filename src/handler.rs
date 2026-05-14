use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;

use bytes::Bytes;
use http_body_util::{BodyExt, StreamBody};
use hyper::body::{Frame, Incoming};
use hyper::{Method, Request, Response, StatusCode};
use tokio::time::Duration;
use tokio_stream::wrappers::ReceiverStream;

use crate::error::{BoxBody, PipeError, ok_response};
use crate::state::{AppState, PipeEntry, PipeMetadata, cleanup_entry};

/// Extract the multipart boundary from Content-Type, if present.
fn multipart_boundary(req: &Request<Incoming>) -> Option<String> {
    let ct = req.headers().get(hyper::header::CONTENT_TYPE)?.to_str().ok()?;

    if !ct.starts_with("multipart/form-data") {
        return None;
    }

    multer::parse_boundary(ct).ok()
}

const READ_BUF_SIZE: usize = 64 * 1024;

pub async fn handle(
    req: Request<Incoming>,
    state: Arc<AppState>,
) -> Result<Response<BoxBody>, Infallible> {
    let key = req.uri().path().trim_start_matches('/').to_string();

    if key.is_empty() {
        return Ok(PipeError::EmptyKey.into_response());
    }

    if key.len() > 512 {
        return Ok(PipeError::KeyTooLong.into_response());
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
    if state.draining.load(Ordering::Acquire) {
        return PipeError::Draining.into_response();
    }

    let boundary = multipart_boundary(&req);

    let content_length = req
        .headers()
        .get(hyper::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok());

    // For raw uploads, capture Content-Type if provided
    let raw_content_type = if boundary.is_none() {
        req.headers()
            .get(hyper::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(String::from)
    } else {
        None
    };

    // Optional: forward upload to an external URL (e.g. S3 presigned PUT)
    let forward_url = req
        .headers()
        .get("x-forward-url")
        .and_then(|v| v.to_str().ok())
        .map(String::from);

    // BLAKE3 hash of the key to avoid collisions (e.g. "a/b" vs "a_b").
    let file_path = state.data_dir.join(format!(
        "pipe-{}-{}",
        std::process::id(),
        blake3::hash(key.as_bytes()).to_hex()
    ));

    let entry = Arc::new(PipeEntry {
        meta: tokio::sync::Mutex::new(PipeMetadata {
            content_length,
            mime_type: raw_content_type,
            filename: None,
            upload_ended_at: None,
            first_get_at: None,
            last_get_at: None,
        }),
        written: 0.into(),
        done: false.into(),
        file: tokio::sync::Mutex::new(None),
        path: file_path,
        notify: tokio::sync::Notify::new(),
    });

    // Atomic check-and-insert via DashMap entry API
    match state.pipes.entry(key.clone()) {
        dashmap::Entry::Occupied(_) => return PipeError::KeyAlreadyExists.into_response(),
        dashmap::Entry::Vacant(e) => {
            e.insert(entry.clone());
        }
    }

    // Create the backing file before any reader can observe data
    let file = match crate::io::create_rw(&entry.path).await {
        Ok(f) => f,
        Err(e) => {
            state.pipes.remove(&key);
            return PipeError::from_io(e).into_response();
        }
    };
    *entry.file.lock().await = Some(file);

    // Notify GETs waiting for this specific key
    if let Some((_, waiter)) = state.key_waiters.remove(&key) {
        waiter.notify_waiters();
    }

    // Set up forward channel if a forward URL was provided
    let (forward_tx, forward_task) = if let Some(url) = forward_url {
        eprintln!("[PUT] key={key} forwarding to {url}");
        let (tx, rx) = tokio::sync::mpsc::channel::<Bytes>(16);
        let body_stream = tokio_stream::StreamExt::map(
            ReceiverStream::new(rx),
            Ok::<_, std::io::Error>,
        );
        let task = tokio::spawn(async move {
            reqwest::Client::new()
                .put(url)
                .body(reqwest::Body::wrap_stream(body_stream))
                .send()
                .await
        });
        (Some(tx), Some(task))
    } else {
        (None, None)
    };

    eprintln!("[PUT] key={key} upload started");

    let resp = if let Some(boundary) = boundary {
        stream_multipart(key, req.into_body(), boundary, entry, state, forward_tx).await
    } else {
        stream_raw(key, req.into_body(), entry, state, forward_tx).await
    };

    // If local upload failed, return that error
    if resp.status() != StatusCode::OK {
        return resp;
    }

    // Check forward result
    if let Some(task) = forward_task {
        match task.await {
            Ok(Ok(r)) if r.status().is_success() => {
                eprintln!("[FORWARD] success ({})", r.status());
            }
            Ok(Ok(r)) => {
                let status = r.status();
                let body = r.text().await.unwrap_or_default();
                return PipeError::ForwardError(format!("{status}: {body}")).into_response();
            }
            Ok(Err(e)) => {
                return PipeError::ForwardError(e.to_string()).into_response();
            }
            Err(e) => {
                return PipeError::ForwardError(e.to_string()).into_response();
            }
        }
    }

    resp
}

/// Stream a raw (non-multipart) body into the pipe.
async fn stream_raw(
    key: String,
    mut body: Incoming,
    entry: Arc<PipeEntry>,
    state: Arc<AppState>,
    forward_tx: Option<tokio::sync::mpsc::Sender<Bytes>>,
) -> Response<BoxBody> {
    loop {
        match body.frame().await {
            Some(Ok(frame)) => {
                if let Ok(data) = frame.into_data() {
                    if !data.is_empty() {
                        if let Some(ref tx) = forward_tx {
                            let _ = tx.send(data.clone()).await;
                        }

                        if let Err(resp) = write_chunk(&entry, &state, &data).await {
                            schedule_cleanup(key, state, entry);
                            return resp;
                        }
                    }
                }
            }
            Some(Err(e)) => {
                fail_upload(key, entry, state);
                return PipeError::UploadError(e).into_response();
            }
            None => {
                finalize_upload(&key, &entry).await;
                break;
            }
        }
    }

    schedule_cleanup(key, state, entry);
    ok_response()
}

/// Parse a multipart body, extract the first file field, and stream it into the pipe.
async fn stream_multipart(
    key: String,
    body: Incoming,
    boundary: String,
    entry: Arc<PipeEntry>,
    state: Arc<AppState>,
    forward_tx: Option<tokio::sync::mpsc::Sender<Bytes>>,
) -> Response<BoxBody> {
    let stream = body.into_data_stream();
    // Multer defaults are u64::MAX for stream/field sizes — our own
    // disk quota handles data limits, so no constraints needed.
    let mut multipart = multer::Multipart::new(stream, boundary);

    // Find the first field (we only support a single file per upload)
    let field = match multipart.next_field().await {
        Ok(Some(f)) => f,
        Ok(None) => {
            fail_upload(key, entry, state);
            return PipeError::EmptyMultipart.into_response();
        }
        Err(e) => {
            fail_upload(key, entry, state);
            return PipeError::MultipartError(e).into_response();
        }
    };

    // Extract metadata from the field before consuming it
    {
        let mut meta = entry.meta.lock().await;
        meta.filename = field.file_name().map(String::from);
        meta.mime_type = field.content_type().map(|m| m.to_string());

        // Content-Length from the outer request includes multipart framing,
        // so it's not meaningful for the file itself — clear it.
        meta.content_length = None;
    }

    eprintln!(
        "[PUT] key={key} multipart file={:?} type={:?}",
        field.file_name().map(String::from),
        field.content_type().map(|m| m.to_string()),
    );

    // Stream the field data
    let mut field = field;

    loop {
        match field.chunk().await {
            Ok(Some(data)) => {
                if !data.is_empty() {
                    if let Some(ref tx) = forward_tx {
                        let _ = tx.send(Bytes::copy_from_slice(&data)).await;
                    }

                    if let Err(resp) = write_chunk(&entry, &state, &data).await {
                        schedule_cleanup(key, state, entry);
                        return resp;
                    }
                }
            }
            Ok(None) => {
                finalize_upload(&key, &entry).await;
                break;
            }
            Err(e) => {
                fail_upload(key, entry, state);
                return PipeError::MultipartError(e).into_response();
            }
        }
    }

    schedule_cleanup(key, state, entry);
    ok_response()
}

async fn finalize_upload(key: &str, entry: &PipeEntry) {
    let total_bytes = entry.written.load(Ordering::Relaxed);
    entry.meta.lock().await.upload_ended_at = Some(Instant::now());
    entry.done.store(true, Ordering::Release);
    entry.notify.notify_waiters();
    eprintln!("[PUT] key={key} upload complete: {total_bytes} bytes");
}

fn schedule_cleanup(key: String, state: Arc<AppState>, entry: Arc<PipeEntry>) {
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(state.put_ttl)).await;
        cleanup_entry(&state, &key, &entry).await;
    });
}

/// Mark upload as failed and schedule cleanup so the entry doesn't leak.
fn fail_upload(key: String, entry: Arc<PipeEntry>, state: Arc<AppState>) {
    entry.done.store(true, Ordering::Release);
    entry.notify.notify_waiters();
    schedule_cleanup(key, state, entry);
}

/// Write a chunk to the pipe's backing file, enforcing disk quota.
async fn write_chunk(
    entry: &PipeEntry,
    state: &AppState,
    data: &[u8],
) -> Result<(), Response<BoxBody>> {
    let len = data.len() as u64;

    // Reserve disk quota optimistically
    if let Some(max) = state.max_disk_usage {
        let prev = state.disk_usage.fetch_add(len, Ordering::Relaxed);

        if prev + len > max {
            state.disk_usage.fetch_sub(len, Ordering::Relaxed);
            entry.done.store(true, Ordering::Release);
            entry.notify.notify_waiters();
            return Err(PipeError::DiskQuotaExceeded.into_response());
        }
    } else {
        state.disk_usage.fetch_add(len, Ordering::Relaxed);
    }

    let file_guard = entry.file.lock().await;
    let file = match file_guard.as_ref() {
        Some(f) => match f.try_clone() {
            Ok(f) => f,
            Err(e) => {
                drop(file_guard);
                state.disk_usage.fetch_sub(len, Ordering::Relaxed);
                entry.done.store(true, Ordering::Release);
                entry.notify.notify_waiters();
                return Err(PipeError::from_io(e).into_response());
            }
        },
        None => {
            drop(file_guard);
            state.disk_usage.fetch_sub(len, Ordering::Relaxed);
            entry.done.store(true, Ordering::Release);
            entry.notify.notify_waiters();
            return Err(PipeError::IoError(std::io::Error::other("missing file handle"))
                .into_response());
        }
    };
    drop(file_guard);

    let offset = entry.written.load(Ordering::Relaxed);

    if let Err(e) = crate::io::write_at(file, data, offset).await {
        state.disk_usage.fetch_sub(len, Ordering::Relaxed);
        entry.done.store(true, Ordering::Release);
        entry.notify.notify_waiters();
        return Err(PipeError::from_io(e).into_response());
    }
    entry.written.fetch_add(len, Ordering::Release);
    entry.notify.notify_waiters();

    Ok(())
}

async fn handle_get(key: String, state: Arc<AppState>) -> Response<BoxBody> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(state.get_wait_timeout);

    // Wait for the key to appear (up to 5s)
    let entry = loop {
        if let Some(entry) = state.pipes.get(&key) {
            break entry.clone();
        }

        // Register a per-key waiter so only the matching PUT wakes us
        let waiter = state
            .key_waiters
            .entry(key.clone())
            .or_insert_with(|| Arc::new(tokio::sync::Notify::new()))
            .clone();

        let notified = waiter.notified();

        // Re-check after registering (the PUT may have arrived between our check and register)
        if let Some(entry) = state.pipes.get(&key) {
            break entry.clone();
        }

        tokio::select! {
            _ = notified => continue,
            _ = tokio::time::sleep_until(deadline) => {
                // Clean up the waiter if we're the last one holding it.
                // The DashMap shard lock ensures no one can clone the Arc
                // between our strong_count check and the remove.
                if let dashmap::Entry::Occupied(e) = state.key_waiters.entry(key) {
                    if Arc::strong_count(e.get()) == 2 {
                        // 2 refs: the map entry + our local `waiter` clone
                        e.remove();
                    }
                }

                return PipeError::KeyNotFound.into_response();
            }
        }
    };

    // Update metadata (behind mutex, but only once per GET — not on hot path)
    let (content_length, mime_type, filename) = {
        let mut meta = entry.meta.lock().await;
        let now = Instant::now();

        if meta.first_get_at.is_none() {
            meta.first_get_at = Some(now);

            // Schedule cleanup get_ttl seconds after upload finishes (or immediately if already done).
            // We must wait for the upload to complete before removing the entry.
            let state_clone = state.clone();
            let key_clone = key.clone();
            let entry_clone = entry.clone();

            tokio::spawn(async move {
                // Wait for the upload to finish first
                loop {
                    if entry_clone.done.load(Ordering::Acquire) {
                        break;
                    }

                    let notified = entry_clone.notify.notified();

                    if entry_clone.done.load(Ordering::Acquire) {
                        break;
                    }

                    notified.await;
                }

                tokio::time::sleep(Duration::from_secs(state_clone.get_ttl)).await;
                cleanup_entry(&state_clone, &key_clone, &entry_clone).await;
            });
        }

        meta.last_get_at = Some(now);
        eprintln!("[GET] key={key}");
        (meta.content_length, meta.mime_type.clone(), meta.filename.clone())
    };

    // Stream the response
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Frame<Bytes>, Infallible>>(2);

    tokio::spawn(async move {
        let mut pos: u64 = 0;

        loop {
            let notified = entry.notify.notified();

            let is_done = entry.done.load(Ordering::Acquire);
            let written = entry.written.load(Ordering::Acquire);

            if pos < written {
                // Clone the file handle once per wake-up, reuse across reads
                let file_guard = entry.file.lock().await;
                let mut file = match file_guard.as_ref().and_then(|f| f.try_clone().ok()) {
                    Some(f) => f,
                    None => {
                        eprintln!("[GET] failed to clone file handle");
                        return;
                    }
                };
                drop(file_guard);

                while pos < written {
                    let to_read = std::cmp::min(READ_BUF_SIZE as u64, written - pos) as usize;

                    match crate::io::read_at(file, pos, to_read).await {
                        Ok((buf, f)) if !buf.is_empty() => {
                            file = f;
                            pos += buf.len() as u64;

                            if tx.send(Ok(Frame::data(Bytes::from(buf)))).await.is_err() {
                                return;
                            }
                        }
                        Ok(_) => break,
                        Err(e) => {
                            eprintln!("[GET] read error: {e}");
                            return;
                        }
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

    if let Some(ref mime) = mime_type {
        response = response.header(hyper::header::CONTENT_TYPE, mime.as_str());
    }

    if let Some(ref name) = filename {
        // Sanitize: strip control chars (CR, LF, NUL) to prevent header injection,
        // and escape quotes for the quoted-string.
        let safe: String = name
            .chars()
            .filter(|c| !c.is_control())
            .collect();
        let safe = safe.replace('\\', "\\\\").replace('"', "\\\"");

        response = response.header(
            hyper::header::CONTENT_DISPOSITION,
            format!("attachment; filename=\"{safe}\""),
        );
    }

    response.body(body).unwrap()
}
