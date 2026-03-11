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
use crate::state::{AppState, PipeEntry, PipeMetadata, cleanup_key};

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

    // Create temp file path (file created lazily on spill)
    let file_path = state.data_dir.join(format!(
        "pipe-{}-{}",
        std::process::id(),
        key.replace('/', "_")
    ));

    let entry = Arc::new(PipeEntry {
        meta: tokio::sync::Mutex::new(PipeMetadata {
            content_length,
            mime_type: raw_content_type,
            filename: None,
            reader_count: 0,
            upload_ended_at: None,
            first_get_at: None,
            last_get_at: None,
        }),
        written: 0.into(),
        done: false.into(),
        buffer: tokio::sync::RwLock::new(Vec::new()),
        spilled: false.into(),
        file: tokio::sync::Mutex::new(None),
        path: file_path,
        notify: tokio::sync::Notify::new(),
    });

    // Check-and-insert under a single write lock to avoid TOCTOU race
    {
        let mut map = state.pipes.write().await;

        if map.contains_key(&key) {
            return PipeError::KeyAlreadyExists.into_response();
        }

        map.insert(key.clone(), entry.clone());
    }

    // Notify GETs waiting for this specific key
    if let Some(waiter) = state.key_waiters.lock().await.remove(&key) {
        waiter.notify_waiters();
    }

    eprintln!("[PUT] key={key} upload started");

    if let Some(boundary) = boundary {
        stream_multipart(key, req.into_body(), boundary, entry, state).await
    } else {
        stream_raw(key, req.into_body(), entry, state).await
    }
}

/// Stream a raw (non-multipart) body into the pipe.
async fn stream_raw(
    key: String,
    mut body: Incoming,
    entry: Arc<PipeEntry>,
    state: Arc<AppState>,
) -> Response<BoxBody> {
    loop {
        match body.frame().await {
            Some(Ok(frame)) => {
                if let Ok(data) = frame.into_data() {
                    if !data.is_empty() {
                        if let Err(resp) = write_chunk(&entry, &state, &data).await {
                            // done already set inside write_chunk
                            schedule_cleanup(key, state);
                            return resp;
                        }
                    }
                }
            }
            Some(Err(e)) => {
                fail_upload(key, &entry, state);
                return PipeError::UploadError(e).into_response();
            }
            None => {
                finalize_upload(&key, &entry).await;
                break;
            }
        }
    }

    schedule_cleanup(key, state);
    ok_response()
}

/// Parse a multipart body, extract the first file field, and stream it into the pipe.
async fn stream_multipart(
    key: String,
    body: Incoming,
    boundary: String,
    entry: Arc<PipeEntry>,
    state: Arc<AppState>,
) -> Response<BoxBody> {
    let stream = body.into_data_stream();
    // Limit multipart header sizes to prevent abuse.
    // Field data size is unlimited here — enforced by our own disk/memory quotas.
    let constraints = multer::Constraints::new().size_limit(
        multer::SizeLimit::new()
            .whole_stream(u64::MAX)
            .per_field(u64::MAX),
    );
    let mut multipart = multer::Multipart::with_constraints(stream, boundary, constraints);

    // Find the first field (we only support a single file per upload)
    let field = match multipart.next_field().await {
        Ok(Some(f)) => f,
        Ok(None) => {
            fail_upload(key, &entry, state);
            return PipeError::EmptyMultipart.into_response();
        }
        Err(e) => {
            fail_upload(key, &entry, state);
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
                    if let Err(resp) = write_chunk(&entry, &state, &data).await {
                        // done already set inside write_chunk
                        schedule_cleanup(key, state);
                        return resp;
                    }
                }
            }
            Ok(None) => {
                finalize_upload(&key, &entry).await;
                break;
            }
            Err(e) => {
                fail_upload(key, &entry, state);
                return PipeError::MultipartError(e).into_response();
            }
        }
    }

    schedule_cleanup(key, state);
    ok_response()
}

async fn finalize_upload(key: &str, entry: &PipeEntry) {
    let total_bytes = entry.written.load(Ordering::Relaxed);
    let spilled = entry.spilled.load(Ordering::Relaxed);
    entry.meta.lock().await.upload_ended_at = Some(Instant::now());
    entry.done.store(true, Ordering::Release);
    entry.notify.notify_waiters();
    eprintln!(
        "[PUT] key={key} upload complete: {total_bytes} bytes ({})",
        if spilled { "disk" } else { "memory" }
    );
}

fn schedule_cleanup(key: String, state: Arc<AppState>) {
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(30)).await;
        cleanup_key(&state, &key).await;
    });
}

/// Mark upload as failed and schedule cleanup so the entry doesn't leak.
fn fail_upload(key: String, entry: &PipeEntry, state: Arc<AppState>) {
    entry.done.store(true, Ordering::Release);
    entry.notify.notify_waiters();
    schedule_cleanup(key, state);
}

/// Write a chunk to the pipe, handling memory → disk spill and quota.
async fn write_chunk(
    entry: &PipeEntry,
    state: &AppState,
    data: &[u8],
) -> Result<(), Response<BoxBody>> {
    let len = data.len() as u64;

    if entry.spilled.load(Ordering::Relaxed) {
        // Already on disk — write directly to file
        return write_to_disk(entry, state, data).await;
    }

    let current_written = entry.written.load(Ordering::Relaxed);

    if current_written + len > state.spill_threshold {
        // Per-file threshold exceeded — spill
        return spill_to_disk(entry, state, data).await;
    }

    // Try to reserve memory (optimistic fetch_add)
    let prev_mem = state.memory_usage.fetch_add(len, Ordering::Relaxed);

    if state.max_memory.is_some_and(|max| prev_mem + len > max) {
        // Memory full — rollback and force-spill to disk
        state.memory_usage.fetch_sub(len, Ordering::Relaxed);
        return spill_to_disk(entry, state, data).await;
    }

    // Memory reserved — append to buffer
    let mut buf = entry.buffer.write().await;
    buf.extend_from_slice(data);
    drop(buf);
    entry.written.fetch_add(len, Ordering::Release);
    entry.notify.notify_waiters();
    Ok(())
}

/// Spill in-memory buffer to disk and write the new chunk.
async fn spill_to_disk(
    entry: &PipeEntry,
    state: &AppState,
    new_data: &[u8],
) -> Result<(), Response<BoxBody>> {
    let buf = entry.buffer.read().await;
    let buf_len = buf.len() as u64;
    let total_len = buf_len + new_data.len() as u64;

    // Reserve disk quota for buffer + new data
    if let Some(max) = state.max_disk_usage {
        let prev = state.disk_usage.fetch_add(total_len, Ordering::Relaxed);

        if prev + total_len > max {
            state.disk_usage.fetch_sub(total_len, Ordering::Relaxed);
            drop(buf);
            entry.done.store(true, Ordering::Release);
            entry.notify.notify_waiters();
            return Err(PipeError::DiskQuotaExceeded.into_response());
        }
    } else {
        state.disk_usage.fetch_add(total_len, Ordering::Relaxed);
    }

    // Create file and write buffer + new data
    let file = match crate::io::create_rw(&entry.path).await {
        Ok(f) => f,
        Err(e) => {
            state.disk_usage.fetch_sub(total_len, Ordering::Relaxed);
            drop(buf);
            entry.done.store(true, Ordering::Release);
            entry.notify.notify_waiters();
            return Err(PipeError::from_io(e).into_response());
        }
    };

    if let Err(e) = crate::io::write_at(&file, &buf, 0).await {
        state.disk_usage.fetch_sub(total_len, Ordering::Relaxed);
        drop(buf);
        entry.done.store(true, Ordering::Release);
        entry.notify.notify_waiters();
        return Err(PipeError::from_io(e).into_response());
    }

    if let Err(e) = crate::io::write_at(&file, new_data, buf_len).await {
        state.disk_usage.fetch_sub(total_len, Ordering::Relaxed);
        drop(buf);
        entry.done.store(true, Ordering::Release);
        entry.notify.notify_waiters();
        return Err(PipeError::from_io(e).into_response());
    }

    drop(buf);

    // Buffer moved from memory to disk — release memory quota
    state.memory_usage.fetch_sub(buf_len, Ordering::Relaxed);

    *entry.file.lock().await = Some(file);
    // Set spilled BEFORE bumping written: readers load written before
    // spilled, so if a reader sees the new written value, its Acquire
    // synchronizes with this Release, guaranteeing it also sees
    // spilled=true and reads from disk instead of the buffer.
    entry.spilled.store(true, Ordering::Release);
    entry
        .written
        .fetch_add(new_data.len() as u64, Ordering::Release);
    entry.notify.notify_waiters();

    Ok(())
}

/// Write a chunk directly to disk (already spilled).
async fn write_to_disk(
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
    let file = file_guard.as_ref().unwrap();
    let offset = entry.written.load(Ordering::Relaxed);

    if let Err(e) = crate::io::write_at(file, data, offset).await {
        drop(file_guard);
        state.disk_usage.fetch_sub(len, Ordering::Relaxed);
        entry.done.store(true, Ordering::Release);
        entry.notify.notify_waiters();
        return Err(PipeError::from_io(e).into_response());
    }

    drop(file_guard);
    entry.written.fetch_add(len, Ordering::Release);
    entry.notify.notify_waiters();

    Ok(())
}

async fn handle_get(key: String, state: Arc<AppState>) -> Response<BoxBody> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);

    // Wait for the key to appear (up to 5s)
    let entry = loop {
        // Check if the key already exists
        {
            let map = state.pipes.read().await;

            if let Some(entry) = map.get(&key) {
                break entry.clone();
            }
        }

        // Register a per-key waiter so only the matching PUT wakes us
        let waiter = {
            let mut waiters = state.key_waiters.lock().await;
            waiters
                .entry(key.clone())
                .or_insert_with(|| Arc::new(tokio::sync::Notify::new()))
                .clone()
        };

        let notified = waiter.notified();

        // Re-check after registering (the PUT may have arrived between our check and register)
        {
            let map = state.pipes.read().await;

            if let Some(entry) = map.get(&key) {
                break entry.clone();
            }
        }

        tokio::select! {
            _ = notified => continue,
            _ = tokio::time::sleep_until(deadline) => {
                // Clean up our waiter on timeout
                state.key_waiters.lock().await.remove(&key);
                return PipeError::KeyNotFound.into_response();
            }
        }
    };

    // Update metadata (behind mutex, but only once per GET — not on hot path)
    let (content_length, mime_type, filename) = {
        let mut meta = entry.meta.lock().await;
        meta.reader_count += 1;
        let now = Instant::now();

        if meta.first_get_at.is_none() {
            meta.first_get_at = Some(now);

            // Schedule cleanup 5s after first GET
            let state_clone = state.clone();
            let key_clone = key.clone();

            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(5)).await;
                cleanup_key(&state_clone, &key_clone).await;
            });
        }

        meta.last_get_at = Some(now);
        let reader_num = meta.reader_count;
        eprintln!("[GET] key={key} reader #{reader_num}");
        (meta.content_length, meta.mime_type.clone(), meta.filename.clone())
    };

    // Stream the response
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Frame<Bytes>, Infallible>>(2);

    tokio::spawn(async move {
        let mut pos: u64 = 0;

        loop {
            let notified = entry.notify.notified();

            let is_done = entry.done.load(Ordering::Acquire);
            // Load written BEFORE spilled: if we see a written value that
            // includes post-spill data, our Acquire synchronizes with the
            // writer's Release on written, which happens-after the Release
            // store of spilled=true. So we are guaranteed to see spilled=true
            // and will read from disk, not the buffer.
            let written = entry.written.load(Ordering::Acquire);
            let spilled = entry.spilled.load(Ordering::Acquire);

            if pos < written {
                if !spilled {
                    // Read from in-memory buffer (read lock — multiple readers OK)
                    let buf = entry.buffer.read().await;
                    let chunk = Bytes::copy_from_slice(&buf[pos as usize..written as usize]);
                    drop(buf);
                    pos = written;

                    if tx.send(Ok(Frame::data(chunk))).await.is_err() {
                        return;
                    }
                } else {
                    // Read from disk using pread — clone handle once, release lock
                    let file_guard = entry.file.lock().await;
                    let file = file_guard.as_ref().unwrap()
                        .try_clone()
                        .expect("failed to clone file handle");
                    drop(file_guard);

                    while pos < written {
                        let to_read = std::cmp::min(READ_BUF_SIZE as u64, written - pos) as usize;

                        match crate::io::read_at(&file, pos, to_read).await {
                            Ok(buf) if !buf.is_empty() => {
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
        response = response.header(
            hyper::header::CONTENT_DISPOSITION,
            format!("attachment; filename=\"{}\"", name.replace('"', "\\\""))
        );
    }

    response.body(body).unwrap()
}
