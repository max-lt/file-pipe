use std::convert::Infallible;
use std::io::Write as _;
use std::os::unix::fs::FileExt;
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
    if state.draining.load(Ordering::Relaxed) {
        return PipeError::Draining.into_response();
    }

    let content_length = req
        .headers()
        .get(hyper::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok());

    // Create temp file path (file created lazily on spill)
    let file_path = state.data_dir.join(format!(
        "pipe-{}-{}",
        std::process::id(),
        key.replace('/', "_")
    ));

    let entry = Arc::new(PipeEntry {
        meta: tokio::sync::Mutex::new(PipeMetadata {
            content_length,
            reader_count: 0,
            upload_ended_at: None,
            first_get_at: None,
            last_get_at: None,
        }),
        written: 0.into(),
        done: false.into(),
        buffer: tokio::sync::Mutex::new(Vec::new()),
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

    // Notify GETs waiting for this key
    state.key_added.notify_waiters();

    eprintln!("[PUT] key={key} upload started");

    // Stream the request body
    let mut body = req.into_body();

    loop {
        match body.frame().await {
            Some(Ok(frame)) => {
                if let Ok(data) = frame.into_data() {
                    if !data.is_empty() {
                        if let Err(resp) = write_chunk(&entry, &state, &data).await {
                            return resp;
                        }
                    }
                }
            }
            Some(Err(e)) => {
                entry.done.store(true, Ordering::Release);
                entry.notify.notify_waiters();
                return PipeError::UploadError(e).into_response();
            }
            None => {
                let total_bytes = entry.written.load(Ordering::Relaxed);
                let spilled = entry.spilled.load(Ordering::Relaxed);
                entry.meta.lock().await.upload_ended_at = Some(Instant::now());
                entry.done.store(true, Ordering::Release);
                entry.notify.notify_waiters();
                eprintln!(
                    "[PUT] key={key} upload complete: {total_bytes} bytes ({})",
                    if spilled { "disk" } else { "memory" }
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
        cleanup_key(&state_clone, &key_clone).await;
    });

    ok_response()
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
    let mut buf = entry.buffer.lock().await;
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
    let buf = entry.buffer.lock().await;
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
    let file = match std::fs::File::options()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&entry.path)
    {
        Ok(f) => f,
        Err(e) => {
            state.disk_usage.fetch_sub(total_len, Ordering::Relaxed);
            drop(buf);
            entry.done.store(true, Ordering::Release);
            entry.notify.notify_waiters();
            return Err(PipeError::from_io(e).into_response());
        }
    };

    if let Err(e) = (&file)
        .write_all(&buf)
        .and_then(|()| (&file).write_all(new_data))
    {
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

    if let Err(e) = (&*file).write_all(data) {
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

    // Update metadata (behind mutex, but only once per GET — not on hot path)
    let content_length = {
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
        meta.content_length
    };

    // Stream the response
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Frame<Bytes>, Infallible>>(2);

    tokio::spawn(async move {
        let mut pos: u64 = 0;

        loop {
            let notified = entry.notify.notified();

            let is_done = entry.done.load(Ordering::Acquire);
            let written = entry.written.load(Ordering::Acquire);
            let spilled = entry.spilled.load(Ordering::Acquire);

            if pos < written {
                if !spilled {
                    // Read from in-memory buffer
                    let buf = entry.buffer.lock().await;
                    let chunk = Bytes::copy_from_slice(&buf[pos as usize..written as usize]);
                    drop(buf);
                    pos = written;

                    if tx.send(Ok(Frame::data(chunk))).await.is_err() {
                        return;
                    }
                } else {
                    // Read from disk using pread
                    let file_guard = entry.file.lock().await;
                    let file = file_guard.as_ref().unwrap();

                    while pos < written {
                        let to_read = std::cmp::min(READ_BUF_SIZE as u64, written - pos) as usize;
                        let mut buf = vec![0u8; to_read];

                        match file.read_at(&mut buf, pos) {
                            Ok(n) if n > 0 => {
                                pos += n as u64;
                                buf.truncate(n);

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

                    drop(file_guard);
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
