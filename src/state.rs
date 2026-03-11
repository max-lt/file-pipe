use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

use tokio::sync::{Mutex, Notify, RwLock};

/// Metadata behind a mutex — only accessed for first GET, not on the hot path.
pub struct PipeMetadata {
    pub content_length: Option<u64>,
    pub reader_count: u32,
    pub upload_ended_at: Option<Instant>,
    pub first_get_at: Option<Instant>,
    pub last_get_at: Option<Instant>,
}

pub struct PipeEntry {
    pub meta: Mutex<PipeMetadata>,
    /// Bytes written so far — updated atomically by the writer, read by readers.
    pub written: AtomicU64,
    /// Upload complete — set once by the writer, checked by readers.
    pub done: AtomicBool,
    /// Shared file handle. Writer appends (single writer).
    /// Readers use `read_at()` (pread) with their own offset — no seek, no conflict.
    pub file: std::fs::File,
    pub path: PathBuf,
    pub notify: Notify,
}

pub struct AppState {
    pub pipes: RwLock<HashMap<String, Arc<PipeEntry>>>,
    pub key_added: Notify,
    pub draining: AtomicBool,
    pub data_dir: PathBuf,
    pub disk_usage: AtomicU64,
    pub max_disk_usage: Option<u64>,
}

pub async fn cleanup_key(state: &AppState, key: &str) {
    let entry = state.pipes.write().await.remove(key);

    if let Some(entry) = entry {
        let written = entry.written.load(Ordering::Relaxed);
        state.disk_usage.fetch_sub(written, Ordering::Relaxed);

        if let Err(e) = std::fs::remove_file(&entry.path) {
            if e.kind() != std::io::ErrorKind::NotFound {
                eprintln!("[CLEANUP] key={key} failed to remove file: {e}");
            }
        }

        eprintln!("[CLEANUP] key={key} removed ({written} bytes freed)");
    }
}
