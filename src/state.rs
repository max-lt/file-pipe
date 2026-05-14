use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

use dashmap::DashMap;
use tokio::sync::{Mutex, Notify};

/// Metadata behind a mutex — only accessed for first GET, not on the hot path.
pub struct PipeMetadata {
    pub content_length: Option<u64>,
    pub mime_type: Option<String>,
    pub filename: Option<String>,
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
    /// File handle (created eagerly when the entry is registered).
    pub file: Mutex<Option<std::fs::File>>,
    pub path: PathBuf,
    pub notify: Notify,
}

pub struct AppState {
    pub pipes: DashMap<String, Arc<PipeEntry>>,
    /// Per-key waiters: GETs register here when their key doesn't exist yet.
    /// The PUT for that key notifies only the relevant waiters.
    pub key_waiters: DashMap<String, Arc<Notify>>,
    pub draining: AtomicBool,
    pub data_dir: PathBuf,
    pub disk_usage: AtomicU64,
    pub max_disk_usage: Option<u64>,
    /// Seconds to keep an entry after PUT completes.
    pub put_ttl: u64,
    /// Seconds to keep an entry after first GET completes.
    pub get_ttl: u64,
    /// Seconds a GET waits for a missing key before returning 404.
    pub get_wait_timeout: u64,
    /// Allow X-Forward-Url to tee uploads to an external URL.
    #[cfg(feature = "forward")]
    pub allow_forward: bool,
}

/// Remove a key's entry only if it is the exact same Arc (pointer equality).
/// This prevents stale cleanup timers from deleting a recycled key's new entry.
pub async fn cleanup_entry(state: &AppState, key: &str, expected: &Arc<PipeEntry>) {
    let removed = match state.pipes.entry(key.to_string()) {
        dashmap::Entry::Occupied(e) if Arc::ptr_eq(e.get(), expected) => Some(e.remove()),
        _ => None,
    };

    if let Some(entry) = removed {
        free_entry(state, key, &entry).await;
    }
}

pub(crate) async fn free_entry(state: &AppState, key: &str, entry: &PipeEntry) {
    let written = entry.written.load(Ordering::Relaxed);
    state.disk_usage.fetch_sub(written, Ordering::Relaxed);

    if let Err(e) = crate::io::remove(&entry.path).await {
        if e.kind() != std::io::ErrorKind::NotFound {
            eprintln!("[CLEANUP] key={key} failed to remove file: {e}");
        }
    }

    eprintln!("[CLEANUP] key={key} removed ({written} bytes freed)");
}
