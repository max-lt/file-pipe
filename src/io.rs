//! Async wrappers around blocking file I/O.
//!
//! All operations are dispatched to the tokio blocking thread pool via
//! `spawn_blocking`. This keeps the async runtime free for network I/O.
//!
//! Both `write_at` and `read_at` take an owned `File` and return it on
//! success, allowing callers to reuse the handle without extra `dup(2)`.
//!
//! When `tokio-uring` matures enough to run on the standard tokio runtime,
//! a Linux-specific implementation using io_uring can be swapped in here
//! behind `#[cfg(target_os = "linux")]`.

use std::fs::File;
use std::io;
use std::path::Path;

/// Create a new file for read+write, truncating if it exists.
pub async fn create_rw(path: &Path) -> io::Result<File> {
    let path = path.to_owned();

    tokio::task::spawn_blocking(move || {
        File::options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
    })
    .await
    .unwrap_or_else(|e| Err(io::Error::other(e)))
}

/// Write `data` to `file` at the given byte offset (pwrite).
/// Returns the file handle on success so the caller can reuse it.
pub async fn write_at(file: File, data: &[u8], offset: u64) -> io::Result<File> {
    use std::os::unix::fs::FileExt;

    let data = data.to_vec();

    tokio::task::spawn_blocking(move || {
        file.write_all_at(&data, offset)?;
        Ok(file)
    })
    .await
    .unwrap_or_else(|e| Err(io::Error::other(e)))
}

/// Read up to `len` bytes from `file` at the given byte offset (pread).
/// Returns the bytes read and the file handle so the caller can reuse it.
pub async fn read_at(file: File, offset: u64, len: usize) -> io::Result<(Vec<u8>, File)> {
    use std::os::unix::fs::FileExt;

    tokio::task::spawn_blocking(move || {
        let mut buf = vec![0u8; len];
        let n = file.read_at(&mut buf, offset)?;
        buf.truncate(n);
        Ok((buf, file))
    })
    .await
    .unwrap_or_else(|e| Err(io::Error::other(e)))
}

/// Remove a file from disk.
pub async fn remove(path: &Path) -> io::Result<()> {
    let path = path.to_owned();

    tokio::task::spawn_blocking(move || std::fs::remove_file(&path))
        .await
        .unwrap_or_else(|e| Err(io::Error::other(e)))
}
