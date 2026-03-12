# file-pipe

HTTP pipe service — stream uploads to downloads in real-time.

PUT a file to a key, GET it from another connection. Data streams through as it arrives, no need to wait for the upload to finish. Multiple readers can consume the same key concurrently.

## Usage

```bash
# Start the server
file-pipe

# Upload (terminal 1)
curl -T file.bin http://localhost:3000/mykey

# Download (terminal 2) — can start before or during the upload
curl http://localhost:3000/mykey -o file.bin
```

Multipart uploads are also supported:

```bash
curl -X PUT -F "file=@report.pdf" http://localhost:3000/mykey
```

The original filename and content type are forwarded to readers via `Content-Disposition` and `Content-Type` headers.

## Options

```
-l, --listen <ADDR>        Address to listen on [default: 0.0.0.0:3000]
-d, --data-dir <DIR>       Directory for temporary files [default: $TMPDIR]
-M, --max-memory <SIZE>    Maximum memory usage (e.g. "256M", "1G")
-D, --max-disk <SIZE>      Maximum disk usage (e.g. "1G", "500M")
-s, --spill-threshold <SIZE>  Files smaller than this stay in memory [default: 1M]
```

Small files stay in memory for fast streaming. When a file exceeds the spill threshold or memory is full, data spills to disk transparently.

## Docker

```bash
# Simple build
docker build -t file-pipe .

# Multi-arch (amd64 + arm64)
docker buildx build -f Dockerfile.multi --platform linux/amd64,linux/arm64 -t file-pipe .

# Run
docker run -p 3000:3000 -v /tmp/pipe-data:/data file-pipe
```

## Build

```bash
cargo build --release
cargo test
```

## Architecture

- **Hybrid storage**: small files in memory, large files on disk with automatic spill
- **Real-time streaming**: readers receive data as the writer sends it (no buffering the full upload)
- **Concurrent readers**: multiple GETs on the same key stream data independently
- **Lock-free hot path**: atomics for writer/reader synchronization, `DashMap` for sharded key lookups
- **Async I/O**: `pread`/`pwrite` via `spawn_blocking` for position-independent concurrent file access
- **Graceful shutdown**: first signal drains (rejects new uploads), second signal cleans up and exits
