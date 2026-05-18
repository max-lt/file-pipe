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

All options can be set via CLI flags or environment variables. CLI flags take precedence.

| Flag                    | Env var             | Default        | Description                                       |
| ----------------------- | ------------------- | -------------- | ------------------------------------------------- |
| `-l, --listen`          | `LISTEN`            | `0.0.0.0:3000` | Address to listen on                              |
| `-d, --data-dir`        | `DATA_DIR`          | `$TMPDIR`      | Directory for temporary files                     |
| `-D, --max-disk`        | `MAX_DISK`          | unlimited      | Maximum total disk usage (e.g. `1G`, `500M`)      |
| `-P, --max-pipe-size`   | `MAX_PIPE_SIZE`     | unlimited      | Maximum bytes for a single pipe                   |
| `--put-ttl`             | `PUT_TTL`           | `30`           | Seconds to keep an entry after PUT completes      |
| `--get-ttl`             | `GET_TTL`           | `5`            | Seconds to keep an entry after first GET completes |
| `--get-wait-timeout`    | `GET_WAIT_TIMEOUT`  | `5`            | Seconds a GET waits for a missing key (404 after) |
| `--allow-forward`       | `ALLOW_FORWARD`     | off            | Enable `X-Forward-Url` tee (requires `forward` feature) |

## Forwarding (optional)

Built behind the `forward` Cargo feature (off by default). When enabled at build time **and** `--allow-forward` is set at runtime, clients can supply an `X-Forward-Url` header to tee the upload to an external URL while it streams through file-pipe:

```bash
cargo build --release --features forward

curl -T file.bin \
  -H "X-Forward-Url: https://example.s3.amazonaws.com/upload?X-Amz-..." \
  http://localhost:3000/mykey
```

For raw uploads with a known `Content-Length`, file-pipe forwards the header to the upstream — works with S3 presigned PUTs that sign a fixed size.

The feature is off by default because it brings in `reqwest` + `rustls`, which roughly triples the release binary size.

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
cargo build --release                    # default — no forwarding
cargo build --release --features forward # with X-Forward-Url support
cargo test                                # default tests
cargo test --features forward             # also runs the forward tests
```

## Logging

Output goes through `tracing`. Set `RUST_LOG` to control verbosity:

```bash
RUST_LOG=warn  file-pipe   # only warnings + errors
RUST_LOG=debug file-pipe   # routine cleanup traces
```

## Architecture

- **Disk-backed storage**: every upload is written through `pwrite` to a temp file. The OS page cache keeps hot data resident in RAM for short-lived pipes; the kernel handles eviction.
- **Real-time streaming**: readers receive data as the writer sends it (no buffering the full upload).
- **Concurrent readers**: multiple GETs on the same key stream data independently via `pread`.
- **Lock-free hot path**: atomics for writer/reader synchronization, `DashMap` for sharded key lookups.
- **Async I/O**: `pread`/`pwrite` via `spawn_blocking` for position-independent concurrent file access.
- **Graceful shutdown**: first signal drains (rejects new uploads), second signal cleans up and exits.
