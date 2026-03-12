FROM rust:1.92-bookworm AS builder

WORKDIR /build
COPY . .
RUN cargo build --release --bin file-pipe

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/file-pipe /usr/local/bin/file-pipe

VOLUME /data
EXPOSE 3000

ENTRYPOINT ["file-pipe"]
CMD ["-d", "/data"]
