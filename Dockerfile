FROM rust:1-bookworm AS builder
WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/solikv /usr/local/bin/solikv
# Not root: a server bug should not be a root shell in the container. The data
# directory belongs to the service user; a bind-mounted host directory must be
# writable by UID 10001 (e.g. `chown 10001 ./data`).
RUN useradd --system --uid 10001 --user-group --home-dir /data --shell /usr/sbin/nologin solikv \
    && mkdir -p /data && chown solikv:solikv /data && chmod 0700 /data
USER solikv
WORKDIR /data
VOLUME /data
ENTRYPOINT ["solikv"]
