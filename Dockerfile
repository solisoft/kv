FROM rust:1-bookworm AS builder
WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/solikv /usr/local/bin/solikv
ENTRYPOINT ["solikv"]
# Published ports need --bind 0.0.0.0; protected mode still requires a password
# (SOLIKV_REQUIREPASS / --requirepass-file) unless --protected-mode no.
CMD ["--bind", "0.0.0.0", "--dir", "/data"]
