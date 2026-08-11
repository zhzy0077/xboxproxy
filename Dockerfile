# syntax=docker/dockerfile:1
FROM rust:1.97 AS builder
WORKDIR /build
COPY Cargo.toml ./
COPY src ./src
COPY templates ./templates
COPY data ./data
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    cargo build --release --locked

FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /build/target/release/xboxproxy /usr/local/bin/xboxproxy
COPY data /app/data
VOLUME ["/app/data"]
EXPOSE 80
ENTRYPOINT ["xboxproxy"]
