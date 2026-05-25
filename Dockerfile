FROM rust:1-slim-bookworm AS builder

WORKDIR /build
COPY Cargo.toml Cargo.lock* ./
COPY src/ ./src/

RUN cargo build --release

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/* && \
    useradd --system --uid 1000 --create-home --home-dir /app hipoglos

WORKDIR /app

COPY --from=builder /build/target/release/hipoglos /app/hipoglos

USER hipoglos
VOLUME ["/app/data"]

ENTRYPOINT ["/app/hipoglos"]
CMD ["sync"]
