FROM rust:1.85.0-slim AS build

RUN adduser \
    --disabled-password \
    --gecos "" \
    --home "/nonexistent" \
    --shell "/sbin/nologin" \
    --no-create-home \
    --uid 10001 \
    "iowa"

RUN --mount=type=bind,source=/src,target=./src \
    --mount=type=bind,source=/Cargo.toml,target=./Cargo.toml \
    --mount=type=bind,source=/Cargo.lock,target=./Cargo.lock \
    --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=./target \
    cargo build --release && \
    cp ./target/release/data_processor ./app-bin

FROM debian:trixie-slim AS final

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*

COPY --from=build /etc/passwd /etc/passwd
COPY --from=build /etc/group /etc/group

USER iowa:iowa

WORKDIR /code

COPY --from=build --chown=iowa:iowa ./app-bin /app/data_processor

ENTRYPOINT ["/app/data_processor"]

