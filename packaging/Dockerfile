# Build from the repository root: docker build -f packaging/Dockerfile .
FROM rust:1.98-bookworm AS build
WORKDIR /src
COPY . .
RUN cargo build --release --locked

FROM debian:bookworm-slim
RUN useradd --system --home /var/lib/rfbouncer --create-home rfbouncer
COPY --from=build /src/target/release/rfbouncer /usr/local/bin/rfbouncer
USER rfbouncer
EXPOSE 5000
# Requests in flight are given shutdown_timeout (30 s unless set) to end:
# run with --stop-timeout at least that long, or they are cut at 10 s.
STOPSIGNAL SIGTERM
ENTRYPOINT ["rfbouncer", "--config", "/etc/rfbouncer/rfbouncer.toml"]
