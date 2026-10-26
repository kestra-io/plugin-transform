FROM rust:1-slim-bookworm AS build
ARG AQ_REVISION=38a8d1fa7a7f130356b498d6b5170c61101408f3
RUN cargo install --locked --git https://github.com/Anaethelion/aq --rev ${AQ_REVISION} --root /usr/local

FROM debian:bookworm-slim
LABEL org.opencontainers.image.source=https://github.com/kestra-io/plugin-transform
LABEL org.opencontainers.image.description="Image with the aq CLI to query Parquet, Arrow IPC, CSV and NDJSON files with jq expressions."
COPY --from=build /usr/local/bin/aq /usr/local/bin/aq
RUN aq --help
