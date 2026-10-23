FROM rust:1.93.1-bookworm AS builder

WORKDIR /workspace
COPY . .
RUN cargo build --locked --release -j 1 -p sparkless-server

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends libgcc-s1 libstdc++6 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /workspace/target/release/sparkless-server /usr/local/bin/sparkless-server

WORKDIR /tmp
USER 65532:65532
EXPOSE 8099
ENTRYPOINT ["sparkless-server"]
CMD ["serve", "--bind", "0.0.0.0", "--port", "8099"]
