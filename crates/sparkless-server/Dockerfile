# Build from the workspace root:
#   docker build -f crates/sparkless-server/Dockerfile -t sparkless-server:local .
FROM rust:1.93-bookworm@sha256:7c4ae649a84014c467d79319bbf17ce2632ae8b8be123ac2fb2ea5be46823f31 AS build
WORKDIR /workspace
COPY . .
RUN cargo build --locked --release -p sparkless-server --no-default-features --features http,connect

FROM gcr.io/distroless/cc-debian12:nonroot@sha256:9dac0a79194e45a7da0158a9c6da57b217585af0786db3845d1f0ec1a0dd182f
COPY --from=build /workspace/target/release/sparkless-server /usr/local/bin/sparkless-server
EXPOSE 8099 15002
ENTRYPOINT ["/usr/local/bin/sparkless-server", "serve", "--host", "0.0.0.0", "--port", "8099", "--connect-host", "0.0.0.0", "--connect-port", "15002"]
