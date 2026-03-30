FROM rust:1.94.1-slim-bullseye AS builder   

RUN apt-get update && apt-get install -y musl-tools

COPY --chmod=777 ./scripts/get_rust_toolchain.sh /get_rust_toolchain.sh

ARG TARGETPLATFORM

ENV RUSTUP_UPDATE_ROOT=https://mirrors.ustc.edu.cn/rust-static/rustup
ENV RUSTUP_DIST_SERVER=https://mirrors.ustc.edu.cn/rust-static

# Setup toolchian 
RUN rustup target add $(/get_rust_toolchain.sh ${TARGETPLATFORM})

# Build source
WORKDIR /src
COPY . /src
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo build --release --target $(/get_rust_toolchain.sh ${TARGETPLATFORM})

RUN cp /src/target/$(/get_rust_toolchain.sh ${TARGETPLATFORM})/release/core_raft /core_raft
RUN cp /src/target/$(/get_rust_toolchain.sh ${TARGETPLATFORM})/release/benchmark /benchmark

FROM scratch

COPY --from=builder /core_raft /core_raft
COPY --from=builder /benchmark /benchmark


# COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

ENTRYPOINT ["/core_raft"]