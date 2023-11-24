FROM ubuntu:22.04 AS build-env

ARG LIB_ROCKS_SYS_VER

RUN apt-get update -yy && \
    DEBIAN_FRONTEND=noninteractive \
    apt-get install -yy make build-essential cmake protobuf-compiler curl clang pkg-config \
    openssl libssl-dev lld wget unzip git bash && \
    rm -rf /var/lib/{apt,cache,log}/

# *RUN* commands on bash
SHELL ["/bin/bash", "-c"]

WORKDIR /xline

COPY rust-toolchain.toml rust-toolchain.toml

ENV PATH=/root/.cargo/bin/:$PATH
# Enable sparse index
ENV CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse

# Install stuff
RUN echo "=== Install rusty stuff 🦀️ ===" && \
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --no-modify-path --default-toolchain none -y && \
    rustup component add rustfmt llvm-tools clippy && \
    rustup show -v && \
    curl -L --proto '=https' --tlsv1.2 -sSf https://raw.githubusercontent.com/cargo-bins/cargo-binstall/main/install-from-binstall-release.sh | bash && \
    cargo binstall -y --no-symlinks cargo-llvm-cov cargo-nextest cargo-hakari cargo-sort cargo-cache cargo-audit && \
    cargo install --locked sccache && \
    cargo cache --autoclean && \
    rm -rf "/root/.cargo/registry/index" && \
    rm -rf "/root/.cargo/registry/cache" && \
    rm -rf "/root/.cargo/git/db" && \
    cargo uninstall cargo-binstall cargo-cache && \
    echo "=== Prebuilt rocksdb ===" && \
    curl -L --proto '=https' --tlsv1.2 -sSf https://crates.io/api/v1/crates/librocksdb-sys/$LIB_ROCKS_SYS_VER/download | tar -zxf - && \
    cd librocksdb-sys-$LIB_ROCKS_SYS_VER/rocksdb/ && \
    make install && \
    cd /xline && \
    rm -rf librocksdb-sys-$LIB_ROCKS_SYS_VER/

# Set some environments that librocksdb-sys will use them to skip building rocksdb
ENV ROCKSDB_STATIC=1
ENV ROCKSDB_INCLUDE_DIR=/usr/local/include
ENV ROCKSDB_LIB_DIR=/usr/local/lib

# No incremental build, which might cause slow build in CI
# Refer to https://matklad.github.io/2021/09/04/fast-rust-builds.html#CI-Workflow
ENV CARGO_INCREMENTAL=0
# Same as above
ENV CARGO_PROFILE_DEV_DEBUG=0
ENV CARGO_HOME=/xline/.cargo
ENV CARGO_TERM_COLOR=always
