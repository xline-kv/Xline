FROM ubuntu:22.04 AS build-env

ARG LIB_ROCKS_SYS_VER

RUN apt-get update -yy && \
    DEBIAN_FRONTEND=noninteractive \
    apt-get install -yy make build-essential cmake protobuf-compiler parallel curl clang \
    openssl libssl-dev lld wget unzip git bash && \
    rm -rf /var/lib/{apt,cache,log}/

# *RUN* commands on bash
SHELL ["/bin/bash", "-c"]

WORKDIR /xline

COPY rust-toolchain.toml rust-toolchain.toml

ENV PATH=/root/.cargo/bin/:$PATH

# Enable sparse index
ENV CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse

# Install rust toolchain & some tools
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --no-modify-path --default-toolchain none -y && \
    rustup component add rustfmt llvm-tools clippy && \
    rustup show -v && \
    curl -L --proto '=https' --tlsv1.2 -sSf https://raw.githubusercontent.com/cargo-bins/cargo-binstall/main/install-from-binstall-release.sh | bash && \
    cargo binstall -y --no-symlinks cargo-llvm-cov cargo-nextest cargo-hakari cargo-sort cargo-cache cargo-audit && \
    cargo cache --autoclean && \
    rm -rf "/root/.cargo/registry/index" && \
    rm -rf "/root/.cargo/registry/cache" && \
    rm -rf "/root/.cargo/git/db" && \
    cargo uninstall cargo-binstall cargo-cache

# Prebuilt rocksdb
RUN curl -L --proto '=https' --tlsv1.2 -sSf https://crates.io/api/v1/crates/librocksdb-sys/$LIB_ROCKS_SYS_VER/download | tar -zxf - && \
    cd librocksdb-sys-$LIB_ROCKS_SYS_VER/rocksdb/ && \
    make install
