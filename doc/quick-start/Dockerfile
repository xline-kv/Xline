FROM rust:1.67.1-slim-bullseye as builder

WORKDIR /build

COPY . .

RUN set -eux && \
  sed -i s@/deb.debian.org/@/mirrors.aliyun.com/@g /etc/apt/sources.list && \
  apt-get update && apt-get install -y git build-essential autoconf autogen libtool clang && \
  git clone --branch v3.21.12 --recurse-submodules https://github.com/protocolbuffers/protobuf && \
  cd protobuf && ./autogen.sh && ./configure && make -j4 && make install && cd .. && ldconfig && \
  cargo build --release

FROM debian:bullseye-slim

RUN set -eux && \
  apt-get update && \
  apt-get install -y iproute2 iputils-ping procps && \
  rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/xline /usr/local/bin
COPY --from=builder /build/target/release/benchmark /usr/local/bin
COPY --from=builder /build/target/release/validation_lock_client /usr/local/bin

CMD ["/usr/local/bin/xline"]
