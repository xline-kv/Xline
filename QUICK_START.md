# Quick Start

## Install dependencies
```bash
sudo apt-get install -y autoconf autogen libtool

# requires protobuf-compiler >= 3.15
git clone --branch v3.21.12  --recurse-submodules https://github.com/protocolbuffers/protobuf
cd protobuf
./autogen.sh
./configure
make -j
sudo make install 
```

## Build an Xline docker image

```bash
# Assume that rust compile environment installed, such as cargo, etc.

# clone source code
git clone https://github.com/datenlord/Xline

# compile Xline
cd Xline
cargo build --release

# compile lock client
RUSTFLAGS="-C target-feature=+crt-static" cargo build --release --bin lock_client --target x86_64-unknown-linux-gnu

# build docker image
cd scripts
cp ../target/release/xline .
cp ../target/release/benchmark .
cp ../target/x86_64-unknown-linux-gnu/release/lock_client .
cp ../xline/tests/{private,public}.pem .

# you may need to add sudo before the command to make it work
docker build . -t datenlord/xline:latest
```

## Start Xline servers

``` bash
./scripts/quick_start.sh
```

## Send Etcd requests

``` bash
# Set Key A's value to 1
docker exec node4 /bin/sh -c "/usr/local/bin/etcdctl --endpoints=\"http://172.20.0.3:2379\" put A 1"

# Get Key A's value
docker exec node4 /bin/sh -c "/usr/local/bin/etcdctl --endpoints=\"http://172.20.0.3:2379\" get A"
```

## Benchmark

```bash
./scripts/benchmark.sh
```

## Validation Test

```bash
./scripts/validation_test.sh
```

# Directory Structure

| directory name | description |
|----------------|-------------|
| benchmark      | a customized benchmark using CURP protocol based client |
| curp           | the CURP protocol |
| xline          | xline services |
| engine         | persistent storage |
| utils          | some utilities, like lock, config, etc. |
| scripts        | the shell scripts for env deployment or benchmarking |
