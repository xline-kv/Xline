# Quick Start

## Build an Xline docker image

```bash
# Assume that docker engine environment is installed.

# clone source code
git clone https://github.com/datenlord/Xline
cd Xline

# build docker image
# you may need to add sudo before the command to make it work
docker build . -t datenlord/xline:latest -f doc/quick-start/Dockerfile
```

## Start Xline servers

``` bash
cp ./xline-test-utils/{private,public}.pem ./scripts

./scripts/quick_start.sh
```

## Send Etcd requests

``` bash
# Set Key A's value to 1
docker exec node4 /bin/sh -c "/usr/local/bin/etcdctl --endpoints=\"http://172.20.0.3:2379\" put A 1"

# Get Key A's value
docker exec node4 /bin/sh -c "/usr/local/bin/etcdctl --endpoints=\"http://172.20.0.3:2379\" get A"
```

## Validation Test

```bash
docker cp node1:/usr/local/bin/lock_client ./scripts

./scripts/validation_test.sh
```

## Benchmark

```bash
./scripts/benchmark.sh
```

## Build from scratch

### Install dependencies

#### Ubuntu/Debian

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

#### macOS

```bash
# Assume that brew is installed, or you could install brew by:
# /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

brew install protobuf
```

### Build Xline from source

```bash
# Assume that rust compile environment installed, such as cargo, etc.

# clone source code
git clone https://github.com/datenlord/Xline

# compile Xline
cd Xline
cargo build --release
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
