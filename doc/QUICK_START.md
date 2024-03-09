# Quick Start

## Single node cluster

### Using docker

```bash
# Assume that docker engine environment is installed.

$ docker run -it --rm --name=xline -e RUST_LOG=xline=debug -p 2379:2379 ghcr.io/xline-kv/xline \
    xline \
    --name xline \
    --storage-engine rocksdb \
    --members xline=127.0.0.1:2379 \
    --data-dir /usr/local/xline/data-dir \
    --client-listen-urls http://0.0.0.0:2379 \
    --peer-listen-urls http://0.0.0.0:2380 \
    --client-advertise-urls http://127.0.0.1:2379 \
    --peer-advertise-urls http://127.0.0.1:2380
```

```bash
# Try with etcdctl

$ ETCDCTL_API=3 etcdctl put A 1
OK
$ ETCDCTL_API=3 etcdctl get A
A
1
```

### Build from source

1. Install dependencies

```bash
# Ubuntu/Debian

$ sudo apt-get install -y autoconf autogen libtool

# Requires protobuf-compiler >= 3.15
$ git clone --branch v3.21.12 --recurse-submodules https://github.com/protocolbuffers/protobuf
$ cd protobuf
$ ./autogen.sh
$ ./configure
$ make -j$(nproc)
$ sudo make install
```

```bash
# macOS

# Assume that brew is installed, or you could install brew by:
# /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

$ brew install protobuf
```

2. Build xline

```bash
# Assume that rust compile environment installed, such as cargo, etc.

# clone source code
$ git clone --recurse-submodules https://github.com/xline-kv/Xline

# compile Xline
$ cd Xline
$ cargo build --release
```

3. Run xline

```bash
$ ./target/release/xline --name xline \
  --storage-engine rocksdb \
  --members xline=127.0.0.1:2379 \
  --data-dir <path-to-data-dir> \
  --client-listen-urls http://0.0.0.0:2379 \
  --peer-listen-urls http://0.0.0.0:2380 \
  --client-advertise-urls http://127.0.0.1:2379 \
  --peer-advertise-urls http://127.0.0.1:2380
```

## Standard xline cluster

1. Start the cluster

```bash
# Pull the latest image from ghcr.io
$ docker pull ghcr.io/xline-kv/xline:latest
# Copy some fixtures which are required by quick_start.sh
$ cp fixtures/{private,public}.pem scripts/
# Using the quick start scripts
$ ./scripts/quick_start.sh
```

2. Basic requests

```bash
# Set Key A's value to 1
$ docker exec client /bin/sh -c "/usr/local/bin/etcdctl --endpoints=\"http://node1:2379\" put A 1"
OK

# Get Key A's value
$ docker exec client /bin/sh -c "/usr/local/bin/etcdctl --endpoints=\"http://node1:2379\" get A"
A
1
```

3. Inspect metrics

After finished `Start the cluster`, you can goto http://127.0.0.1:9090/graph.
You should be able to see a web ui of Prometheus.

For example:

This means the `node1` is the leader.

![](./img/prom_demo.png)

For more metrics, please goto [metrics.md](./metrics.md)

4. Benchmark

```bash
$ ./scripts/quick_start.sh stop
$ ./scripts/benchmark.sh xline
```

## Directory Structure

| directory name | description                                             |
|----------------|---------------------------------------------------------|
| benchmark      | a customized benchmark using CURP protocol based client |
| curp           | the CURP protocol                                       |
| xline          | xline services                                          |
| engine         | persistent storage                                      |
| utils          | some utilities, like lock, config, etc.                 |
| scripts        | the shell scripts for env deployment or benchmarking    |
