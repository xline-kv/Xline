# Quick Start

## Run Xline from a pre-built image

```bash
# Assume that docker engine environment is installed.

docker run -it --name=xline ghcr.io/xline-kv/xline \
  xline \
  --name xline \
  --storage-engine rocksdb \
  --members xline=127.0.0.1:2379 \
  --data-dir /usr/local/xline/data-dir
```

## Run Xline from source code

### Install dependencies

#### Ubuntu/Debian

```bash
sudo apt-get install -y autoconf autogen libtool

# requires protobuf-compiler >= 3.15
git clone --branch v3.21.12 --recurse-submodules https://github.com/protocolbuffers/protobuf
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
git clone --recurse-submodules https://github.com/datenlord/Xline

# compile Xline
cd Xline
cargo build --release
```

### Run Xline

```bash
./target/release/xline --name xline \
  --storage-engine rocksdb \
  --members xline=127.0.0.1:2379 \
  --data-dir <path-to-data-dir>
```

## Test Xline cluster

### Pull or Build image for validation

#### Pull the latest image from ghcr.io
```bash
# Assume that docker engine environment is installed.
 docker pull ghcr.io/xline-kv/xline:latest
 ```

#### Build image
```bash
# Assume that docker engine environment is installed.

# clone source code
git clone --recurse-submodules https://github.com/datenlord/Xline
cd Xline

# build docker image
# you may need to add sudo before the command to make it work
docker build . -t ghcr.io/xline-kv/xline -f doc/quick-start/Dockerfile
```

### Start Xline servers

```bash
cp ./fixtures/{private,public}.pem ./scripts

./scripts/quick_start.sh
```

### Test basic etcd requests

```bash
# Set Key A's value to 1
docker exec client /bin/sh -c "/usr/local/bin/etcdctl --endpoints=\"http://172.20.0.3:2379\" put A 1"

# Get Key A's value
docker exec client /bin/sh -c "/usr/local/bin/etcdctl --endpoints=\"http://172.20.0.3:2379\" get A"
```

### Membership Change
```bash
# Before member add
$ docker exec client /bin/sh -c "/usr/local/bin/etcdctl --endpoints=\"http://172.20.0.3:2379\" member list -w table"
+------------------+---------+-------+---------------------------------+---------------------------------+------------+
|        ID        | STATUS  | NAME  |           PEER ADDRS            |          CLIENT ADDRS           | IS LEARNER |
+------------------+---------+-------+---------------------------------+---------------------------------+------------+
| c446f1764cf82129 | started | node3 | 172.20.0.5:2379,172.20.0.5:2380 | 172.20.0.5:2379,172.20.0.5:2380 |      false |
| 536070dcd739623d | started | node1 | 172.20.0.3:2379,172.20.0.3:2380 | 172.20.0.3:2379,172.20.0.3:2380 |      false |
| c58a7f879100c944 | started | node2 | 172.20.0.4:2379,172.20.0.4:2380 | 172.20.0.4:2379,172.20.0.4:2380 |      false |
+------------------+---------+-------+---------------------------------+---------------------------------+------------+

# do the member add
$ docker exec client /bin/sh -c "/usr/local/bin/etcdctl --endpoints=\"http://172.20.0.3:2379\" member add node4 --peer-urls=http://172.20.0.6:2379,http://172.20.0.6:2380"
Member 7bcbb7db4adc6890 added to cluster 73917cf4cbc75001

ETCD_NAME="node4"
ETCD_INITIAL_CLUSTER="node4=http://172.20.0.6:2379,node4=http://172.20.0.6:2380,node2=172.20.0.4:2379,node2=172.20.0.4:2380,node3=172.20.0.5:2379,node3=172.20.0.5:2380,node1=172.20.0.3:2379,node1=172.20.0.3:2380"
ETCD_INITIAL_ADVERTISE_PEER_URLS="http://172.20.0.6:2379,http://172.20.0.6:2380"
ETCD_INITIAL_CLUSTER_STATE="existing"

# boot up a new node
$ docker run -d -it --rm --name=node4 --net=xline_net --ip=172.20.0.6 --cap-add=NET_ADMIN --cpu-shares=1024 -m=512M -v /home/jiawei/Xline/scripts:/mnt ghcr.io/xline-kv/xline:latest bash

$ docker exec -e RUST_LOG=debug -d node4 "/usr/local/bin/xline --name node4 --members node1=172.20.0.3:2379,172.20.0.3:2380,node2=172.20.0.4:2379,172.20.0.4:2380,node3=172.20.0.5:2379,172.20.0.5:2380,node4=172.20.0.6:2379,172.20.0.6:2380 --storage-engine rocksdb --data-dir /usr/local/xline/data-dir --auth-public-key /mnt/public.pem --auth-private-key /mnt/private.pem --initial-cluster-state=existing"

# check whether the new member adding success or not
$ docker exec client /bin/sh -c "/usr/local/bin/etcdctl --endpoints=\"http://172.20.0.3:2379\" member list -w table"
+------------------+---------+-------+-----------------------------------------------+-----------------------------------------------+------------+
|        ID        | STATUS  | NAME  |                  PEER ADDRS                   |                 CLIENT ADDRS                  | IS LEARNER |
+------------------+---------+-------+-----------------------------------------------+-----------------------------------------------+------------+
| 7bcbb7db4adc6890 | started | node4 | http://172.20.0.6:2379,http://172.20.0.6:2380 | http://172.20.0.6:2379,http://172.20.0.6:2380 |      false |
| c58a7f879100c944 | started | node2 |               172.20.0.4:2379,172.20.0.4:2380 |               172.20.0.4:2379,172.20.0.4:2380 |      false |
| c446f1764cf82129 | started | node3 |               172.20.0.5:2379,172.20.0.5:2380 |               172.20.0.5:2379,172.20.0.5:2380 |      false |
| 536070dcd739623d | started | node1 |               172.20.0.3:2379,172.20.0.3:2380 |               172.20.0.3:2379,172.20.0.3:2380 |      false |
+------------------+---------+-------+-----------------------------------------------+-----------------------------------------------+------------+

# do the member remove
$ docker exec client /bin/sh -c "/usr/local/bin/etcdctl --endpoints=\"http://172.20.0.3:2379\" member remove 7bcbb7db4adc6890"
Member 7bcbb7db4adc6890 removed from cluster 73917cf4cbc75001

# check whether the target member removed success or not
$ docker exec client /bin/sh -c "/usr/local/bin/etcdctl --endpoints=\"http://172.20.0.3:2379\" member list -w table"
+------------------+---------+-------+---------------------------------+---------------------------------+------------+
|        ID        | STATUS  | NAME  |           PEER ADDRS            |          CLIENT ADDRS           | IS LEARNER |
+------------------+---------+-------+---------------------------------+---------------------------------+------------+
| c58a7f879100c944 | started | node2 | 172.20.0.4:2379,172.20.0.4:2380 | 172.20.0.4:2379,172.20.0.4:2380 |      false |
| c446f1764cf82129 | started | node3 | 172.20.0.5:2379,172.20.0.5:2380 | 172.20.0.5:2379,172.20.0.5:2380 |      false |
| 536070dcd739623d | started | node1 | 172.20.0.3:2379,172.20.0.3:2380 | 172.20.0.3:2379,172.20.0.3:2380 |      false |
+------------------+---------+-------+---------------------------------+---------------------------------+------------+
```

### Validation test

```bash
docker cp node1:/usr/local/bin/lock_client ./scripts

./scripts/validation_test.sh
```

### Benchmark

```bash
./scripts/benchmark.sh
```

# Directory Structure

| directory name | description                                             |
|----------------|---------------------------------------------------------|
| benchmark      | a customized benchmark using CURP protocol based client |
| curp           | the CURP protocol                                       |
| xline          | xline services                                          |
| engine         | persistent storage                                      |
| utils          | some utilities, like lock, config, etc.                 |
| scripts        | the shell scripts for env deployment or benchmarking    |
