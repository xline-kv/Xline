Contribution Guide
----

# Quick Start

## Build an Xline docker image

```bash
# Assume that rust compile environment installed, such as cargo, etc.

# install dependency
sudo apt install -y libprotobuf-dev protobuf-compiler

# clone source code
git clone https://github.com/datenlord/Xline

# compile Xline
cd Xline
cargo build --release

# build docker image
cd scripts
cp ../target/release/xline .
cp ../target/release/benchmark .
# you may need to add sudo before the command to make it work
docker build . -t datenlord/xline:latest
```

## Start Xline servers

**Note: this script will stop all the running docker containers**

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

# Directory Structure

| directory name | description |
|----------------|-------------|
| benchmark      | a customized benchmark using CURP protocol based client |
| curp           | the CURP protocol |
| xline          | xline services |
| scripts        | the shell scripts for env deployment or benchmarking |
