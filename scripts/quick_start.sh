#!/bin/bash

# Ensure the script is executed from the script's directory
cd "$(dirname "$0")"

# Function to start the environment
start_environment() {
    echo "Starting the environment using docker-compose..."
    docker-compose -f ../ci/docker-compose.yml up -d
    echo "The environment is started."
}

<<<<<<< HEAD
# Function to stop and clean up the environment
stop_environment() {
    echo "Stopping and removing the environment..."
    docker-compose -f ../ci/docker-compose.yml down
    echo "The environment is stopped and cleaned up."
}

# Check command line arguments
if [[ "$1" == "stop" ]]; then
    stop_environment
=======
# run xline node by index
# args:
#   $1: index of the node
run_xline() {
    cmd="/usr/local/bin/xline \
    --name node${1} \
    --members ${MEMBERS} \
    --storage-engine rocksdb \
    --data-dir /usr/local/xline/data-dir \
    --auth-public-key /mnt/public.pem \
    --auth-private-key /mnt/private.pem \
    --client-listen-urls=http://${SERVERS[$1]}:2379 \
    --peer-listen-urls=http://${SERVERS[$1]}:2380,http://${SERVERS[$1]}:2381 \
    --client-advertise-urls=http://${SERVERS[$1]}:2379 \
    --peer-advertise-urls=http://${SERVERS[$1]}:2380,http://${SERVERS[$1]}:2381"

    if [ -n "$LOG_LEVEL" ]; then
        cmd="${cmd} --log-level ${LOG_LEVEL}"
    fi

    if [ ${1} -eq 1 ]; then
        cmd="${cmd} --is-leader"
    fi

    docker exec -e RUST_LOG=debug -d node${1} ${cmd}
    log::info "command is: docker exec -e RUST_LOG=debug -d node${1} ${cmd}"
}

# run cluster of xline/etcd in container
run_cluster() {
    log::info cluster starting
    run_xline 1 &
    run_xline 2 &
    run_xline 3 &
    wait
    log::info cluster started
}

# run container of xline/etcd use specified image
# args:
#   $1: size of cluster
run_container() {
    log::info container starting
    size=${1}
    image="ghcr.io/xline-kv/xline:latest"
    for ((i = 1; i <= ${size}; i++)); do
        docker run -d -it --rm --name=node${i} --net=xline_net \
            --ip=${SERVERS[$i]} --cap-add=NET_ADMIN --cpu-shares=1024 \
            -m=512M -v ${DIR}:/mnt ${image} bash &
    done
    docker run -d -it --rm  --name=client \
        --net=xline_net --ip=${SERVERS[0]} --cap-add=NET_ADMIN \
        --cpu-shares=1024 -m=512M -v ${DIR}:/mnt ghcr.io/xline-kv/etcdctl:v3.5.9 bash &
    wait
    log::info container started
}

# run prometheus
run_prometheus() {
    docker run -d -it --rm --name=prometheus --net=xline_net -p 9090:9090 \
        --ip=${1} --cap-add=NET_ADMIN -v ${DIR}/prometheus.yml:/etc/prometheus/prometheus.yml \
        prom/prometheus
}

if [ -z "$1" ]; then
    stop_all
    docker network create --subnet=172.20.0.0/24 xline_net >/dev/null 2>&1
    log::warn "A Docker network named 'xline_net' is created for communication among various xline nodes. You can use the command 'docker network rm xline_net' to remove it after use."
    run_container 3
    run_cluster
    run_prometheus "172.20.0.6"
    echo "Prometheus starts on http://172.20.0.6:9090/graph and http://127.0.0.1:9090/graph"
    exit 0
elif [ "$1" == "stop" ]; then
    stop_all
    exit 0
else
    start_environment
fi
