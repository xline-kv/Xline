#!/bin/bash
WORKDIR=$(pwd)
SERVERS=("172.20.0.2" "172.20.0.3" "172.20.0.4" "172.20.0.5")
CLUSTER_PEERS=(
    ""
    "${SERVERS[2]}:2379 ${SERVERS[3]}:2379"
    "${SERVERS[1]}:2379 ${SERVERS[3]}:2379"
    "${SERVERS[1]}:2379 ${SERVERS[2]}:2379"
)

# run xline node by index
# args:
#   $1: index of the node
run_xline() {
    cmd="/usr/local/bin/xline \
    --name node${1} \
    --cluster-peers ${CLUSTER_PEERS[$1]} \
    --self-ip-port ${SERVERS[$1]}:2379 \
    --leader-ip-port ${SERVERS[1]}:2379"

    if [ ${1} -eq 1 ]; then
        cmd="${cmd} --is-leader"
    fi

    docker exec -e RUST_LOG=debug -d node${1} ${cmd}
    echo "command is: docker exec -e RUST_LOG=debug -d node${1} ${cmd}"
}

# run cluster of xline/etcd in container
run_cluster() {
    echo cluster starting
    run_xline 1 &
    run_xline 2 &
    run_xline 3 &
    wait
    echo cluster started
}

# stop all containers
stop_all() {
    echo stopping
    docker stop $(docker ps -a -q)
    sleep 1
    echo stopped
}

# run container of xline/etcd use specified image
# args:
#   $1: size of cluster
run_container() {
    echo container starting
    size=${1}
    image="datenlord/xline:latest"
    for ((i = 1; i <= ${size}; i++)); do
        docker run -d -it --rm --name=node${i} --net=xline_net --ip=172.20.0.$((i + 2)) --cap-add=NET_ADMIN --cpu-shares=1024 -m=512M -v ${WORKDIR}:/mnt ${image} bash &
    done
        docker run -d -it --rm --name=node4 --net=xline_net --ip=172.20.0.2 --cap-add=NET_ADMIN --cpu-shares=1024 -m=512M -v ${WORKDIR}:/mnt gcr.io/etcd-development/etcd:v3.5.5 bash &
    wait
    echo container started
}

stop_all
docker network create --subnet=172.20.0.0/24 xline_net >/dev/null 2>&1

run_container 3
run_cluster
