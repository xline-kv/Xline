#!/bin/bash
DIR=$(
    cd "$(dirname "$0")"
    pwd
)
SERVERS=("172.20.0.2" "172.20.0.3" "172.20.0.4" "172.20.0.5")
MEMBERS="node1=${SERVERS[1]}:2379,node2=${SERVERS[2]}:2379,node3=${SERVERS[3]}:2379"

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
    --auth-private-key /mnt/private.pem"

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
    for name in "node1" "node2" "node3" "node4"; do
        docker_id=$(docker ps -qf "name=${name}")
        if [ -n "$docker_id" ]; then
            docker stop $docker_id
        fi
    done
    sleep 1
    echo stopped
}

# run container of xline/etcd use specified image
# args:
#   $1: size of cluster
run_container() {
    echo container starting
    size=${1}
    image="ghcr.io/xline-kv/xline:latest"
    for ((i = 1; i <= ${size}; i++)); do
        docker run -d -it --rm --name=node${i} --net=xline_net --ip=${SERVERS[$i]} --cap-add=NET_ADMIN --cpu-shares=1024 -m=512M -v ${DIR}:/mnt ${image} bash &
    done
    docker run -d -it --rm --name=node4 --net=xline_net --ip=${SERVERS[0]} --cap-add=NET_ADMIN --cpu-shares=1024 -m=512M -v ${DIR}:/mnt gcr.io/etcd-development/etcd:v3.5.5 bash &
    wait
    echo container started
}

stop_all
docker network create --subnet=172.20.0.0/24 xline_net >/dev/null 2>&1

run_container 3
run_cluster
