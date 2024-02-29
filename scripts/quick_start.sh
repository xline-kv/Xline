#!/bin/bash
DIR=$(
    cd "$(dirname "$0")"
    pwd
)
SERVERS=("172.20.0.2" "172.20.0.3" "172.20.0.4" "172.20.0.5")
MEMBERS="node1=${SERVERS[1]}:2380,${SERVERS[1]}:2381,node2=${SERVERS[2]}:2380,${SERVERS[2]}:2381,node3=${SERVERS[3]}:2380,${SERVERS[3]}:2381"

source $DIR/log.sh

# stop all containers
stop_all() {
    log::info stopping
    for name in "node1" "node2" "node3" "client"; do
        docker_id=$(docker ps -qf "name=${name}")
        if [ -n "$docker_id" ]; then
            docker stop $docker_id
        fi
    done
    docker network rm xline_net >/dev/null 2>&1
    docker stop "prometheus"
    sleep 1
    log::info stopped
}

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
        mount_point="-v ${DIR}:/mnt"
        if [ -n "$LOG_PATH" ]; then
            mkdir -p ${LOG_PATH}/node${i}
            mount_point="${mount_point} -v ${LOG_PATH}/node${i}:/var/log/xline"
        fi
        docker run -d -it --rm --name=node${i} --net=xline_net \
            --ip=${SERVERS[$i]} --cap-add=NET_ADMIN --cpu-shares=1024 \
            -m=512M ${mount_point} ${image} bash &
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
    echo "Unexpected argument: $1"
    exit 1
fi
