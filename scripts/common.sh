#!/bin/bash

SERVERS=("172.20.0.2" "172.20.0.3" "172.20.0.4" "172.20.0.5" "172.20.0.7")
MEMBERS="node1=${SERVERS[1]}:2380,${SERVERS[1]}:2381,node2=${SERVERS[2]}:2380,${SERVERS[2]}:2381,node3=${SERVERS[3]}:2380,${SERVERS[3]}:2381"
NEWMEMBERS="${MEMBERS},node4=${SERVERS[4]}:2380,${SERVERS[4]}:2381"

XLINE_IMAGE="ghcr.io/xline-kv/xline:latest"
ETCDCTL_IMAGE="ghcr.io/xline-kv/etcdctl:v3.5.9"

function common::run_container() {
    ith=${1}
    mount_point="-v ${DIR}:/mnt"
    if [ -n "$LOG_PATH" ]; then
        mkdir -p ${LOG_PATH}/node${ith}
        mount_point="${mount_point} -v ${LOG_PATH}/node${ith}:/var/log/xline"
    fi
    log::info starting container node${ith} ...
    docker run -d -it --rm --name=node${ith} --net=xline_net \
            --ip=${SERVERS[$ith]} --cap-add=NET_ADMIN --cpu-shares=1024 \
            -m=512M ${mount_point} ${XLINE_IMAGE} bash &
    wait $!
    log::info container node${ith} started
}

function common::stop_container() {
    image_name=$1
    log::info stopping container ${image_name} ...
    docker_id=$(docker ps -qf "name=${image_name}")
    if [ -n "$docker_id" ]; then
        docker stop $docker_id
    fi
    wait $!
    log::info container ${image_name} stopped
}


function common::run_etcd_client() {
    log::info starting container etcdctl ...
    docker run -d -it --rm  --name=client \
        --net=xline_net --ip=${SERVERS[0]} --cap-add=NET_ADMIN \
        --cpu-shares=1024 -m=512M -v ${DIR}:/mnt ${ETCDCTL_IMAGE} bash &
    wait $!
    log::info container etcdctl started
}

# run xline node by index
# args:
#   $1: index of the node
#   $2: members
#   $3: initial cluster state
function common::run_xline() {
    cmd="/usr/local/bin/xline \
    --name node${1} \
    --members ${2} \
    --storage-engine rocksdb \
    --data-dir /usr/local/xline/data-dir \
    --auth-public-key /mnt/public.pem \
    --auth-private-key /mnt/private.pem \
    --client-listen-urls=http://${SERVERS[$1]}:2379 \
    --peer-listen-urls=http://${SERVERS[$1]}:2380,http://${SERVERS[$1]}:2381 \
    --client-advertise-urls=http://${SERVERS[$1]}:2379 \
    --peer-advertise-urls=http://${SERVERS[$1]}:2380,http://${SERVERS[$1]}:2381 \
    --initial-cluster-state=${3}"

    if [ -n "${LOG_PATH}" ]; then
        cmd="${cmd} --log-file ${LOG_PATH}/node${1}"
    fi

    if [ -n "$LOG_LEVEL" ]; then
        cmd="${cmd} --log-level ${LOG_LEVEL}"
    fi

    if [ ${1} -eq 1 ]; then
        cmd="${cmd} --is-leader"
    fi

    docker exec -e RUST_LOG=info -d node${1} ${cmd}
    log::info "command is: docker exec -e RUST_LOG=debug -d node${1} ${cmd}"
}
