#!/bin/bash

DIR=$(
    cd "$(dirname "$0")"
    pwd
)
source $DIR/common.sh
source $DIR/log.sh

# stop all containers
stop_all() {
    log::info stopping
    for name in "node1" "node2" "node3" "client"; do
        common::stop_container ${name}
    done
    docker network rm xline_net >/dev/null 2>&1
    docker stop "prometheus" > /dev/null 2>&1
    sleep 1
    log::info stopped
}

# run cluster of xline/etcd in container
run_cluster() {
    log::info cluster starting
    common::run_xline 1 ${MEMBERS} new
    common::run_xline 2 ${MEMBERS} new
    common::run_xline 3 ${MEMBERS} new
    common::run_etcd_client
    wait
    log::info cluster started
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
    run_cluster
    run_prometheus "172.20.0.6"
    echo "Prometheus starts on http://172.20.0.6:9090/graph and http://127.0.0.1:9090/graph (if you are using Docker Desktop)."
    exit 0
elif [ "$1" == "stop" ]; then
    stop_all
    exit 0
else
    echo "Unexpected argument: $1"
    exit 1
fi
