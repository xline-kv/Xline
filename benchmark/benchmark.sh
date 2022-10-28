#!/bin/bash
WORKDIR=$(pwd)
OUTPUT_DIR="${WORKDIR}/out"
SERVERS=("172.20.0.2" "172.20.0.3" "172.20.0.4" "172.20.0.5")
CLUSTER_PEERS=(
    ""
    "${SERVERS[2]}:2379 ${SERVERS[3]}:2379"
    "${SERVERS[1]}:2379 ${SERVERS[3]}:2379"
    "${SERVERS[1]}:2379 ${SERVERS[2]}:2379"
)
# container endpoints
XLINE_TESTCASE=(
    "node1   ${SERVERS[1]}:2379"
    "node2   ${SERVERS[2]}:2379"
    "client  ${SERVERS[3]}:2379"
    # TODO add use curp testcase
)
ETCD_TESTCASE=(
    "node1   ${SERVERS[1]}:2379"
    "node2   ${SERVERS[2]}:2379"
    "client  ${SERVERS[3]}:2379"
)
KEY_SPACE_SIZE=("1" "100000")
CLIENTS_TOTAL=("1 50" "10 300" "50 1000" "100 3000" "200 5000")
FORMAT="%-8s\t%-12s\t%-8s\t%-s\n"

benchmark_cmd() {
    container_name=$1
    endpoints=$2
    use_curp=$3
    clients=$4
    total=$5
    key_space_size=$6
    echo "docker exec ${container_name} /mnt/benchmark --endpoints=http://${endpoints} --conns=${clients} --clients=${clients} put --key-size=8 --val-size=256 --total=${total} --key-space-size=${key_space_size}"
}

run_xline() {
    cmd="/usr/local/bin/xline \
    --name node$1 \
    --cluster-peers ${CLUSTER_PEERS[$1]} \
    --self-ip-port ${SERVERS[$1]}:2379 \
    --leader-ip-port ${SERVERS[1]}:2379"

    if [ $1 -eq 1 ]; then
        cmd="${cmd} --is-leader"
    fi
    docker exec -d node$1 $cmd
}

run_etcd() {
    cmd="/usr/local/bin/etcd --name node$1 \
    --data-dir /tmp/node$1 \
    --listen-client-urls http://0.0.0.0:2379 \
    --listen-peer-urls http://0.0.0.0:2380 \
    --advertise-client-urls http://${SERVERS[$1]}:2379 \
    --initial-advertise-peer-urls http://${SERVERS[$1]}:2380 \
    --initial-cluster-token etcd-cluster-1 \
    --initial-cluster node1=http://${SERVERS[1]}:2380,node2=http://${SERVERS[2]}:2380,node3=http://${SERVERS[3]}:2380 \
    --initial-cluster-state new \
    --logger zap"
    docker exec -d node$1 $cmd
}

run_cluster() {
    echo cluster starting
    case $1 in
    xline)
        run_xline 1 &
        run_xline 2 &
        run_xline 3 &
        ;;
    etcd)
        run_etcd 1
        sleep 3
        run_etcd 2 &
        run_etcd 3 &
        ;;
    esac
    wait
    echo cluster started
}

stop_cluster() {
    echo cluster stopping
    for x in 1 2 3; do
        docker exec node${x} pkill -9 ${1} &
        docker exec node${x} rm -rf /tmp/node${x} &
    done
    wait
    echo cluster stopped
}

stop_all() {
    echo stoping
    docker stop $(docker ps -a -q)
    sleep 1
    echo stoped
}

set_latency() {
    docker exec ${1} tc filter add dev eth0 protocol ip parent 1:0 u32 match ip dst ${2} flowid 1:${4}
    docker exec ${1} tc qdisc add dev eth0 parent 1:${4} handle ${4}0: netem delay ${3}
}

set_cluster_latency() {
    cluster_size=$1
    docker exec client tc qdisc add dev eth0 root handle 1: prio bands $((cluster_size + 4))
    for ((j = 1; j < ${cluster_size}; j++)); do
        set_latency client 172.20.0.$((j + 2)) 75ms $((j + 3)) &
    done
    set_latency client 172.20.0.$((cluster_size + 2)) 50ms $((cluster_size + 3)) &
    for ((i = 1; i <= ${cluster_size}; i++)); do
        docker exec node$i tc qdisc add dev eth0 root handle 1: prio bands $((cluster_size + 4))
        idx=4
        for ((j = 1; j <= ${cluster_size}; j++)); do
            if [ ${i} -ne ${j} ]; then
                set_latency node$i 172.20.0.$((j + 2)) 50ms ${idx} &
                idx=$((idx + 1))
            fi
        done
        if [[ ${i} -eq ${cluster_size} ]]; then
            set_latency node${i} 172.20.0.2 50ms ${idx} &
        else
            set_latency node${i} 172.20.0.2 75ms ${idx} &
        fi
    done
    wait
}

run_container() {
    echo container starting
    case $2 in
    xline)
        image="datenlord/xline:latest"
        ;;
    etcd)
        image="datenlord/etcd:v3.5.5"
        ;;
    esac
    docker run -d -it --rm --name=client --net=xline_net --ip=172.20.0.2 --cap-add=NET_ADMIN --cpu-shares=512 -m=512M -v ${WORKDIR}:/mnt ${image} bash &
    for ((i = 1; i <= $1; i++)); do
        docker run -d -it --rm --name=node$i --net=xline_net --ip=172.20.0.$((i + 2)) --cap-add=NET_ADMIN --cpu-shares=1024 -m=512M -v ${WORKDIR}:/mnt ${image} bash &
    done
    wait
    set_cluster_latency $1
    echo container started
}

stop_all
docker network create --subnet=172.20.0.0/24 xline_net >/dev/null 2>&1
rm -r ${OUTPUT_DIR} >/dev/null 2>&1
mkdir ${OUTPUT_DIR}
mkdir ${OUTPUT_DIR}/logs

for server in xline etcd; do
    count=0
    logs_dir=${OUTPUT_DIR}/logs/${server}_logs
    mkdir -p ${logs_dir}
    result_file=${OUTPUT_DIR}/${server}.txt
    printf ${FORMAT} "QPS" "Latency(ms)" "Time(s)" "Command" >>${result_file}
    case ${server} in
    xline)
        TESTCASE=("${XLINE_TESTCASE[@]}")
        ;;
    etcd)
        TESTCASE=("${ETCD_TESTCASE[@]}")
        ;;
    esac
    run_container 3 ${server}
    for testcase in "${TESTCASE[@]}"; do

        tmp=(${testcase})
        container_name=${tmp[0]}
        use_curp="" # TODO: add use curp testcase
        endpoints=${tmp[@]:1}
        for key_space_size in "${KEY_SPACE_SIZE[@]}"; do
            for clients_total in "${CLIENTS_TOTAL[@]}"; do
                tmp=(${clients_total})
                clients=${tmp[0]}
                total=${tmp[1]}
                output_file=${logs_dir}/case_${count}.txt
                count=$((count + 1))

                run_cluster ${server}
                cmd=$(benchmark_cmd "${container_name}" "${endpoints}" "${use_curp}" "${clients}" "${total}" "${key_space_size}")
                ${cmd} >${output_file}
                stop_cluster ${server}

                Latency=$(cat ${output_file} | grep Average | awk '{printf "%.1f",$2*1000}')
                QPS=$(cat ${output_file} | grep Requests/sec | awk '{printf "%.1f",$2}')
                Time=$(cat ${output_file} | grep Total | awk '{printf "%.1f",$2}')

                printf ${FORMAT} ${QPS} ${Latency} ${Time} "${cmd}" >>${result_file}

            done
        done
    done
    stop_all
done
