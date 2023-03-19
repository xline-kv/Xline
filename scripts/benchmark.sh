#!/bin/bash
WORKDIR=$(pwd)
OUTPUT_DIR="${WORKDIR}/out"
SERVERS=("172.20.0.2" "172.20.0.3" "172.20.0.4" "172.20.0.5")
MEMBERS="node1=${SERVERS[1]}:2379,node2=${SERVERS[2]}:2379,node3=${SERVERS[3]}:2379"

# container use_curp endpoints
XLINE_TESTCASE=(
    "node1  false node1=${SERVERS[1]}:2379"
    "node2  false node2=${SERVERS[2]}:2379"
    "client false node3=${SERVERS[3]}:2379"
    "client true  ${MEMBERS}"
)
ETCD_TESTCASE=(
    "node1  false node1=${SERVERS[1]}:2379"
    "node2  false node2=${SERVERS[2]}:2379"
    "client false node3=${SERVERS[3]}:2379"
)
KEY_SPACE_SIZE=("1" "100000")
CLIENTS_TOTAL=("1 50" "10 300" "50 1000" "100 3000" "200 5000")
FORMAT="%-8s\t%-12s\t%-8s\t%-s\n"

# generate benchmark command by arguments
# args:
#   $1: container to run benchmark
#   $2: endpoints to connect
#   $3: use_curp flag
#   $4: clients number
#   $5: total requests number
#   $6: size of the key in the request
benchmark_cmd() {
    container_name=${1}
    endpoints=${2}
    use_curp=${3}
    clients=${4}
    total=${5}
    key_space_size=${6}
    echo "docker exec ${container_name} /usr/local/bin/benchmark --endpoints ${endpoints} ${use_curp} --clients=${clients} --stdout put --key-size=8 --val-size=256 --total=${total} --key-space-size=${key_space_size}"
}

# run xline node by index
# args:
#   $1: index of the node
run_xline() {
    cmd="/usr/local/bin/xline \
    --name node${1} \
    --members ${MEMBERS} \
    --storage-engine memory \
    --data-dir /usr/local/xline/data-dir \
    --retry-timeout 150ms \
    --rpc-timeout 5s   \
    --server-wait-synced-timeout 10s \
    --client-wait-synced-timeout 10s \
    --client_propose_timeout 5s \
    --cmd-workers 16"

    if [ ${1} -eq 1 ]; then
        cmd="${cmd} --is-leader"
    fi

    docker exec -e RUST_LOG=debug -d node${1} ${cmd}
}

# run etcd node by index
# args:
#   $1: index of the node
run_etcd() {
    cmd="/usr/local/bin/etcd --name node${1} \
    --data-dir /tmp/node${1} \
    --listen-client-urls http://0.0.0.0:2379 \
    --listen-peer-urls http://0.0.0.0:2380 \
    --advertise-client-urls http://${SERVERS[$1]}:2379 \
    --initial-advertise-peer-urls http://${SERVERS[$1]}:2380 \
    --initial-cluster-token etcd-cluster-1 \
    --initial-cluster node1=http://${SERVERS[1]}:2380,node2=http://${SERVERS[2]}:2380,node3=http://${SERVERS[3]}:2380 \
    --initial-cluster-state new \
    --logger zap"
    docker exec -d node${1} ${cmd}
}

# run cluster of xline/etcd in container
# args:
#   $1: xline/etcd cluster
run_cluster() {
    server=${1}
    echo cluster starting
    case ${server} in
    xline)
        run_xline 1 &
        run_xline 2 &
        run_xline 3 &
        sleep 0.2
        ;;
    etcd)
        run_etcd 1
        sleep 3 # in order to let etcd node1 become leader
        run_etcd 2 &
        run_etcd 3 &
        ;;
    esac
    wait
    echo cluster started
}

# stop cluster of xline/etcd and remove etcd data files
# args:
#   $1: xline/etcd cluster
stop_cluster() {
    server=${1}
    echo cluster stopping
    for x in 1 2 3; do
        docker exec node${x} pkill -9 ${server} &
        docker exec node${x} rm -rf /tmp/node${x} &
    done
    wait
    echo cluster stopped
}

# stop all containers
stop_all() {
    echo stopping
    for name in "node1" "node2" "node3" "client"; do
        docker_id=$(docker ps -qf "name=${name}")
        if [ -n "$docker_id" ]; then
            docker stop $docker_id
        fi
    done
    sleep 1
    echo stopped
}

# set latency between two containers
# args:
#   $1: source container
#   $2: destination ip address
#   $3: latency between two nodes
#   $4: idx required by tc
set_latency() {
    container_name=${1}
    dst_ip=${2}
    latency=${3}
    idx=${4}
    docker exec ${container_name} tc filter add dev eth0 protocol ip parent 1:0 u32 match ip dst ${dst_ip} flowid 1:${idx}
    docker exec ${container_name} tc qdisc add dev eth0 parent 1:${idx} handle ${idx}0: netem delay ${latency}
}

# set latency of cluster
# args:
#   $1: size of cluster
set_cluster_latency() {
    cluster_size=${1}
    docker exec client tc qdisc add dev eth0 root handle 1: prio bands $((cluster_size + 4))
    for ((i = 1; i < ${cluster_size}; i++)); do
        set_latency client 172.20.0.$((i + 2)) 75ms $((i + 3)) &
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

# run container of xline/etcd use specified image
# args:
#   $1: size of cluster
#   $2: image name
run_container() {
    echo container starting
    size=${1}
    case ${2} in
    xline)
        image="datenlord/xline:latest"
        ;;
    etcd)
        image="datenlord/etcd:v3.5.5"
        ;;
    esac
    docker run -d -it --rm --name=client --net=xline_net --ip=172.20.0.2 --cap-add=NET_ADMIN --cpu-shares=512 -m=512M -v ${WORKDIR}:/mnt ${image} bash &
    for ((i = 1; i <= ${size}; i++)); do
        docker run -d -it --rm --name=node${i} --net=xline_net --ip=172.20.0.$((i + 2)) --cap-add=NET_ADMIN -m=1024M -v ${WORKDIR}:/mnt ${image} bash &
    done
    wait
    set_cluster_latency ${size}
    echo container started
}

stop_all
docker network create --subnet=172.20.0.0/24 xline_net >/dev/null 2>&1
rm -r ${OUTPUT_DIR} >/dev/null 2>&1
mkdir ${OUTPUT_DIR}
mkdir ${OUTPUT_DIR}/logs

for server in "xline" "etcd"; do
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
        case ${tmp[1]} in
        true)
            use_curp="--use-curp"
            ;;
        false)
            use_curp=""
            ;;
        esac
        endpoints=${tmp[@]:2}
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
