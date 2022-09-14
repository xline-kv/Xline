#!/bin/bash
OUTPUT_DIR="./out"
REPEAT=1
SERVERS=("172.20.0.2" "172.20.0.3" "172.20.0.4")
CLIENT="172.20.0.5"
CLUSTER_PEERS=(
    "${SERVERS[1]}:2380 ${SERVERS[2]}:2380"
    "${SERVERS[0]}:2380 ${SERVERS[2]}:2380"
    "${SERVERS[0]}:2380 ${SERVERS[1]}:2380"
)
TEST_CASE=(
    "docker exec node0  ./benchmark --endpoints=http://${SERVERS[0]}:2379 --conns=1   --clients=1   put --key-size=8 --val-size=256 --total=50"
    "docker exec node0  ./benchmark --endpoints=http://${SERVERS[0]}:2379 --conns=10  --clients=10  put --key-size=8 --val-size=256 --total=300"
    "docker exec node0  ./benchmark --endpoints=http://${SERVERS[0]}:2379 --conns=50  --clients=50  put --key-size=8 --val-size=256 --total=1000"
    "docker exec node0  ./benchmark --endpoints=http://${SERVERS[0]}:2379 --conns=100 --clients=100 put --key-size=8 --val-size=256 --total=3000"
    "docker exec node0  ./benchmark --endpoints=http://${SERVERS[0]}:2379 --conns=200 --clients=200 put --key-size=8 --val-size=256 --total=5000"
    "docker exec node0  ./benchmark --endpoints=http://${SERVERS[0]}:2379 --conns=1   --clients=1   put --key-size=8 --val-size=256 --total=50   --key-space-size=100000"
    "docker exec node0  ./benchmark --endpoints=http://${SERVERS[0]}:2379 --conns=10  --clients=10  put --key-size=8 --val-size=256 --total=300  --key-space-size=100000"
    "docker exec node0  ./benchmark --endpoints=http://${SERVERS[0]}:2379 --conns=50  --clients=50  put --key-size=8 --val-size=256 --total=1000 --key-space-size=100000"
    "docker exec node0  ./benchmark --endpoints=http://${SERVERS[0]}:2379 --conns=100 --clients=100 put --key-size=8 --val-size=256 --total=3000 --key-space-size=100000"
    "docker exec node0  ./benchmark --endpoints=http://${SERVERS[0]}:2379 --conns=200 --clients=200 put --key-size=8 --val-size=256 --total=5000 --key-space-size=100000"

    "docker exec node1  ./benchmark --endpoints=http://${SERVERS[1]}:2379 --conns=1   --clients=1   put --key-size=8 --val-size=256 --total=50"
    "docker exec node1  ./benchmark --endpoints=http://${SERVERS[1]}:2379 --conns=10  --clients=10  put --key-size=8 --val-size=256 --total=300"
    "docker exec node1  ./benchmark --endpoints=http://${SERVERS[1]}:2379 --conns=50  --clients=50  put --key-size=8 --val-size=256 --total=1000"
    "docker exec node1  ./benchmark --endpoints=http://${SERVERS[1]}:2379 --conns=100 --clients=100 put --key-size=8 --val-size=256 --total=3000"
    "docker exec node1  ./benchmark --endpoints=http://${SERVERS[1]}:2379 --conns=200 --clients=200 put --key-size=8 --val-size=256 --total=5000"
    "docker exec node1  ./benchmark --endpoints=http://${SERVERS[1]}:2379 --conns=1   --clients=1   put --key-size=8 --val-size=256 --total=50   --key-space-size=100000"
    "docker exec node1  ./benchmark --endpoints=http://${SERVERS[1]}:2379 --conns=10  --clients=10  put --key-size=8 --val-size=256 --total=300  --key-space-size=100000"
    "docker exec node1  ./benchmark --endpoints=http://${SERVERS[1]}:2379 --conns=50  --clients=50  put --key-size=8 --val-size=256 --total=1000 --key-space-size=100000"
    "docker exec node1  ./benchmark --endpoints=http://${SERVERS[1]}:2379 --conns=100 --clients=100 put --key-size=8 --val-size=256 --total=3000 --key-space-size=100000"
    "docker exec node1  ./benchmark --endpoints=http://${SERVERS[1]}:2379 --conns=200 --clients=200 put --key-size=8 --val-size=256 --total=5000 --key-space-size=100000"

    "docker exec client ./benchmark --endpoints=http://${SERVERS[2]}:2379 --conns=1   --clients=1   put --key-size=8 --val-size=256 --total=50"
    "docker exec client ./benchmark --endpoints=http://${SERVERS[2]}:2379 --conns=10  --clients=10  put --key-size=8 --val-size=256 --total=300"
    "docker exec client ./benchmark --endpoints=http://${SERVERS[2]}:2379 --conns=50  --clients=50  put --key-size=8 --val-size=256 --total=1000"
    "docker exec client ./benchmark --endpoints=http://${SERVERS[2]}:2379 --conns=100 --clients=100 put --key-size=8 --val-size=256 --total=3000"
    "docker exec client ./benchmark --endpoints=http://${SERVERS[2]}:2379 --conns=200 --clients=200 put --key-size=8 --val-size=256 --total=5000"
    "docker exec client ./benchmark --endpoints=http://${SERVERS[2]}:2379 --conns=1   --clients=1   put --key-size=8 --val-size=256 --total=50   --key-space-size=100000"
    "docker exec client ./benchmark --endpoints=http://${SERVERS[2]}:2379 --conns=10  --clients=10  put --key-size=8 --val-size=256 --total=300  --key-space-size=100000"
    "docker exec client ./benchmark --endpoints=http://${SERVERS[2]}:2379 --conns=50  --clients=50  put --key-size=8 --val-size=256 --total=1000 --key-space-size=100000"
    "docker exec client ./benchmark --endpoints=http://${SERVERS[2]}:2379 --conns=100 --clients=100 put --key-size=8 --val-size=256 --total=3000 --key-space-size=100000"
    "docker exec client ./benchmark --endpoints=http://${SERVERS[2]}:2379 --conns=200 --clients=200 put --key-size=8 --val-size=256 --total=5000 --key-space-size=100000"
)
FORMAT="%-8s\t%-12s\t%-8s\t%-s\n"

run_xline() {
    cmd="./xline \
    --name node$1 \
    --cluster-peers ${CLUSTER_PEERS[$1]} \
    --self-ip-port ${SERVERS[$1]}:2380 \
    --ip-port 0.0.0.0:2379 \
    --leader-ip-port ${SERVERS[0]}:2380"

    if [ $1 -eq 0 ]; then
        cmd="${cmd} --is-leader"
    fi
    docker run -d -it --rm --name=node$1 --net=xline_net --ip=${SERVERS[$1]} --cap-add=NET_ADMIN --cpu-shares=1024 -m=512M xline $cmd
}

run_etcd() {
    cmd="./etcd --name node$1 \
    --listen-client-urls http://0.0.0.0:2379 \
    --listen-peer-urls http://0.0.0.0:2380 \
    --advertise-client-urls http://${SERVERS[$1]}:2379 \
    --initial-advertise-peer-urls http://${SERVERS[$1]}:2380 \
    --initial-cluster-token etcd-cluster-1 \
    --initial-cluster node0=http://${SERVERS[0]}:2380,node1=http://${SERVERS[1]}:2380,node2=http://${SERVERS[2]}:2380 \
    --initial-cluster-state new \
    --logger zap"
    docker run -d -it --rm --name=node$1 --net=xline_net --ip=${SERVERS[$1]} --cap-add=NET_ADMIN --cpu-shares=1024 -m=512M xline $cmd
}

run_client() {
    docker run -d -it --rm --name=client --net=xline_net --ip=172.20.0.5 --cap-add=NET_ADMIN --cpu-shares=512 -m=512M xline bash
}

run_cluster() {
    echo starting
    run_client
    case $1 in
    xline)
        run_xline 0 &
        run_xline 1 &
        run_xline 2 &
        ;;
    etcd)
        run_etcd 0
        sleep 3
        run_etcd 1 &
        run_etcd 2 &
        ;;
    esac
    wait
    echo started
}

stop_all() {
    echo stoping
    docker stop $(docker ps -a -q)
    sleep 1
    echo stoped
}

set_latency() {
    docker exec $1 tc qdisc add dev eth0 root handle 1: prio bands 7
    docker exec $1 tc qdisc add dev eth0 parent 1:4 handle 40: netem delay $5
    docker exec $1 tc qdisc add dev eth0 parent 1:5 handle 50: netem delay $6
    docker exec $1 tc qdisc add dev eth0 parent 1:6 handle 60: netem delay $7
    docker exec $1 tc filter add dev eth0 protocol ip parent 1:0 u32 match ip dst $2 flowid 1:4
    docker exec $1 tc filter add dev eth0 protocol ip parent 1:0 u32 match ip dst $3 flowid 1:5
    docker exec $1 tc filter add dev eth0 protocol ip parent 1:0 u32 match ip dst $4 flowid 1:6
}

set_cluster_latency() {
    set_latency node0 ${SERVERS[1]} ${SERVERS[2]} ${CLIENT} 50ms 50ms 75ms &
    set_latency node1 ${SERVERS[0]} ${SERVERS[2]} ${CLIENT} 50ms 50ms 75ms &
    set_latency node2 ${SERVERS[0]} ${SERVERS[1]} ${CLIENT} 50ms 50ms 25ms &
    set_latency client ${SERVERS[0]} ${SERVERS[1]} ${SERVERS[2]} 75ms 75ms 25ms &
    wait
}

stop_all
docker network create --subnet=172.20.0.0/24 xline_net >/dev/null 2>&1
rm -r ${OUTPUT_DIR} >/dev/null 2>&1
mkdir ${OUTPUT_DIR}
mkdir ${OUTPUT_DIR}/logs

for server in xline etcd; do
    for ((n = 0; n < ${REPEAT}; n++)); do
        logs_dir=${OUTPUT_DIR}/logs/${server}_${n}_logs
        mkdir -p ${logs_dir}
        result_file=${OUTPUT_DIR}/${server}_${n}.txt
        printf ${FORMAT} "QPS" "Latency(ms)" "Time(s)" "Command" >>${result_file}
        for ((i = 0; i < ${#TEST_CASE[@]}; i++)); do
            run_cluster $server
            set_cluster_latency
            output_file=${logs_dir}/case_${i}.txt

            echo running test_case_$i:
            ${TEST_CASE[$i]} >>${output_file}

            Latency=$(cat ${output_file} | grep Average | awk '{printf "%.1f",$2*1000}')
            QPS=$(cat ${output_file} | grep Requests/sec | awk '{printf "%.1f",$2}')
            Time=$(cat ${output_file} | grep Total | awk '{printf "%.1f",$2}')

            printf ${FORMAT} ${QPS} ${Latency} ${Time} "${TEST_CASE[$i]}" >>${result_file}
            stop_all
        done
    done
done
