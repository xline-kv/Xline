#!/bin/bash
# Set the working directory and output directory
WORKDIR=$(pwd)
OUTPUT_DIR="${WORKDIR}/out"
mkdir -p "${OUTPUT_DIR}" # Ensure output directory exists

# Define the benchmark test cases
# Adjust the MEMBERS strings as necessary to match your docker-compose service names and configurations
MEMBERS="node1=http://node1:2379,node2=http://node2:2379,node3=http://node3:2379"

XLINE_TESTCASE=(
    "client true ${MEMBERS}"
    "client false node3=http://node3:2379"
    "client false node1=http://node1:2379"
)

# Additional benchmark parameters
KEY_SPACE_SIZE=("1" "100000")
CLIENTS_TOTAL=("1 50" "10 300" "50 1000" "100 3000" "200 5000")
FORMAT="%-8s\\t%-12s\\t%-8s\\t%-s\\n"

# Function to run the benchmark command
benchmark_cmd() {
    local container_name=$1
    local endpoints=$2
    local use_curp=$3
    local clients=$4
    local total=$5
    local key_space_size=$6
    # Note: Adjust the command as per your actual benchmark tool's syntax
    echo "docker-compose exec ${container_name} benchmark --endpoints ${endpoints} ${use_curp} --clients=${clients} --stdout put --key-size=8 --val-size=256 --total=${total} --key-space-size=${key_space_size}"
}

# Ensure docker-compose is up and running
echo "Starting the test environment using docker-compose..."
docker-compose -f "${WORKDIR}/ci/docker-compose.yml" up -d

# Run benchmark tests
echo "Starting benchmark tests..."
for testcase in "${XLINE_TESTCASE[@]}"; do
    IFS=' ' read -r container_name use_curp_flag endpoints <<< "$testcase"
    use_curp=""
    if [[ $use_curp_flag == "true" ]]; then
        use_curp="--use-curp"
    fi
    for key_space_size in "${KEY_SPACE_SIZE[@]}"; do
        for clients_total in "${CLIENTS_TOTAL[@]}"; do
            IFS=' ' read -r clients total <<< "$clients_total"
            cmd=$(benchmark_cmd "${container_name}" "${endpoints}" "${use_curp}" "${clients}" "${total}" "${key_space_size}")
            echo "Executing benchmark: $cmd"
            eval $cmd
        done
    done
done

# Stop and remove the test environment
echo "Stopping and removing the test environment..."
docker-compose -f "${WORKDIR}/ci/docker-compose.yml" down

echo "Benchmark tests completed."
