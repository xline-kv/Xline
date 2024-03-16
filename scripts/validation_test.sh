#!/bin/bash
<<<<<<< HEAD
=======
DIR="$(dirname $0)"
QUICK_START="${DIR}/quick_start.sh"
ETCDCTL="docker exec -i client etcdctl --endpoints=http://172.20.0.3:2379,http://172.20.0.4:2379"
LOCK_CLIENT="docker exec -i client /mnt/validation_lock_client --endpoints=http://172.20.0.3:2379,http://172.20.0.4:2379,http://172.20.0.5:2379"
>>>>>>> ef29834 (chore: mount LOG_PATH as xline container log dir)

# Define directories and Docker Compose configuration
DIR="$(dirname "$0")"
DOCKER_COMPOSE_FILE="${DIR}/../ci/docker-compose.yml"

<<<<<<< HEAD
# Source log functions
source "${DIR}/log.sh"
=======
LOG_PATH=${DIR}/logs LOG_LEVEL=debug bash ${QUICK_START}
source $DIR/log.sh
>>>>>>> ef29834 (chore: mount LOG_PATH as xline container log dir)

# Function to stop all services using Docker Compose
stop() {
    docker-compose -f "${DOCKER_COMPOSE_FILE}" down
}

# Trap signals to ensure cleanup
trap stop EXIT INT TERM

# Ensure the environment is running
docker-compose -f "${DOCKER_COMPOSE_FILE}" up -d

# Function to run validation tests
run_validation_tests() {
    # Example validation test
    log::info "Starting validation tests..."
    
    # Add your validation logic here, for example:
    # 1. Check if services are healthy
    # 2. Execute specific commands against your services
    
    log::info "Validation tests completed."
}

# Execute the validation tests
run_validation_tests
