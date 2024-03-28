#!/bin/bash

# This script is designed to quickly start the Xline environment using Docker Compose.
# It ensures that all required services are up and running in a containerized setup.

# Navigate to the directory containing docker-compose.yml
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="${DIR}/../ci/docker-compose.yml"

# Function to start the environment
start_environment() {
    echo "Starting the Xline environment with Docker Compose..."
    docker-compose -f "${COMPOSE_FILE}" up -d
    echo "The Xline environment has started successfully."
}

# Function to stop and clean up the environment
stop_environment() {
    echo "Stopping and cleaning up the Xline environment..."
    docker-compose -f "${COMPOSE_FILE}" down
    echo "The Xline environment has been stopped and cleaned up."
}

# Check if the first command-line argument is 'stop'; if so, call stop_environment
if [ "$1" == "stop" ]; then
    stop_environment
else
    start_environment
fi
