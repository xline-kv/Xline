#!/bin/bash

# Ensure the script is executed from the script's directory
cd "$(dirname "$0")"

# Function to start the environment
start_environment() {
    echo "Starting the environment using docker-compose..."
    docker-compose -f ../ci/docker-compose.yml up -d
    echo "The environment is started."
}

# Function to stop and clean up the environment
stop_environment() {
    echo "Stopping and removing the environment..."
    docker-compose -f ../ci/docker-compose.yml down
    echo "The environment is stopped and cleaned up."
}

# Check command line arguments
if [[ "$1" == "stop" ]]; then
    stop_environment
else
    start_environment
fi
