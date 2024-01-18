#!/usr/bin/env bash

if [ -n "$CODECOV_TOKEN" ]; then
  token="-t ${CODECOV_TOKEN}"
else
  token=""
fi

set -euo pipefail
curl -Os https://uploader.codecov.io/latest/linux/codecov && chmod +x codecov

max_retry=5
counter=0
sha=$1
NONE='\033[0m'
RED='\033[0;31m'

set +e

while true; do
    ./codecov -f lcov.info -Z -C $sha $token
    if [ $? -eq 0 ]; then
        break
    fi

    ((counter++))

    if [ $counter -eq $max_retry ]; then
        echo "Max retries reached. Exiting."
        exit 1
    fi
    echo -e "${RED}Fail to upload, retring #${counter}${NONE}"
    sleep 5
done
