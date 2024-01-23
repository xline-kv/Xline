#!/usr/bin/env bash

if [ -n "$CODECOV_TOKEN" ]; then
  token="-t ${CODECOV_TOKEN}"
else
  token=""
fi

set -euo pipefail
curl -Os https://uploader.codecov.io/latest/linux/codecov && chmod +x codecov

max_retry=5
delay=30
counter=0
file=$1
NONE='\033[0m'
RED='\033[0;31m'

set +e

while true; do
    ./codecov -f $file -Z $token
    if [ $? -eq 0 ]; then
        break
    fi

    ((counter++))

    if [ $counter -eq $max_retry ]; then
        echo "${RED}Max retries reached. Exiting.${NONE}"
        exit 1
    fi
    echo -e "${RED}Fail to upload, retring ${counter}/${max_retry}${NONE}"
    sleep $delay
done
