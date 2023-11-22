#!/bin/bash

if [ -z "$HOSTNAME" ]; then
    echo -e "Env HOSTNAME is not set"
    exit 1
fi

if [ -z "$MEMBERS" ]; then
    echo -e "Env MEMBERS is not set"
    exit 1
fi

RUST_LOG="${RUST_LOG:-debug}"
ENGINE="${STORAGE_ENGINE:-rocksdb}"
DATA_DIR="${DATA_DIR:-/usr/local/xline/data-dir}"
PUBLIC_KEY="${PUBLIC_KEY:-/mnt/public.pem}"
PRIVATE_KEY="${PRIVATE_KEY:-/mnt/private.pem}"

if [ -z "$IS_LEADER" ]; then
    RUST_LOG=$RUST_LOG /usr/local/bin/xline \
        --name $HOSTNAME \
        --members $MEMBERS \
        --storage-engine rocksdb \
        --data-dir /usr/local/xline/data-dir \
        --auth-public-key /mnt/public.pem \
        --auth-private-key /mnt/private.pem
else
    RUST_LOG=$RUST_LOG /usr/local/bin/xline \
        --name $HOSTNAME \
        --members $MEMBERS \
        --storage-engine rocksdb \
        --data-dir /usr/local/xline/data-dir \
        --auth-public-key /mnt/public.pem \
        --auth-private-key /mnt/private.pem \
        --is-leader
fi
