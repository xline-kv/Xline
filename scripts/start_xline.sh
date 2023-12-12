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

cmd="/usr/local/bin/xline \
        --name $HOSTNAME \
        --members $MEMBERS \
        --storage-engine $ENGINE \
        --data-dir $DATA_DIR "


if [ -n "$AUTH_PUBLIC_KEY" ] && [ -n "$AUTH_PRIVATE_KEY" ]; then
    cmd="${cmd} \
        --auth-public-key $AUTH_PUBLIC_KEY \
        --auth-private-key $AUTH_PRIVATE_KEY
    "
fi


if [ "$HOSTNAME" = "$INIT_LEADER" ]; then
    cmd="${cmd} --is-leader"
fi

if [ -n "$LOG_FILE"]; then
    cmd="${cmd} \
        --log-file $LOG_FILE \
        --log-level $RUST_LOG \
    "
fi

RUST_LOG=$RUST_LOG ${cmd}
