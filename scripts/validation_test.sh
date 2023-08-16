#!/bin/bash
DIR="$(dirname $0)"
QUICK_START="${DIR}/quick_start.sh"
bash ${QUICK_START}
ETCDCTL="docker exec -i node4 etcdctl --endpoints=http://172.20.0.3:2379"
LOCK_CLIENT="docker exec -i node4 /mnt/lock_client --endpoints=http://172.20.0.3:2379"

# run a command with expect output
# args:
#   $1: command to run
#   $2: expect output
run_with_expect() {
    cmd="${1}"
    res=$(eval ${cmd})
    # echo -e "res:\n${res}\n"
    expect=$(echo -e ${2})
    if [ "${res}" == "${expect}" ]; then
        echo "command run success"
    else
        echo "command run failed"
        echo "command: ${cmd}"
        echo "expect: ${expect}"
        echo "result: ${res}"
        exit 1
    fi
}

# run a command with expect output, based on key word match
# args:
#   $1: command to run
#   $2: expect output
run_with_match() {
    cmd="${1}"
    res=$(eval ${cmd} 2>&1)
    expect=$(echo -e ${2})
    if echo "${res}" | grep -q "${expect}"; then
        echo "command run success"
    else
        echo "command run failed"
        echo "command: ${cmd}"
        echo "expect: ${expect}"
        echo "result: ${res}"
        exit 1
    fi
}

# validate kv requests
kv_validation() {
    echo "kv validation test running..."
    run_with_expect "${ETCDCTL} put key1 value1" "OK"
    stdin='value(\"key1\") = \"value1\"\n\nput key2 success\n\nput key2 failure\n'
    run_with_expect "echo -e \"${stdin}\" | ${ETCDCTL} txn" "SUCCESS\n\nOK"
    run_with_expect "${ETCDCTL} get key2" "key2\nsuccess"
    run_with_expect "${ETCDCTL} del key1" "1"
    run_with_match "${ETCDCTL} put '' value" "etcdserver: key is not provided"
    run_with_match "${ETCDCTL} put non_exist_key --ignore-value" "etcdserver: key not found"

    # TODO compact
    echo "kv validation test passed"
}

watch_progress_validation() {
    expect << EOF
    spawn ${ETCDCTL} watch -i
    send "watch foo\r"
    send "progress\r"
    expect {
        -timeout 3
        "progress notify" {
            exit 0
        }
        timeout {
            exit 1
        }
    }
    expect eof
EOF
    if [ $? -eq 0 ]; then
        echo "command run success"
    else
        echo "watch_progress_validation run failed"
        exit 1
    fi
}

# validate watch requests
watch_validation() {
    echo "watch validation test running..."
    command="${ETCDCTL} watch watch_key"
    want=("PUT" "watch_key" "value" "DELETE" "watch_key" "value" "watch_key")
    ${command} | while read line; do
        if [ "${line}" == "${want[0]}" ]; then
            unset want[0]
            want=("${want[@]}")
        else
            echo "command run failed"
            echo "command: ${command}"
            echo "expect: ${want[0]}"
            echo "result: ${line}"
            exit 1
        fi
    done &
    sleep 0.1 # wait watch
    run_with_expect "${ETCDCTL} put watch_key value" "OK"
    run_with_expect "${ETCDCTL} del watch_key" "1"
    watch_progress_validation
    echo "watch validation test passed"
}

# validate lease requests
lease_validation() {
    echo "lease validation test running..."
    command="${ETCDCTL} lease grant 5"
    out=$(${command})
    pattern='lease [0-9a-z]+ granted with TTL\([0-9]+s\)'
    if [[ ${out} =~ ${pattern} ]]; then
        echo "command run success"
    else
        echo "command run failed"
        echo "command: ${command}"
        echo "expect: ${pattern}"
        echo "result: ${out}"
        exit 1
    fi
    lease_id=$(echo ${out} | awk '{print $2}')
    run_with_expect "${ETCDCTL} lease list" "found 1 leases\n${lease_id}"
    command="${ETCDCTL} lease keep-alive ${lease_id}"
    ${command} | while read line; do

        if [ "${line}" != "lease ${lease_id} keepalived with TTL(5)" ]; then
            if [ "${line}" != "lease ${lease_id} expired or revoked." ]; then
                echo "command run failed"
                echo "command: ${command}"
                echo "expect: lease ${lease_id} keepalived with TTL(5) or lease ${lease_id} expired or revoked."
                echo "result: ${line}"
                exit 1
            fi
        fi
    done &
    run_with_expect "${ETCDCTL} put key1 value1 --lease=${lease_id}" "OK"
    run_with_expect "${ETCDCTL} get key1" "key1\nvalue1"
    command="${ETCDCTL} lease timetolive ${lease_id}"
    remaining=$(${command} | sed -r "s/lease ${lease_id} granted with TTL\(5s\), remaining\(([0-9]+)s\)/\1/g")
    if [ ${remaining} -le 5 ]; then
        echo "command run success"
    else
        echo "command run failed"
        echo "command: ${command}"
        echo "remaining should be less than 5, but got ${remaining}"
        exit 1
    fi

    run_with_expect "${ETCDCTL} lease revoke ${lease_id}" "lease ${lease_id} revoked"
    run_with_expect "${ETCDCTL} get key1" ""
    run_with_match "${ETCDCTL} lease revoke 255" "etcdserver: requested lease not found"
    run_with_match "${ETCDCTL} lease grant 10000000000" "etcdserver: too large lease TTL"
    echo "lease validation test passed"
}

# validate auth requests
auth_validation() {
    echo "auth validation test running..."
    run_with_expect "${ETCDCTL} user add root:root" "User root created"
    run_with_expect "${ETCDCTL} role add root" "Role root created"
    run_with_expect "${ETCDCTL} user grant-role root root" "Role root is granted to user root"
    run_with_match "${ETCDCTL} --user root:root user list" "etcdserver: authentication is not enabled"
    run_with_expect "${ETCDCTL} auth enable" "Authentication Enabled"
    run_with_match "${ETCDCTL} --user root:rot user list" "etcdserver: authentication failed, invalid user ID or password"
    run_with_expect "${ETCDCTL} --user root:root auth status" "Authentication Status: true\nAuthRevision: 4"
    run_with_expect "${ETCDCTL} --user root:root user add u:u" "User u created"
    run_with_match "${ETCDCTL} --user u:u user add f:f" "etcdserver: permission denied"
    run_with_expect "${ETCDCTL} --user root:root role add r" "Role r created"
    run_with_expect "${ETCDCTL} --user root:root user grant-role u r" "Role r is granted to user u"
    run_with_expect "${ETCDCTL} --user root:root role grant-permission r readwrite key1" "Role r updated"
    run_with_expect "${ETCDCTL} --user u:u put key1 value1" "OK"
    run_with_expect "${ETCDCTL} --user u:u get key1" "key1\nvalue1"
    run_with_expect "${ETCDCTL} --user u:u role get r" "Role r\nKV Read:\n\tkey1\nKV Write:\n\tkey1"
    run_with_expect "${ETCDCTL} --user u:u user get u" "User: u\nRoles: r"
    run_with_expect "echo 'new_password' | ${ETCDCTL} --user root:root user passwd --interactive=false u" "Password updated"
    run_with_expect "${ETCDCTL} --user root:root role revoke-permission r key1" "Permission of key key1 is revoked from role r"
    run_with_expect "${ETCDCTL} --user root:root user revoke-role u r" "Role r is revoked from user u"
    run_with_expect "${ETCDCTL} --user root:root user list" "root\nu"
    run_with_expect "${ETCDCTL} --user root:root role list" "r\nroot"
    run_with_expect "${ETCDCTL} --user root:root user delete u" "User u deleted"
    run_with_expect "${ETCDCTL} --user root:root role delete r" "Role r deleted"
    run_with_match "${ETCDCTL} --user root:root user get non_exist_user" "etcdserver: user name not found"
    run_with_match "${ETCDCTL} --user root:root user add root:root" "etcdserver: user name already exists"
    run_with_match "${ETCDCTL} --user root:root role get non_exist_role" "etcdserver: role name not found"
    run_with_match "${ETCDCTL} --user root:root role add root" "etcdserver: role name already exists"
    run_with_match "${ETCDCTL} --user root:root user revoke root r" "etcdserver: role is not granted to the user"
    run_with_match "${ETCDCTL} --user root:root role revoke root non_exist_key" "etcdserver: permission is not granted to the role"
    run_with_match "${ETCDCTL} --user root:root user delete root" "etcdserver: invalid auth management"
    run_with_expect "${ETCDCTL} --user root:root auth disable" "Authentication Disabled"
    echo "auth validation test passed"
}

lock_validation() {
    echo "lock validation test running..."
    run_with_expect "${ETCDCTL} lock mutex echo success" "success"
    echo "lock validation test passed"
}

lock_rpc_validation() {
    echo "lock rpc validation test running..."
    command="${LOCK_CLIENT} lock mutex"
    key=$(${command})
    if [[ ${key} == mutex* ]]; then
        echo "command run success"
    else
        echo "command run failed"
        echo "command: ${command}"
        echo "expect: mutex*"
        echo "result: ${key}"
    fi
    run_with_expect "${LOCK_CLIENT} unlock ${key}" "unlock success"
    echo "lock rpc validation test passed"
}

# validate maintenance requests
maintenance_validation() {
    echo "maintenance validation test running..."
    run_with_expect "${ETCDCTL} snapshot save snap.db" "Snapshot saved at snap.db"
    echo "maintenance validation test passed"
}

# validate compact requests
compact_validation() {
    echo "compact validation test running..."
    for value in "value1" "value2" "value3" "value4" "value5" "value6";
    do
        run_with_expect "${ETCDCTL} put key ${value}" "OK"
    done
    run_with_expect "${ETCDCTL} get --rev=4 key" "key\nvalue3"
    run_with_expect "${ETCDCTL} compact 5" "compacted revision 5"
    run_with_match "${ETCDCTL} get --rev=4 key" "etcdserver: mvcc: required revision has been compacted"
    run_with_match "${ETCDCTL} watch --rev=4 key " "watch was canceled (etcdserver: mvcc: required revision has been compacted)"
    echo "compact validation test pass..."
}

compact_validation
kv_validation
watch_validation
lease_validation
auth_validation
lock_validation
lock_rpc_validation
maintenance_validation
