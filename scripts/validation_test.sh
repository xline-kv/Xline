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

# validate kv requests
kv_validation() {
    echo "kv validation test running..."
    run_with_expect "${ETCDCTL} put key1 value1" "OK"
    stdin='value(\"key1\") = \"value1\"\n\nput key2 success\n\nput key2 failure\n'
    run_with_expect "echo -e \"${stdin}\" | ${ETCDCTL} txn" "SUCCESS\n\nOK"
    run_with_expect "${ETCDCTL} get key2" "key2\nsuccess"
    run_with_expect "${ETCDCTL} del key1" "1"
    # TODO compact
    echo "kv validation test passed"
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
    echo "watch validation test passed"
}

# validate lease requests
lease_validation() {
    echo "lease validation test running..."
    command="${ETCDCTL} lease grant 5"
    out=$(${command})
    patten='lease [0-9a-z]+ granted with TTL\([0-9]+s\)'
    if [[ ${out} =~ ${patten} ]]; then
        echo "command run success"
    else
        echo "command run failed"
        echo "command: ${command}"
        echo "expect: ${patten}"
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
    echo "lease validation test passed"
}

# validate auth requests
auth_validation() {
    echo "auth validation test running..."
    run_with_expect "${ETCDCTL} user add root:root" "User root created"
    run_with_expect "${ETCDCTL} role add root" "Role root created"
    run_with_expect "${ETCDCTL} user grant-role root root" "Role root is granted to user root"
    run_with_expect "${ETCDCTL} auth enable" "Authentication Enabled"
    run_with_expect "${ETCDCTL} --user root:root auth status" "Authentication Status: true\nAuthRevision: 5"
    run_with_expect "${ETCDCTL} --user root:root user add u:u" "User u created"
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

kv_validation
watch_validation
lease_validation
auth_validation
lock_validation
lock_rpc_validation
