#!/bin/bash
DIR="$(dirname $0)"
source $DIR/common.sh
source $DIR/log.sh

QUICK_START="${DIR}/quick_start.sh"
ETCDCTL="docker exec -i client etcdctl --endpoints=http://172.20.0.3:2379,http://172.20.0.4:2379"
LOCK_CLIENT="docker exec -i client /mnt/validation_lock_client --endpoints=http://172.20.0.3:2379,http://172.20.0.4:2379,http://172.20.0.5:2379"
export LOG_PATH=/mnt/logs
export LOG_LEVEL=debug

bash ${QUICK_START}

stop() {
    docker_id=$(docker ps -qf "name=node4")
    if [ -n "$docker_id" ]; then
        docker stop $docker_id
    fi
    bash ${QUICK_START} stop
}

trap stop EXIT
trap stop INT
trap stop TERM

res=""

function run_new_member() {
     docker_id=$(docker ps -qf "name=node4")
    if [ -n "$docker_id" ]; then
        docker stop $docker_id
    fi
    common::run_container 4
    common::run_xline 4 ${NEWMEMBERS} existing
}

function parse_result() {
    local tmp_res=""
    while read -r line; do
        log::debug $line
        tmp_res="${tmp_res}${line}\n"
    done
    res="${tmp_res}"
}

# check_positive X indicates that the X must appear in the result
function check_positive() {
    local pattern=$1
    if [[ $(echo -e $res) =~ $pattern ]]; then
        log::info "pass"
    else
        log::fatal "result not match pattern\n\tpattern: $pattern\n\tresult: $res"
    fi
}

# check_negative X indicates that the X should not appear in the result
function check_negative() {
    local pattern=$1
    if [[ $(echo -e $res) =~ $pattern ]]; then
        log::fatal "result not match pattern\n\tpattern: $pattern\n\tresult: $res"
    else
        log::info "pass"
    fi
}

function run() {
    command=$@
    log::info "running: $command"
    local run_res="$(eval $command 2>&1)"
    parse_result <<< $run_res
}

# validate compact requests
compact_validation() {
    local _ETCDCTL="docker exec -i client etcdctl --endpoints=http://172.20.0.3:2379"
    log::info "compact validation test running..."

    for value in "value1" "value2" "value3" "value4" "value5" "value6"; do
        run "${ETCDCTL} put key ${value}"
        check_positive "OK"
    done
    run "${ETCDCTL} get --rev=4 key"
    check_positive $'key\nvalue3'
    run "${_ETCDCTL} compact --physical 5"
    check_positive "compacted revision 5"
    run "${_ETCDCTL} get --rev=4 key"
    check_positive "etcdserver: mvcc: required revision has been compacted"
    run "${_ETCDCTL} watch --rev=4 key"
    check_positive "watch was canceled \(etcdserver: mvcc: required revision has been compacted\)"

    log::info "compact validation test pass..."
}

# validate kv requests
kv_validation() {
    log::info "kv validation test running..."

    run "${ETCDCTL} put key1 value1"
    check_positive "OK"
    stdin='value(\"key1\") = \"value1\"\n\nput key2 success\n\nput key2 failure\n'
    run "echo -e \"${stdin}\" | ${ETCDCTL} txn"
    check_positive $'SUCCESS\n\nOK'
    run "${ETCDCTL} get key2"
    check_positive $'key2\nsuccess'
    run "${ETCDCTL} del key1"
    check_positive "1"
    run "${ETCDCTL} put '' value"
    check_positive "etcdserver: key is not provided"
    run "${ETCDCTL} put non_exist_key --ignore-value"
    check_positive "etcdserver: key not found"

    log::info "kv validation test passed"
}

watch_progress_validation() {
    log::info "watch progress validation test running..."
    expect <<EOF
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
        log::info "pass"
    else
        log::fatal "watch_progress_validation run failed"
    fi
}


# validate watch requests
watch_validation() {
    log::info "watch validation test running..."

    command="${ETCDCTL} watch watch_key"
    log::info "running: ${command}"
    want=("PUT" "watch_key" "value" "DELETE" "watch_key")
    ${command} | while read line; do
        log::debug ${line}
        if [ "${line}" == "${want[0]}" ]; then
            unset want[0]
            want=("${want[@]}")
        else
            log::fatal "result not match pattern\n\tpattern: ${want[0]}\n\tresult: ${line}"
        fi
    done &
    sleep 0.1 # wait watch
    run "${ETCDCTL} put watch_key value"
    check_positive "OK"
    run "${ETCDCTL} del watch_key"
    check_positive "1"
    watch_progress_validation

    log::info "watch validation test passed"
}

# validate lease requests
lease_validation() {
    log::info "lease validation test running..."

    run "${ETCDCTL} lease grant 5"
    check_positive 'lease\s+[0-9a-z]+ granted with TTL\([0-9]+s\)'
    lease_id=$(echo -e ${res} | awk '{print $2}')
    run "${ETCDCTL} lease list"
    check_positive $'found 1 leases\n'${lease_id}
    command="${ETCDCTL} lease keep-alive ${lease_id}"
    log::info "running: ${command}"
    ${command} | while read line; do
        log::debug ${line}
        if [ "${line}" != "lease ${lease_id} keepalived with TTL(5)" ]; then
            if [ "${line}" != "lease ${lease_id} expired or revoked." ]; then
                log::fatal "result not match pattern\n\tpattern: lease ${lease_id} keepalived with TTL(5) or lease ${lease_id} expired or revoked.\n\tresult: ${line}"
            fi
        fi
    done &
    run "${ETCDCTL} put key1 value1 --lease=${lease_id}"
    check_positive "OK"
    run "${ETCDCTL} get key1"
    check_positive $'key1\nvalue1'
    run "${ETCDCTL} lease timetolive ${lease_id}"
    check_positive "lease\s+${lease_id} granted with TTL\(5s\), remaining\([0-5]s\)"
    run "${ETCDCTL} lease revoke ${lease_id}"
    check_positive "lease\s+${lease_id} revoked"
    run "${ETCDCTL} get key1"
    check_positive ""
    run "${ETCDCTL} lease revoke 255"
    check_positive "etcdserver: requested lease not found"
    run "${ETCDCTL} lease grant 10000000000"
    check_positive "etcdserver: too large lease TTL"

    log::info "lease validation test passed"
}

# validate auth requests
auth_validation() {
    log::info "auth validation test running..."

    run "${ETCDCTL} user add root:root"
    check_positive "User root created"
    run "${ETCDCTL} role add root"
    check_positive "Role root created"
    run "${ETCDCTL} user grant-role root root"
    check_positive "Role root is granted to user root"
    run "${ETCDCTL} --user root:root user list"
    check_positive "etcdserver: authentication is not enabled"
    run "${ETCDCTL} auth enable"
    check_positive "Authentication Enabled"
    run "${ETCDCTL} --user root:rot user list"
    check_positive "etcdserver: authentication failed, invalid user ID or password"
    run "${ETCDCTL} --user root:root auth status"
    check_positive $'Authentication Status: true\nAuthRevision: 4'
    run "${ETCDCTL} --user root:root user add u:u"
    check_positive "User u created"
    run "${ETCDCTL} --user u:u user add f:f"
    check_positive "etcdserver: permission denied"
    run "${ETCDCTL} --user root:root role add r"
    check_positive "Role r created"
    run "${ETCDCTL} --user root:root user grant-role u r"
    check_positive "Role r is granted to user u"
    run "${ETCDCTL} --user root:root role grant-permission r readwrite key1"
    check_positive "Role r updated"
    run "${ETCDCTL} --user u:u put key1 value1"
    check_positive "OK"
    run "${ETCDCTL} --user u:u get key1"
    check_positive $'key1\nvalue1'
    run "${ETCDCTL} --user u:u role get r"
    check_positive $'Role r\nKV Read:\nkey1\nKV Write:\nkey1'
    run "${ETCDCTL} --user u:u user get u"
    check_positive $'User: u\nRoles: r'
    run "echo 'new_password' | ${ETCDCTL} --user root:root user passwd --interactive=false u"
    check_positive "Password updated"
    run "${ETCDCTL} --user root:root role revoke-permission r key1"
    check_positive "Permission of key key1 is revoked from role r"
    run "${ETCDCTL} --user root:root user revoke-role u r"
    check_positive "Role r is revoked from user u"
    run "${ETCDCTL} --user root:root user list"
    check_positive $'root\nu'
    run "${ETCDCTL} --user root:root role list"
    check_positive $'r\nroot'
    run "${ETCDCTL} --user root:root user delete u"
    check_positive "User u deleted"
    run "${ETCDCTL} --user root:root role delete r"
    check_positive "Role r deleted"
    run "${ETCDCTL} --user root:root user get non_exist_user"
    check_positive "etcdserver: user name not found"
    run "${ETCDCTL} --user root:root user add root:root"
    check_positive "etcdserver: user name already exists"
    run "${ETCDCTL} --user root:root role get non_exist_role"
    check_positive "etcdserver: role name not found"
    run "${ETCDCTL} --user root:root role add root"
    check_positive "etcdserver: role name already exists"
    run "${ETCDCTL} --user root:root user revoke root r"
    check_positive "etcdserver: role is not granted to the user"
    run "${ETCDCTL} --user root:root role revoke root non_exist_key"
    check_positive "etcdserver: permission is not granted to the role"
    run "${ETCDCTL} --user root:root user delete root"
    check_positive "etcdserver: invalid auth management"
    run "${ETCDCTL} --user root:root auth disable"
    check_positive "Authentication Disabled"

    log::info "auth validation test passed"
}

lock_validation() {
    log::info "lock validation test running..."

    run "${ETCDCTL} lock mutex echo success"
    check_positive "success"

    log::info "lock validation test passed"
}

lock_rpc_validation() {
    log::info "lock rpc validation test running..."

    run "${LOCK_CLIENT} lock mutex"
    check_positive "mutex.*"
    run "${LOCK_CLIENT} unlock ${res}"
    check_positive "unlock success"

    log::info "lock rpc validation test passed"
}

# validate maintenance requests
maintenance_validation() {
    # snapshot save request only works on one endpoint
    local _ETCDCTL="docker exec -i client etcdctl --endpoints=http://172.20.0.3:2379"
    log::info "maintenance validation test running..."

    run "${_ETCDCTL} snapshot save snap.db"
    check_positive "Snapshot saved at snap.db"

    log::info "maintenance validation test passed"
}


cluster_validation() {
    log::info "cluster validation test running..."
    node4_client_url="http://${SERVERS[4]}:2379"
    updated_node4_url="http://${SERVERS[4]}:2380"
    node4_url="${updated_node4_url},http://${SERVERS[4]}:2381"
    run "${ETCDCTL} member list"
    check_positive "\s*[0-9a-z]+, started, node[0-9], 172.20.0.[0-9]:2380,172.20.0.[0-9]:2381, http://172.20.0.[0-9]:2379, false"
    run "${ETCDCTL} member add node4 --peer-urls=${node4_url} --learner=true"
    check_positive "Member\s+[a-zA-Z0-9]+ added to cluster\s+[a-zA-Z0-9]+"
    node_id=$(echo -e ${res} | awk '{print $2}')
    cluster_id=$(echo -e ${res} | awk '{print $6}')
    run "${ETCDCTL} member list"
    check_positive "\s*[0-9a-z]+, unstarted, , ${node4_url}, , true"
    run_new_member
    sleep 2
    run "${ETCDCTL} member list"
    check_positive "\s*[0-9a-z]+, started, node4, ${node4_url}, ${node4_client_url}, true"
    run "${ETCDCTL} member promote ${node_id}"
    check_positive "Member\s+${node_id} promoted in cluster\s+${cluster_id}"
    run "${ETCDCTL} member list"
    check_positive "\s*[0-9a-z]+, started, node4, ${node4_url}, ${node4_client_url}, false"
    run "${ETCDCTL} member update ${node_id} --peer-urls=${updated_node4_url}"
    check_positive "Member\s+${node_id} updated in cluster\s+${cluster_id}"
    run "${ETCDCTL} member list"
    check_positive "\s*[0-9a-z]+, started, node4, ${updated_node4_url}, ${node4_client_url}, false"
    run "${ETCDCTL} member remove ${node_id}"
    check_positive "Member\s+${node_id} removed from cluster\s+${cluster_id}"
    run "${ETCDCTL} member list"
    check_negative "\s*[0-9a-z]+, started, node4, ${updated_node4_url}, ${node4_client_url}, false"
    log::info "cluster validation test passed"
}


compact_validation
kv_validation
watch_validation
lease_validation
auth_validation
lock_validation
lock_rpc_validation
maintenance_validation
cluster_validation
