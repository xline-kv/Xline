# Usage

## Configurations

The Xline configuration file is written in toml format and the default path is /etc/xline_server.conf. If you need to change the path of the configuration file, you can set it via the environment variable XLINE_SERVER_CONFIG.

The configuration file has four sections, as follows:

1. cluster section: contains information about curp cluster, including basic information, cluster member configuration, curp server timeout settings (optional), curp client timeout settings (optional).
2. log section: contains the Xline log-related configuration, where path is required, rotation (optional, default value is 'daily'), level (optional, default value is 'info')
3. trace section: contains the jaeger's trace mode (online or offline), trace level and the log directory in offline mode
4. auth section: contains the address of the key pair required for authentication

A minimum config file looks like:

```toml
[cluster]
name = 'node1'                  # server identity
is_leader = true                # leader flag which indicates that the current server
                                # is a leader or not when booting up a cluster

[cluster.members]
node1 = '127.0.0.1:2379'        # the server name and its address
node2 = '127.0.0.1:2380'
node3 = '127.0.0.1:2381'

[log]
path = '/var/log/xline'

[trace]
jaeger_online = false           # jaeger tracing online pattern
jaeger_offline = false          # jaeger tracing offline pattern
jaeger_output_dir = 'var/log/xline/jaeger_jsons'
jaeger_level = 'info'           # tracing log level

[auth]
auth_public_key = '/etc/xline/public_key.pem'
auth_private_key = '/etc/xline/private_key.pem'
```

For tuning and development purpose, the cluster section provides two subsections, curp_cfg, and client_config, with the following definitions and default values.

```toml
[cluster.curp_config]
heartbeat_interval = '300ms'    # the curp heartbeat(tick) interval of which default value is 300ms
wait_synced_timeout = '5s'      # server wait synced timeout
rpc_timeout = '50ms'            # the rpc timeout of which default value is 50ms
retry_timeout = '50ms'          # the rpc retry interval of which default value is 50ms
follower_timeout_ticks = 5      # if a follower cannot receive heartbeats from a leader during
                                # after `follower_timeout_ticks` ticks, then it will issue an
                                # election. Its default value is 5.
candidate_timeout_ticks = 2     # if a candidate cannot win an election, it will retry election
                                # after `candidate_timeout_ticks` ticks. Its default value is 2


[cluster.client_config]
propose_timeout = '1s'          # client propose timeout
wait_synced_timeout = '2s'      # client wait synced timeout
retry_timeout = '50ms'          # the rpc retry interval, of which the default is 50ms
```

## Boot up an Xline cluster

1. Download binary from [release]() page.
2. Use the following command to start cluster:

    ```bash
    # Run in 3 terminals. If you want more logs, add `RUST_LOG=debug` before the command.

    ./xline --name node1 --members node1=127.0.0.1:2379,node2=127.0.0.1:2380,node3=127.0.0.1:2381 --is-leader

    ./xline --name node2 --members node1=127.0.0.1:2379,node2=127.0.0.1:2380,node3=127.0.0.1:2381

    ./xline --name node3 --members node1=127.0.0.1:2379,node2=127.0.0.1:2380,node3=127.0.0.1:2381
    ```

3. Download or build `etcdctl` from [etcd](https://github.com/etcd-io/etcd) project.
4. Use `etcdctl` to operate the cluster:

    ```bash
    etcdctl --endpoints=http://127.0.0.1:2379 put foo bar

    etcdctl --endpoints=http://127.0.0.1:2379 get foo
    ```
