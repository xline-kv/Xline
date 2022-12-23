# Usage

1. Download binary from [release]() page.
2. Use the following command to start cluster:
    ```bash
    # Run in 3 terminals. If you want more logs, add `RUST_LOG=debug` before the command.

    ./xline --name node1 --cluster-peers 127.0.0.1:2380 127.0.0.1:2381 --self-ip-port 127.0.0.1:2379 --leader-ip-port 127.0.0.1:2379 

    ./xline --name node2 --cluster-peers 127.0.0.1:2379 127.0.0.1:2381 --self-ip-port 127.0.0.1:2380 --leader-ip-port 127.0.0.1:2379 

    ./xline --name node3 --cluster-peers 127.0.0.1:2379 127.0.0.1:2380 --self-ip-port 127.0.0.1:2381 --leader-ip-port 127.0.0.1:2379 
    ```
3. Download or build `etcdctl` from [etcd](https://github.com/etcd-io/etcd) project.
4. Use `etcdctl` to operate the cluster:
    ```bash
    etcdctl --endpoints=http://127.0.0.1:2379 put foo bar

    etcdctl --endpoints=http://127.0.0.1:2379 get foo
    ```