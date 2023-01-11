# Usage

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
