## Xline metrics

Many metrics are similar to those in [etcd](https://etcd.io/docs/v3.5/metrics/).

### CURP Server

1. `leader_changes`: Counter
The number of leader changes seen.

2. `learner_promote_failed`: Counter
The total number of failed learner promotions (likely learner not ready) while this member is leader.

3. `learner_promote_succeed`: Counter
The total number of successful learner promotions while this member is leader.

4. `heartbeat_send_failures`: Counter
The total number of leader heartbeat send failures (likely overloaded from slow disk).

5. `apply_snapshot_in_progress`: UpDownCounter
not equals to 0 if the server is applying the incoming snapshot. 0 if none.

6. `proposals_committed`: ObservableGauge
The total number of consensus proposals committed.

7. `proposals_failed`: Counter
The total number of failed proposals seen.

8. `proposals_applied`: ObservableGauge
The total number of consensus proposals applied.

9. `proposals_pending`: ObservableGauge
The current number of pending proposals to commit.

10.  `snapshot_install_total_duration_seconds`: Histogram
The total latency distributions of save called by install_snapshot.

11.  `client_id_revokes`: Counter
The total number of client id revokes times.

12.  `has_leader`: ObservableGauge
Whether or not a leader exists. 1 is existence, 0 is not.

13.  `is_leader`: ObservableGauge
Whether or not this member is a leader. 1 if is, 0 otherwise.

14.  `is_learner`: ObservableGauge
Whether or not this member is a learner. 1 if is, 0 otherwise.

15.  `server_id`: ObservableGauge
Server or member ID in hexadecimal format. 1 for 'server_id' label with the current ID.

16.  `sp_cnt`: ObservableGauge
The speculative pool size of this server.

17.  `online_clients`: ObservableGauge
The online client IDs count of this server if it is the leader.

### CURP Client

1. `client_retry_count`: Counter
The total number of retries when the client propose to the cluster.

2. `client_fast_path_count`: Counter
The total number of fast path when the client propose to the cluster.

3. `client_slow_path_count`: Counter
The total number of slow path when the client propose to the cluster.

4. `client_fast_path_fallback_slow_path_count`: Counter
The total number of fast path fallbacks into slow path when the client propose to the cluster.

### Xline

1. `slow_read_indexes`: Counter
The total number of pending read indexes not in sync with leader's or timed out read index requests.

2. `read_indexes_failed`: Counter
The total number of failed read indexes seen.

3. `lease_expired`: Counter
The total number of expired leases.

4. `fd_used`: ObservableGauge
The number of used file descriptors.

5. `fd_limit`: ObservableGauge
The file descriptor limit.

6. `current_version`: ObservableGauge
Which version is running. 1 for 'server_version' label with the current version.

7. `current_rust_version`: ObservableGauge
Which Rust version the server is running with. 1 for 'server_rust_version' label with the current version.


### Engine

1. `engine_apply_snapshot_duration_seconds`: Histogram
The backend engine apply snapshot duration in seconds.

2. `engine_write_batch_duration_seconds`: Histogram
The backend engine write batch engine, `batch_size` refer to the size and `sync` if sync option is on.

### Network

1. `peer_sent_bytes`: Counter
The total number of bytes sent to peers.

2. `peer_sent_failures`: Counter
The total number of send failures to peers.

3. `peer_round_trip_time_seconds`: Histogram
The round-trip-time histogram between peers.
