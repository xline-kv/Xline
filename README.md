# Xline

[![Join the chat at https://gitter.im/datenlord/Xline](https://badges.gitter.im/datenlord/Xline.svg)](https://gitter.im/datenlord/Xline?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

[![Apache 2.0 licensed][apache-badge]][apache-url]
[![Build Status][actions-badge]][actions-url]

[apache-badge]: https://img.shields.io/badge/license-Apache--2.0-brightgreen
[apache-url]: https://github.com/datenlord/Xline/blob/master/LICENSE
[actions-badge]: https://github.com/datenlord/xline/actions/workflows/ci.yml/badge.svg?branch=master
[actions-url]: https://github.com/datenlord/xline/actions
[![codecov](https://codecov.io/gh/datenlord/xline/branch/master/graph/badge.svg)](https://codecov.io/gh/datenlord/xline)

`Xline` is a geo-distributed KV store for metadata management. It provides the
following features:

- Etcd compatible API.
- Geo-distributed friendly deployment.
- Compatible with K8s.

## Motivation

With the wide adoption of cloud computing, multi-cloud has become the mainstream IT architecture for enterprise customers.
Multi-cloud (or similarly multi-datacenter), however, obstacles data access across different cloud (or data center) providers to some extent.
Further, data isolation and data fragmentation resulting from cloud barriers have become
impediments to business growth. The biggest challenge of multi-datacenter
architecture is how to maintain **strong data consistency** and ensure **high
performance** in the race condition of multi-datacenter scenario.
Traditional single datacenter solutions cannot meet the
availability, performance, and consistency requirements of multi-data center
scenarios. This project targets the multi-datacenter scenario, aiming to
realize a high-performance multi-cloud metadata management solution, which is
critical for businesses with geo-distributed and multi-active
deployment requirements.

## Innovation

Cross-datacenter network latency is the most important factor that impacts the
performance of geo-distributed systems, especially when a consensus protocol is
used. We know consensus protocols are popular to use to achieve high
availability. For instance, Etcd uses the [Raft](https://raft.github.io/)
protocol, which is quite popular in recently developed systems.

Although Raft is stable and easy to implement, it takes 2 RTTs to complete a
consensus request from the view of a client. One RTT takes place between the
client and the leader server, and the leader server takes another RTT to
broadcast the message to the follower leaders. In a geo-distributed environment,
an RTT is quite long, varying from tens of milliseconds to hundreds of
milliseconds, so 2 RTTs are too long in such cases.

We adopt a new consensus protocol named
[CURP](https://www.usenix.org/system/files/nsdi19-park.pdf) to resolve the above
issue. Please refer to the paper for a detailed description. The main benefit of
the protocol is reducing 1 RTT when contention is not too high. As far as we
know, Xline is the first product to use CURP.

## Performance Comparison

We compared Xline with Etcd in a simulated multi-cluster environment. The
details of the deployment is shown below.

![test deployment](./img/xline_test_deployment.jpg)

We compared the performance with two different workloads. One is 1 key case, the
other is 100K key space case. Here's the test result.

![1 key test](./img/1-key-perf.png)

![100k_key_test](./img/100k-key-perf.png)

It's easy to tell Xline has a better performance than Etcd in a geo-distributed
multi-cluster environment.

## Quick Start

Please refer to the Contribute Guide for the quick start guide.

## [Contribute Guide](./CONTRIBUTING.md)

## [Code of Conduct](./CODE_OF_CONDUCT.md)

## Roadmap

- v0.1 ~ v0.2
  - Support all major ETCD APIs
  - Pass validation tests
- v0.3 ~ v0.5
  - Enable persistent storage
  - Enable snapshot
  - Enable cluster membership change
- v1.0 ~
  - Enable chaos engineering to validate the system's stability
  - Integration with other CNCF components
