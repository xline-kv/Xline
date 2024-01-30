# TLA+ specification of the CURP consensus algorithm

This directory contains the TLA+ specification of the modified CURP consensus algorithm used in Xline.

The original CURP Replication Protocol is described in the paper [Exploiting Commutativity For
Practical Fast Replication](https://www.usenix.org/system/files/nsdi19-park.pdf). We extended CURP into a consensus protocol front-end and paired it with the [Raft Consensus Algorithm](https://raft.github.io/).

Since Raft is already proven to be correct, we only need to prove the correctness of the CURP consensus protocol we implemented. The details of syncing commands and electing leaders are omitted in the specification.

The specification is written in the TLA+ language and can be verified using the TLC model checker.
