---- MODULE curp ----

EXTENDS FiniteSets, Naturals, Sequences

CONSTANTS COMMANDS, \* set of records like [key: STRING, value: STRING]
          REPLICAS  \* set of replicas

ASSUME IsFiniteSet(REPLICAS)

VARIABLES msgPool,        \* messages in transit
          leader,         \* current leader
          epoch,          \* current epoch
          specPools,      \* specPool of each replica
          unsyncedCmds,   \* unsynced commands (backend)
          syncedCmds,     \* synced commands (backend)
          requestedCmds,  \* client requested commands
          committedCmds   \* commands that client believes are stable in the RSM

vars == <<msgPool,
          leader,
          epoch,
          specPools,
          syncedCmds,
          unsyncedCmds,
          requestedCmds,
          committedCmds>>

(************************************************************************************)
(* The initial state of the system.                                                 *)
(************************************************************************************)
Init ==
    /\ msgPool = {}
    /\ leader \in REPLICAS
    /\ epoch = 1
    /\ specPools = [r \in REPLICAS |-> {}]
    /\ unsyncedCmds = <<>>
    /\ syncedCmds = <<>>
    /\ requestedCmds = {}
    /\ committedCmds = {}

(************************************************************************************)
(* Helper function for converting a set to a sequence.                              *)
(************************************************************************************)
SetToSeqs(set) ==
    LET len == 1..Cardinality(set)
        seqs == {f \in [len -> set]: \A i, j \in len: i # j => f[i] # f[j]}
    IN seqs

(************************************************************************************)
(* Helper function for checking if a target exists in a sequence.                   *)
(************************************************************************************)
ExistInSeq(seq, target) ==
    \E i \in 1..Len(seq): seq[i] = target

(************************************************************************************)
(* SuperQuorums:                                                                    *)
(*     In N = 2 * f + 1 replicas, a SuperQuorum is a set of replicas that contains  *)
(*     at least f + (f + 1) / 2 + 1 replicas (including the leader).                *)
(*                                                                                  *)
(*     The client can consider a command as committed if and only if it receives    *)
(*     positive responses from a SuperQuorum.                                       *)
(*                                                                                  *)
(*     This defines the set consisting of all SuperQuorums.                         *)
(************************************************************************************)
SuperQuorums ==
    {q \in SUBSET REPLICAS:
        /\ Cardinality(q) >= (Cardinality(REPLICAS) * 3) \div 4 + 1
        /\ leader \in q}

(************************************************************************************)
(* LeastQuorums:                                                                    *)
(*     In N = 2 * f + 1 replicas, a LeastQuorum is a set of replicas that contains  *)
(*     at least (f + 1) / 2 + 1 replicas.                                           *)
(*                                                                                  *)
(*     When a replica becomes a leader, it must recover the command if and only if  *)
(*     the command is a LeastQuorum of replicas' specPool.                          *)
(*                                                                                  *)
(*     This defines the set consisting of all LeastQuorums.                         *)
(************************************************************************************)
LeastQuorums ==
    {q \in SUBSET REPLICAS:
        Cardinality(q) >= Cardinality(REPLICAS) \div 4 + 1}

(************************************************************************************)
(* RecoveryQuorums:                                                                 *)
(*     In N = 2 * f + 1 replicas, a RecoveryQuorum is a set of replicas that        *)
(*     contains at least f + 1 replicas.                                            *)
(*                                                                                  *)
(*     A replica must gather a RecoveryQuorum of replicas' specPool to recover the  *)
(*     commands that need to be recovered.                                          *)
(*                                                                                  *)
(*     This defines the set consisting of all RecoveryQuorums.                      *)
(************************************************************************************)
RecoveryQuorums ==
    {q \in SUBSET REPLICAS:
        Cardinality(q) >= Cardinality(REPLICAS) \div 2 + 1}

(************************************************************************************)
(* Client sends a request to all replicas.                                          *)
(************************************************************************************)
ClientSendRequest(cmd) ==
    /\ requestedCmds' = requestedCmds \cup {cmd}
    /\ msgPool' = msgPool \cup
        [type: {"request"},
         cmd: {cmd},
         dst: REPLICAS]
    /\ UNCHANGED <<leader,
                   epoch,
                   specPools,
                   syncedCmds,
                   unsyncedCmds,
                   committedCmds>>

(************************************************************************************)
(* Replica receives a request from the client.                                      *)
(*                                                                                  *)
(* If there is no conflict command (command on the same key) in the specPool, the   *)
(* replica adds the command to its specPool.                                        *)
(*                                                                                  *)
(* The leader will always add the command to unsyncedCmds.                          *)
(************************************************************************************)
ReplicaReceiveRequest(r, msg) ==
    IF ~ExistInSeq(syncedCmds, msg.cmd) THEN
        LET conflict == (\E cmd \in specPools[r]: cmd.key = msg.cmd.key) IN
            /\ specPools' = [specPools EXCEPT ![r] =
                IF conflict THEN @ ELSE @ \cup {msg.cmd}]
            /\ unsyncedCmds' =
                IF r = leader THEN Append(unsyncedCmds, msg.cmd)
                ELSE unsyncedCmds
            /\ msgPool' = (msgPool \ {msg}) \cup
                {[type |-> "response",
                  cmd |-> msg.cmd,
                  ok |-> ~conflict,
                  src |-> r]}
            /\ UNCHANGED <<leader,
                           epoch,
                           syncedCmds,
                           requestedCmds,
                           committedCmds>>
    ELSE \* If the command is already synced, the replica does nothing.
        /\ msgPool' = msgPool \ {msg}
        /\ UNCHANGED <<leader,
                       epoch,
                       specPools,
                       syncedCmds,
                       unsyncedCmds,
                       requestedCmds,
                       committedCmds>>

(************************************************************************************)
(* Client receives a response from a replica.                                       *)
(*                                                                                  *)
(* If the client got positive responses from a SuperQuorum, the client considers    *)
(* the command as committed.                                                        *)
(*                                                                                  *)
(* If the client got negative responses from a LeastQuorum, the command can never   *)
(* accepted by a SuperQuorum. Thus the client stops waiting for it.                 *)
(************************************************************************************)
ClientReceiveResponse(msg) ==
    LET sameCmdResp ==
        {resp \in msgPool: resp.type = "response" /\ resp.cmd = msg.cmd}
    IN
        \/ /\ {m.src: m \in {resp \in sameCmdResp: resp.ok}} \in SuperQuorums
           /\ committedCmds' = committedCmds \cup {msg.cmd}
           /\ msgPool' = msgPool \ sameCmdResp
           /\ UNCHANGED <<leader,
                          epoch,
                          specPools,
                          syncedCmds,
                          unsyncedCmds,
                          requestedCmds>>
        \/ /\ {m.src: m \in {resp \in sameCmdResp: ~resp.ok}} \in LeastQuorums
           /\ msgPool' = msgPool \ sameCmdResp
           /\ UNCHANGED <<leader,
                          epoch,
                          specPools,
                          syncedCmds,
                          unsyncedCmds,
                          requestedCmds,
                          committedCmds>>

(************************************************************************************)
(* Client Actions                                                                   *)
(************************************************************************************)
ClientAction ==
    \/ \E cmd \in (COMMANDS \ requestedCmds): ClientSendRequest(cmd)
    \/ \E msg \in msgPool: /\ msg.type = "response"
                           /\ ClientReceiveResponse(msg)

(************************************************************************************)
(* Replica Actions                                                                  *)
(************************************************************************************)
ReplicaAction ==
    \E msg \in msgPool:
        /\ msg.type = "request"
        /\ ReplicaReceiveRequest(msg.dst, msg)

(************************************************************************************)
(* Syncing an `unsyncedCmd`                                                         *)
(************************************************************************************)
SyncAction ==
    /\ unsyncedCmds # <<>>
    /\ specPools' = [r \in REPLICAS |-> specPools[r] \ {Head(unsyncedCmds)}]
    /\ syncedCmds' = Append(syncedCmds, Head(unsyncedCmds))
    /\ unsyncedCmds' = Tail(unsyncedCmds)
    /\ UNCHANGED <<msgPool,
                   leader,
                   epoch,
                   requestedCmds,
                   committedCmds>>

(************************************************************************************)
(* Leader Change Action                                                             *)
(*                                                                                  *)
(* The new leader should gather at least a RecoveryQuorum of replicas' specPool to  *)
(* recover the commands.                                                            *)
(*                                                                                  *)
(* Commands occurring in the specPool of a LeastQuorum of replicas need to be       *)
(* recovered.                                                                       *)
(************************************************************************************)
LeaderChangeAction ==
    \E newLeader \in REPLICAS:
        /\ newLeader # leader
        /\ \E recoveryQuorum \in RecoveryQuorums:
            LET specPoolCmds == UNION {({
                    [cmd |-> cmd, r |-> r]: cmd \in specPools[r]
                }): r \in recoveryQuorum}
                filteredSpecPoolCmds == {c1 \in specPoolCmds: {
                    c.r: c \in {c2 \in specPoolCmds: c1.cmd = c2.cmd}
                } \in LeastQuorums}
                newSpecPool == {c.cmd: c \in filteredSpecPoolCmds}
            IN
                /\ specPools' = [specPools EXCEPT ![newLeader] = newSpecPool]
                /\ LET newUnsyncedCmds ==
                        CHOOSE s \in SetToSeqs(newSpecPool): TRUE
                   IN unsyncedCmds' = unsyncedCmds \o newUnsyncedCmds
                /\ leader' = newLeader
                /\ epoch' = epoch + 1
                /\ UNCHANGED <<msgPool,
                               syncedCmds,
                               requestedCmds,
                               committedCmds>>

Next ==
    \/ LeaderChangeAction
    \/ ClientAction
    \/ ReplicaAction
    \/ SyncAction

Spec == Init /\ [][Next]_vars /\ WF_vars(Next)

(************************************************************************************)
(* Type Check                                                                       *)
(************************************************************************************)
TypeOK ==
    /\ msgPool \subseteq
        [type: {"request"},
         cmd: COMMANDS,
         dst: REPLICAS] \cup
        [type: "response",
         cmd: COMMANDS,
         ok: {TRUE, FALSE},
         src: REPLICAS]
    /\ leader \in REPLICAS
    /\ epoch \in Nat
    /\ \A r \in REPLICAS:
        LET specPool == specPools[r] IN
            /\ specPool \subseteq COMMANDS
            /\ \A cmd1, cmd2 \in specPool:
                cmd1.key # cmd2.key
    /\ syncedCmds \in {SetToSeqs(s): s \in SUBSET COMMANDS}
    /\ unsyncedCmds \in {SetToSeqs(s): s \in SUBSET COMMANDS}
    /\ requestedCmds \subseteq COMMANDS
    /\ committedCmds \subseteq COMMANDS

(************************************************************************************)
(* Stability Property                                                               *)
(*                                                                                  *)
(* If the client considers a command has been committed, the command must           *)
(* eventually be synced.                                                            *)
(************************************************************************************)
Stability ==
    \A cmd \in COMMANDS:
        cmd \in committedCmds ~> cmd \in {syncedCmds[i]: i \in DOMAIN syncedCmds}

THEOREM Spec => ([]TypeOK) /\ Stability
====