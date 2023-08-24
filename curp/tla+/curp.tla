---- MODULE curp ----

EXTENDS FiniteSets, Naturals, Sequences

(************************************************************************************)
(* Constants:                                                                       *)
(*     commands: set of records like [key |-> "key", value |-> "value"].            *)
(*               each command should be unique in the set.                          *)
(*     replicas: set of replicas.                                                   *)
(************************************************************************************)
CONSTANTS commands, replicas

(************************************************************************************)
(* Variables:                                                                       *)
(*     leader: the current leader.                                                  *)
(*     epoch: the current epoch (the number of leader changes).                     *)
(*     specPools: the spec pool of each replica.                                    *)
(*     requested: the set of requested commands (by client).                        *)
(*     committed: the set of committed commands by CURP. It also records the index  *)
(*                of the last same-key command in the synced sequence at the time.  *)
(*     unsynced: the sequence of unsynced commands.                                 *)
(*     synced: the sequence of synced commands.                                     *)
(************************************************************************************)
VARIABLES leader, epoch, specPools, requested, committed, unsynced, synced

(************************************************************************************)
(* The initial state of the system.                                                 *)
(************************************************************************************)
Init ==
    /\ leader \in replicas
    /\ epoch = 1
    /\ specPools = [r \in replicas |-> {}]
    /\ requested = {}
    /\ committed = {}
    /\ unsynced = <<>>
    /\ synced = <<>>

(************************************************************************************)
(* Helper function for converting a set to a set containing all sequences           *)
(* containing the elements of the set exactly once and no other elements.           *)
(************************************************************************************)
SetToSeqs(set) ==
    LET len == 1..Cardinality(set) IN
        {f \in [len -> set]: \A i, j \in len: i # j => f[i] # f[j]}

(************************************************************************************)
(* Helper function for getting the index of the last element in a sequence          *)
(* satisfying the predicate.                                                        *)
(************************************************************************************)
GetIdxInSeq(seq, Pred(_)) ==
    LET I == {i \in 1..Len(seq): Pred(seq[i])} IN 
        IF I # {} THEN CHOOSE i \in I: \A j \in I: j <= i ELSE 0

(************************************************************************************)
(* SuperQuorum:                                                                     *)
(*     In N = 2 * f + 1 replicas, a SuperQuorum is a set of replicas that contains  *)
(*     at least f + (f + 1) / 2 + 1 replicas.                                       *)
(*                                                                                  *)
(*     The client can consider a command as committed if and only if it receives    *)
(*     positive responses from a set of replicas larger then a SuperQuorum.         *)
(************************************************************************************)
IsSuperQuorum(S) ==
    LET f == Cardinality(replicas) \div 2
        size == f + (f + 1) \div 2 + 1
    IN Cardinality(S) >= size

(************************************************************************************)
(* RecoverQuorum:                                                                   *)
(*     In N = 2 * f + 1 replicas, a RecoverQuorum is a set of replicas that         *)
(*     contains at least (f + 1) / 2 + 1 replicas.                                  *)
(*                                                                                  *)
(*     When a replica becomes a leader, it must recover the command if and only if  *)
(*     the command is in the specPool of a set of replicas larger then a            *)
(*     RecoverQuorum.                                                               *)
(************************************************************************************)
IsRecoverQuorum(S) ==
    LET f == Cardinality(replicas) \div 2
        size == (f + 1) \div 2 + 1
    IN Cardinality(S) >= size

(************************************************************************************)
(* Quorum:                                                                          *)
(*     In N = 2 * f + 1 replicas, a Quorum is a set of replicas that contains       *)
(*     at least f + 1 replicas.                                                     *)
(*                                                                                  *)
(*     A replica must gather a quorum of replicas' specPool to recover a command.   *)
(*                                                                                  *)
(*     This defines the set containing all quorums.                                 *)
(************************************************************************************)
quorums ==
    LET f == Cardinality(replicas) \div 2
        size == f + 1
    IN {q \in SUBSET replicas: Cardinality(q) = size}

(************************************************************************************)
(* The Abstraction of the normal procedure of CURP.                                 *)
(************************************************************************************)
Request ==
    \E cmd \in commands \ requested:
        /\ requested' = requested \cup {cmd}

        \* To simulate the unreliability of the network,
        \* only a subset of replicas could receive the request.
        /\ \E received \in SUBSET replicas:
            \* The set of replicas that got no conflict in the spec pool.
            /\ LET acceptedReplicas ==
                {r \in received:
                    \A specCmd \in specPools[r]:
                        specCmd.key # cmd.key}
               IN
                \* Update the specPool.
                /\ specPools' = [r \in replicas |->
                    IF r \in acceptedReplicas
                    THEN specPools[r] \cup {cmd}
                    ELSE specPools[r]]

                \* If there is at least a superquorum set of replicas that accepted
                \* the request, and the leader can execute the command,
                \* the request is committed.
                /\ LET CompareKey(elem) == elem.key = cmd.key IN
                    IF
                        /\ IsSuperQuorum(acceptedReplicas)
                        /\ leader \in acceptedReplicas
                        /\ GetIdxInSeq(unsynced, CompareKey) = 0
                    THEN
                        \* The previous state of the key is also recorded.
                        \* This is used to check the correctness of the property.
                        LET prevIdx == GetIdxInSeq(synced, CompareKey) IN
                            committed' = committed \cup {[
                                cmd |-> cmd,
                                prevIdx |-> prevIdx]}
                    ELSE committed' = committed

            \* No matter if the request is committed or not,
            \* as long as the leader is in the received set,
            \* the command should be synced afterward.
            /\ IF leader \in received
               THEN unsynced' = Append(unsynced, cmd)
               ELSE unsynced' = unsynced

        /\ UNCHANGED <<leader, epoch, synced>>

(************************************************************************************)
(* Syncing a command using the back-end protocol like Raft. The implementation      *)
(* details of the back-end protocol are omitted.                                    *)
(************************************************************************************)
Sync ==
    /\ unsynced # <<>>
    /\ specPools' = [r \in replicas |-> specPools[r] \ {Head(unsynced)}]
    /\ synced' = Append(synced, Head(unsynced))
    /\ unsynced' = Tail(unsynced)
    /\ UNCHANGED <<leader, epoch, requested, committed>>

(************************************************************************************)
(* Leader Change Action                                                             *)
(*                                                                                  *)
(* The new leader should gather at least a quorum of replicas' specPool to recover  *)
(* the commands.                                                                    *)
(*                                                                                  *)
(* Commands existed in the specPool of a RecoverQuorum of replicas need to be       *)
(* recovered.                                                                       *)
(************************************************************************************)
LeaderChange ==
    \E newLeader \in (replicas \ {leader}):
        /\ leader' = newLeader
        /\ epoch' = epoch + 1
        /\ \E q \in quorums:
            LET specCmds == UNION {specPools[r] : r \in q}
                newSpecPool == {cmd \in specCmds: IsRecoverQuorum({r \in replicas: cmd \in specPools[r]})}
            IN
                /\ specPools' = [specPools EXCEPT ![newLeader] = newSpecPool]
                /\ unsynced' \in SetToSeqs(newSpecPool)
        /\ UNCHANGED <<requested, committed, synced>>

Next ==
    \/ Request
    \/ Sync
    \/ LeaderChange

Spec == Init /\ [][Next]_<<leader, epoch, specPools, requested, committed, unsynced, synced>>

(************************************************************************************)
(* Type Check                                                                       *)
(************************************************************************************)
TypeOK ==
    /\ leader \in replicas
    /\ epoch \in Nat
    /\ \A r \in replicas: specPools[r] \subseteq commands
    /\ requested \subseteq commands
    /\ \A committedCmd \in committed:
        /\ committedCmd.cmd \in commands
        /\ committedCmd.prevIdx \in 0..Len(synced)
    /\ synced \in {SetToSeqs(s): s \in SUBSET commands}
    /\ unsynced \in {SetToSeqs(s): s \in SUBSET commands}

(************************************************************************************)
(* Stability Property                                                               *)
(*                                                                                  *)
(* This is the key property of CURP. There are two parts of the property.           *)
(*                                                                                  *)
(* 1. If a command is committed by CURP, command will eventually be synced by the   *)
(*    back-end protocol.                                                            *)
(*                                                                                  *)
(* 2. If a command is committed by CURP, when the command is synced be the back-end *)
(*    protocol, there will never be a command with the same key between the command *)
(*    and the recorded previous same-key command in the synced sequence.            *)
(************************************************************************************)
Stability ==
    \A committedCmd \in committed:
        LET CompareExact(elem) == elem = committedCmd.cmd
            syncedIdx == GetIdxInSeq(synced, CompareExact)
        IN
            /\ syncedIdx # 0
            /\ \A j \in (committedCmd.prevIdx + 1)..(syncedIdx - 1):
                synced[j].key # committedCmd.cmd.key

THEOREM Spec => []TypeOK /\ <>Stability
====
