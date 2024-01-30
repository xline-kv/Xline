------------------------------------ MODULE curp ------------------------------------

EXTENDS FiniteSets, Naturals, Sequences

(************************************************************************************)
(* Constants:                                                                       *)
(*     `commands`: set of records like [key |-> "key", value |-> "value"].          *)
(*     `replicas`: set of replicas.                                                 *)
(************************************************************************************)
CONSTANTS commands, replicas

ASSUME IsFiniteSet(replicas)

(************************************************************************************)
(* Variables:                                                                       *)
(*     `leader`: records the leader of each epoch.                                  *)
(*     `epoch`: current epoch (the number of leader changes).                       *)
(*     `proposedCmds`: the set of proposed commands.                                *)
(*     `proposeRequests`: the set of propose could be received by each replica.     *)
(*     `proposeResponses`: the set of responses in each epoch to each proposed      *)
(*                         command.                                                 *)
(*     `specPools`: the speculative pool of each replica.                           *)
(*     `uncommittedCmds`: the sequence of back-end protocol uncommitted commands.   *)
(*     `committedCmds`: the sequence of back-end protocol committed commands.       *)
(*     `commitMsgs`: the set of commit messages could be received by each replica.  *)
(*     `specExecPrevCmd`: the index of the last same-key command in the committed   *)
(*                        sequence at the time the leader responds to the proposal. *)
(************************************************************************************)
VARIABLES leader, epoch, proposedCmds, proposeRequests,
          proposeResponses, specPools, uncommittedCmds,
          committedCmds, commitMsgs, specExecPrevCmd

(************************************************************************************)
(* The epoch space.                                                                 *)
(************************************************************************************)
epoches == Nat

(************************************************************************************)
(* Special `noLeader` value, for future epoches.                                    *)
(************************************************************************************)
noLeader == CHOOSE r: r \notin replicas

(************************************************************************************)
(* In N = 2 * f + 1 replicas:                                                       *)
(*     `quorum`: a set of replicas that contains at least f + 1 replicas.           *)
(*     `superQuorum`: a set of replicas that contains at least f + (f + 1) / 2 + 1  *)
(*                    replicas.                                                     *)
(*     `recoverQuorum`: a set of replicas that contains at least (f + 1) / 2 + 1    *)
(*                      replicas.                                                   *)
(************************************************************************************)
quorums ==
    LET f == Cardinality(replicas) \div 2
        size == f + 1
    IN {q \in SUBSET replicas: Cardinality(q) >= size}

superQuorums ==
    LET f == Cardinality(replicas) \div 2
        size == f + (f + 1) \div 2 + 1
    IN {q \in SUBSET replicas: Cardinality(q) >= size}

recoverQuorums ==
    LET f == Cardinality(replicas) \div 2
        size == (f + 1) \div 2 + 1
    IN {q \in SUBSET replicas: Cardinality(q) >= size}

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
(* Propose a command.                                                               *)
(* This is done by the client sending a proposeRequest to all replicas.             *)
(************************************************************************************)
Propose(cmd) ==
    /\ proposedCmds' = proposedCmds \cup {cmd}
    /\ proposeRequests' =
        [r \in replicas |-> proposeRequests[r] \cup {cmd}]
    /\ UNCHANGED <<leader, epoch, specPools, proposeResponses,
                   uncommittedCmds, committedCmds, commitMsgs,
                   specExecPrevCmd>>

(************************************************************************************)
(* How the leader process a proposeRequest.                                         *)
(************************************************************************************)
ProcessProposeLeader(r, cmd) ==
    LET specPoolHasConflict ==
            \E specCmd \in specPools[r]: specCmd.key = cmd.key
        uncommittedCmdsHasConflict ==
            GetIdxInSeq(uncommittedCmds, LAMBDA e: e.key = cmd.key) # 0
    IN
        /\ proposeRequests' =
            [proposeRequests EXCEPT ![r] = @ \ {cmd}]
        /\ specPools' =
            [specPools EXCEPT ![r] =
                IF ~specPoolHasConflict THEN @ \cup {cmd} ELSE @]
        /\ uncommittedCmds' = Append(uncommittedCmds, cmd)
        /\ proposeResponses' =
            [proposeResponses EXCEPT ![cmd][epoch] =
                IF ~specPoolHasConflict /\ ~uncommittedCmdsHasConflict
                THEN @ \cup {r}
                ELSE @]
        /\ specExecPrevCmd' =
            [specExecPrevCmd EXCEPT ![cmd] =
                IF ~specPoolHasConflict /\ ~uncommittedCmdsHasConflict
                THEN GetIdxInSeq(committedCmds, LAMBDA e: e.key = cmd.key)
                ELSE @]
        /\ UNCHANGED <<leader, epoch, proposedCmds, committedCmds,
                       commitMsgs>>

(************************************************************************************)
(* How a non-leader replica process a proposeRequest.                               *)
(************************************************************************************)
ProcessProposeNonLeader(r, cmd) ==
    LET specPoolHasConflict ==
            \E specCmd \in specPools[r]: specCmd.key = cmd.key
    IN
        /\ proposeRequests' =
            [proposeRequests EXCEPT ![r] = @ \ {cmd}]
        /\ specPools' =
            [specPools EXCEPT ![r] =
                IF ~specPoolHasConflict THEN @ \cup {cmd} ELSE @]
        /\ proposeResponses' =
            [proposeResponses EXCEPT ![cmd][epoch] =
                IF ~specPoolHasConflict THEN @ \cup {r} ELSE @]
        /\ UNCHANGED <<leader, epoch, proposedCmds, uncommittedCmds,
                       committedCmds, commitMsgs, specExecPrevCmd>>

(************************************************************************************)
(* Syncing a command using the back-end protocol (Raft). The implementation details *)
(* are omitted.                                                                     *)
(*                                                                                  *)
(* A replica may not be able to receive the commit message at the exact time the    *)
(* the leader sends it.                                                             *)
(************************************************************************************)
Commit ==
    /\ committedCmds' = Append(committedCmds, Head(uncommittedCmds))
    /\ commitMsgs' =
        [r \in replicas |-> commitMsgs[r] \cup {Head(uncommittedCmds)}]
    /\ uncommittedCmds' = Tail(uncommittedCmds)
    /\ UNCHANGED <<leader, epoch, specPools, proposedCmds, proposeRequests,
                   proposeResponses, specExecPrevCmd>>

(************************************************************************************)
(* How a replica process a commit message.                                          *)
(************************************************************************************)
ProcessCommitMsg(r, cmd) ==
    /\ commitMsgs' =
        [commitMsgs EXCEPT ![r] = @ \ {cmd}]
    /\ specPools' = [specPools EXCEPT ![r] = @ \ {cmd}]
    /\ UNCHANGED <<leader, epoch, proposedCmds, proposeRequests,
                   proposeResponses, uncommittedCmds, committedCmds,
                   specExecPrevCmd>>

(************************************************************************************)
(* Leader Change Action                                                             *)
(*                                                                                  *)
(* The new leader should gather at least a quorum of replicas' specPool to recover  *)
(* the commands.                                                                    *)
(*                                                                                  *)
(* Commands existed in the specPool of a RecoverQuorum of replicas need to be       *)
(* recovered.                                                                       *)
(************************************************************************************)
LeaderChange(l) ==
    /\ leader' = [e \in epoches |-> IF e = epoch + 1 THEN l ELSE leader[e]]
    /\ epoch' = epoch + 1
    /\ \E q \in quorums:
        LET specCmds == UNION {specPools[r] : r \in q}
            newSpecPool ==
                {cmd \in specCmds:
                    {r \in q: cmd \in specPools[r]} \in recoverQuorums}
        IN
            /\ specPools' = [specPools EXCEPT ![l] = newSpecPool]
            /\ uncommittedCmds' \in SetToSeqs(newSpecPool)
    /\ UNCHANGED <<proposedCmds, proposeRequests, proposeResponses,
                   committedCmds, commitMsgs, specExecPrevCmd>>

(************************************************************************************)
(* The initial state of the system.                                                 *)
(************************************************************************************)
Init ==
    \E r \in replicas:
        LET initEpoch == 1 initLeader == r IN
            /\ leader = [e \in epoches |->
                IF e = initEpoch THEN initLeader ELSE noLeader]
            /\ epoch = initEpoch
            /\ proposedCmds = {}
            /\ proposeRequests = [replica \in replicas |-> {}]
            /\ proposeResponses =
                [cmd \in commands |-> [e \in epoches |-> {}]]
            /\ specPools = [replica \in replicas |-> {}]
            /\ uncommittedCmds = <<>>
            /\ committedCmds = <<>>
            /\ commitMsgs = [replica \in replicas |-> {}]
            /\ specExecPrevCmd = [cmd \in commands |-> 0]

Next ==
    \/ \E cmd \in (commands \ proposedCmds): Propose(cmd)
    \/ \E r \in replicas: \E cmd \in proposeRequests[r]:
        IF leader[epoch] = r
        THEN ProcessProposeLeader(r, cmd)
        ELSE ProcessProposeNonLeader(r, cmd)
    \/ uncommittedCmds # <<>> /\ Commit
    \/ \E r \in replicas: \E cmd \in commitMsgs[r]: ProcessCommitMsg(r, cmd)
    \/ \E l \in replicas: LeaderChange(l)

Spec == Init /\ [][Next]_<<leader, epoch, specPools, proposedCmds,
                           proposeRequests, proposeResponses,
                           uncommittedCmds, committedCmds, commitMsgs,
                           specExecPrevCmd>>

(************************************************************************************)
(* Type Invariants                                                                  *)
(************************************************************************************)
TypeOK ==
    /\ leader \in [epoches -> (replicas \cup {noLeader})]
    /\ epoch \in epoches
    /\ proposedCmds \subseteq commands
    /\ proposeRequests \in [replicas -> SUBSET commands]
    /\ proposeResponses \in [commands -> [epoches -> SUBSET replicas]]
    /\ specPools \in [replicas -> SUBSET commands]
    /\ uncommittedCmds \in UNION {SetToSeqs(s): s \in SUBSET commands}
    /\ committedCmds \in UNION {SetToSeqs(s): s \in SUBSET commands}
    /\ commitMsgs \in [replicas -> SUBSET commands]
    /\ specExecPrevCmd \in [commands -> 0..Cardinality(commands)]

(************************************************************************************)
(* Stability Property                                                               *)
(*                                                                                  *)
(* This is the key property of CURP:                                                *)
(*                                                                                  *)
(* 1. If a command is committed by CURP, command will eventually be synced by the   *)
(*    back-end protocol.                                                            *)
(*                                                                                  *)
(* 2. If a command is committed by CURP, when the command is synced be the back-end *)
(*    protocol, there will never be a command with the same key between the command *)
(*    and the recorded previous same-key command in the synced sequence.            *)
(************************************************************************************)
Stability ==
    \A cmd \in commands: \A e \in epoches:
        (/\ leader[e] \in proposeResponses[cmd][e]
         /\ proposeResponses[cmd][e] \in superQuorums) =>
            LET idx == GetIdxInSeq(committedCmds, LAMBDA t: t = cmd)
                prevExecCmds == SubSeq(committedCmds, 1, idx)
            IN
                /\ idx # 0
                /\ GetIdxInSeq(prevExecCmds, LAMBDA t: t.key = cmd.key) =
                    specExecPrevCmd[cmd]

THEOREM Spec => []TypeOK /\ <>Stability

======================================================================================
