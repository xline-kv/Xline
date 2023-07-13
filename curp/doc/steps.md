The document describes the CURP steps in detail.

Definition:
1. The request tuple consists of request command and following parameters.
2. The response tuple consists of return code and following states.

# Normal Operation
## A. Client Send Propose

1. Client send request <Propose, Cmd, RpcId, TermId, WitnessListVersion> to the master and all witnesses.

Note:
- `TermId` works the same way as in the Raft, which tells the leader version.
- `RpcId` is the universal id used for the command request. And the resent request reuse the RpcId to avoid repeated execution.
- `WitnessListVersion` is used in witness change during membership change. When witness set changes, its value increases 1.
- As `TermId` is used in all request and response, we don't put it in the following tuples.

## B. Master Handle Client Propose

Request: <Propose, Cmd, RpcId, WitnessListVersion>

1. Check `TermId`
    - if `TermId` doesn't match, reply tuple <WrongTermId> back to the client.
    - otherwise continue.
2. Check `WitnessListVersion`
    - if `WitnessListVersion` doesn't match, reply tuple <WrongWitnessListVersion, WitnessListVersion>
    - otherwise continue.
3. Check `RpcId`
    - if we've stored a Cmd with the same `RpcId` in `Speculative Cmd Pool`, reply <Accepted-Repeated>.
    - if we've stored a Cmd with the same `RpcId` in `Synced cmds`, reply <Synced-Repeated>.
    - otherwise continue.
4. Check whether `cmd` conflict with any cmd in the `Speculative Cmd Pool`
    - if there's no confliction, master executes the cmd and put the Cmd in the `Speculative Cmd Pool`, and then reply tuple <Accepted, Execution Result>.
    - otherwise reply tuple <NeedSync>, temporarily store the Propose tuple and send request <Sync, All Speculative Cmds RpcIds, TermId> to all the backups.

Note:
- Master have an async task/thread to send request <Sync, All Speculative Cmds RpcIds> to all the backups even there's no confliction.

## C. Witness Handle Client Propose

Request: <Propose, Cmd, RpcId, WitnessListVersion>

1. Check `TermId`
    - if `TermId` doesn't match, reply tuple <WrongTermId> back to the client.
    - otherwise continue.
2. Check `WitnessListVersion`
    - if `WitnessListVersion` doesn't match, reply tuple <WrongWitnessListVersion, WitnessListVersion>
    - otherwise continue.
3. Check whether `cmd` conflicts with any cmd in the witness
    - if there's no confliction, put the cmd in the witness, record tuple <Cmd, RpcId>, reply tuple <Accepted>
    - otherwise reply tuple <NeedSync>

## D. Client Handle Init Request Reply

Step 1 and 2 can run at the same time.

1. Wait response from mater:
    - If master replies tuple <WrongTermId>, request the latest master information from all witnesses (describe later), then restart from Step A.
    - If master replies tuple <WrongWitnessListVersion, WitnessListVersion>, request new `WitnessListVersion` from config server. Then restart step A with new withness list, ignore the following all replies from the same Propose.
    - If master replies <NeedSync>, send Request <Sync, RpcId> to the master. Wait for reply from the master.
        - If master replies <Synced>, continue.
        - If master replies <WrongTermId>, request the latest master information from all witnesses (describe later), then restart from Step A.
    - If master replies tuple <Accepted, Execution Result> or <Accepted-Repeated> or <Synced-Repeated>, continue.

2. Wait response from at least (f + 1) witnesses:
    - If we get (f + (f + 1) / 2 + 1) <Accepted> replies, continue.
    - If we get as least (f + 1) but less then (f + (f + 1) / 2 + 1) <Accepted> replies
        - If master replied <Synced> or <Synced-Repeated>, continue.
        - If master replied <Accepted, ...> or <Accepted-Repeated>, send Request <Sync, RpcId> to the master. Wait for the <Synced> reply from the master, continue.
        - if master replied <NeedSync>, wait for the <Synced> reply later, continue.
        - otherwise stop processing.

3. We've passed all the checks above (the continue branches), the command is executed successfully.

## E. Master Get Sync Request

Request: <Sync, RpcId>

- if <RpcId> is already in `Synced Cmd`, reply <Synced>.
- if <RpcId> is in the `Speculative Cmd Pool` and hasn't send sync request to backups, send request <Sync, RpcId>, and wait for replies.
    - if any backup replies <RequestSyncDetail, DetailedRpcIdList>, send request <SyncDetail, Cmd, RpcId> to the backup, and continue waiting for the reply.
    - if at least f + 1 success replies from backups and then reply <Synced>.
    - if at least f + 1 <WrongTermId> with the same replies, means this master is not the leader any more, reply <WrongTermId>.
    - if timeout threshold met, retry.

## F. Backups Handle Sync Request

Request: <Sync, RpcIdList> or <SyncDetail, CmdList, RpcIdList>

1. Check `TermId`
    - If not match, reply <WrongTermId>.
    - otherwise, continue.
2. Check request type
    - If it's <SyncDetail, CmdList, RpcIdList>, sync the CmdList in the backup, reply <Synced, RpcIdList> to the master.
    - If it's <Sync, RpcIdList>, check whether each `RpcId` in `RpcIdList` is in the witness.
        - If yes, move `RpcIdList` cmd from the witness to the backup, reply <Synced, RpcIdList> to the master.
        - If no, collect the RpcId to `DetailedRpcIdList`.
3. If `DetailedRpcIdList` is not empty, reply <RequestSyncDetail, DetailedRpcIdList> tuple to the master, wait for reply and sync the detailed cmds.

# Master Crash
## G. Client Request the Latest Master Information

Send request <QueryMasterInfo> to all witnesses. Wait for replies:
    - If the replies with the highest TermId number >= (f + 1), and the masterinfo from all of them are the same, client accept the master info and term number.
    - otherwise, wait a random timeout and retry.

## H. Witness Handle Latest Master Information

Request: <QueryMasterInfo>

Reply <TermId, MasterInfo>

## I. Witness Send Latest Master

Use the same protocol from the Raft.

<!-- ## J. Recovery From Master

1. Send <StopService> to all the other witnesses, after getting confirmed reply from f witnesses, continue.
2. Send <Collect-Witness-Backup> to all the confirmed witnesses, wait for all replies from them.
3. Find <> -->
