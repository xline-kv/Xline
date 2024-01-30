# xlinectl

This crate provides a command line client for Xline.

## Global Options

- endpoints <SERVER_NAME ADDR>... -- Set Xline endpoints, which are separated by ','

  ```bash
  # connect to servers with specific addresses
  ./xlinectl --endpoints "127.0.0.1:2379"
  ```

- user <USERNAME[:PASSWD]> -- The name of the user, this provide a shorthand to set password

  ```bash
  # connect to servers using user `foo` with password `bar`
  ./xlinectl --user foo:bar
  ```

- password <PASSWD> -- The password of the user, should exist if password not set in `--user`

  ```bash
  # connect to servers using user `foo` with password `bar`
  ./xlinectl --user foo --password bar
  ```

- wait_synced_timeout <TIMEOUT> -- The timeout for Curp client waiting synced(in secs) [default: 2]

- propose_timeout <TIMEOUT> -- The timeout for Curp client proposing request(in secs) [default: 1]

- retry_timeout <TIMEOUT> -- The timeout for Curp client retry interval(in millis) [default: 50]

- printer_type <TYPE> -- The format of the result that will be printed [default: SIMPLE] [possible values: SIMPLE, FIELD]

## Output Format

The Simple printer will only print simplified output.
The Field printer will first print the response header and then print each of the response fields.

## Key-value commands

### PUT

Puts the given key-value into the store. If key already holds a value, it is overwritten.

#### Usage

```bash
put [options] <key> <value>
```

#### Options

- lease -- lease ID to attach to the key [default: 0]
- prev_kv --  return the previous key-value pair before modification
- ignore_value --  updates the key using its current value
- ignore_lease --  updates the key using its current lease

#### Output

```
OK
```

#### Examples

```bash
# put key `foo` with value `bar` and attach the lease `123` to the key
./xlinectl put foo bar --lease=123
OK

# detach the lease by updating with empty lease
./xlinectl put foo --ignore-value
OK

# same as above
./xlienctl put foo bar --lease=123
OK

# use existing lease
./xlinectl put foo bar1 --ignore-lease
OK
```

### GET

Gets the key or a range of keys

#### Usage

```bash
get [options] <key> [range_end]
```

#### Options

- consistency -- Linearizable(L) or Serializable(S) [default: L]
- order -- Order of results; ASCEND or DESCEND
- sort_by -- Sort target; CREATE, KEY, MODIFY, VALUE, or VERSION
- limit -- Maximum number of results [default: 0]
- prefix -- Get keys with matching prefix (conflicts with range_end)
- from_key -- Get keys that are greater than or equal to the given key using byte compare (conflicts with prefix and range_end)
- rev -- Specify the kv revision [default: 0]
- keys_only -- Get only the keys
- count_only -- Get only the count (conflicts with keys_only)

#### Output

```
<key0>
<value0>
<key1>
<value1>
...
```

#### Examples

```bash
./xlinectl put foo bar
OK
./xlinectl put foo1 bar1
OK
./xlinectl put foo2 bar2
OK

# get the key `foo`
./xlinectl get foo
foo
bar

# get all keys prefixed with `foo`
./xlinectl get foo --prefix
foo
bar
foo1
bar1
foo2
bar2

# get all keys prefixed with `foo` sort in descend order
./xlinectl get foo --prefix --order DESCEND
foo2
bar2
foo1
bar1
foo
bar

# get all keys from `foo` to `foo3`
./xlinectl get foo1 foo3
foo1
bar1
foo2
bar2
```

### DELETE
Deletes the key or a range of keys

#### Usage

```bash
delete [options] <key> [range_end]
```

#### Options
- prefix -- delete keys with matching prefix
- prev_kv -- return deleted key-value pairs
- from_key -- delete keys that are greater than or equal to the given key using byte compare

#### Output

```
<number_of_keys_deleted>
[prev_key]
[prev_value]
...
```

#### Examples

```bash
./xlinectl put foo bar
# delete the key `foo`
./xlinectl delete foo
1

./xlinectl put foo bar
./xlinectl put foo1 bar1
./xlinectl put foo2 bar2
# delete all keys prefixed with `foo`
./xlinectl delete foo --prefix
3

```

### TXN
Txn processes all the requests in one transaction

#### Usage

```bash
txn [options]
```

#### Options
- interactive -- set interactive mode

#### Input Format

The ebnf is the same as etcdctl
```ebnf
<Txn> ::= <CMP>* "\n" <THEN> "\n" <ELSE> "\n"
<CMP> ::= (<CMPCREATE>|<CMPMOD>|<CMPVAL>|<CMPVER>|<CMPLEASE>) "\n"
<CMPOP> ::= "<" | "=" | ">"
<CMPCREATE> := ("c"|"create")"("<KEY>")" <CMPOP> <REVISION>
<CMPMOD> ::= ("m"|"mod")"("<KEY>")" <CMPOP> <REVISION>
<CMPVAL> ::= ("val"|"value")"("<KEY>")" <CMPOP> <VALUE>
<CMPVER> ::= ("ver"|"version")"("<KEY>")" <CMPOP> <VERSION>
<CMPLEASE> ::= "lease("<KEY>")" <CMPOP> <LEASE>
<THEN> ::= <OP>*
<ELSE> ::= <OP>*
<OP> ::= ((see put, get, del xlinectl command syntax)) "\n"
<KEY> ::= (%q formatted string)
<VALUE> ::= (%q formatted string)
<REVISION> ::= "\""[0-9]+"\""
<VERSION> ::= "\""[0-9]+"\""
<LEASE> ::= "\""[0-9]+\""
```

#### Output

```
<succeed>
[child requests]
```

`<succeed>` can be SUCCESS | FAILURE

#### Examples

non-interactive mode
```bash
./xlinectl txn <<<'mod("key1") > "0"

get key1

put key1 "val1"
put key2 "some-val"

'
```

interactive mode
```bash
./xlinectl txn --interactive
compares:
mod("key1") > "0"

successful request:
get key1

failure request:
put key1 "val1"
put key2 "some-val"
```
### COMPACTION
COMPACTION discards all Xline event history prior to a given revision. Since Xline uses a multiversion concurrency control model, it preserves all key updates as event history. When the event history up to some revision is no longer needed, all superseded keys may be compacted away to reclaim storage space in the Xline backend database.

#### Usage

```bash
compaction [options] <revision>
```

#### Options
- physical -- To wait for compaction to physically remove all old revisions

#### Output

```
Compacted
...
```

#### Examples

```bash
# compact revision less than 123
./xlinectl compaction 123
Compacted
# wait for compaction to physically remove all old revisions
./xlinectl compaction 234 --physical
Compacted
```

### WATCH
Watches events stream on keys or prefixes

#### Usage
```bash
watch [options] [key] [range_end]
```

#### Options
- prefix -- Watch a prefix
- rev -- Revision to start watching
- pre_kv -- Get the previous key-value pair before the event happens
- progress_notify -- Get periodic watch progress notification from server
- interactive -- Interactive mode

#### Output

```
<event0>
[prev_key0]
[prev_value0]
<key0>
<value0>
<event1>
[prev_key1]
[prev_value1]
<key1>
<value1>
...

```

`<event>` can be PUT | DELETE

#### Examples

non-interactive mode, which will continuously get output from response stream
```bash
# Watch key `foo`
./xlinectl watch foo
```
```bash
# Watch a range of keys from foo to foo3
./xlinectl watch foo foo3
```

interactive mode, which can start or cancel a watch
```bash
./xlinectl watch --interactive

watch foo foo3

# assume the returned watch id is 100 in previous command
cancel 100
```


### LEASE

### LEASE GRANT
Create a lease with a given TTL

#### Usage
```bash
grant <ttl>
```

#### Output

```
<lease_id>
```

#### Examples
```bash
# create a new lease with 100s TTL
./xlinectl lease grant 100
8927807922788821579
```

### LEASE REVOKE
Revoke a lease

#### Usage

```bash
revoke <leaseId>
```
#### Output

```
Revoked
```

#### Examples
```bash
./xlinectl lease grant 100
8927807922788821579
# Revoke a lease with leaseId 123
./xlinectl lease revoke 8927807922788821579
Revoked
```

### LEASE TIMETOLIVE
Get lease ttl information

#### Usage

```bash
timetolive <leaseId>
```

#### Output

```
<TTL>
```

#### Examples

```bash
./xlinectl lease grant 100
8927807922788821579
# Get the TTL of a lease with leaseId 123
./xlinectl lease timetolive 8927807922788821579
93
```

### LEASE LIST
List all active leases

#### Usage

```bash
list
```

#### Output

```
<lease0>
<lease1>
...
```

#### Examples
```bash
./xlinectl lease grant 100
8927807922788821579
./xlinectl lease grant 100
6334008358021467690

# List all leases
./xlinectl lease list
8927807922788821579
6334008358021467690
```

### LEASE KEEP-ALIVE
lease keep alive periodically

#### Usage

```bash
keep_alive [options] <leaseId>
```

#### Options
- once -- keep alive once

#### Output

```
<TTL>
```

#### Examples

```bash
./xlinectl lease grant 100
8927807922788821579
# keep alive forever with leaseId 123 until the process receive `SIGINT`, each time print the TTL
./xlinectl lease keep_alive 8927807922788821579
100
100
...
```

```bash
# renew the lease ttl only once
./xlinectl lease keep_alive 123 --once
100
```

## Cluster maintenance commands

### MEMBER
MEMBER provides commands for managing Xline cluster membership.

### MEMBER ADD
MEMBER ADD introduces a new member into the Xline cluster as a new peer.

#### Usage

```bash
member add [options] <peer_urls>
```

#### Options
- is_learner -- Add as learner


#### Output

```
<member_id>
```

#### Examples
```bash
# Add a member whose addresses are [127.0.0.1:2379, 127.0.0.1:2380]
./xlinectl member add "10.0.0.1:2379,10.0.0.2:2379"
16151281779493828828
```

### MEMBER UPDATE
MEMBER UPDATE sets the peer URLs for an existing member in the Xline cluster.

#### Usage

```bash
member update <ID> <peer_urls>
```

#### Output

```
Member updated
```

#### Examples
```bash
./xlinectl member add "10.0.0.1:2379,10.0.0.2:2379"
16151281779493828828
# Add a member whose addresses are [127.0.0.1:2379, 127.0.0.1:2380]
./xlinectl member update 16151281779493828828 "10.0.0.3:2379,10.0.0.4:2379"
```

### MEMBER LIST
MEMBER ADD introduces a new member into the Xline cluster as a new peer.

#### Usage

```bash
member list
```

#### Options
- linearizable -- to use linearizable fetch


#### Output

```
<member_id1>
<member_id2>
...
```

#### Examples
```bash
# List all members
./xlinectl member list
16151281779493828828
16375331871075283369
16171997749406652082
```

### MEMBER REMOVE
MEMBER REMOVE removes a member of an Xline cluster from participating in cluster consensus.

#### Usage

```bash
member remove <ID>
```

#### Output

```
Member removed
```

#### Examples
```bash
# Remove a member
./xlinectl member remove 16151281779493828828
Member removed
```

### MEMBER PROMOTE
MEMBER PROMOTE promotes a learner of an Xline cluster to member

#### Usage

```bash
member promote <ID>
```

#### Output

```
Member promoted
```

#### Examples
```bash
# Remove a member
./xlinectl member promote 16151281779493828828
Member promoted
```

### SNAPSHOT
Get snapshots of xline nodes

### SNAPSHOT SAVE
Save snapshot to file

#### Usage

```bash
snapshot save <filename>
```

#### Output

```
snapshot saved to: <filename>
```

#### Examples

```bash
# Save snapshot to /tmp/foo
./xlinectl snapshot save /tmp/foo.snapshot
snapshot saved to: /tmp/foo.snapshot
```

## Concurrency commands

### LOCK
Acquire a lock, which will return a unique key that exists so long as the lock is held.

#### Usage

```bash
lock <lockname>
```

#### Examples

```bash
# Hold a lock named foo until `SIGINT` is received
./xlinectl lock foo
```
## Authentication commands

### AUTH
Manage authentication

### AUTH ENABLE
Enable authentication

#### Usage

```bash
auth enable
```

#### Output

```
Authentication enabled
```

#### Examples
```bash
./xlinectl user add root rootpw
./xlienctl user grant-role root root
# Enable authentication
./xlinectl auth enable
Authentication enabled
```

### AUTH DISABLE
Disable authentication

#### Usage

```bash
auth disable
```

#### Output

```
Authentication disabled
```

#### Examples
```bash
# Disable authentication
./xlinectl --user root:root auth disable
Authentication disabled
```

### AUTH STATUS
Status of authentication

#### Usage

```bash
auth status
```

#### Examples
```bash
# Check the status of authentication
./xlinectl auth status
```

### ROLE
Role related commands

### ROLE ADD
Create a new role

#### Usage

```bash
add <name>
```

#### Output

```
Role added
```

#### Examples

```bash
# add a new role named 'foo'
./xlinectl --user=root:root role add foo
Role added
```

### ROLE GET
List role information

#### Usage

```bash
get <name>
```

#### Output

```
Permmision: <perm_type0>
<key0>
[range_end0]
Permmision: <perm_type1>
<key1>
[range_end1]
...
```

#### Examples

```bash
./xlinectl --user=root:root grant_perm foo Read key
./xlinectl --user=root:root grant_perm foo ReadWrite key1
# Get role named 'foo'
./xlinectl --user=root:root role get foo
Permission: Read
key
Permission: Read
key1
```

### ROLE DELETE
Delete a role

#### Usage

```bash
delete <name>
```

#### Output

```
Role deleted
```

#### Examples
```bash
./xlinectl --user=root:root role add foo
# delete the role named `foo`
./xlinectl --user=root:root role delete foo
Role deleted
```

### ROLE LIST
List all roles

#### Usage

```bash
list
```

#### Output

```
<role0>
<role1>
...
```

#### Examples

```bash
./xlinectl --user=root:root role add foo
./xlinectl --user=root:root role add foo1
# list all roles
./xlinectl --user=root:root role list
foo
foo1
```

### ROLE GRANT-PERMISSION
Grant permission to a role, including Read, Write or ReadWrite

#### Usage

```bash
grant_perm [options] <name> <perm_type> <key> [range_end]
```

#### Options
- prefix -- Get keys with matching prefix
- from_key -- Get keys that are greater than or equal to the given key using byte compare (conflicts with `range_end`)

#### Output

```
Permission granted
```

#### Examples

```bash
# Grant read permission to role 'foo' for key 'bar' with read permission
./xlinectl --user=root:root role grant_perm foo READ bar
Permission granted
```

### ROLE REVOKE-PERMISSION
Revoke permission from a role

#### Usage

```bash
revoke_perm <name> <key> [range_end]
```

#### Output

```
Permission revoked
```

#### Examples

```bash
# Revoke permission from role 'foo' for the range from 'bar' to 'bar2'
./xlinectl --user=root:root role revoke_perm foo bar bar2
Permission revoked
```

### USER

### USER ADD
Add a new user

#### Usage

```bash
add [options] <name> [password]
```

#### Options
- no_password -- Create without password

#### Output

```
User added
```

#### Examples
```bash
# Add a new user with a specified password
./xlinectl --user=root:root user add foo bar
User added

# Add a new user without a password
./xlinectl --user=root:root user add foo1 --no_password
User added
```

### USER GET
Get a new user

#### Usage

```bash
get [options] <name>
```

#### Options
- detail -- Show permissions of roles granted to the user

#### Output

```
<role0>
<role1>
...
```

#### Examples
```bash
./xlinectl --user=root:root user grant_role foo role0
./xlinectl --user=root:root user grant_role foo role1
# Get a user named `foo`
./xlinectl --user=root:root user get foo
role0
role1
```

### USER LIST
List all users

#### Usage

```bash
list
```

#### Output

```
<user0>
<user1>
...
```

#### Examples
```bash
./xlinectl --user=root:root user add foo bar
./xlinectl --user=root:root user add foo1 bar1
# List all users
./xlinectl --user=root:root user list
foo
foo1
```

### USER PASSWD
Change the password of a user

#### Usage

```bash
passwd <name> <password>
```

#### Output

```
Password updated
```

#### Examples

```bash
# Change the password of user `foo` to `bar`
./xlinectl --user=root:root user passwd foo bar
Password updated
```

### USER GRANT-ROLE
Grant role to a user

#### Usage

```bash
grant_role <name> <role>
```

#### Output

```
Role granted
```

#### Examples

```bash
./xlinectl --user=root:root user add foo
./xlinectl --user=root:root role add bar
# Grant `bar` role to the user `foo`
./xlinectl --user=root:root grant_role foo bar
Role granted
```

### USER REVOKE-ROLE
Revoke role from a user

#### Usage

```bash
revoke_role <name> <role>
```

#### Output

```
Role revoked
```

#### Examples

```bash
./xlinectl --user=root:root user add foo
./xlinectl --user=root:root role add bar
./xlinectl --user=root:root grant_role foo bar

# Revoke 'bar' role from the user 'foo'
./xlinectl --user=root:root revoke_role foo bar
Role revoked
```
