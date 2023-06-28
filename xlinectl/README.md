# xlinectl

This crate provides a command line client for Xline.

## Global Options
- endpoints <SERVER_NAME ADDR>... -- Set Xline endpoints, which are separated by ','
    ```bash
    # connect to servers with specific addresses
    ./xlinectl --endpoints "server0 10.0.0.1:2379, server1 10.0.0.2:2379, server2 10.0.0.3:2379"
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

All command output will first print the response header, and then print the response fields

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

#### Examples

```bash
# put key `foo` with value `bar` and attach the lease `123` to the key
./xlinectl put foo bar --lease=123

# detach the lease by updating with empty lease
./xlinectl put foo --ignore-value
```

```bash
# same as above
./xlienctl put foo bar --lease=123

# use existing lease
./xlinectl put foo bar1 --ignore-lease
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

#### Examples

```bash
# get the key `foo`
./xlinectl get foo
```

```bash
# get all keys prefixed with `foo`
./xlinectl get foo --prefix
```

```bash
# get all keys prefixed with `foo` sort in descend order
./xlinectl get foo --prefix --order DESCEND
```

```bash
# get all keys from `foo` to `foo3`
./xlinectl get foo foo3
```

### TXN
### WATCH
### LEASE
### LEASE GRANT
### LEASE REVOKE
### LEASE TIMETOLIVE
### LEASE LIST
### LEASE KEEP-ALIVE

## Cluster maintenance commands
### SNAPSHOT

## Concurrency commands
### LOCK

## Authentication commands
### AUTH
### ROLE
### ROLE ADD
### ROLE GET
### ROLE DELETE
### ROLE LIST
### ROLE GRANT-PERMISSION
### ROLE REVOKE-PERMISSION
### USER
### USER ADD
### USER GET
### USER DELETE
### USER LIST
### USER PASSWD
### USER GRANT-ROLE
### USER REVOKE-ROLE
