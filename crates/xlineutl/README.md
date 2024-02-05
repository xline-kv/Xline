# xlineutl

This crate provides a command line utility for Xline.

## Snapshot command

### Restore

Restore xline snapshot from a snapshot file

#### Usage

```bash
restore [options] <filename>
```

#### Options

- data-dir -- path to the output data directory

#### Examples

```bash
# restore snapshot to data dir
./xlineutl snapshot restore /path/to/snapshot --data-dir /path/to/target/dir
