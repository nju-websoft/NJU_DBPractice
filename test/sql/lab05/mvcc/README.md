# Lab05 MVCC SQL Tests

This suite validates MVCC snapshot visibility, write conflicts, transaction
abort consistency, repeated writes, logical deletes, indexed access, stress
workloads, and simple coexistence with a fixed-protocol 2PL database.

Run it with:

```bash
test/sql/lab05/mvcc/evaluate.sh build/bin test/sql/lab05
```

The default suite intentionally remains bounded at 8 concurrent clients and
1000 rows. `NetController` uses `MAX_CLIENTS=10` as its listen backlog, so the
tests leave capacity for setup, verification, and transient reconnects.

Append the optional deterministic randomized stress test with:

```bash
python3 test/sql/lab05/mvcc/lab05_mvcc_concurrency.py \
  --bin-dir build/bin \
  --sql-root test/sql/lab05 \
  --stress \
  --stress-clients 8 \
  --stress-rounds 100 \
  --stress-seed 20260612
```

`--stress-clients` is restricted to `2..8`, and `--stress-rounds` to
`1..1000`. Each client owns a disjoint row partition, randomly commits or
aborts indexed updates, and the final table and owner index are checked against
a deterministic in-memory model.
