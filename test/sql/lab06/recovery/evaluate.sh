#!/usr/bin/env bash
set -euo pipefail

# Usage: ./evaluate.sh <njudb bin directory> <lab06 sql directory>

bin_dir=$(cd "$1" && pwd)
sql_dir=$(cd "$2" && pwd)

args=(--bin-dir "$bin_dir" --sql-root "$sql_dir")
if [ "${NJUDB_INCLUDE_MVCC_BONUS:-0}" = "1" ]; then
    args+=(--include-mvcc)
fi

python3 "$sql_dir"/recovery/lab06_recovery_crash.py "${args[@]}"
