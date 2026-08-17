#!/usr/bin/env bash
set -euo pipefail

# Usage: ./evaluate.sh <njudb bin directory> <sql directory>

bin_dir=$(cd "$1" && pwd)
sql_dir=$(cd "$2" && pwd)

cd "$bin_dir"
mkdir -p "$sql_dir"/mvcc/output

rm -rf data/lab05_mvcc data/lab05_cross_2pl
rm -f data/njudb.log

echo "Start njudb..."
./njudb &
server_pid=$!
trap 'kill "$server_pid" 2>/dev/null || true' EXIT

python3 - "$server_pid" <<'PY'
import os
import socket
import sys
import time

pid = int(sys.argv[1])
deadline = time.monotonic() + 15
while time.monotonic() < deadline:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        raise SystemExit("njudb exited before accepting connections")
    try:
        with socket.create_connection(("127.0.0.1", 5001), timeout=0.2):
            break
    except OSError:
        time.sleep(0.1)
else:
    raise SystemExit("timed out waiting for njudb on 127.0.0.1:5001")
PY

./client -i "$sql_dir"/mvcc/init.sql

passed=0
failed=0
total=0

normalize_prepare_output() {
    awk -F'|' '
        BEGIN { OFS = "|" }
        $2 ~ /Database/ && $4 ~ /Index/ {
            in_index_table = 1
            key_schema = ""
        }
        in_index_table && NF == 7 {
            if ($2 !~ /Database/) {
                fragment = $6
                gsub(/[[:space:]]/, "", fragment)
                key_schema = key_schema fragment
            }
            $6 = "              "
        }
        { print }
        in_index_table && /^Total tuple/ {
            gsub(/#[0-9]+/, "#<fd>", key_schema)
            print "KeySchema(normalized): " key_schema
            in_index_table = 0
        }
    ' "$1"
}

for file in $(find "$sql_dir"/mvcc -maxdepth 1 -name '[0-9][0-9]_*.sql' | sort); do
    total=$((total + 1))
    filename=$(basename -- "$file")
    filename="${filename%.*}"
    if ! ./client -i "$file" -o "$sql_dir"/mvcc/output/"$filename".out; then
        echo "Error in $filename: client execution failed"
        failed=$((failed + 1))
        continue
    fi
    # CREATE INDEX prints a process-local file descriptor in KeySchema. Rejoin
    # wrapped schema fragments and normalize only that descriptor.
    if [ "$filename" = "01_prepare" ]; then
        if diff <(normalize_prepare_output "$sql_dir"/mvcc/output/"$filename".out) \
                <(normalize_prepare_output "$sql_dir"/mvcc/expected/"$filename".out); then
            echo "Passed: $filename"
            passed=$((passed + 1))
        else
            echo "Error in $filename: output differs"
            failed=$((failed + 1))
        fi
        continue
    fi
    if diff "$sql_dir"/mvcc/output/"$filename".out "$sql_dir"/mvcc/expected/"$filename".out; then
        echo "Passed: $filename"
        passed=$((passed + 1))
    else
        echo "Error in $filename: output differs"
        failed=$((failed + 1))
    fi
done

echo "Passed [$passed/$total] lab05 MVCC sequential SQL tests"
if [ "$failed" -ne 0 ]; then
    echo "Failed [$failed/$total] lab05 MVCC sequential SQL tests"
fi

kill "$server_pid" 2>/dev/null || true
wait "$server_pid" 2>/dev/null || true
trap - EXIT

python3 "$sql_dir"/mvcc/lab05_mvcc_concurrency.py --bin-dir "$bin_dir" --sql-root "$sql_dir"

if [ "$failed" -ne 0 ]; then
    exit 1
fi
