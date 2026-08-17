#!/usr/bin/env bash
set -euo pipefail

# read all the sql files in the directory and evaluate them comparing the output with the expected output
# Usage: ./evaluate.sh <njudb build directory or bin directory> <lab#> <sql case directory>

root_dir=$(cd "$(dirname "$0")"/../.. && pwd)
input_dir=$(cd "$1" && pwd)
if [ -x "$input_dir"/njudb ] && [ -x "$input_dir"/client ]; then
    bin_dir="$input_dir"
else
    cmake --build "$input_dir" -j
    bin_dir="$input_dir"/bin
fi

lab_dir="$root_dir"/test/sql/"$2"
case_dir="$lab_dir"/"$3"
cd "$bin_dir"

# mkdir output under sql_dir if it does not exist
if [ ! -d "$case_dir"/output ]; then
    mkdir "$case_dir"/output
fi

# delete data/db2025 directory if it exists
if [ -d "data/db2025" ]; then
    rm -r data/db2025
fi
if [ -f "data/njudb.log" ]; then
    rm -f data/njudb.log
fi

# start a new process for njudb
echo "Start njudb..."
./njudb &
server_pid=$!
cleanup() {
    kill "$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true
}
trap cleanup EXIT

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

# run client
if ! ./client -i "$lab_dir"/init.sql; then
    echo "Error: failed to initialize $2"
    exit 1
fi

sql_dir="$case_dir"
# get total sql file number
total=$(find "$sql_dir" -type f -name "[0-9][0-9]_*.sql" | wc -l)
# counter for the number of passed tests
passed=0
failed=0

# CREATE INDEX prints a process-local file descriptor in KeySchema. Rejoin
# wrapped schema fragments and normalize only that descriptor so grading is
# stable across runs while every other byte remains significant.
normalize_index_fd() {
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

# iterate over all sql files in the directory matching 01_*.sql, 02_*.sql, etc., in sorted order
for file in $(ls "$sql_dir"/[0-9][0-9]_*.sql | sort); do
    # get the name of the file without the extension
    filename=$(basename -- "$file")
    filename="${filename%.*}"
    # run the sql file
    if ! ./client -i "$file" -o "$sql_dir"/output/$filename.out; then
        echo "Error in $filename: client execution failed"
        failed=$((failed+1))
        continue
    fi
    # compare the output with the expected output
    if ! diff <(normalize_index_fd "$sql_dir"/output/"$filename".out) \
              <(normalize_index_fd "$sql_dir"/expected/"$filename".out); then
        echo "Error in $filename"
        failed=$((failed+1))
    else
        echo "Passed: $filename"
        passed=$((passed+1))
    fi
done

# print the number of passed tests
echo "Passed [$passed/$total] tests for $2: $3"

# kill the njudb process
cleanup
trap - EXIT

if [ "$failed" -ne 0 ]; then
    exit 1
fi
