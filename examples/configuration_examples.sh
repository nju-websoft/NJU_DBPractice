#!/bin/bash

# NJUDB Configuration Examples
# The preferred workflow is to edit the configuration block in configure.sh.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

echo "=== NJUDB Lab Configuration Examples ==="
echo
echo 'Edit SOURCE_LABS at the top of configure.sh:'
echo '  Lab01 only: SOURCE_LABS="01"'
echo '  Lab02 only: SOURCE_LABS="02"'
echo '  Lab03 only: SOURCE_LABS="03"'
echo '  Lab04 only: SOURCE_LABS="04"'
echo '  Lab05 only: SOURCE_LABS="05"'
echo '  Lab06 only: SOURCE_LABS="06"'
echo '  Lab05+06:   SOURCE_LABS="05 06"'
echo '  All source: SOURCE_LABS="all"'
echo '  All gold:   SOURCE_LABS=""'
echo
echo 'Lab05 MVCC Bonus:'
echo '  ENABLE_LAB05_MVCC=true'
echo
echo 'Lab06 WAL and recovery:'
echo '  ENABLE_LAB06_WAL=true'
echo
echo 'After editing, run:'
echo '  ./configure.sh'
echo '  cmake --build build -j'
echo
echo 'CLI flags remain available for temporary overrides:'
echo '  ./configure.sh --all-source --enable-lab06-wal --clean'
echo

read -r -p "Run configure.sh with its current editable settings? [y/N] " answer
case "$answer" in
  y|Y)
    ./configure.sh
    ;;
  *)
    echo "No configuration was changed."
    ;;
esac
