#!/bin/bash

set -euo pipefail

PROJECT_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
SOURCE_SCRIPT="$PROJECT_ROOT/configure.sh"
TEST_ROOT=$(mktemp -d "${TMPDIR:-/tmp}/njudb-configure-test.XXXXXX")
trap 'rm -rf "$TEST_ROOT"' EXIT

FAKE_BIN="$TEST_ROOT/fake-bin"
mkdir -p "$FAKE_BIN"
printf '%s\n' \
  '#!/bin/bash' \
  'printf '\''%s\n'\'' "$@" > "$CMAKE_CAPTURE_FILE"' \
  > "$FAKE_BIN/cmake"
chmod +x "$FAKE_BIN/cmake"

fail() {
  printf 'FAIL: %s\n' "$*" >&2
  exit 1
}

prepare_case() {
  local case_name=$1
  local source_labs=$2
  local enable_mvcc=$3
  local enable_wal=$4
  local case_root="$TEST_ROOT/$case_name"
  local repo_root="$case_root/repo"

  mkdir -p "$repo_root/lib-gold/macos/debug"
  mkdir -p "$repo_root/lib-gold/macos/release"
  mkdir -p "$repo_root/lib-gold/linux/debug"
  mkdir -p "$repo_root/lib-gold/linux/release"
  cp "$SOURCE_SCRIPT" "$repo_root/configure.sh"
  chmod +x "$repo_root/configure.sh"

  awk \
    -v source_labs="$source_labs" \
    -v enable_mvcc="$enable_mvcc" \
    -v enable_wal="$enable_wal" '
      /^SOURCE_LABS=/ {
        printf "SOURCE_LABS=\"%s\"\n", source_labs
        next
      }
      /^ENABLE_LAB05_MVCC=/ {
        printf "ENABLE_LAB05_MVCC=%s\n", enable_mvcc
        next
      }
      /^ENABLE_LAB06_WAL=/ {
        printf "ENABLE_LAB06_WAL=%s\n", enable_wal
        next
      }
      { print }
    ' "$repo_root/configure.sh" > "$repo_root/configure.sh.tmp"
  mv "$repo_root/configure.sh.tmp" "$repo_root/configure.sh"
  chmod +x "$repo_root/configure.sh"

  printf '%s\n' "$case_root"
}

run_configure() {
  local case_root=$1
  shift
  local repo_root="$case_root/repo"
  local capture_file="$case_root/cmake.args"
  local output_file="$case_root/configure.out"

  (
    cd "$repo_root"
    PATH="$FAKE_BIN:$PATH" CMAKE_CAPTURE_FILE="$capture_file" \
      ./configure.sh "$@"
  ) > "$output_file" 2>&1
}

assert_arg() {
  local case_root=$1
  local expected=$2
  local capture_file="$case_root/cmake.args"

  [ -f "$capture_file" ] || fail "CMake was not called for ${case_root##*/}"
  grep -Fqx -- "$expected" "$capture_file" ||
    fail "missing CMake argument '$expected' for ${case_root##*/}"
}

assert_no_cmake_call() {
  local case_root=$1
  [ ! -e "$case_root/cmake.args" ] ||
    fail "CMake was called for invalid case ${case_root##*/}"
}

assert_all_lab_args() {
  local case_root=$1
  local expected_value=$2
  local lab
  for lab in 01 02 03 04 05 06; do
    assert_arg "$case_root" "-DUSE_GOLD_LAB${lab}=${expected_value}"
  done
}

default_case=$(prepare_case default_all all false false)
run_configure "$default_case"
assert_all_lab_args "$default_case" OFF
assert_arg "$default_case" -DENABLE_LAB05_MVCC=OFF
assert_arg "$default_case" -DENABLE_LAB06_WAL=OFF
printf 'PASS: default all-source configuration\n'

lab05_case=$(prepare_case lab05_source 05 false false)
run_configure "$lab05_case"
for lab in 01 02 03 04 06; do
  assert_arg "$lab05_case" "-DUSE_GOLD_LAB${lab}=ON"
done
assert_arg "$lab05_case" -DUSE_GOLD_LAB05=OFF
printf 'PASS: one source lab and five gold labs\n'

lab0506_case=$(prepare_case lab0506_features "05 06" true true)
run_configure "$lab0506_case"
for lab in 01 02 03 04; do
  assert_arg "$lab0506_case" "-DUSE_GOLD_LAB${lab}=ON"
done
assert_arg "$lab0506_case" -DUSE_GOLD_LAB05=OFF
assert_arg "$lab0506_case" -DUSE_GOLD_LAB06=OFF
assert_arg "$lab0506_case" -DENABLE_LAB05_MVCC=ON
assert_arg "$lab0506_case" -DENABLE_LAB06_WAL=ON
printf 'PASS: multiple source labs and runtime features\n'

all_gold_case=$(prepare_case all_gold "" false false)
run_configure "$all_gold_case"
assert_all_lab_args "$all_gold_case" ON
printf 'PASS: empty source list selects all gold\n'

invalid_lab_case=$(prepare_case invalid_lab 07 false false)
if run_configure "$invalid_lab_case"; then
  fail "invalid lab number was accepted"
fi
assert_no_cmake_call "$invalid_lab_case"
printf 'PASS: invalid lab number rejected before CMake\n'

mixed_all_case=$(prepare_case mixed_all "all 05" false false)
if run_configure "$mixed_all_case"; then
  fail "all mixed with a lab number was accepted"
fi
assert_no_cmake_call "$mixed_all_case"
printf 'PASS: all cannot be mixed with lab numbers\n'

invalid_bool_case=$(prepare_case invalid_boolean 05 maybe false)
if run_configure "$invalid_bool_case"; then
  fail "invalid MVCC boolean was accepted"
fi
assert_no_cmake_call "$invalid_bool_case"
printf 'PASS: invalid runtime boolean rejected before CMake\n'

override_case=$(prepare_case cli_override 05 true true)
run_configure "$override_case" \
  --all-source --disable-lab05-mvcc --disable-lab06-wal
assert_all_lab_args "$override_case" OFF
assert_arg "$override_case" -DENABLE_LAB05_MVCC=OFF
assert_arg "$override_case" -DENABLE_LAB06_WAL=OFF
printf 'PASS: CLI overrides editable defaults\n'

compat_case=$(prepare_case cli_compatibility all false false)
mkdir -p "$compat_case/repo/build"
printf 'remove me\n' > "$compat_case/repo/build/inside.sentinel"
printf 'keep me\n' > "$compat_case/outside.sentinel"
run_configure "$compat_case" --lab06-gold --build-type Release --clean
for lab in 01 02 03 04 05; do
  assert_arg "$compat_case" "-DUSE_GOLD_LAB${lab}=OFF"
done
assert_arg "$compat_case" -DUSE_GOLD_LAB06=ON
assert_arg "$compat_case" -DCMAKE_BUILD_TYPE=Release
[ ! -e "$compat_case/repo/build/inside.sentinel" ] ||
  fail "--clean did not remove the repository build directory"
[ -e "$compat_case/outside.sentinel" ] ||
  fail "--clean removed a file outside the repository build directory"
printf 'PASS: existing CLI remains compatible\n'

printf 'Passed [9/9] configure.sh workflow tests\n'
