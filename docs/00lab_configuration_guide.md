# NJUDB Lab Configuration Guide

NJUDB supports starting any lab independently. The lab being implemented is compiled from source; every other lab can use the supplied gold libraries.

## Quick reference

Edit the student configuration block at the top of `configure.sh`, then run the
script without arguments:

```bash
# ==================== Student configuration ====================
SOURCE_LABS="05"
ENABLE_LAB05_MVCC=false
ENABLE_LAB06_WAL=false
BUILD_TYPE="Debug"
CLEAN_BUILD=true
# ===============================================================
```

`SOURCE_LABS` lists the labs compiled from source; every omitted lab uses gold:

| Goal | `SOURCE_LABS` |
| --- | --- |
| Work on Lab01 only | `"01"` |
| Work on Lab02 only | `"02"` |
| Work on Lab03 only | `"03"` |
| Work on Lab04 only | `"04"` |
| Work on Lab05 only | `"05"` |
| Work on Lab06 only | `"06"` |
| Work on Lab05 and Lab06 | `"05 06"` |
| Compile every lab from source | `"all"` |
| Use every supplied implementation | `""` |

After editing:

```bash
./configure.sh
cmake --build build -j
ctest --test-dir build --output-on-failure
```

Always use a clean build directory when changing source/gold selections because imported targets and RPATH values are configuration-specific.

## Gold selection versus runtime features

The two kinds of switches are independent:

- `USE_GOLD_LAB05` and `USE_GOLD_LAB06` decide whether the corresponding implementation comes from source or a gold library.
- `ENABLE_LAB05_MVCC` enables the optional MVCC server path.
- `ENABLE_LAB06_WAL` enables WAL, checkpoint, and recovery in the server.

The same editable block controls both kinds of switches without conflating them:

```bash
# Lab05 MVCC Bonus, all other labs gold
SOURCE_LABS="05"
ENABLE_LAB05_MVCC=true
ENABLE_LAB06_WAL=false

# Lab06 required path, all other labs gold
SOURCE_LABS="06"
ENABLE_LAB05_MVCC=false
ENABLE_LAB06_WAL=true

# Lab05 + Lab06 including MVCC recovery
SOURCE_LABS="05 06"
ENABLE_LAB05_MVCC=true
ENABLE_LAB06_WAL=true
```

Existing CLI flags remain supported for automation or one-off overrides. For
example, `./configure.sh --all-source --enable-lab06-wal --clean` overrides the
values edited into the file for that invocation only. CLI options are processed
from left to right.

## Lab 05 and Lab 06 target boundaries

Lab 05 gold provides:

- `libconcurrency`: SS2PL, transaction lifecycle, MVCC timestamps/versions/GC.
- `libmvcc_execution`: the MVCC write coordinator used by the Bonus path.

Lab 06 gold provides:

- `liblog`: log records, LogManager, checkpoints, and ARIES recovery.
- `libhandle_db`: recovery-time database handling and index rebuilding.

Cross-layer calls in `TableHandle`, `BufferPoolManager`, and the transaction log chain are supplied framework glue. They remain at the same source paths as `wsdb` and do not move public APIs between files.

## Focused tests

```bash
# Lab01
./build/bin/replacer_test
./build/bin/buffer_pool_test
./build/bin/page_guard_test

# Lab04
./build/bin/b_plus_tree_test
./build/bin/hash_index_test

# Lab05 required
ctest --test-dir build --output-on-failure \
  -R 'lock_manager|strict_2pl|two_pl_index_abort'

# Lab05 including MVCC Bonus
ctest --test-dir build --output-on-failure \
  -R 'lock_manager|strict_2pl|two_pl_index_abort|version_chain|timestamp_mvcc|snapshot_isolation|garbage_collector|mvcc_write_conflict|mvcc_atomic_write'

# Lab06
ctest --test-dir build --output-on-failure \
  -R 'log_record|log_manager|wal_integration|recovery|index_recovery|wal_flush_all_pages|mvcc_recovery_undo'
```

## Platform and build-type lookup

Gold libraries are selected from:

```text
lib-gold/macos/debug/    # macOS Debug
lib-gold/macos/release/  # macOS Release
lib-gold/linux/debug/    # Linux Debug
lib-gold/linux/release/  # Linux Release
```

The selected directory must contain every file required by an enabled gold lab. CMake stops with the exact missing path rather than silently compiling that lab from source.

## Linux files to build for Lab 05/06 support

The repository includes the macOS artifacts. The aligned `wsdb` now produces the
same target layout directly, including `libmvcc_execution.so`; no temporary CMake
patch or source-file move is required.

From the directory that contains both repositories, build both configurations:

```bash
cmake -S wsdb -B /tmp/wsdb-gold-debug \
  -DCMAKE_BUILD_TYPE=Debug \
  -DCMAKE_BUILD_RPATH_USE_ORIGIN=ON
cmake --build /tmp/wsdb-gold-debug -j

cmake -S wsdb -B /tmp/wsdb-gold-release \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_BUILD_RPATH_USE_ORIGIN=ON
cmake --build /tmp/wsdb-gold-release -j
```

Create the destination directories and copy the ten required targets from each
build:

```bash
mkdir -p NJU_DBPractice/lib-gold/linux/debug
mkdir -p NJU_DBPractice/lib-gold/linux/release

for name in concurrency mvcc_execution log handle_db executor_basic executor_index \
            handle_page handle_index handle_table storage_buffer; do
  cp "/tmp/wsdb-gold-debug/lib/lib${name}.so" \
     NJU_DBPractice/lib-gold/linux/debug/
  cp "/tmp/wsdb-gold-release/lib/lib${name}.so" \
     NJU_DBPractice/lib-gold/linux/release/
done
```

Verify that all 20 copied ELF files use no absolute answer-build path. The
following command must print nothing and exit successfully:

```bash
checked_gold_files=0
for config in debug release; do
  for name in concurrency mvcc_execution log handle_db executor_basic executor_index \
              handle_page handle_index handle_table storage_buffer; do
    gold_file="NJU_DBPractice/lib-gold/linux/${config}/lib${name}.so"
    if [ ! -f "$gold_file" ]; then
      echo "ERROR: missing $gold_file" >&2
      exit 1
    fi
    if ! dynamic_section=$(readelf -d "$gold_file"); then
      echo "ERROR: $gold_file is not a readable ELF dynamic library" >&2
      exit 1
    fi
    if printf '%s\n' "$dynamic_section" \
        | grep -E '/tmp/wsdb-gold-(debug|release)|/[^ ]*/wsdb'; then
      echo "ERROR: $gold_file contains an absolute wsdb build path" >&2
      exit 1
    fi
    checked_gold_files=$((checked_gold_files + 1))
  done
done
test "$checked_gold_files" -eq 20
```

`$ORIGIN` entries are intentional: they allow sibling gold libraries to find
one another without referring to the answer repository or its build tree.

The result must contain exactly these Lab 05/06 support files:

```text
lib-gold/linux/debug/libconcurrency.so
lib-gold/linux/debug/libmvcc_execution.so
lib-gold/linux/debug/liblog.so
lib-gold/linux/debug/libhandle_db.so
lib-gold/linux/debug/libexecutor_basic.so
lib-gold/linux/debug/libexecutor_index.so
lib-gold/linux/debug/libhandle_page.so
lib-gold/linux/debug/libhandle_index.so
lib-gold/linux/debug/libhandle_table.so
lib-gold/linux/debug/libstorage_buffer.so

lib-gold/linux/release/libconcurrency.so
lib-gold/linux/release/libmvcc_execution.so
lib-gold/linux/release/liblog.so
lib-gold/linux/release/libhandle_db.so
lib-gold/linux/release/libexecutor_basic.so
lib-gold/linux/release/libexecutor_index.so
lib-gold/linux/release/libhandle_page.so
lib-gold/linux/release/libhandle_index.so
lib-gold/linux/release/libhandle_table.so
lib-gold/linux/release/libstorage_buffer.so
```

`libexecutor_basic` must be rebuilt without `mvcc_write_coordinator.cpp`; that file is compiled into `libmvcc_execution`. `libexecutor_index` must expose the current `IdxScanExecutor(..., Context *)` constructor. `libhandle_page` must expose `PageHandle::SetXmin`, and `libhandle_index` must expose `IndexHandle::Search(const Record &)`. `libhandle_table` and `libstorage_buffer` must be rebuilt against the current concurrency/logging interfaces. Do not copy macOS `.dylib` files to Linux.

## Troubleshooting

If CMake reports an old Bison/Flex version on macOS, put the Homebrew tools first for that command:

```bash
PATH=/opt/homebrew/opt/bison/bin:/opt/homebrew/opt/flex/bin:$PATH \
  ./configure.sh
```

Set `CLEAN_BUILD=true` in the editable block before rerunning if the previous
configuration cached the system Bison/Flex paths.

For a missing gold file, use the full path in the CMake error to determine platform, build type, target name, and extension. A loader error after switching configurations usually means the build directory was reused; rerun with `--clean`.
