include_directories(${PROJECT_SOURCE_DIR}/third_party/googletest/googletest/include)
include_directories(${PROJECT_SOURCE_DIR}/third_party/googletest/googlemock/include)

add_executable(hello_test hello.cpp)
target_link_libraries(hello_test gtest fmt::fmt)

add_executable(replacer_test storage/replacer_test.cpp)
# Determine which storage_buffer library to use
if(USE_GOLD_LAB01)
    target_link_libraries(replacer_test storage_buffer log gtest)
elseif(TARGET storage_buffer)
    target_link_libraries(replacer_test storage_buffer log gtest)
else()
    message(FATAL_ERROR "storage_buffer library is not available")
endif()

add_executable(buffer_pool_test storage/buffer_pool_manager_test.cpp)
# Determine which storage_buffer library to use
if(USE_GOLD_LAB01)
    target_link_libraries(buffer_pool_test storage_buffer log storage_disk fmt::fmt gtest)
elseif(TARGET storage_buffer)
    target_link_libraries(buffer_pool_test storage_buffer log storage_disk fmt::fmt gtest)
else()
    message(FATAL_ERROR "storage_buffer library is not available")
endif()

add_executable(page_guard_test storage/page_guard_test.cpp)
# Determine which storage_buffer library to use
if(USE_GOLD_LAB01)
    target_link_libraries(page_guard_test storage_buffer log storage_disk gtest)
elseif(TARGET storage_buffer)
    target_link_libraries(page_guard_test storage_buffer log storage_disk gtest)
else()
    message(FATAL_ERROR "storage_buffer library is not available")
endif()

add_executable(table_handle_test system/table_handle_test.cpp)
# system_handle and system_table are always compiled from source (not lab-specific)
target_link_libraries(table_handle_test handle_page handle_table log gtest system_table)
if(USE_GOLD_LAB01)
    target_link_libraries(table_handle_test handle_page handle_table log gtest system_table)
elseif(TARGET handle_table)
    target_link_libraries(table_handle_test handle_page handle_table log gtest system_table)
else()
    message(FATAL_ERROR "storage_buffer library is not available")
endif()

add_executable(b_plus_tree_test storage/bptree_test.cpp)
# Link basic libraries first
target_link_libraries(b_plus_tree_test storage_disk log gtest handle_index)

# Add storage_buffer library conditionally
if(USE_GOLD_LAB01)
    target_link_libraries(b_plus_tree_test storage_buffer)
elseif(TARGET storage_buffer)
    target_link_libraries(b_plus_tree_test storage_buffer)
else()
    message(FATAL_ERROR "storage_buffer library is not available")
endif()

# Add storage_index library conditionally
if(USE_GOLD_LAB04)
    target_link_libraries(b_plus_tree_test storage_index)
elseif(TARGET storage_index)
    target_link_libraries(b_plus_tree_test storage_index)
else()
    message(FATAL_ERROR "storage_index library is not available")
endif()

add_executable(hash_index_test storage/hashindex_test.cpp)
# Link basic libraries first
target_link_libraries(hash_index_test storage_disk log gtest handle_index)

# Add storage_buffer library conditionally
if(USE_GOLD_LAB01)
    target_link_libraries(hash_index_test storage_buffer)
elseif(TARGET storage_buffer)
    target_link_libraries(hash_index_test storage_buffer)
else()
    message(FATAL_ERROR "storage_buffer library is not available")
endif()

# Add storage_index library conditionally
if(USE_GOLD_LAB04)
    target_link_libraries(hash_index_test storage_index)
elseif(TARGET storage_index)
    target_link_libraries(hash_index_test storage_index)
else()
    message(FATAL_ERROR "storage_index library is not available")
endif()

# Log Record Test (Lab06 - Phase 1)
add_executable(log_record_test log/log_record_test.cpp)
target_link_libraries(log_record_test
        log concurrency handle_db handle_table system_table system_index
        storage_buffer storage_disk gtest)

# Log Manager Test (Lab06 - Phase 2)
add_executable(log_manager_test log/log_manager_test.cpp)
target_link_libraries(log_manager_test
        log concurrency handle_db handle_table system_table system_index
        storage_buffer storage_disk gtest)

# WAL Integration Test (Lab06 - Phase 3)
add_executable(wal_integration_test log/wal_integration_test.cpp)
target_link_libraries(wal_integration_test
        log concurrency handle_db handle_table system_table system_index
        storage_buffer storage_disk gtest gtest_main)

# Recovery Test (Lab06 - Phase 4)
add_executable(recovery_test log/recovery_test.cpp)
target_link_libraries(recovery_test
        log concurrency handle_db handle_table system_table system_index
        storage_buffer storage_disk gtest gtest_main)

# Index Recovery Test (Lab06 - Phase 5)
add_executable(index_recovery_test log/index_recovery_test.cpp)
target_link_libraries(index_recovery_test
        log concurrency handle_db handle_table system_table system_index
        storage_buffer storage_disk gtest gtest_main)

# Lock Manager Test
add_executable(lock_manager_test concurrency/lock_manager_test.cpp)
target_link_libraries(lock_manager_test concurrency log handle_db handle_table system_table system_index gtest gtest_main)

# Strict 2PL Integration Test
add_executable(strict_2pl_test concurrency/strict_2pl_test.cpp)
target_link_libraries(strict_2pl_test system concurrency log handle_db handle_table system_table system_index gtest gtest_main)

# Strict 2PL table/index abort consistency test
add_executable(two_pl_index_abort_test concurrency/two_pl_index_abort_test.cpp)
target_link_libraries(two_pl_index_abort_test system concurrency log handle_db handle_table system_table system_index gtest gtest_main)

# Version Chain Infrastructure Test (Phase 4.1)
add_executable(version_chain_test concurrency/version_chain_test.cpp)
target_link_libraries(version_chain_test concurrency log handle_db handle_table system_table system_index gtest gtest_main)

# Timestamp and MVCC Integration Test (Phase 4.2)
add_executable(timestamp_mvcc_test concurrency/timestamp_mvcc_test.cpp)
target_link_libraries(timestamp_mvcc_test concurrency log handle_db handle_table system_table system_index gtest gtest_main)

# Snapshot Isolation Integration Test (Phase 4.3)
add_executable(snapshot_isolation_test concurrency/snapshot_isolation_test.cpp)
target_link_libraries(snapshot_isolation_test system concurrency log handle_db handle_table system_table system_index gtest gtest_main)

# Garbage Collector Test (Phase 4.4)
add_executable(garbage_collector_test concurrency/garbage_collector_test.cpp)
target_link_libraries(garbage_collector_test system concurrency log handle_db handle_table system_table system_index gtest gtest_main)

# ACID Issue Tests - Targeted tests for critical ACID violations

# Issue #3: MVCC Write-Write Conflict Detection Test
add_executable(mvcc_write_conflict_test concurrency/mvcc_write_conflict_test.cpp)
target_link_libraries(mvcc_write_conflict_test concurrency log handle_db handle_table system_table system_index gtest gtest_main)

# Atomic MVCC write coordination test (bonus)
add_executable(mvcc_atomic_write_test concurrency/mvcc_atomic_write_test.cpp)
target_link_libraries(mvcc_atomic_write_test
        executor_basic mvcc_execution concurrency log handle_db handle_table system_table system_index
        storage_buffer storage_disk gtest gtest_main)

if(UNIX AND NOT APPLE)
    foreach(EARLY_LAB_TEST_TARGET
            replacer_test
            buffer_pool_test
            page_guard_test
            table_handle_test
            b_plus_tree_test
            hash_index_test)
        target_link_options(${EARLY_LAB_TEST_TARGET} PRIVATE "-Wl,--allow-shlib-undefined")
    endforeach()

    foreach(CONCURRENCY_TEST_TARGET
            lock_manager_test
            strict_2pl_test
            two_pl_index_abort_test
            version_chain_test
            timestamp_mvcc_test
            snapshot_isolation_test
            garbage_collector_test
            mvcc_write_conflict_test
            mvcc_atomic_write_test)
        target_link_options(${CONCURRENCY_TEST_TARGET} PRIVATE "-Wl,--allow-shlib-undefined")
    endforeach()
endif()

# Issue #1: FlushAllPages WAL Protocol Test
add_executable(wal_flush_all_pages_test log/wal_flush_all_pages_test.cpp)
target_link_libraries(wal_flush_all_pages_test
        log concurrency handle_db handle_table system_table system_index
        storage_buffer storage_disk gtest)

# Issue #7: MVCC Recovery Undo Test
add_executable(mvcc_recovery_undo_test log/mvcc_recovery_undo_test.cpp)
target_link_libraries(mvcc_recovery_undo_test
        log concurrency handle_db handle_table system_table system_index
        storage_buffer storage_disk gtest gtest_main)

# Lab06 log/recovery tests link against gold libraries whose transitive
# dependencies (e.g. handle_db/handle_table) may be dropped by --as-needed;
# allow unresolved symbols in shared libraries to keep the link well-defined.
if(UNIX AND NOT APPLE)
    foreach(LOG_RECOVERY_TEST_TARGET
            log_record_test
            log_manager_test
            wal_integration_test
            recovery_test
            index_recovery_test
            wal_flush_all_pages_test
            mvcc_recovery_undo_test)
        target_link_options(${LOG_RECOVERY_TEST_TARGET} PRIVATE "-Wl,--allow-shlib-undefined")
    endforeach()
endif()

# Keep every published unit-test executable discoverable through one CTest
# entry point.  This list intentionally mirrors every test/*.cpp source.
set(NJUDB_ALL_TEST_TARGETS
        hello_test
        replacer_test
        buffer_pool_test
        page_guard_test
        table_handle_test
        b_plus_tree_test
        hash_index_test
        lock_manager_test
        strict_2pl_test
        two_pl_index_abort_test
        version_chain_test
        timestamp_mvcc_test
        snapshot_isolation_test
        garbage_collector_test
        mvcc_write_conflict_test
        mvcc_atomic_write_test
        log_record_test
        log_manager_test
        wal_integration_test
        recovery_test
        index_recovery_test
        wal_flush_all_pages_test
        mvcc_recovery_undo_test)

foreach(NJUDB_TEST_TARGET IN LISTS NJUDB_ALL_TEST_TARGETS)
    add_test(NAME ${NJUDB_TEST_TARGET} COMMAND ${NJUDB_TEST_TARGET})
endforeach()
