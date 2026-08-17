njudb_should_compile_from_source(COMPILE_CONCURRENCY_FROM_SOURCE "05")
if(COMPILE_CONCURRENCY_FROM_SOURCE)
    add_library(concurrency SHARED
            lock_manager.cpp
            txn_manager.cpp
            concurrency_controller.cpp
            undo_record.cpp
            version_chain.cpp
            version_table.cpp
            timestamp_oracle.cpp
            version_manager.cpp
            mvcc_write_context.cpp
            garbage_collector.cpp
    )
    target_link_libraries(concurrency log fmt::fmt)

    # Allow undefined symbols for TableHandle/DatabaseHandle (resolved at runtime)
    if(APPLE)
        target_link_options(concurrency PRIVATE "-undefined" "dynamic_lookup")
    endif()
endif()
