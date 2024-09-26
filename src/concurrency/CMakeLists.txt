add_library(concurrency SHARED
        lock_manager.cpp
        txn_manager.cpp
)
target_link_libraries(concurrency fmt::fmt)