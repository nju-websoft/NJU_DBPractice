add_library(log SHARED log_manager.cpp recovery.cpp)
target_link_libraries(log storage_index fmt::fmt)
