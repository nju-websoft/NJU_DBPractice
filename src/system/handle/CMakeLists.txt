add_library(system_handle SHARED
        page_handle.cpp
        table_handle.cpp
        index_handle.cpp
)
add_library(system_db SHARED 
        database_handle.cpp
)

target_link_libraries(system_handle
        storage_disk
        storage_buffer
        storage_index
        fmt::fmt
)

target_link_libraries(system_db system_handle system_table system_index)
