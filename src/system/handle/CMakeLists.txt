add_library(system_handle SHARED
        record_handle.cpp
        page_handle.cpp
        table_handle.cpp
        index_handle.cpp
        database_handle.cpp
)
target_link_libraries(system_handle
        storage_disk
        storage_buffer
        storage_index
        system_table
        system_index
)