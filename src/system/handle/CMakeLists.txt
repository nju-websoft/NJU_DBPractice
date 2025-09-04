# Lab01: page handle and table handle
njudb_should_compile_from_source(COMPILE_FROM_SOURCE_ONE "01")
if(COMPILE_FROM_SOURCE_ONE)
    add_library(handle_page SHARED
            page_handle.cpp
    )

    target_link_libraries(handle_page
            storage_disk
            storage_buffer
            storage_index
            fmt::fmt
    )

    add_library(handle_table SHARED
            table_handle.cpp
    )

    target_link_libraries(handle_table
            handle_page
            storage_disk
            storage_buffer
            storage_index
            fmt::fmt
    )
endif()


# Lab04: System Handle (part of Lab04)
njudb_should_compile_from_source(COMPILE_FROM_SOURCE_FOUR "04")
if(COMPILE_FROM_SOURCE_FOUR)
    add_library(handle_index SHARED
            index_handle.cpp
    )

    target_link_libraries(handle_index
            storage_disk
            storage_buffer
            storage_index
            fmt::fmt
    )
endif()

add_library(handle_db SHARED 
        database_handle.cpp
)

# Link to basic dependencies first
target_link_libraries(handle_db system_table system_index)

# Conditionally link to handle_page and handle_table (Lab01)
if(USE_GOLD_LAB01)
    target_link_libraries(handle_db handle_page handle_table)
elseif(TARGET handle_page AND TARGET handle_table)
    target_link_libraries(handle_db handle_page handle_table)
else()
    message(FATAL_ERROR "handle_page and handle_table libraries are not available")
endif()

# Conditionally link to handle_index (Lab04)
if(USE_GOLD_LAB04)
    target_link_libraries(handle_db handle_index)
elseif(TARGET handle_index)
    target_link_libraries(handle_db handle_index)
else()
    message(FATAL_ERROR "handle_index library is not available")
endif()

# Create system_handle as an alias or interface library that combines all handle components
add_library(system_handle INTERFACE)

# Link system_handle to all the handle components
target_link_libraries(system_handle INTERFACE handle_db)

# Conditionally link to handle_page and handle_table (Lab01)
if(USE_GOLD_LAB01)
    target_link_libraries(system_handle INTERFACE handle_page handle_table)
elseif(TARGET handle_page AND TARGET handle_table)
    target_link_libraries(system_handle INTERFACE handle_page handle_table)
endif()

# Conditionally link to handle_index (Lab04)
if(USE_GOLD_LAB04)
    target_link_libraries(system_handle INTERFACE handle_index)
elseif(TARGET handle_index)
    target_link_libraries(system_handle INTERFACE handle_index)
endif()
