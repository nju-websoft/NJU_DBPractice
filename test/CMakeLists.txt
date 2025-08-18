include_directories(${PROJECT_SOURCE_DIR}/third_party/googletest/googletest/include)
include_directories(${PROJECT_SOURCE_DIR}/third_party/googletest/googlemock/include)

add_executable(hello_test hello.cpp)
target_link_libraries(hello_test gtest fmt::fmt)

add_executable(replacer_test storage/replacer_test.cpp)
# Determine which storage_buffer library to use
if(USE_GOLD_LAB01)
    target_link_libraries(replacer_test storage_buffer gtest)
elseif(TARGET storage_buffer)
    target_link_libraries(replacer_test storage_buffer gtest)
else()
    message(FATAL_ERROR "storage_buffer library is not available")
endif()

add_executable(buffer_pool_test storage/buffer_pool_manager_test.cpp)
# Determine which storage_buffer library to use
if(USE_GOLD_LAB01)
    target_link_libraries(buffer_pool_test storage_buffer storage_disk fmt::fmt gtest)
elseif(TARGET storage_buffer)
    target_link_libraries(buffer_pool_test storage_buffer storage_disk fmt::fmt gtest)
else()
    message(FATAL_ERROR "storage_buffer library is not available")
endif()

add_executable(page_guard_test storage/page_guard_test.cpp)
# Determine which storage_buffer library to use
if(USE_GOLD_LAB01)
    target_link_libraries(page_guard_test storage_buffer storage_disk gtest)
elseif(TARGET storage_buffer)
    target_link_libraries(page_guard_test storage_buffer storage_disk gtest)
else()
    message(FATAL_ERROR "storage_buffer library is not available")
endif()

add_executable(table_handle_test system/table_handle_test.cpp)
# system_handle and system_table are always compiled from source (not lab-specific)
target_link_libraries(table_handle_test handle_page handle_table gtest system_table)
if(USE_GOLD_LAB01)
    target_link_libraries(table_handle_test handle_page handle_table gtest system_table)
elseif(TARGET handle_table)
    target_link_libraries(table_handle_test handle_page handle_table gtest system_table)
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
