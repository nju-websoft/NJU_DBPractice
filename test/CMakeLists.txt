include_directories(${PROJECT_SOURCE_DIR}/third_party/googletest)

add_executable(hello_test hello.cpp)
target_link_libraries(hello_test gtest fmt::fmt)

add_executable(replacer_test storage/replacer_test.cpp)
target_link_libraries(replacer_test storage_buffer gtest)
add_executable(buffer_pool_test storage/buffer_pool_manager_test.cpp)
target_link_libraries(buffer_pool_test storage_buffer storage_disk fmt::fmt gtest)

add_executable(page_guard_test storage/page_guard_test.cpp)
target_link_libraries(page_guard_test storage_buffer storage_disk gtest)

add_executable(b_plus_tree_test storage/bptree_test.cpp)
target_link_libraries(b_plus_tree_test storage_buffer storage_disk storage_index system_handle log gtest)

add_executable(hash_index_test storage/hashindex_test.cpp)
target_link_libraries(hash_index_test storage_buffer storage_disk storage_index system_handle log gtest)

add_executable(table_handle_test system/table_handle_test.cpp)
target_link_libraries(table_handle_test system_handle gtest system_table)