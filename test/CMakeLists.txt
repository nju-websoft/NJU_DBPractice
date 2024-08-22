include_directories(${PROJECT_SOURCE_DIR}/third_party/googletest)

add_executable(hello_test hello.cpp)
target_link_libraries(hello_test gtest)

add_executable(replacer_test storage/replacer_test.cpp)
target_link_libraries(replacer_test storage_buffer gtest)
add_executable(buffer_pool_test storage/buffer_pool_manager_test.cpp)
target_link_libraries(buffer_pool_test storage_buffer storage_disk fmt::fmt gtest)

add_executable(table_handle_test system/table_handle_test.cpp)
target_link_libraries(table_handle_test system_handle gtest)