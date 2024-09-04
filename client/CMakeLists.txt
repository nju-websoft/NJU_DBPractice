add_executable(client
        client.cpp)

target_link_libraries(client common_net)
target_link_libraries(client readline)