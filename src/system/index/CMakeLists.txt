add_library(system_index SHARED
        index_manager.cpp)

target_link_libraries(system_index handle_index)