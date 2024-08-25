add_subdirectory(handle)
add_subdirectory(table)
add_subdirectory(index)

add_library(system SHARED
        system.cpp
)
target_link_libraries(system
        parser
        planner
        optimizer
        execution
        log
        concurrency
        server_net
)
