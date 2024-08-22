
add_subdirectory(common)
add_subdirectory(parser)
add_subdirectory(analyse)
add_subdirectory(concurrency)
add_subdirectory(log)
add_subdirectory(execution)
add_subdirectory(optimizer)
add_subdirectory(plan)
add_subdirectory(storage)
add_subdirectory(system)
add_subdirectory(expr)
add_subdirectory(net)

set(SHARED_LIBS
        system
)

add_executable(${PROJECT_NAME} main.cpp)
target_link_libraries(${PROJECT_NAME} ${SHARED_LIBS} pthread)