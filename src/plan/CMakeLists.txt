add_library(planner SHARED planner.cpp)
target_link_libraries(planner server_common system_handle)