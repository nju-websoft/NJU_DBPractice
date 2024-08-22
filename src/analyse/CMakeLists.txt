add_library(analyser SHARED analyser.cpp)
target_link_libraries(analyser parser server_common system_handle)