# add dynamic libraries
add_library(common_net SHARED net/net.cpp)
target_link_libraries(common_net fmt::fmt)