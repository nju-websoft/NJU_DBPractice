add_library(storage_index SHARED index_abstract.cpp index_bp_tree.cpp index_hash.cpp)

target_link_libraries(storage_index fmt::fmt)