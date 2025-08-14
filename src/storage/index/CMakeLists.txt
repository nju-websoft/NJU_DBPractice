add_library(storage_index SHARED index_abstract.cpp index_bptree.cpp index_hash.cpp)

target_link_libraries(storage_index storage_buffer fmt::fmt)