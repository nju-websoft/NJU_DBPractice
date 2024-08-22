set(SOURCES disk_manager.cpp)
add_library(storage_disk SHARED ${SOURCES})
target_link_libraries(storage_disk fmt::fmt)