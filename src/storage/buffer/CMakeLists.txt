set(SOURCES
        buffer_pool_manager.cpp
        replacer/lru_replacer.cpp
        replacer/lru_k_replacer.cpp
        replacer/replacer.cpp
)

add_library(storage_buffer SHARED ${SOURCES})
target_link_libraries(storage_buffer storage_disk fmt::fmt)