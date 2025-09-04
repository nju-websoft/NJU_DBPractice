# Lab01: Buffer Pool
njudb_should_compile_from_source(COMPILE_FROM_SOURCE "01")
if(COMPILE_FROM_SOURCE)
    set(SOURCES
            buffer_pool_manager.cpp
            page_guard.cpp
            replacer/lru_replacer.cpp
            replacer/lru_k_replacer.cpp
            replacer/replacer.cpp
    )

    add_library(storage_buffer SHARED ${SOURCES})
    target_link_libraries(storage_buffer storage_disk fmt::fmt)
endif()