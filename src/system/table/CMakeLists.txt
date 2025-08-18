add_library(system_table SHARED
        table_manager.cpp
)

target_link_libraries(system_table handle_table)