# Lab02: Executor Basic
njudb_should_compile_from_source(COMPILE_BASIC_FROM_SOURCE "02")
if(COMPILE_BASIC_FROM_SOURCE)
    set(BASIC_SOURCES
            executor_ddl.cpp
            executor_delete.cpp
            executor_seqscan.cpp
            executor_insert.cpp
            executor_filter.cpp
            executor_projection.cpp
            executor_update.cpp
            executor_sort.cpp
            executor_limit.cpp
    )

    add_library(executor_basic SHARED ${BASIC_SOURCES})
    target_link_libraries(executor_basic handle_db)
endif()

# Lab03: Executor Analysis
njudb_should_compile_from_source(COMPILE_ANALYSIS_FROM_SOURCE "03")
if(COMPILE_ANALYSIS_FROM_SOURCE)
    set(ANALYSIS_SOURCES
            executor_join.cpp
            executor_join_nestedloop.cpp
            executor_join_hash.cpp
            executor_join_sortmerge.cpp
            executor_aggregate.cpp
    )

    add_library(executor_analysis SHARED ${ANALYSIS_SOURCES})
    target_link_libraries(executor_analysis handle_db expr)
endif()

# Lab04: Executor Index (part of Lab04)
njudb_should_compile_from_source(COMPILE_INDEX_FROM_SOURCE "04")
if(COMPILE_INDEX_FROM_SOURCE)
    set(INDEX_EXECUTION_SOURCES
            executor_idxscan.cpp
    )

    add_library(executor_index SHARED ${INDEX_EXECUTION_SOURCES})
    target_link_libraries(executor_index handle_db expr)
endif()

# Execution library that aggregates all executors
add_library(execution SHARED executor.cpp)

# Always link to basic dependencies first
target_link_libraries(execution server_net expr handle_db)

# Determine which executor libraries are available and link appropriately
# This avoids circular dependency issues by not mixing gold and source library paths
set(EXECUTOR_LIBS "")

# Check each executor library and add to list if available
if(USE_GOLD_LAB02)
    list(APPEND EXECUTOR_LIBS executor_basic)
elseif(TARGET executor_basic)
    list(APPEND EXECUTOR_LIBS executor_basic)
endif()

if(USE_GOLD_LAB03)
    list(APPEND EXECUTOR_LIBS executor_analysis)
elseif(TARGET executor_analysis)
    list(APPEND EXECUTOR_LIBS executor_analysis)
endif()

if(USE_GOLD_LAB04)
    list(APPEND EXECUTOR_LIBS executor_index)
elseif(TARGET executor_index)
    list(APPEND EXECUTOR_LIBS executor_index)
endif()

# Link all available executor libraries
if(EXECUTOR_LIBS)
    target_link_libraries(execution ${EXECUTOR_LIBS})
endif()

message(STATUS "Execution library linking to: ${EXECUTOR_LIBS}")