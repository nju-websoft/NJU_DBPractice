set(SOURCES
        executor.cpp
        executor_ddl.cpp
        executor_delete.cpp
        executor_seqscan.cpp
        executor_idxscan.cpp
        executor_insert.cpp
        executor_filter.cpp
        executor_projection.cpp
        executor_update.cpp
        executor_join.cpp
        executor_join_nestedloop.cpp
        executor_join_sortmerge.cpp
        executor_aggregate.cpp
        executor_sort.cpp
        executor_limit.cpp
)

add_library(execution SHARED ${SOURCES})
target_link_libraries(execution system_handle expr server_net)