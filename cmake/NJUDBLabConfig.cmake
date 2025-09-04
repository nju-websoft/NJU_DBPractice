# NJUDB Lab Configuration Helper
# This module provides functions to conditionally link against gold libraries or source libraries

# Function to conditionally create or link library targets based on lab configuration
function(njudb_add_lab_library TARGET_NAME LAB_NUMBER GOLD_LIB_NAME)
    set(USE_GOLD_VAR "USE_GOLD_LAB${LAB_NUMBER}")
    
    if(${${USE_GOLD_VAR}})
        # Use gold library
        set(GOLD_LIB_PATH "${LIB_GOLD_PATH}/${GOLD_LIB_NAME}")
        if(EXISTS ${GOLD_LIB_PATH})
            add_library(${TARGET_NAME} SHARED IMPORTED GLOBAL)
            set_target_properties(${TARGET_NAME} PROPERTIES
                IMPORTED_LOCATION ${GOLD_LIB_PATH}
            )
            # Set the target as available for linking
            set_target_properties(${TARGET_NAME} PROPERTIES
                INTERFACE_INCLUDE_DIRECTORIES "${CMAKE_SOURCE_DIR}/src"
            )
            message(STATUS "Using gold library for ${TARGET_NAME}: ${GOLD_LIB_PATH}")
        else()
            message(FATAL_ERROR "Gold library not found: ${GOLD_LIB_PATH}")
        endif()
    else()
        # Will be created from source in the subdirectory
        message(STATUS "Compiling ${TARGET_NAME} from source")
    endif()
endfunction()

# Function to check if we should compile a lab component from source
function(njudb_should_compile_from_source RESULT_VAR LAB_NUMBER)
    set(USE_GOLD_VAR "USE_GOLD_LAB${LAB_NUMBER}")
    if(${${USE_GOLD_VAR}})
        set(${RESULT_VAR} FALSE PARENT_SCOPE)
    else()
        set(${RESULT_VAR} TRUE PARENT_SCOPE)
    endif()
endfunction()

# Function to get the appropriate library extension based on platform
function(njudb_get_lib_extension RESULT_VAR)
    if(APPLE)
        set(${RESULT_VAR} "dylib" PARENT_SCOPE)
    elseif(UNIX)
        set(${RESULT_VAR} "so" PARENT_SCOPE)
    else()
        set(${RESULT_VAR} "dll" PARENT_SCOPE)
    endif()
endfunction()

# Function to pre-declare all gold libraries
function(njudb_predeclare_gold_libraries)
    njudb_get_lib_extension(LIB_EXT)
    
    # Set up RPATH for mixed gold/source library configurations
    set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE PARENT_SCOPE)
    set(CMAKE_BUILD_WITH_INSTALL_RPATH FALSE PARENT_SCOPE)
    
    # Lab01: Buffer Pool
    if(USE_GOLD_LAB01)
        set(GOLD_LIB_PATH "${LIB_GOLD_PATH}/libstorage_buffer.${LIB_EXT}")
        if(EXISTS ${GOLD_LIB_PATH})
            add_library(storage_buffer SHARED IMPORTED GLOBAL)
            set_target_properties(storage_buffer PROPERTIES
                IMPORTED_LOCATION ${GOLD_LIB_PATH}
                INTERFACE_INCLUDE_DIRECTORIES "${CMAKE_SOURCE_DIR}/src"
                IMPORTED_NO_SONAME ON
            )
            # Add dependencies for gold library
            target_link_libraries(storage_buffer INTERFACE storage_disk fmt::fmt)
            message(STATUS "Pre-declared gold library for storage_buffer: ${GOLD_LIB_PATH}")
        else()
            message(FATAL_ERROR "Gold library not found: ${GOLD_LIB_PATH}")
        endif()

        # handle_page
        set(GOLD_LIB_PATH "${LIB_GOLD_PATH}/libhandle_page.${LIB_EXT}")
        if(EXISTS ${GOLD_LIB_PATH})
            add_library(handle_page SHARED IMPORTED GLOBAL)
            set_target_properties(handle_page PROPERTIES
                IMPORTED_LOCATION ${GOLD_LIB_PATH}
                INTERFACE_INCLUDE_DIRECTORIES "${CMAKE_SOURCE_DIR}/src"
                IMPORTED_NO_SONAME ON
            )
            # Add dependencies for gold library
            target_link_libraries(handle_page INTERFACE storage_disk storage_buffer storage_index fmt::fmt)
            message(STATUS "Pre-declared gold library for handle_page: ${GOLD_LIB_PATH}")
        else()
            message(FATAL_ERROR "Gold library not found: ${GOLD_LIB_PATH}")
        endif()
        
        # handle_table
        set(GOLD_LIB_PATH "${LIB_GOLD_PATH}/libhandle_table.${LIB_EXT}")
        if(EXISTS ${GOLD_LIB_PATH})
            add_library(handle_table SHARED IMPORTED GLOBAL)
            set_target_properties(handle_table PROPERTIES
                IMPORTED_LOCATION ${GOLD_LIB_PATH}
                INTERFACE_INCLUDE_DIRECTORIES "${CMAKE_SOURCE_DIR}/src"
                IMPORTED_NO_SONAME ON
            )
            # Add dependencies for gold library
            target_link_libraries(handle_table INTERFACE handle_page storage_disk storage_buffer storage_index fmt::fmt)
            message(STATUS "Pre-declared gold library for handle_table: ${GOLD_LIB_PATH}")
        else()
            message(FATAL_ERROR "Gold library not found: ${GOLD_LIB_PATH}")
        endif()

    endif()
    
    # Lab02: Executor Basic
    if(USE_GOLD_LAB02)
        set(GOLD_LIB_PATH "${LIB_GOLD_PATH}/libexecutor_basic.${LIB_EXT}")
        if(EXISTS ${GOLD_LIB_PATH})
            add_library(executor_basic SHARED IMPORTED GLOBAL)
            set_target_properties(executor_basic PROPERTIES
                IMPORTED_LOCATION ${GOLD_LIB_PATH}
                INTERFACE_INCLUDE_DIRECTORIES "${CMAKE_SOURCE_DIR}/src"
                IMPORTED_NO_SONAME ON
            )
            message(STATUS "Pre-declared gold library for executor_basic: ${GOLD_LIB_PATH}")
        else()
            message(FATAL_ERROR "Gold library not found: ${GOLD_LIB_PATH}")
        endif()
    endif()
    
    # Lab03: Executor Analysis
    if(USE_GOLD_LAB03)
        set(GOLD_LIB_PATH "${LIB_GOLD_PATH}/libexecutor_analysis.${LIB_EXT}")
        if(EXISTS ${GOLD_LIB_PATH})
            add_library(executor_analysis SHARED IMPORTED GLOBAL)
            set_target_properties(executor_analysis PROPERTIES
                IMPORTED_LOCATION ${GOLD_LIB_PATH}
                INTERFACE_INCLUDE_DIRECTORIES "${CMAKE_SOURCE_DIR}/src"
                IMPORTED_NO_SONAME ON
            )
            message(STATUS "Pre-declared gold library for executor_analysis: ${GOLD_LIB_PATH}")
        else()
            message(FATAL_ERROR "Gold library not found: ${GOLD_LIB_PATH}")
        endif()
    endif()
    
    # Lab04: Executor Index & Storage Index
    if(USE_GOLD_LAB04)
        # storage_index
        set(GOLD_LIB_PATH "${LIB_GOLD_PATH}/libstorage_index.${LIB_EXT}")
        if(EXISTS ${GOLD_LIB_PATH})
            add_library(storage_index SHARED IMPORTED GLOBAL)
            set_target_properties(storage_index PROPERTIES
                IMPORTED_LOCATION ${GOLD_LIB_PATH}
                INTERFACE_INCLUDE_DIRECTORIES "${CMAKE_SOURCE_DIR}/src"
                IMPORTED_NO_SONAME ON
            )
            # Add dependencies for gold library
            target_link_libraries(storage_index INTERFACE storage_buffer fmt::fmt)
            message(STATUS "Pre-declared gold library for storage_index: ${GOLD_LIB_PATH}")
        else()
            message(FATAL_ERROR "Gold library not found: ${GOLD_LIB_PATH}")
        endif()
        
        # executor_index
        set(GOLD_LIB_PATH "${LIB_GOLD_PATH}/libexecutor_index.${LIB_EXT}")
        if(EXISTS ${GOLD_LIB_PATH})
            add_library(executor_index SHARED IMPORTED GLOBAL)
            set_target_properties(executor_index PROPERTIES
                IMPORTED_LOCATION ${GOLD_LIB_PATH}
                INTERFACE_INCLUDE_DIRECTORIES "${CMAKE_SOURCE_DIR}/src"
                IMPORTED_NO_SONAME ON
            )
            message(STATUS "Pre-declared gold library for executor_index: ${GOLD_LIB_PATH}")
        else()
            message(FATAL_ERROR "Gold library not found: ${GOLD_LIB_PATH}")
        endif()
        
        # handle_index
        set(GOLD_LIB_PATH "${LIB_GOLD_PATH}/libhandle_index.${LIB_EXT}")
        if(EXISTS ${GOLD_LIB_PATH})
            add_library(handle_index SHARED IMPORTED GLOBAL)
            set_target_properties(handle_index PROPERTIES
                IMPORTED_LOCATION ${GOLD_LIB_PATH}
                INTERFACE_INCLUDE_DIRECTORIES "${CMAKE_SOURCE_DIR}/src"
                IMPORTED_NO_SONAME ON
            )
            # Add dependencies for gold library
            target_link_libraries(handle_index INTERFACE storage_disk storage_buffer storage_index fmt::fmt)
            message(STATUS "Pre-declared gold library for handle_index: ${GOLD_LIB_PATH}")
        else()
            message(FATAL_ERROR "Gold library not found: ${GOLD_LIB_PATH}")
        endif()
    endif()
endfunction()
