# cmake minimum required version
cmake_minimum_required(VERSION 3.16)

set(CMAKE_CXX_STANDARD 20)

project(njudb)

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

# set debug or release
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Debug)
endif()

set(CMAKE_CXX_FLAGS "-Wall -O3 -fPIC")

set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -Wall -O0 -g -fPIC")
set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -Wall -O0 -g -fPIC")

# Lab configuration options - allows users to choose between source compilation and gold libraries
option(USE_GOLD_LAB01 "Use gold library for Lab01 (Buffer Pool)" OFF)
option(USE_GOLD_LAB02 "Use gold library for Lab02 (Executor Basic)" OFF)
option(USE_GOLD_LAB03 "Use gold library for Lab03 (Executor Analysis)" OFF)
option(USE_GOLD_LAB04 "Use gold library for Lab04 (Executor Index & Storage Index)" OFF)
option(USE_GOLD_LAB05 "Use gold libraries for Lab05 (Concurrency and MVCC Bonus)" OFF)
option(USE_GOLD_LAB06 "Use gold libraries for Lab06 (WAL and Recovery)" OFF)
option(ENABLE_LAB05_MVCC "Enable the Lab05 MVCC bonus in the NJUDB server" OFF)
option(ENABLE_LAB06_WAL "Enable Lab06 WAL, checkpoint and recovery in the NJUDB server" OFF)

if(ENABLE_LAB05_MVCC)
    add_compile_definitions(NJUDB_ENABLE_LAB05_MVCC=1)
else()
    add_compile_definitions(NJUDB_ENABLE_LAB05_MVCC=0)
endif()

if(ENABLE_LAB06_WAL)
    add_compile_definitions(NJUDB_ENABLE_LAB06_WAL=1)
else()
    add_compile_definitions(NJUDB_ENABLE_LAB06_WAL=0)
endif()

# Detect platform
if(APPLE)
    set(LIB_GOLD_PLATFORM "macos")
elseif(UNIX)
    set(LIB_GOLD_PLATFORM "linux")
else()
    message(FATAL_ERROR "Unsupported platform")
endif()

# Set build type for gold libraries
string(TOLOWER ${CMAKE_BUILD_TYPE} BUILD_TYPE_LOWER)
set(LIB_GOLD_PATH "${CMAKE_SOURCE_DIR}/lib-gold/${LIB_GOLD_PLATFORM}/${BUILD_TYPE_LOWER}")

# Check if gold libraries exist when needed
if(USE_GOLD_LAB01 OR USE_GOLD_LAB02 OR USE_GOLD_LAB03 OR USE_GOLD_LAB04 OR USE_GOLD_LAB05 OR USE_GOLD_LAB06)
    if(NOT EXISTS ${LIB_GOLD_PATH})
        message(FATAL_ERROR "Gold library path does not exist: ${LIB_GOLD_PATH}")
    endif()
    
    # Add gold library path to RPATH for runtime library resolution
    # Prefer a target compiled in this build. Gold libraries may themselves
    # depend on another lab's logical target name; putting the gold directory
    # first would silently replace the source lab under test at load time.
    set(CMAKE_INSTALL_RPATH "${CMAKE_LIBRARY_OUTPUT_DIRECTORY};${LIB_GOLD_PATH}")
    set(CMAKE_BUILD_RPATH "${CMAKE_LIBRARY_OUTPUT_DIRECTORY};${LIB_GOLD_PATH}")
    set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)
    
    # Suppress RPATH warnings for mixed gold/source configurations
    # These warnings are generally safe when using compatible libraries
    set(CMAKE_SUPPRESS_REGENERATION TRUE)
endif()

# Print configuration
message(STATUS "NJUDB Lab Configuration:")
message(STATUS "  Lab01 (Buffer Pool): ${USE_GOLD_LAB01}")
message(STATUS "  Lab02 (Executor Basic): ${USE_GOLD_LAB02}")
message(STATUS "  Lab03 (Executor Analysis): ${USE_GOLD_LAB03}")
message(STATUS "  Lab04 (Executor Index & Storage Index): ${USE_GOLD_LAB04}")
message(STATUS "  Lab05 (Concurrency and MVCC Bonus): ${USE_GOLD_LAB05}")
message(STATUS "  Lab06 (WAL and Recovery): ${USE_GOLD_LAB06}")
message(STATUS "  Lab05 MVCC Bonus: ${ENABLE_LAB05_MVCC}")
message(STATUS "  Lab06 WAL/Recovery: ${ENABLE_LAB06_WAL}")
message(STATUS "  Platform: ${LIB_GOLD_PLATFORM}")
message(STATUS "  Build Type: ${CMAKE_BUILD_TYPE}")
if(USE_GOLD_LAB01 OR USE_GOLD_LAB02 OR USE_GOLD_LAB03 OR USE_GOLD_LAB04 OR USE_GOLD_LAB05 OR USE_GOLD_LAB06)
    message(STATUS "  Gold Library Path: ${LIB_GOLD_PATH}")
    message(STATUS "  Note: RPATH warnings about circular dependencies are generally safe and can be ignored")
endif()

# Include the lab configuration helper
list(APPEND CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake")
include(NJUDBLabConfig)

# Pre-declare gold libraries if needed
njudb_predeclare_gold_libraries()

include_directories(src)
include_directories(third_party/fmt/include)
include_directories(third_party/argparse/include)

add_subdirectory(src)
add_subdirectory(client)
add_subdirectory(common)

set(INSTALL_GTEST OFF)
add_subdirectory(third_party/googletest)

add_subdirectory(third_party/fmt)

enable_testing()
add_subdirectory(test)
