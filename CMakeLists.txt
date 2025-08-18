# cmake minimum required version
cmake_minimum_required(VERSION 3.16)

set(CMAKE_CXX_STANDARD 20)

project(wsdb)

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
if(USE_GOLD_LAB01 OR USE_GOLD_LAB02 OR USE_GOLD_LAB03 OR USE_GOLD_LAB04)
    if(NOT EXISTS ${LIB_GOLD_PATH})
        message(FATAL_ERROR "Gold library path does not exist: ${LIB_GOLD_PATH}")
    endif()
    
    # Add gold library path to RPATH for runtime library resolution
    # Prioritize locally built libraries by putting CMAKE_LIBRARY_OUTPUT_DIRECTORY first
    set(CMAKE_INSTALL_RPATH "${CMAKE_LIBRARY_OUTPUT_DIRECTORY};${LIB_GOLD_PATH}")
    set(CMAKE_BUILD_RPATH "${CMAKE_LIBRARY_OUTPUT_DIRECTORY};${LIB_GOLD_PATH}")
    set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)
    
    # Suppress RPATH warnings for mixed gold/source configurations
    # These warnings are generally safe when using compatible libraries
    set(CMAKE_SUPPRESS_REGENERATION TRUE)
endif()

# Print configuration
message(STATUS "WSDB Lab Configuration:")
message(STATUS "  Lab01 (Buffer Pool): ${USE_GOLD_LAB01}")
message(STATUS "  Lab02 (Executor Basic): ${USE_GOLD_LAB02}")
message(STATUS "  Lab03 (Executor Analysis): ${USE_GOLD_LAB03}")
message(STATUS "  Lab04 (Executor Index & Storage Index): ${USE_GOLD_LAB04}")
message(STATUS "  Platform: ${LIB_GOLD_PLATFORM}")
message(STATUS "  Build Type: ${CMAKE_BUILD_TYPE}")
if(USE_GOLD_LAB01 OR USE_GOLD_LAB02 OR USE_GOLD_LAB03 OR USE_GOLD_LAB04)
    message(STATUS "  Gold Library Path: ${LIB_GOLD_PATH}")
endif()

# Include the lab configuration helper
list(APPEND CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake")
include(WSDBLabConfig)

# Pre-declare gold libraries if needed
wsdb_predeclare_gold_libraries()

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