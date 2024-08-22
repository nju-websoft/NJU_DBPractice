# cmake minimum required version
cmake_minimum_required(VERSION 3.16)

set(CMAKE_CXX_STANDARD 20)

project(wsdb)

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

set(CMAKE_CXX_FLAGS "-Wall -O3 -fPIC")

set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -Wall -O0 -g -fPIC")
set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -Wall -O0 -g -fPIC")


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