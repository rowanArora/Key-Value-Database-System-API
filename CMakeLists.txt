cmake_minimum_required(VERSION 3.0)
project(csc443_group_project)

# Set C++ standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g")

# Set build type (Debug or Release)
set(CMAKE_BUILD_TYPE Release)

# Directories
set(SRC_DIR "${CMAKE_CURRENT_SOURCE_DIR}/src")
set(TEST_DIR "${CMAKE_CURRENT_SOURCE_DIR}/tests")
set(EXPERIMENT_DIR "${CMAKE_CURRENT_SOURCE_DIR}/experiments")
set(INCLUDE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/include")

# Include directories
include_directories(${INCLUDE_DIR})
include_directories(${SRC_DIR})
include_directories(${TEST_DIR})
include_directories(${EXPERIMENT_DIR})

# Source files
file(GLOB SRCFILES "${SRC_DIR}/*.cpp")
file(GLOB TESTFILES "${TEST_DIR}/*.cpp")
file(GLOB EXPERIMENTFILES "${EXPERIMENT_DIR}/*.cpp")

# Shared sources
set(SHARED_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/globals.cpp" "${TEST_DIR}/test_helpers.cpp")

# Main project sources
set(MAIN_SOURCES ${SRCFILES} "${CMAKE_CURRENT_SOURCE_DIR}/main.cpp")
list(REMOVE_ITEM MAIN_SOURCES "${TEST_DIR}/tests_main.cpp")

# Main executable
add_executable(main_project ${MAIN_SOURCES} ${SHARED_SOURCES})

# Test executable
set(TEST_SOURCES ${SRCFILES} ${TESTFILES} ${SHARED_SOURCES})
list(REMOVE_ITEM TEST_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/main.cpp")
add_executable(tests_main ${TEST_SOURCES})

# Experiment executables
add_executable(experiment1 ${EXPERIMENT_DIR}/binary_search_vs_btree.cpp ${SRCFILES} ${SHARED_SOURCES})
add_executable(experiment2 ${EXPERIMENT_DIR}/experiments_step3.cpp ${SRCFILES} ${SHARED_SOURCES})

# Output messages for debugging
message(STATUS "Source files: ${SRCFILES}")
message(STATUS "Test files: ${TESTFILES}")
message(STATUS "Experiment files: ${EXPERIMENTFILES}")
