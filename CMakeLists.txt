# cluster: a UDP-multicast cluster substrate (message bus + Raft + membership gossip) on
# the Kronuz/reactor Asio runtime.
#
#   cmake -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build && ctest --test-dir build
#
# Header-only. The generic half of a gossip/discovery service: a versioned, token-scoped,
# typed multicast Bus that a Raft consensus module, a membership gossip, and an app's own
# messages all ride. The node type, the cluster state hooks, and the apply callback are
# the caller's (a search engine, another distributed store, ...). Standalone-Asio only.

cmake_minimum_required(VERSION 3.16)
project(cluster LANGUAGES CXX)

include(FetchContent)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
if(NOT CMAKE_BUILD_TYPE)
	set(CMAKE_BUILD_TYPE Release)
endif()

find_package(Threads REQUIRED)

# --- reactor: the Asio runtime (UdpServer + PeriodicTimer + Signal + the reactor pool) ---
# Provides reactor::reactor (the include path + standalone Asio + ASIO_STANDALONE + Threads).
# Set -DFETCHCONTENT_SOURCE_DIR_REACTOR=<path> to build against a local checkout.
FetchContent_Declare(reactor GIT_REPOSITORY https://github.com/Kronuz/reactor.git GIT_TAG main)
FetchContent_MakeAvailable(reactor)

# --- the cluster substrate (header-only) ---
add_library(cluster INTERFACE)
target_include_directories(cluster INTERFACE "${CMAKE_CURRENT_SOURCE_DIR}")
target_compile_features(cluster INTERFACE cxx_std_20)
target_link_libraries(cluster INTERFACE reactor::reactor)
add_library(cluster::cluster ALIAS cluster)

# --- tests (built only when this repo is top-level) ---
if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
	enable_testing()

	add_executable(cluster_bus_test test/bus_test.cc)
	target_link_libraries(cluster_bus_test PRIVATE cluster)
	add_test(NAME cluster_bus COMMAND cluster_bus_test)
	set_tests_properties(cluster_bus PROPERTIES TIMEOUT 60)
endif()
