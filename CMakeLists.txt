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

# --- varint: the length/string/bool/char wire codec behind length.h ---
FetchContent_Declare(varint GIT_REPOSITORY https://github.com/Kronuz/varint.git GIT_TAG main)
FetchContent_MakeAvailable(varint)

# --- the cluster substrate (header-only) ---
add_library(cluster INTERFACE)
target_include_directories(cluster INTERFACE "${CMAKE_CURRENT_SOURCE_DIR}")
target_compile_features(cluster INTERFACE cxx_std_20)
target_link_libraries(cluster INTERFACE reactor::reactor varint::varint)
add_library(cluster::cluster ALIAS cluster)

# Generic durable storage has no reactor, socket, or consensus dependency.
add_subdirectory(journal)
add_subdirectory(consensus)

# --- example + benchmark + tests (built only when this repo is top-level) ---
if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
	enable_testing()
	file(MAKE_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/.scratch")

	# A runnable gossip demo (three nodes announce themselves over the bus).
	add_executable(cluster_gossip examples/gossip.cc)
	target_link_libraries(cluster_gossip PRIVATE cluster)

	# A Raft demo: five in-memory nodes elect a leader + replicate a command.
	add_executable(cluster_raft_election examples/raft_election.cc)
	target_link_libraries(cluster_raft_election PRIVATE cluster)

	# The per-message overhead the bus adds (framing + parse/validate).
	add_executable(cluster_bus_bench benchmarks/bus_bench.cc)
	target_link_libraries(cluster_bus_bench PRIVATE cluster)

	# Raft election latency over many trials (consensus formation time).
	add_executable(cluster_raft_bench benchmarks/raft_bench.cc)
	target_link_libraries(cluster_raft_bench PRIVATE cluster)
	target_include_directories(cluster_raft_bench PRIVATE examples)   # mem_cluster.h support

	add_executable(cluster_bus_test test/bus_test.cc)
	target_link_libraries(cluster_bus_test PRIVATE cluster)
	add_test(NAME cluster_bus COMMAND cluster_bus_test)
	set_tests_properties(cluster_bus PROPERTIES TIMEOUT 60)

	add_executable(cluster_raft_test test/raft_test.cc)
	target_link_libraries(cluster_raft_test PRIVATE cluster)
	add_test(NAME cluster_raft COMMAND cluster_raft_test)
	set_tests_properties(cluster_raft PROPERTIES TIMEOUT 60)

	add_executable(cluster_raft_safety_test test/raft_safety_test.cc)
	target_link_libraries(cluster_raft_safety_test PRIVATE cluster)
	add_test(NAME cluster_raft_safety COMMAND cluster_raft_safety_test)
	set_tests_properties(cluster_raft_safety PROPERTIES TIMEOUT 10)

	add_executable(cluster_journal_test test/journal_test.cc)
	target_link_libraries(cluster_journal_test PRIVATE cluster::journal)
	add_test(NAME cluster_journal COMMAND cluster_journal_test)
	set_tests_properties(cluster_journal PROPERTIES TIMEOUT 30)
	add_executable(cluster_inventory_test test/inventory_test.cc)
	target_link_libraries(cluster_inventory_test PRIVATE cluster::journal)
	add_test(NAME cluster_inventory COMMAND cluster_inventory_test)
	set_tests_properties(cluster_inventory PROPERTIES TIMEOUT 30)
	add_executable(cluster_admission_test test/admission_test.cc)
	target_link_libraries(cluster_admission_test PRIVATE cluster::journal)
	add_test(NAME cluster_admission COMMAND cluster_admission_test)
	set_tests_properties(cluster_admission PROPERTIES TIMEOUT 30)

	add_executable(cluster_metrics_bench benchmarks/metrics_bench.cc)
	target_link_libraries(cluster_metrics_bench PRIVATE cluster::consensus)
	add_executable(cluster_metrics_test test/metrics_test.cc)
	target_link_libraries(cluster_metrics_test PRIVATE cluster::consensus Threads::Threads)
	add_test(NAME cluster_metrics COMMAND cluster_metrics_test)
	set_tests_properties(cluster_metrics PROPERTIES TIMEOUT 10)
	add_executable(cluster_checkpoint_encoder_test test/checkpoint_encoder_test.cc)
	target_link_libraries(cluster_checkpoint_encoder_test PRIVATE cluster::consensus)
	add_test(NAME cluster_checkpoint_encoder COMMAND cluster_checkpoint_encoder_test)
	set_tests_properties(cluster_checkpoint_encoder PROPERTIES TIMEOUT 30)
	add_executable(cluster_retained_log_test test/retained_log_test.cc)
	target_link_libraries(cluster_retained_log_test PRIVATE cluster::consensus)
	add_test(NAME cluster_retained_log COMMAND cluster_retained_log_test)
	set_tests_properties(cluster_retained_log PROPERTIES TIMEOUT 30)

	add_executable(cluster_consensus_test test/consensus_test.cc)
	target_link_libraries(cluster_consensus_test PRIVATE cluster::consensus)
	add_test(NAME cluster_consensus COMMAND cluster_consensus_test)
	set_tests_properties(cluster_consensus PROPERTIES TIMEOUT 30)
	add_executable(cluster_worker_test test/worker_test.cc)
	target_link_libraries(cluster_worker_test PRIVATE cluster::consensus)
	add_test(NAME cluster_worker COMMAND cluster_worker_test)
	set_tests_properties(cluster_worker PROPERTIES TIMEOUT 180)
	add_test(NAME cluster_worker_terminal COMMAND cluster_worker_test --terminal-regressions)
	set_tests_properties(cluster_worker_terminal PROPERTIES TIMEOUT 30)
	if(APPLE OR CMAKE_SYSTEM_NAME STREQUAL "FreeBSD" OR CMAKE_SYSTEM_NAME STREQUAL "Linux")
		add_executable(cluster_completion_test test/completion_test.cc)
		target_link_libraries(cluster_completion_test PRIVATE cluster::journal_completion)
		add_test(NAME cluster_completion COMMAND cluster_completion_test)
		set_tests_properties(cluster_completion PROPERTIES TIMEOUT 30)
		add_executable(cluster_worker_completion_bench EXCLUDE_FROM_ALL consensus/worker_completion_bench.cc)
		target_link_libraries(cluster_worker_completion_bench PRIVATE cluster::journal_completion reactor::reactor)
		add_executable(cluster_completion_asio_test test/completion_asio_test.cc)
		target_link_libraries(cluster_completion_asio_test PRIVATE cluster::journal_completion reactor::reactor)
		add_test(NAME cluster_completion_asio COMMAND cluster_completion_asio_test)
		set_tests_properties(cluster_completion_asio PROPERTIES TIMEOUT 30)
	endif()
	if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
		add_executable(cluster_linux_completion_test test/linux_completion_test.cc)
		target_link_libraries(cluster_linux_completion_test PRIVATE cluster::journal_completion)
		add_test(NAME cluster_linux_completion COMMAND cluster_linux_completion_test)
		set_tests_properties(cluster_linux_completion PROPERTIES TIMEOUT 30)
	endif()


	add_executable(cluster_liveness_test test/liveness_test.cc)
	target_link_libraries(cluster_liveness_test PRIVATE cluster)
	add_test(NAME cluster_liveness COMMAND cluster_liveness_test)
	set_tests_properties(cluster_liveness PROPERTIES TIMEOUT 60)
endif()
