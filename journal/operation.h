#pragma once

#include "primitives.h"
#include <exception>

namespace kronuz::journal {

struct MutationToken {
	Identity owner{};
	std::uint64_t operation = 0, step = 0;
	bool operator==(const MutationToken &) const = default;
};

enum class PrimitiveKind { Write, Sync, Create, Replace, DirectorySync, Read };

// Views belong to the operation. An accepted driver retains that operation
// until the original primitive completes, including after cancellation.
struct MutationRequest {
	MutationToken token;
	PrimitiveKind kind = PrimitiveKind::Sync;
	std::shared_ptr<File> file;
	std::uint64_t offset = 0;
	std::string_view bytes, source, destination;
	std::span<char> destination_bytes;
};
struct MutationCompletion {
	MutationToken token;
	std::size_t count = 0;
	std::unique_ptr<File> file;
	std::exception_ptr error;
};

class IOOperation {
  public:
	virtual ~IOOperation() = default;
	virtual const MutationRequest &request() const = 0;
	virtual void submitted() = 0;
	virtual bool complete(MutationCompletion completion) noexcept = 0;
	virtual bool done() const noexcept = 0;
	virtual bool in_flight() const noexcept = 0;
	virtual IO &io() const noexcept = 0;
};

namespace detail {
inline MutationCompletion execute_primitive(IO &io, const MutationRequest &request) noexcept {
	MutationCompletion result;
	result.token = request.token;
	try {
		switch (request.kind) {
		case PrimitiveKind::Read:
			result.count = request.file->read_at(request.offset, request.destination_bytes);
			break;
		case PrimitiveKind::Write:
			result.count = request.file->write_at(request.offset, request.bytes);
			break;
		case PrimitiveKind::Sync:
			request.file->sync();
			break;
		case PrimitiveKind::Create:
			result.file = io.create_exclusive(request.source);
			break;
		case PrimitiveKind::Replace:
			io.replace(request.source, request.destination);
			break;
		case PrimitiveKind::DirectorySync:
			io.sync_directory();
			break;
		}
	} catch (...) {
		result.error = std::current_exception();
	}
	return result;
}
} // namespace detail

inline void drive_synchronously(IOOperation &operation) {
	while (!operation.done()) {
		operation.submitted();
		operation.complete(detail::execute_primitive(operation.io(), operation.request()));
	}
}
} // namespace kronuz::journal
