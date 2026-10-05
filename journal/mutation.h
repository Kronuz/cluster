#pragma once

#include "artifact.h"
#include <exception>

namespace kronuz::journal {

struct MutationToken {
	Identity owner{};
	std::uint64_t operation = 0, step = 0;
	bool operator==(const MutationToken &) const = default;
};

enum class PrimitiveKind { Write, Sync, Create, Replace, DirectorySync };

// Views belong to the operation. An accepted driver retains that operation
// until the original primitive completes, including after cancellation.
struct MutationRequest {
	MutationToken token;
	PrimitiveKind kind = PrimitiveKind::Sync;
	std::shared_ptr<File> file;
	std::uint64_t offset = 0;
	std::string_view bytes, source, destination;
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
	virtual const MutationRequest& request() const = 0;
	virtual void submitted() = 0;
	virtual bool complete(MutationCompletion completion) noexcept = 0;
	virtual bool done() const noexcept = 0;
	virtual bool in_flight() const noexcept = 0;
	virtual IO& io() const noexcept = 0;
};

namespace detail {

// Shared manifest sequencing for bootstrap, append and checkpoint publication.
// The state never performs IO. A driver executes exactly the exposed primitive.
class PublicationSteps {
  public:
	PublicationSteps(std::string encoded)
		: encoded_(std::move(encoded)), temporary_("manifest.pending-" + hexadecimal(random_identity())) {}
	bool done() const noexcept { return phase_ == Phase::Done; }
	MutationRequest request() const {
		MutationRequest result;
		switch (phase_) {
		case Phase::Create:
			result.kind = PrimitiveKind::Create;
			result.source = temporary_;
			break;
		case Phase::Write:
			result.kind = PrimitiveKind::Write;
			result.file = file_;
			result.offset = written_;
			result.bytes = std::string_view(encoded_).substr(written_);
			break;
		case Phase::Sync:
			result.kind = PrimitiveKind::Sync;
			result.file = file_;
			break;
		case Phase::Replace:
			result.kind = PrimitiveKind::Replace;
			result.source = temporary_;
			result.destination = "manifest";
			break;
		case Phase::DirectorySync:
			result.kind = PrimitiveKind::DirectorySync;
			break;
		case Phase::Done:
			throw std::logic_error("publication is complete");
		}
		return result;
	}
	void complete(MutationCompletion completion) {
		if (completion.error) {
			std::rethrow_exception(completion.error);
		}
		switch (phase_) {
		case Phase::Create:
			if (!completion.file) {
				throw std::runtime_error("create completed without a file");
			}
			file_ = std::move(completion.file);
			phase_ = Phase::Write;
			break;
		case Phase::Write:
			if (!completion.count || completion.count > encoded_.size() - written_) {
				throw std::runtime_error("manifest write made invalid progress");
			}
			written_ += completion.count;
			if (written_ == encoded_.size()) {
				phase_ = Phase::Sync;
			}
			break;
		case Phase::Sync:
			phase_ = Phase::Replace;
			break;
		case Phase::Replace:
			phase_ = Phase::DirectorySync;
			break;
		case Phase::DirectorySync:
			phase_ = Phase::Done;
			break;
		case Phase::Done:
			throw std::logic_error("publication completion repeated");
		}
	}

  private:
	enum class Phase { Create, Write, Sync, Replace, DirectorySync, Done };
	std::string encoded_, temporary_;
	std::shared_ptr<File> file_;
	std::size_t written_ = 0;
	Phase phase_ = Phase::Create;
};

inline MutationCompletion execute_primitive(IO &io, const MutationRequest &request) noexcept {
	MutationCompletion result;
	result.token = request.token;
	try {
		switch (request.kind) {
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

// One append transaction, including the final manifest namespace barrier.
// Drivers retain this object through every accepted primitive. Completion and
// destruction run on the storage owner; backends only schedule completions.
class AppendMutation : public IOOperation {
  public:
	AppendMutation(const AppendMutation &) = delete;
	AppendMutation &operator=(const AppendMutation &) = delete;
	~AppendMutation() override {
		// This is a driver ownership bug, not a request for blocking cleanup.
		if (in_flight_) {
			std::terminate();
		}
		if (started_ && active_) {
			owner_->failed = true;
		}
		if (active_) {
			owner_->mutation_active = false;
		}
	}
	const MutationRequest &request() const override {
		if (done()) {
			throw std::logic_error("append mutation is complete");
		}
		return request_;
	}
	void submitted() override {
		if (in_flight_ || done()) {
			throw std::logic_error("append submission is not available");
		}
		started_ = in_flight_ = true;
	}
	bool complete(MutationCompletion completion) noexcept override {
		if (!in_flight_ || completion.token != request_.token) {
			return false;
		}
		in_flight_ = false;
		try {
			if (owner_->failed) {
				throw std::runtime_error("storage owner fenced during append");
			}
			if (completion.error) {
				std::rethrow_exception(completion.error);
			}
			if (phase_ == Phase::Publication) {
				publication_.complete(std::move(completion));
			} else if (phase_ == Phase::Sync) {
				phase_ = Phase::Publication;
			} else {
				auto &bytes = phase_ == Phase::Header ? header_ : payload_;
				if (!completion.count || completion.count > std::min<std::size_t>(detail::artifact_chunk_size,
																				  bytes.size() - written_)) {
					throw std::runtime_error("journal write made invalid progress");
				}
				written_ += completion.count;
				if (written_ == bytes.size()) {
					written_ = 0;
					phase_ = phase_ == Phase::Header && !payload_.empty() ? Phase::Payload : Phase::Sync;
				}
			}
			if (!done()) {
				++request_.token.step;
				refresh_request();
			}
		} catch (...) {
			error_ = std::current_exception();
			owner_->failed = true;
		}
		return true;
	}
	bool done() const noexcept override { return error_ || (phase_ == Phase::Publication && publication_.done()); }
	bool in_flight() const noexcept override { return in_flight_; }
	void result() const {
		if (!done() || in_flight_) {
			throw std::logic_error("append has not completed");
		}
		if (error_) {
			std::rethrow_exception(error_);
		}
	}
	IO &io() const noexcept override { return *io_; }

  private:
	friend class Journal;
	void settled() noexcept {
		owner_->mutation_active = false;
		active_ = false;
	}
	AppendMutation(std::shared_ptr<IO> io, std::shared_ptr<detail::OwnerSession> owner,
				   std::shared_ptr<File> data, std::uint64_t offset, std::string header, std::string payload,
				   std::string manifest, MutationToken token)
		: io_(std::move(io)), owner_(std::move(owner)), data_(std::move(data)), offset_(offset),
		  header_(std::move(header)), payload_(std::move(payload)), publication_(std::move(manifest)) {
		request_.token = token;
		refresh_request();
		owner_->mutation_active = true;
	}
	void refresh_request() {
		auto token = request_.token;
		if (phase_ == Phase::Publication) {
			request_ = publication_.request();
		} else {
			request_ = {};
			request_.file = data_;
			request_.kind = phase_ == Phase::Sync ? PrimitiveKind::Sync : PrimitiveKind::Write;
			if (phase_ != Phase::Sync) {
				const auto &bytes = phase_ == Phase::Header ? header_ : payload_;
				request_.offset = offset_ + (phase_ == Phase::Payload ? header_.size() : 0) + written_;
				request_.bytes = std::string_view(bytes).substr(written_, detail::artifact_chunk_size);
			}
		}
		request_.token = token;
	}
	enum class Phase { Header, Payload, Sync, Publication };
	std::shared_ptr<IO> io_;
	std::shared_ptr<detail::OwnerSession> owner_;
	std::shared_ptr<File> data_;
	std::uint64_t offset_;
	std::string header_, payload_;
	detail::PublicationSteps publication_;
	MutationRequest request_;
	std::size_t written_ = 0;
	Phase phase_ = Phase::Header;
	bool started_ = false, in_flight_ = false, active_ = true;
	std::exception_ptr error_;
};

inline void drive_synchronously(AppendMutation &operation) {
	while (!operation.done()) {
		operation.submitted();
		operation.complete(detail::execute_primitive(operation.io(), operation.request()));
	}
	operation.result();
}
} // namespace kronuz::journal
