#pragma once

#include "artifact.h"

namespace kronuz::journal {

struct ReclaimStats {
	std::size_t scanned = 0, removed = 0, protected_files = 0, unknown_files = 0;
	std::uint64_t logical_bytes = 0;
	bool logical_bytes_saturated = false, complete = false;
};

// The format supplies pure, bounded header validation. Only the owner makes
// protection/deletion decisions; backend workers execute individual IO calls.
class ReclaimMutation final : public IOOperation {
  public:
	ReclaimMutation(const ReclaimMutation &) = delete;
	ReclaimMutation &operator=(const ReclaimMutation &) = delete;
	~ReclaimMutation() override {
		if (in_flight_) {
			std::terminate();
		}
		if (started_ && gated_) {
			owner_->failed = true;
		}
		if (gated_) {
			owner_->mutation_active = false;
		}
	}
	const MutationRequest &request() const override {
		if (done()) {
			throw std::logic_error("reclamation has completed");
		}
		return request_;
	}
	void submitted() override {
		if (done() || in_flight_ || owner_->failed) {
			throw std::logic_error("reclamation submission unavailable");
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
				throw std::logic_error("storage fenced during reclamation");
			}
			if (completion.error) {
				std::rethrow_exception(completion.error);
			}
			switch (phase_) {
			case Phase::Scan:
				if (!completion.cursor) {
					throw std::runtime_error("scan returned no cursor");
				}
				cursor_ = std::move(completion.cursor);
				phase_ = Phase::Next;
				break;
			case Phase::Next:
				if (!completion.name) {
					stats_.complete = true;
					cursor_.reset();
					finish();
					break;
				}
				name_ = std::move(*completion.name);
				++stats_.scanned;
				if (name_.size() > 255) {
					throw std::length_error("directory basename exceeds bound");
				}
				if (protected_name()) {
					++stats_.protected_files;
					advance();
				} else if (!candidate_(name_)) {
					advance();
				} else {
					phase_ = Phase::Open;
				}
				break;
			case Phase::Open:
				if (!completion.file) {
					++stats_.unknown_files;
					advance();
					break;
				}
				file_ = std::move(completion.file);
				phase_ = Phase::Size;
				break;
			case Phase::Size:
				length_ = completion.length;
				header_size_ = extent_(name_, length_);
				written_ = 0;
				if (!header_size_) {
					++stats_.unknown_files;
					advance();
				} else if (header_size_ > buffer_.size()) {
					throw std::length_error("reclamation header exceeds funded buffer");
				} else {
					phase_ = Phase::Read;
				}
				break;
			case Phase::Read:
				if (!completion.count || completion.count > header_size_ - written_) {
					throw Corruption("reclamation read made invalid progress");
				}
				written_ += completion.count;
				if (written_ == header_size_) {
					if (!validate_(identity_, name_, length_,
								   std::string_view(buffer_.data(), header_size_))) {
						++stats_.unknown_files;
						advance();
					} else if (protected_name()) {
						++stats_.protected_files;
						advance();
					} else {
						file_.reset();
						phase_ = Phase::Remove;
					}
				}
				break;
			case Phase::Remove:
				++stats_.removed;
				if (length_ > std::numeric_limits<std::uint64_t>::max() - stats_.logical_bytes) {
					stats_.logical_bytes = std::numeric_limits<std::uint64_t>::max();
					stats_.logical_bytes_saturated = true;
				} else {
					stats_.logical_bytes += length_;
				}
				advance();
				break;
			case Phase::DirectorySync:
				phase_ = Phase::Done;
				break;
			case Phase::Done:
				break;
			}
			if (!done()) {
				++request_.token.step;
				refresh();
			}
		} catch (...) {
			error_ = std::current_exception();
			owner_->failed = true;
		}
		return true;
	}
	bool done() const noexcept override { return error_ || phase_ == Phase::Done; }
	bool in_flight() const noexcept override { return in_flight_; }
	IO &io() const noexcept override { return *io_; }
	ReclaimStats result() const {
		if (!done()) {
			throw std::logic_error("reclamation has not completed");
		}
		if (error_) {
			std::rethrow_exception(error_);
		}
		return stats_;
	}

  private:
	friend class Journal;
	using Candidate = bool (*)(std::string_view);
	using Extent = std::size_t (*)(std::string_view, std::uint64_t);
	using Validate = bool (*)(Identity, std::string_view, std::uint64_t, std::string_view);
	ReclaimMutation(std::shared_ptr<IO> io, std::shared_ptr<detail::OwnerSession> owner, Identity identity,
					std::shared_ptr<DirectoryCursor> cursor, std::vector<std::string> roots,
					std::size_t budget, std::size_t maximum_header, Candidate candidate, Extent extent,
					Validate validate)
		: io_(std::move(io)), owner_(std::move(owner)), cursor_(std::move(cursor)), roots_(std::move(roots)),
		  identity_(identity), buffer_(maximum_header), budget_(budget), candidate_(candidate),
		  extent_(extent), validate_(validate) {
		if (owner_->failed || owner_->mutation_active) {
			throw std::logic_error("reclamation unavailable");
		}
		if (owner_->mutation_sequence == std::numeric_limits<std::uint64_t>::max()) {
			throw std::overflow_error("IO operation token exhausted");
		}
		phase_ = cursor_ ? Phase::Next : Phase::Scan;
		request_.token = {owner_->mutation_identity, ++owner_->mutation_sequence, 0};
		refresh();
		owner_->mutation_active = true;
	}
	bool protected_name() const {
		if (std::find(roots_.begin(), roots_.end(), name_) != roots_.end()) {
			return true;
		}
		if (owner_->preparing_identity &&
			name_ == "artifact-" + detail::hexadecimal(*owner_->preparing_identity)) {
			return true;
		}
		for (const auto &[identity, references] : owner_->pins) {
			if (name_ == "artifact-" + detail::hexadecimal(identity)) {
				return true;
			}
		}
		return false;
	}
	void finish() noexcept { phase_ = stats_.removed ? Phase::DirectorySync : Phase::Done; }
	void advance() noexcept {
		file_.reset();
		if (stats_.scanned >= budget_) {
			finish();
		} else {
			phase_ = Phase::Next;
		}
	}
	void settled() noexcept {
		owner_->mutation_active = false;
		gated_ = false;
	}
	void refresh() {
		auto token = request_.token;
		request_ = {};
		request_.token = token;
		switch (phase_) {
		case Phase::Scan:
			request_.kind = PrimitiveKind::Scan;
			break;
		case Phase::Next:
			request_.kind = PrimitiveKind::Next;
			request_.cursor = cursor_;
			break;
		case Phase::Open:
			request_.kind = PrimitiveKind::OpenCandidate;
			request_.source = name_;
			break;
		case Phase::Size:
			request_.kind = PrimitiveKind::Size;
			request_.file = file_;
			break;
		case Phase::Read:
			request_.kind = PrimitiveKind::Read;
			request_.file = file_;
			request_.offset = written_;
			request_.destination_bytes = std::span<char>(buffer_).subspan(written_, header_size_ - written_);
			break;
		case Phase::Remove:
			request_.kind = PrimitiveKind::Remove;
			request_.source = name_;
			break;
		case Phase::DirectorySync:
			request_.kind = PrimitiveKind::DirectorySync;
			break;
		case Phase::Done:
			break;
		}
	}
	enum class Phase { Scan, Next, Open, Size, Read, Remove, DirectorySync, Done };
	std::shared_ptr<IO> io_;
	std::shared_ptr<detail::OwnerSession> owner_;
	std::shared_ptr<DirectoryCursor> cursor_;
	std::vector<std::string> roots_;
	Identity identity_;
	std::vector<char> buffer_;
	std::shared_ptr<File> file_;
	std::string name_;
	MutationRequest request_;
	ReclaimStats stats_;
	std::exception_ptr error_;
	std::uint64_t length_ = 0;
	std::size_t budget_, written_ = 0, header_size_ = 0;
	Candidate candidate_;
	Extent extent_;
	Validate validate_;
	Phase phase_ = Phase::Scan;
	bool in_flight_ = false, started_ = false, gated_ = true;
};
} // namespace kronuz::journal
