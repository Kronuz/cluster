#pragma once
#include "allocation.h"
#include "api.h"
#include <algorithm>
#include <limits>
#include <system_error>
#include <variant>

namespace kronuz::io::completion {

namespace detail {
// Legacy callers transfer their existing string allocation. Managed callers
// copy borrowed names into storage retaining the allocating resource.
class ImageName {
	using OwnedString = std::basic_string<char, std::char_traits<char>, OwnedAllocator<char>>;

  public:
	explicit ImageName(std::string name) : value_(std::move(name)) {}
	ImageName(AllocationContext context, std::string_view name)
		: value_(std::in_place_type<OwnedString>, name, OwnedAllocator<char>(std::move(context))) {}
	operator std::string_view() const noexcept {
		return std::visit([](const auto &name) { return std::string_view(name.data(), name.size()); },
						  value_);
	}

  private:
	std::variant<std::string, OwnedString> value_;
};
} // namespace detail

// The caller supplies a matching immutable view and lifetime owner. Accepted
// operations retain the owner even when their initiating facade disappears.
struct ImmutableBytes {
	std::shared_ptr<const void> owner;
	std::string_view view;
};

// The application supplies its owner-lock and admission lease together. Jobs
// retain that lease, the backend and every buffer through original settlement.
class PreparedImage {
  public:
	explicit operator bool() const noexcept { return static_cast<bool>(state_); }
	std::string_view name() const {
		if (!state_)
			throw std::logic_error("prepared image moved");
		return state_->name;
	}
	AllocationContext allocation_context() const noexcept {
		return state_ ? state_->context : AllocationContext{};
	}

  private:
	struct State {
		explicit State(std::string name) : name(std::move(name)) {}
		State(AllocationContext context, std::string_view name)
			: context(context), name(std::move(context), name) {}
		AllocationContext context;
		std::shared_ptr<IO> io;
		std::shared_ptr<void> lease;
		std::unique_ptr<File> file;
		detail::ImageName name;
		bool selected = false;
	};
	explicit PreparedImage(std::shared_ptr<State> state) : state_(std::move(state)) {}
	std::shared_ptr<State> state_;
	friend class ImagePreparation;
	friend class ImagePublication;
};

enum class PreparationFilePolicy { Retain, CloseAfterSeal };

class ImagePreparation final : public Operation {
  public:
	ImagePreparation(std::shared_ptr<IO> io, std::shared_ptr<void> lease, Token token, std::string name,
					 std::shared_ptr<const std::string> bytes, std::size_t maximum)
		: ImagePreparation(std::move(io), std::move(lease), token, std::move(name), bytes,
						   bytes ? std::string_view(*bytes) : std::string_view{}, maximum) {}
	ImagePreparation(std::shared_ptr<IO> io, std::shared_ptr<void> lease, Token token, std::string name,
					 std::shared_ptr<const void> owner, std::string_view bytes, std::size_t maximum)
		: state_(std::make_shared<PreparedImage::State>(std::move(name))), bytes_{std::move(owner), bytes},
		  token_(token) {
		initialize(std::move(io), std::move(lease), maximum);
	}
	ImagePreparation(AllocationContext context, std::shared_ptr<IO> io, std::shared_ptr<void> lease,
					 Token token, std::string_view name, std::shared_ptr<const void> owner,
					 std::string_view bytes, std::size_t maximum)
		: state_(std::allocate_shared<PreparedImage::State>(OwnedAllocator<PreparedImage::State>(context),
															context, name)),
		  bytes_{std::move(owner), bytes}, token_(token) {
		initialize(std::move(io), std::move(lease), maximum);
	}
	ImagePreparation(AllocationContext context, std::shared_ptr<IO> io, std::shared_ptr<void> lease,
					 Token token, std::string_view name, std::shared_ptr<const std::string> bytes,
					 std::size_t maximum)
		: ImagePreparation(std::move(context), std::move(io), std::move(lease), token, name, bytes,
						   bytes ? std::string_view(*bytes) : std::string_view{}, maximum) {}

	ImagePreparation(AllocationContext context, std::shared_ptr<IO> io, std::shared_ptr<void> lease,
					 Token token, std::string_view name, std::shared_ptr<const void> owner,
					 std::string_view bytes, std::size_t maximum, PreparationFilePolicy policy)
		: ImagePreparation(std::move(context), std::move(io), std::move(lease), token, name, std::move(owner),
						   bytes, maximum) {
		configure_policy(policy);
	}
	ImagePreparation(AllocationContext context, std::shared_ptr<IO> io, std::shared_ptr<void> lease,
					 Token token, std::string_view name, std::shared_ptr<const std::string> bytes,
					 std::size_t maximum, PreparationFilePolicy policy)
		: ImagePreparation(std::move(context), std::move(io), std::move(lease), token, name, bytes,
						   bytes ? std::string_view(*bytes) : std::string_view{}, maximum, policy) {}

  private:
	void configure_policy(PreparationFilePolicy policy) {
		if (policy != PreparationFilePolicy::Retain && policy != PreparationFilePolicy::CloseAfterSeal)
			throw std::invalid_argument("unsupported preparation file policy");
		if (policy == PreparationFilePolicy::CloseAfterSeal) {
			if (!state_->io->managed_capabilities().file_close)
				throw std::invalid_argument("managed preparation requires explicit file close");
			const auto maximum_step = std::numeric_limits<std::uint64_t>::max();
			// One-byte short writes bound the worst case, plus seal and cleanup.
			if (bytes_.view.size() > maximum_step - 3 || token_.step > maximum_step - 3 - bytes_.view.size())
				throw std::length_error("preparation exceeds cleanup token capacity");
		}
		policy_ = policy;
	}
	void terminal() noexcept {
		done_ = true;
		bytes_ = {};
	}
	void close_or_finish() {
		if (!state_->file) {
			terminal();
			return;
		}
		if (phase_ != 4) {
			if (token_.step == std::numeric_limits<std::uint64_t>::max())
				throw std::overflow_error("preparation close token exhausted");
			++token_.step;
			phase_ = 4;
			prepare_request();
		}
	}
	void initialize(std::shared_ptr<IO> io, std::shared_ptr<void> lease, std::size_t maximum) {
		if (!io || !lease || !bytes_.owner || bytes_.view.size() > maximum)
			throw std::invalid_argument("image preparation admission invalid");
		state_->io = std::move(io);
		state_->lease = std::move(lease);
		prepare_request();
	}

  public:
	const Request &request() const override { return request_; }
	void submitted() override {
		if (done_ || in_flight_)
			throw std::logic_error("image request already submitted");
		in_flight_ = true;
	}
	bool complete(Result result) noexcept override {
		if (!in_flight_ || result.token != request_.token)
			return false;
		in_flight_ = false;
		if (result.error && !original_error_)
			original_error_ = result.error;
		const bool managed = policy_ == PreparationFilePolicy::CloseAfterSeal;
		// An authentic returned handle belongs to this original, even on error.
		if (managed && phase_ == 0 && result.file)
			state_->file = std::move(result.file);
		if (managed && phase_ == 4) {
			cleanup_error_ = result.error;
			if (!error_)
				error_ = result.error;
			terminal(); // Close consumes its native handle; never retry an error.
			return true;
		}
		try {
			if (result.error)
				std::rethrow_exception(result.error);
			if (managed && error_) {
				close_or_finish();
				return true;
			}
			if (phase_ == 0) {
				if (!managed)
					state_->file = std::move(result.file);
				if (!state_->file)
					throw std::runtime_error("image create returned no file");
				phase_ = bytes_.view.empty() ? 2 : 1;
			} else if (phase_ == 1) {
				if (!result.count || result.count > request_.bytes.size())
					throw std::runtime_error("invalid image write completion");
				offset_ += result.count;
				if (offset_ == bytes_.view.size())
					phase_ = 2;
			} else if (phase_ == 2) {
				phase_ = 3;
			} else {
				if (managed)
					close_or_finish();
				else
					terminal();
				return true;
			}
			if (token_.step == std::numeric_limits<std::uint64_t>::max())
				throw std::overflow_error("image token exhausted");
			++token_.step;
			prepare_request();
		} catch (...) {
			if (!error_)
				error_ = std::current_exception();
			if (managed) {
				try {
					close_or_finish();
				} catch (...) {
					if (!cleanup_error_)
						cleanup_error_ = std::current_exception();
					terminal();
				}
			} else
				done_ = true;
		}
		return true;
	}
	// Only the opt-in managed policy can stop work and drain native cleanup.
	// The host continues driving this operation through authentic settlement.
	bool cancel() noexcept {
		if (done_ || policy_ != PreparationFilePolicy::CloseAfterSeal)
			return false;
		if (!error_) {
			try {
				throw std::system_error(std::make_error_code(std::errc::operation_canceled));
			} catch (...) {
				error_ = std::current_exception();
			}
		}
		if (!in_flight_) {
			try {
				close_or_finish();
			} catch (...) {
				cleanup_error_ = std::current_exception();
				terminal();
			}
		}
		return true;
	}

	bool done() const noexcept override { return done_; }
	bool in_flight() const noexcept override { return in_flight_; }
	IO &io() const noexcept override { return *state_->io; }
	PreparedImage prepared() const {
		if (!done_ || error_)
			throw std::logic_error("image is not prepared");
		return PreparedImage(state_);
	}
	std::exception_ptr error() const noexcept { return error_; }
	std::exception_ptr cleanup_error() const noexcept { return cleanup_error_; }
	std::exception_ptr original_error() const noexcept { return original_error_; }

  private:
	void prepare_request() {
		request_ = {};
		request_.token = token_;
		// Retain the admitted state without allocating a second file control.
		if (state_->file)
			request_.file = std::shared_ptr<File>(state_, state_->file.get());
		if (phase_ == 0) {
			request_.kind = Kind::Create;
			request_.source = state_->name;
		} else if (phase_ == 1) {
			request_.kind = Kind::Write;
			request_.offset = offset_;
			request_.bytes =
				bytes_.view.substr(offset_, std::min<std::size_t>(65536, bytes_.view.size() - offset_));
		} else
			request_.kind = phase_ == 2 ? Kind::Sync : phase_ == 4 ? Kind::CloseFile : Kind::DirectorySync;
	}
	std::shared_ptr<PreparedImage::State> state_;
	ImmutableBytes bytes_;
	Token token_;
	Request request_;
	std::exception_ptr error_, cleanup_error_, original_error_;
	PreparationFilePolicy policy_ = PreparationFilePolicy::Retain;
	std::size_t offset_ = 0;
	unsigned phase_ = 0;
	bool done_ = false, in_flight_ = false;
};

// Publish a prepared descriptor, never an application image itself. Its opaque
// bytes bind the immutable image generation. The application verifies expected
// base before constructing this job and fences any uncertain result.
class ImagePublication final : public Operation {
  public:
	ImagePublication(PreparedImage descriptor, std::string selected_name, Token token)
		: state_(std::move(descriptor.state_)), selected_name_(std::move(selected_name)), token_(token) {
		initialize();
	}
	ImagePublication(AllocationContext context, PreparedImage descriptor, std::string_view selected_name,
					 Token token)
		: state_(std::move(descriptor.state_)), selected_name_(std::move(context), selected_name),
		  token_(token) {
		initialize();
	}

  private:
	void initialize() {
		if (!state_ || state_->selected || token_.step == std::numeric_limits<std::uint64_t>::max())
			throw std::invalid_argument("descriptor publication unavailable");
		state_->selected = true;
		prepare_request();
	}

  public:
	const Request &request() const override { return request_; }
	void submitted() override {
		if (done_ || in_flight_)
			throw std::logic_error("publication already submitted");
		in_flight_ = true;
		uncertain_ = true;
	}
	bool complete(Result result) noexcept override {
		if (!in_flight_ || result.token != request_.token)
			return false;
		in_flight_ = false;
		if (result.error) {
			error_ = result.error;
			done_ = true;
			return true;
		}
		if (!replaced_) {
			replaced_ = true;
			++token_.step;
			prepare_request();
		} else {
			uncertain_ = false;
			done_ = true;
		}
		return true;
	}
	bool done() const noexcept override { return done_; }
	bool in_flight() const noexcept override { return in_flight_; }
	IO &io() const noexcept override { return *state_->io; }
	bool uncertain() const noexcept { return uncertain_; }
	std::exception_ptr error() const noexcept { return error_; }

  private:
	void prepare_request() noexcept {
		request_ = {};
		request_.token = token_;
		request_.kind = replaced_ ? Kind::DirectorySync : Kind::Replace;
		request_.source = state_->name;
		request_.destination = selected_name_;
	}
	std::shared_ptr<PreparedImage::State> state_;
	detail::ImageName selected_name_;
	Token token_;
	Request request_;
	std::exception_ptr error_;
	bool replaced_ = false, uncertain_ = false, done_ = false, in_flight_ = false;
};

template <class... Args>
std::shared_ptr<ImagePreparation> make_image_preparation(AllocationContext context, Args &&...args) {
	return std::allocate_shared<ImagePreparation>(OwnedAllocator<ImagePreparation>(context), context,
												  std::forward<Args>(args)...);
}
inline std::shared_ptr<ImagePublication> make_image_publication(PreparedImage descriptor,
																std::string_view selected_name, Token token) {
	auto context = descriptor.allocation_context();
	return std::allocate_shared<ImagePublication>(OwnedAllocator<ImagePublication>(context), context,
												  std::move(descriptor), selected_name, token);
}
} // namespace kronuz::io::completion
