#include "completion/api.h"
#include "completion/asio_completion.h"
#include "completion/native_completion.h"
#include <filesystem>
#include <iostream>

namespace c = kronuz::io::completion;
namespace j = kronuz::journal;
namespace {
void check(bool value) {
	if (!value)
		throw std::runtime_error("Asio extension invariant failed");
}
struct State {
	std::shared_ptr<c::IO> io;
	std::unique_ptr<j::DirectoryCursor> cursor;
	std::unique_ptr<c::File> file;
	std::array<char, 255> name{};
	std::optional<c::EntryInspection> named;
	std::exception_ptr error;
	unsigned completed = 0;
};
class Operation final : public c::Operation {
  public:
	explicit Operation(std::shared_ptr<State> state, std::optional<c::Kind> single = {})
		: state_(std::move(state)), single_(single) {
		prepare();
	}
	const c::Request &request() const override { return request_; }
	void submitted() override {
		if (done_ || submitted_)
			throw std::logic_error("duplicate coroutine original");
		submitted_ = true;
	}
	bool complete(c::Result result) noexcept override {
		if (!submitted_ || done_ || result.token != request_.token)
			return false;
		submitted_ = false;
		try {
			if (result.error)
				std::rethrow_exception(result.error);
			if (single_) {
				done_ = true;
				return true;
			}
			switch (request_.kind) {
			case c::Kind::Scan:
				check(static_cast<bool>(result.cursor));
				state_->cursor = std::move(result.cursor);
				break;
			case c::Kind::NextInto:
				check(result.count == (step_ == 1 ? 6 : 0));
				if (step_ == 1)
					check(std::string_view(state_->name.data(), result.count) == "anchor");
				break;
			case c::Kind::InspectEntry:
				check(result.inspection && result.inspection->footprint.logical_bytes == 5);
				state_->named = result.inspection;
				break;
			case c::Kind::Open:
				check(static_cast<bool>(result.file));
				state_->file = std::move(result.file);
				break;
			case c::Kind::InspectFile:
				check(result.inspection == state_->named);
				break;
			case c::Kind::SharedLease:
			case c::Kind::ExclusiveLease:
				check(result.count == 1);
				break;
			default:
				break;
			}
			++state_->completed;
			if (++step_ == kinds_.size()) {
				done_ = true;
				return true;
			}
			prepare();
		} catch (...) {
			state_->error = std::current_exception();
			done_ = true;
		}
		return true;
	}
	bool done() const noexcept override { return done_; }
	bool in_flight() const noexcept override { return submitted_; }
	c::IO &io() const noexcept override { return *state_->io; }

  private:
	void prepare() {
		request_ = {};
		request_.token.operation = 1;
		request_.token.step = step_;
		request_.kind = single_.value_or(kinds_[step_]);
		request_.source = single_ ? "../invalid" : "anchor";
		request_.destination_bytes = state_->name;
		if (state_->cursor)
			request_.cursor = std::shared_ptr<j::DirectoryCursor>(state_, state_->cursor.get());
		if (state_->file)
			request_.file = std::shared_ptr<c::File>(state_, state_->file.get());
	}
	inline static constexpr std::array kinds_{
		c::Kind::Scan, c::Kind::NextInto,		c::Kind::NextInto,	  c::Kind::InspectEntry,
		c::Kind::Open, c::Kind::InspectFile,	c::Kind::SharedLease, c::Kind::CloseFile,
		c::Kind::Open, c::Kind::ExclusiveLease, c::Kind::CloseFile,	  c::Kind::CloseCursor};
	std::shared_ptr<State> state_;
	std::optional<c::Kind> single_;
	c::Request request_;
	std::size_t step_ = 0;
	bool submitted_ = false, done_ = false;
};
} // namespace
int main(int argc, char **argv) {
	try {
		if (argc != 2)
			throw std::invalid_argument("scratch directory required");
		std::filesystem::path directory(argv[1]);
		check(std::filesystem::create_directory(directory));
		std::filesystem::permissions(directory, std::filesystem::perms::owner_all);
		auto backend = std::make_shared<c::PosixIO>(directory);
		{
			auto file = backend->create_exclusive("anchor");
			check(file->write_at(0, "proof") == 5);
		}
		for (bool metrics : {false, true}) {
			asio::io_context context;
			auto queue = std::make_shared<c::NativeQueue>();
			auto driver = std::make_shared<j::AsioCompletionDriver<c::NativeQueue>>(context.get_executor(),
																					queue, metrics);
			auto state = std::make_shared<State>();
			state->io = backend;
			auto operation = std::make_shared<Operation>(state);
			std::weak_ptr<Operation> weak = operation;
			std::exception_ptr failure;
			bool done = false;
			asio::co_spawn(context, driver->run(operation), [&](std::exception_ptr error) {
				failure = error;
				done = true;
			});
			operation.reset();
			check(!weak.expired());
			context.run();
			if (failure)
				std::rethrow_exception(failure);
			if (state->error)
				std::rethrow_exception(state->error);
			check(done && weak.expired() && state->completed == 12 && !queue->busy());
			check(driver->stats().submitted_to_reaped.size() == 12);
			context.restart();
			auto invalid = std::make_shared<Operation>(state, static_cast<c::Kind>(999));
			asio::co_spawn(context, driver->run(invalid), [&](std::exception_ptr error) { failure = error; });
			context.run();
			check(static_cast<bool>(failure) && !invalid->in_flight() && !queue->busy());
			context.restart();
			state->error = {};
			auto failed = std::make_shared<Operation>(state, c::Kind::InspectEntry);
			asio::co_spawn(context, driver->run(failed), [&](std::exception_ptr error) { failure = error; });
			context.run();
			check(!failure && static_cast<bool>(state->error) && failed->done());
			check(driver->stats().original_drive.count == (metrics ? 2 : 0));
			auto stats = driver->stats();
			for (std::size_t at = 0; at < j::completion_extension_count; ++at)
				check(stats.extension_submitted_to_reaped[at].count ==
					  (metrics ? (at == 0 || at == 5 ? 2 : 1) : 0));
			check(stats.extension_failed_submitted_to_reaped[1].count == (metrics ? 1 : 0));
		}
		std::filesystem::remove_all(directory);
		std::cout << "coroutine extension dispatch, original retention and metrics modes passed\n";
	} catch (const std::exception &error) {
		std::cerr << error.what() << '\n';
		return 1;
	}
}
