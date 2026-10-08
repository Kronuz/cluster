#include "completion/api.h"
#include "completion/native_completion.h"
#include <atomic>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <thread>

namespace c = kronuz::io::completion;
namespace j = kronuz::journal;
namespace {
void check(bool value) {
	if (!value)
		throw std::runtime_error("cursor close invariant failed");
}
template <class Action> void rejects(Action action) {
	bool failed = false;
	try {
		action();
	} catch (const std::exception &) {
		failed = true;
	}
	check(failed);
}
struct Probe {
	std::thread::id owner, closer;
	std::atomic<unsigned> calls{0}, destroyed{0};
};
class ObservedCursor final : public j::DirectoryCursor {
  public:
	ObservedCursor(DIR *stream, Probe &probe, bool fail) : cursor_(stream), probe_(probe), fail_(fail) {}
	~ObservedCursor() override { ++probe_.destroyed; }
	std::optional<std::string> next() override { return cursor_.next(); }
	std::optional<std::size_t> next_into(std::span<char> bytes) override { return cursor_.next_into(bytes); }
	void close() override {
		probe_.closer = std::this_thread::get_id();
		++probe_.calls;
		cursor_.close();
		if (fail_)
			throw std::runtime_error("injected error after native cursor consumption");
	}

  private:
	j::detail::PosixCursor cursor_;
	Probe &probe_;
	bool fail_;
};
class LegacyCursor final : public j::DirectoryCursor {
  public:
	std::optional<std::string> next() override { return {}; }
};
class CloseOperation final : public c::Operation {
	struct Owner {
		std::shared_ptr<c::IO> io;
		std::unique_ptr<j::DirectoryCursor> cursor;
	};

  public:
	CloseOperation(std::shared_ptr<c::IO> io, std::unique_ptr<j::DirectoryCursor> cursor)
		: owner_(std::make_shared<Owner>()) {
		owner_->io = std::move(io);
		owner_->cursor = std::move(cursor);
		request_.kind = c::Kind::CloseCursor;
		request_.cursor = std::shared_ptr<j::DirectoryCursor>(owner_, owner_->cursor.get());
		request_.token.operation = 1;
	}
	const c::Request &request() const override { return request_; }
	void submitted() override {
		if (done_ || submitted_)
			throw std::logic_error("duplicate close original");
		submitted_ = true;
	}
	bool complete(c::Result result) noexcept override {
		if (!submitted_ || done_ || result.token != request_.token)
			return false;
		failure = result.error;
		submitted_ = false;
		done_ = true;
		return true;
	}
	bool done() const noexcept override { return done_; }
	bool in_flight() const noexcept override { return submitted_; }
	c::IO &io() const noexcept override { return *owner_->io; }
	void cancel() noexcept { cancelled = true; }
	bool cancelled = false;
	std::exception_ptr failure;

  private:
	std::shared_ptr<Owner> owner_;
	c::Request request_;
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
		{
			c::PosixIO io(directory);
			auto cursor = io.scan_directory();
			cursor->close();
			rejects([&] { cursor->close(); });
			rejects([&] { cursor->next(); });
			std::array<char, 255> bytes;
			rejects([&] { cursor->next_into(bytes); });
			LegacyCursor legacy;
			rejects([&] { legacy.close(); });
			c::Request request;
			request.kind = c::Kind::CloseCursor;
			check(static_cast<bool>(j::detail::execute_primitive(io, request).error));
			request.cursor = std::make_shared<LegacyCursor>();
			check(static_cast<bool>(j::detail::execute_primitive(io, request).error));
		}
		for (bool fail : {false, true}) {
			Probe probe;
			probe.owner = std::this_thread::get_id();
			auto backend = std::make_shared<c::PosixIO>(directory);
			auto queue = std::make_unique<c::NativeQueue>();
			auto stream = ::opendir(directory.c_str());
			check(stream != nullptr);
			auto descriptor = ::dirfd(stream);
			check(descriptor >= 0);
			auto operation = std::make_shared<CloseOperation>(
				backend, std::make_unique<ObservedCursor>(stream, probe, fail));
			std::weak_ptr<CloseOperation> weak = operation;
			check(queue->submit(operation));
			operation->cancel();
			operation.reset();
			backend.reset();
			check(!weak.expired());
			std::optional<j::OwnedCompletion> held;
			while (!held) {
				held = queue->poll();
				if (!held)
					std::this_thread::sleep_for(std::chrono::milliseconds(1));
			}
			check(probe.calls == 1 && probe.closer != probe.owner && probe.destroyed == 0);
			check(static_cast<bool>(held->completion.error) == fail);
			// A held completion still retains its cancelled initiating operation.
			auto job = std::static_pointer_cast<CloseOperation>(held->operation);
			check(job->cancelled && job->in_flight());
			c::Result stale;
			stale.token = job->request().token;
			++stale.token.step;
			check(!job->complete(std::move(stale)) && job->in_flight());
			check(job->complete(std::move(held->completion)) && job->done() &&
				  static_cast<bool>(job->failure) == fail);
			auto reused = ::open((directory / "reuse").c_str(), O_CREAT | O_RDWR | O_CLOEXEC, 0600);
			check(reused >= 0 && reused == descriptor);
			job.reset();
			held.reset();
			check(weak.expired() && probe.destroyed == 1 && probe.calls == 1 &&
				  ::fcntl(reused, F_GETFD) >= 0);
			check(queue->stats().fallback_extensions[static_cast<std::size_t>(c::Kind::CloseCursor) - 12] ==
				  1);
			queue.reset();
			check(::fcntl(reused, F_GETFD) >= 0);
			::close(reused);
			weak.reset();
		}
		std::filesystem::remove_all(directory);
		std::cout << "worker cursor closure, cancelled held originals, consumed errors and reused descriptor "
					 "retention passed\n";
	} catch (const std::exception &error) {
		std::cerr << error.what() << '\n';
		return 1;
	}
}
