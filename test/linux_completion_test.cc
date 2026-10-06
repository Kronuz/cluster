#include "journal/linux_completion.h"
#include <filesystem>
#include <iostream>
#include <sys/wait.h>

using namespace kronuz::journal;
namespace {
unsigned checks = 0;
void check(bool value, const char *message) {
	++checks;
	if (!value)
		throw std::runtime_error(message);
}
class Primitive final : public IOOperation {
  public:
	Primitive(std::shared_ptr<PosixIO> io, MutationRequest request) : io_(std::move(io)), request_(request) {
		request_.token.operation = 1;
	}
	const MutationRequest &request() const override { return request_; }
	void submitted() override {
		check(!started_, "primitive executed twice");
		started_ = true;
	}
	bool complete(MutationCompletion result) noexcept override {
		if (!started_ || done_ || result.token != request_.token)
			return false;
		result_ = std::move(result);
		done_ = true;
		return true;
	}
	bool done() const noexcept override { return done_; }
	bool in_flight() const noexcept override { return started_ && !done_; }
	IO &io() const noexcept override { return *io_; }
	MutationCompletion result_;

  private:
	std::shared_ptr<PosixIO> io_;
	MutationRequest request_;
	bool started_ = false, done_ = false;
};
void reap(LinuxCompletionQueue &queue, const std::shared_ptr<Primitive> &operation) {
	const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
	while (std::chrono::steady_clock::now() < deadline) {
		if (auto result = queue.poll()) {
			check(result->operation == operation, "original operation ownership");
			check(operation->complete(std::move(result->completion)), "original token settlement");
			return;
		}
		std::this_thread::sleep_for(std::chrono::milliseconds(1));
	}
	throw std::runtime_error("kernel completion deadline");
}
int enter_calls = 0;
int transient_enter(int fd) {
	if (++enter_calls == 1) {
		errno = EAGAIN;
		return -1;
	}
	const auto result = static_cast<int>(::syscall(__NR_io_uring_enter, fd, 1, 0, 0, nullptr, 0));
	if (result >= 0) {
		errno = EINTR;
		return -1;
	} // Original was consumed before reported error.
	return result;
}
bool corrupt_once = true;
void wrong_token(io_uring_cqe &completion) {
	if (corrupt_once) {
		corrupt_once = false;
		++completion.user_data;
	}
}
int unavailable(unsigned, io_uring_params *) {
	errno = ENOSYS;
	return -1;
}

int permanent_enter(int) {
	errno = EIO;
	return -1;
}
void permanent_wrong_token(io_uring_cqe &completion) { ++completion.user_data; }
int termination_code = 88;
void permanent_faults(const std::filesystem::path &directory) {
	for (bool corrupt : {false, true}) {
		auto child_directory = directory / (corrupt ? "permanent-cq" : "permanent-enter");
		std::filesystem::create_directory(child_directory);
		check(::chmod(child_directory.c_str(), 0700) == 0, "private ownership fault directory");
		const auto child = ::fork();
		check(child >= 0, "spawn ownership fault witness");
		if (!child) {
			std::set_terminate([] { ::_exit(termination_code); });
			auto io = std::make_shared<PosixIO>(child_directory);
			auto owner = io->acquire_owner(true);
			std::shared_ptr<File> file = io->create_exclusive("original");
			LinuxCompletionQueue queue(
				LinuxCompletionPolicy::RequireNative,
				{nullptr, corrupt ? nullptr : permanent_enter, corrupt ? permanent_wrong_token : nullptr});
			MutationRequest request;
			request.kind = PrimitiveKind::Sync;
			request.file = file;
			auto operation = std::make_shared<Primitive>(io, request);
			bool rejected = false;
			try {
				queue.submit(operation);
				if (corrupt)
					reap(queue, operation);
			} catch (const std::system_error &error) {
				rejected = !corrupt && error.code().value() == EIO;
			} catch (const std::logic_error &) {
				rejected = corrupt;
			}
			if (!rejected || !queue.busy() || operation->done() || !operation->in_flight() ||
				queue.stats().native_submitted != 1 || queue.stats().fallback_submitted != 0)
				::_exit(87);
			termination_code = 86;
			// The pending queue's destructor must fail stop; uncertain accepted IO
			// cannot silently release resources or execute through fallback.
		} else {
			int result = 0;
			check(::waitpid(child, &result, 0) == child, "reap ownership fault witness");
			check(WIFEXITED(result) && WEXITSTATUS(result) == 86,
				  "permanent ownership fault retains original and fails stop");
		}
	}
}
void schedules(const std::filesystem::path &directory) {
	auto io = std::make_shared<PosixIO>(directory);
	auto owner = io->acquire_owner(true);
	std::shared_ptr<File> file = io->create_exclusive("source");
	LinuxCompletionQueue queue(LinuxCompletionPolicy::RequireNative, {nullptr, transient_enter});
	std::string payload = "owned original";
	MutationRequest request;
	request.kind = PrimitiveKind::Write;
	request.file = file;
	request.bytes = payload;
	auto write = std::make_shared<Primitive>(io, request);
	check(queue.submit(write) && !queue.submit(write), "one reserved primitive slot");
	check(queue.busy() && !write->done(), "submission cannot settle the original");
	reap(queue, write);
	check(!write->result_.error && write->result_.count == payload.size(), "original write result");
	check(enter_calls == 2 && queue.stats().ring_errors == 2,
		  "unconsumed and consumed errors never duplicate IO");
	check(file->size() == payload.size(), "exact written extent");
	std::array<char, 64> bytes{};
	request = {};
	request.kind = PrimitiveKind::Read;
	request.file = file;
	request.destination_bytes = bytes;
	auto read = std::make_shared<Primitive>(io, request);
	queue.submit(read);
	reap(queue, read);
	check(!read->result_.error && read->result_.count == payload.size(),
		  "short read reports actual byte count");
	check(std::string_view(bytes.data(), payload.size()) == payload, "short read preserves bytes");
	request = {};
	request.kind = PrimitiveKind::Sync;
	request.file = file;
	auto sync = std::make_shared<Primitive>(io, request);
	queue.submit(sync);
	reap(queue, sync);
	check(!sync->result_.error, "native full file barrier");
	request = {};
	request.kind = PrimitiveKind::DirectorySync;
	auto directory_sync = std::make_shared<Primitive>(io, request);
	queue.submit(directory_sync);
	reap(queue, directory_sync);
	check(!directory_sync->result_.error, "native directory barrier");
	request = {};
	request.kind = PrimitiveKind::Replace;
	request.source = "source";
	request.destination = "destination";
	auto rename = std::make_shared<Primitive>(io, request);
	queue.submit(rename);
	reap(queue, rename);
	check(!rename->result_.error && std::filesystem::exists(directory / "destination"),
		  "native namespace replacement");
	request = {};
	request.kind = PrimitiveKind::Remove;
	request.source = "destination";
	for (int i = 0; i < 2; ++i) {
		auto remove = std::make_shared<Primitive>(io, request);
		queue.submit(remove);
		reap(queue, remove);
		check(!remove->result_.error, "removal includes idempotent missing name");
	}
	request = {};
	request.kind = PrimitiveKind::Replace;
	request.source = "missing";
	request.destination = "destination";
	auto failed_rename = std::make_shared<Primitive>(io, request);
	queue.submit(failed_rename);
	reap(queue, failed_rename);
	check(static_cast<bool>(failed_rename->result_.error) && !queue.stats().fallback_submitted,
		  "executed namespace failure never reruns through fallback");
	request.source = "../escape";
	auto invalid = std::make_shared<Primitive>(io, request);
	bool invalid_rejected = false;
	try {
		queue.submit(invalid);
	} catch (const std::invalid_argument &) {
		invalid_rejected = true;
	}
	check(invalid_rejected && !invalid->in_flight() && !queue.busy(),
		  "basename rejection precedes kernel publication");
	auto stats = queue.stats();
	check(stats.native_completed == 8 && stats.fallback_submitted == 0 && stats.maximum_outstanding == 1,
		  "real kernel must qualify all selected primitive classes");
	LinuxCompletionQueue mismatched(LinuxCompletionPolicy::RequireNative, {nullptr, nullptr, wrong_token});
	request = {};
	request.kind = PrimitiveKind::Sync;
	request.file = file;
	auto wrong = std::make_shared<Primitive>(io, request);
	mismatched.submit(wrong);
	bool rejected = false;
	try {
		reap(mismatched, wrong);
	} catch (const std::logic_error &) {
		rejected = true;
	}
	check(rejected && mismatched.busy() && !wrong->done(), "wrong completion retains original resources");
	reap(mismatched, wrong);
	check(!wrong->result_.error, "only matching original completion settles");
	bool require_failed = false;
	try {
		LinuxCompletionQueue unavailable_queue(LinuxCompletionPolicy::RequireNative, {unavailable});
	} catch (const std::system_error &) {
		require_failed = true;
	}
	check(require_failed, "require native fails setup closed");
	LinuxCompletionQueue automatic(LinuxCompletionPolicy::Automatic, {unavailable});
	check(!automatic.native_available() && automatic.setup_error() == ENOSYS,
		  "automatic fallback exposes setup error");
	auto fallback = std::make_shared<Primitive>(io, request);
	automatic.submit(fallback);
	reap(automatic, fallback);
	check(!fallback->result_.error && automatic.stats().native_submitted == 0 &&
			  automatic.stats().fallback_completed == 1,
		  "unavailable setup selects bounded fallback before publication");
	LinuxCompletionQueue forced(LinuxCompletionPolicy::Fallback);
	check(!forced.native_available() && !forced.setup_error(),
		  "explicit fallback differs from setup failure");
}
} // namespace
int main() {
	const auto directory =
		std::filesystem::current_path() / ".scratch" / ("linux-completion-" + std::to_string(::getpid()));
	try {
		std::filesystem::create_directories(directory);
		::chmod(directory.c_str(), 0700);
		schedules(directory);
		permanent_faults(directory);
		std::filesystem::remove_all(directory);
		std::cout << checks << " Linux completion checks\n";
		return 0;
	} catch (const std::exception &error) {
		std::cerr << error.what() << '\n';
		std::filesystem::remove_all(directory);
		return 1;
	}
}
