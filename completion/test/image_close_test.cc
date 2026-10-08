#include "completion/image.h"
#include "completion/native_completion.h"
#include <chrono>
#include <filesystem>
#include <iostream>
#include <thread>
namespace c = kronuz::io::completion;
namespace j = kronuz::journal;
namespace {
void check(bool value) {
	if (!value)
		throw std::runtime_error("managed image closure invariant failed");
}
template <class Action> void rejects(Action action) {
	bool rejected = false;
	try {
		action();
	} catch (const std::exception &) {
		rejected = true;
	}
	check(rejected);
}
struct Probe {
	unsigned created = 0, closed = 0, destroyed = 0, unclosed = 0;
	std::thread::id closer;
	bool close_error = false;
};
class File final : public c::File {
  public:
	File(std::unique_ptr<c::File> file, Probe &probe) : file_(std::move(file)), probe_(probe) {
		++probe_.created;
	}
	~File() override {
		++probe_.destroyed;
		if (!closed_)
			++probe_.unclosed;
	}
	std::uint64_t size() override { return file_->size(); }
	std::size_t read_at(std::uint64_t at, std::span<char> bytes) override {
		return file_->read_at(at, bytes);
	}
	std::size_t write_at(std::uint64_t at, std::string_view bytes) override {
		return file_->write_at(at, bytes.substr(0, std::min<std::size_t>(2, bytes.size())));
	}
	void truncate(std::uint64_t bytes) override { file_->truncate(bytes); }
	void sync() override { file_->sync(); }
	void close() override {
		check(!closed_);
		closed_ = true;
		probe_.closer = std::this_thread::get_id();
		++probe_.closed;
		file_->close();
		if (probe_.close_error)
			throw std::runtime_error("close failure");
	}

  private:
	std::unique_ptr<c::File> file_;
	Probe &probe_;
	bool closed_ = false;
};
class IO final : public c::IO {
  public:
	IO(std::shared_ptr<c::PosixIO> io, Probe &probe) : backend(std::move(io)), probe(probe) {}
	c::ManagedCapabilities managed_capabilities() const noexcept override {
		c::ManagedCapabilities caps;
		caps.file_close = close_capability;
		return caps;
	}
	std::unique_ptr<c::OwnerLock> acquire_owner(bool create) override {
		return backend->acquire_owner(create);
	}
	std::unique_ptr<c::File> open_existing(std::string_view name) override {
		return backend->open_existing(name);
	}
	std::unique_ptr<c::File> create_exclusive(std::string_view name) override {
		return std::make_unique<File>(backend->create_exclusive(name), probe);
	}
	void replace(std::string_view from, std::string_view to) override { backend->replace(from, to); }
	void remove(std::string_view name) override { backend->remove(name); }
	void sync_directory() override { backend->sync_directory(); }
	std::shared_ptr<c::PosixIO> backend;
	Probe &probe;
	bool close_capability = true;
};
auto job(const std::shared_ptr<IO> &io, std::string_view name, std::shared_ptr<const std::string> bytes,
		 c::Token token = {}) {
	return c::make_image_preparation({}, io, std::make_shared<int>(1), token, name, bytes, bytes->size(),
									 c::PreparationFilePolicy::CloseAfterSeal);
}
void drive(c::ImagePreparation &operation, std::optional<c::Kind> failure = {}, bool after = false) {
	bool injected = false;
	while (!operation.done()) {
		operation.submitted();
		c::Result result;
		result.token = operation.request().token;
		if (!failure || injected || operation.request().kind != *failure || after)
			result = j::detail::execute_primitive(operation.io(), operation.request());
		if (failure && !injected && operation.request().kind == *failure) {
			result.error = std::make_exception_ptr(std::runtime_error("primary failure"));
			injected = true;
		}
		check(operation.complete(std::move(result)));
	}
}
void primary(std::exception_ptr error, std::string_view expected) {
	check(bool(error));
	try {
		std::rethrow_exception(error);
	} catch (const std::runtime_error &error) {
		check(error.what() == expected);
	}
}
} // namespace
int main(int argc, char **argv) {
	try {
		check(argc == 2);
		const std::filesystem::path directory(argv[1]);
		check(std::filesystem::create_directory(directory));
		std::filesystem::permissions(directory, std::filesystem::perms::owner_all);
		auto backend = std::make_shared<c::PosixIO>(directory);
		auto lock = backend->acquire_owner(true);
		auto empty = std::make_shared<const std::string>();
		auto bytes = std::make_shared<const std::string>("payload");
		unsigned sequence = 0;
		auto name = [&] { return "image-" + std::to_string(++sequence); };
		for (auto body : {empty, bytes}) {
			Probe probe;
			auto io = std::make_shared<IO>(backend, probe);
			auto operation = job(io, name(), body);
			while (operation->request().kind != c::Kind::CloseFile) {
				operation->submitted();
				check(
					operation->complete(j::detail::execute_primitive(operation->io(), operation->request())));
			}
			check(!operation->done() && probe.closed == 0);
			rejects([&] { operation->prepared(); });
			drive(*operation);
			check(!operation->error() && probe.closed == 1);
			std::optional<c::PreparedImage> prepared = operation->prepared();
			operation.reset();
			check(probe.destroyed == 0);
			auto retained = prepared;
			prepared.reset();
			check(probe.destroyed == 0);
			retained.reset();
			check(probe.destroyed == 1 && probe.unclosed == 0);
		}
		for (auto kind :
			 {c::Kind::Create, c::Kind::Write, c::Kind::Sync, c::Kind::DirectorySync, c::Kind::CloseFile}) {
			for (bool after : {false, true}) {
				Probe probe;
				auto io = std::make_shared<IO>(backend, probe);
				auto operation = job(io, name(), bytes);
				if (kind == c::Kind::CloseFile)
					probe.close_error = true;
				if (kind == c::Kind::CloseFile) {
					drive(*operation);
				} else
					drive(*operation, kind, after);
				check(bool(operation->error()));
				rejects([&] { operation->prepared(); });
				if (kind == c::Kind::CloseFile) {
					primary(operation->cleanup_error(), "close failure");
				} else
					primary(operation->error(), "primary failure");
				check(probe.closed == (kind == c::Kind::Create && !after ? 0 : 1));
				operation.reset();
				check(probe.unclosed == 0 && probe.destroyed == probe.created);
			}
		}
		{
			Probe probe;
			probe.close_error = true;
			auto io = std::make_shared<IO>(backend, probe);
			auto operation = job(io, name(), bytes);
			drive(*operation, c::Kind::Write);
			primary(operation->error(), "primary failure");
			primary(operation->cleanup_error(), "close failure");
		}
		for (auto phase :
			 {c::Kind::Create, c::Kind::Write, c::Kind::Sync, c::Kind::DirectorySync, c::Kind::CloseFile}) {
			Probe probe;
			auto io = std::make_shared<IO>(backend, probe);
			auto operation = job(io, name(), bytes);
			while (operation->request().kind != phase) {
				operation->submitted();
				check(
					operation->complete(j::detail::execute_primitive(operation->io(), operation->request())));
			}
			operation->submitted();
			auto result = j::detail::execute_primitive(operation->io(), operation->request());
			check(operation->cancel() && operation->in_flight());
			check(operation->complete(std::move(result)));
			drive(*operation);
			check(bool(operation->error()) && probe.closed == 1);
			rejects([&] { operation->prepared(); });
			operation.reset();
			check(probe.unclosed == 0);
		}
		{
			Probe probe;
			auto io = std::make_shared<IO>(backend, probe);
			auto operation = job(io, name(), bytes);
			check(operation->cancel() && operation->done() && probe.created == 0);
		}
		{
			Probe probe;
			auto io = std::make_shared<IO>(backend, probe);
			auto operation = job(io, name(), bytes);
			operation->submitted();
			check(operation->cancel());
			auto result = j::detail::execute_primitive(operation->io(), operation->request());
			result.error = std::make_exception_ptr(std::runtime_error("primary failure"));
			check(operation->complete(std::move(result)));
			drive(*operation);
			primary(operation->original_error(), "primary failure");
			check(probe.closed == 1);
		}
		{
			Probe probe;
			auto io = std::make_shared<IO>(backend, probe);
			io->close_capability = false;
			rejects([&] { job(io, name(), bytes); });
			check(probe.created == 0);
			io->close_capability = true;
			c::Token token;
			token.step = UINT64_MAX;
			rejects([&] { job(io, name(), empty, token); });
			token.step = UINT64_MAX - 3;
			auto operation = job(io, name(), empty, token);
			drive(*operation);
			check(!operation->error() && probe.closed == 1);
		}
		{
			Probe probe;
			auto io = std::make_shared<IO>(backend, probe);
			auto operation = c::make_image_preparation({}, io, std::make_shared<int>(1), c::Token{}, name(),
													   bytes, bytes->size());
			check(!operation->cancel());
			drive(*operation);
			check(!operation->error() && probe.closed == 0);
			std::optional<c::PreparedImage> prepared = operation->prepared();
			operation.reset();
			prepared.reset();
			check(probe.unclosed == 1);
		}
		{
			Probe probe;
			auto io = std::make_shared<IO>(backend, probe);
			auto operation = job(io, name(), bytes);
			std::weak_ptr<c::ImagePreparation> weak = operation;
			c::NativeQueue queue;
			check(queue.submit(operation));
			operation->cancel();
			operation.reset();
			io.reset();
			for (;;) {
				std::optional<j::OwnedCompletion> held;
				const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
				while (!held) {
					held = queue.poll();
					if (!held) {
						check(std::chrono::steady_clock::now() < deadline);
						std::this_thread::sleep_for(std::chrono::milliseconds(1));
					}
				}
				operation = std::static_pointer_cast<c::ImagePreparation>(held->operation);
				c::Result stale;
				stale.token = held->completion.token;
				++stale.token.step;
				check(!operation->complete(std::move(stale)) && operation->in_flight());
				check(operation->complete(std::move(held->completion)));
				if (operation->done())
					break;
				check(queue.submit(operation));
				operation.reset();
			}
			check(probe.closed == 1 && probe.closer != std::this_thread::get_id());
			operation.reset();
			check(weak.expired() && probe.unclosed == 0);
		}
		lock.reset();
		backend.reset();
		std::filesystem::remove_all(directory);
		std::cout << "managed preparation seal/close, error drainage, cancellation, token bounds and legacy "
					 "retention passed\n";
	} catch (const std::exception &error) {
		std::cerr << error.what() << '\n';
		return 1;
	}
}
