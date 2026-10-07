#include "completion/api.h"
#include "completion/image.h"
#include "completion/native_completion.h"
#include "journal/operation.h"
#include <chrono>
#include <filesystem>
#include <iostream>
#include <thread>
#include <type_traits>

namespace c = kronuz::io::completion;
static_assert(std::is_same_v<c::Operation, kronuz::journal::IOOperation>);
static_assert(std::is_same_v<c::Request, kronuz::journal::MutationRequest>);

class OnePrimitive final : public c::Operation {
  public:
	OnePrimitive(c::IO &io, c::Request request) : io_(io), request_(request) {}
	const c::Request &request() const override { return request_; }
	void submitted() override {
		if (submitted_)
			throw std::logic_error("duplicate submission");
		submitted_ = true;
	}
	bool complete(c::Result result) noexcept override {
		if (!submitted_ || done_ || result.token != request_.token)
			return false;
		result_ = std::move(result);
		done_ = true;
		return true;
	}
	bool done() const noexcept override { return done_; }
	bool in_flight() const noexcept override { return submitted_ && !done_; }
	c::IO &io() const noexcept override { return io_; }
	c::Result result_;

  private:
	c::IO &io_;
	c::Request request_;
	bool submitted_ = false, done_ = false;
};

int main(int argc, char **argv) {
	try {
		if (argc != 2)
			throw std::invalid_argument("scratch directory required");
		const std::filesystem::path directory(argv[1]);
		if (!std::filesystem::create_directory(directory))
			throw std::runtime_error("test directory already exists");
		std::filesystem::permissions(directory, std::filesystem::perms::owner_all);
		{
			c::PosixIO io(directory, 0644);
			auto owner = io.acquire_owner(true);
			if ((std::filesystem::status(directory / "owner.lock").permissions() &
				 std::filesystem::perms::mask) !=
				(std::filesystem::perms::owner_read | std::filesystem::perms::owner_write))
				throw std::runtime_error("owner lock is not private");
			c::Request request;
			request.token.operation = 1;
			request.kind = c::Kind::Create;
			request.source = "candidate";
			OnePrimitive create(io, request);
			c::drive_synchronously(create);
			if (create.result_.error || !create.result_.file)
				throw std::runtime_error("create failed");
			auto permissions = std::filesystem::status(directory / "candidate").permissions();
			if ((permissions & std::filesystem::perms::others_read) == std::filesystem::perms::none ||
				(permissions & std::filesystem::perms::others_write) != std::filesystem::perms::none)
				throw std::runtime_error("artifact sharing permissions differ");
			auto file = std::shared_ptr<c::File>(std::move(create.result_.file));
			request.kind = c::Kind::Write;
			request.file = file;
			request.bytes = "immutable image";
			OnePrimitive write(io, request);
			c::drive_synchronously(write);
			if (write.result_.error || write.result_.count != request.bytes.size())
				throw std::runtime_error("write failed");
			request.kind = c::Kind::Sync;
			OnePrimitive sync(io, request);
			c::drive_synchronously(sync);
			if (sync.result_.error)
				std::rethrow_exception(sync.result_.error);
			request.kind = c::Kind::Replace;
			request.source = "candidate";
			request.destination = "published";
			OnePrimitive replace(io, request);
			c::drive_synchronously(replace);
			if (replace.result_.error)
				std::rethrow_exception(replace.result_.error);
			request.kind = c::Kind::DirectorySync;
			OnePrimitive seal(io, request);
			c::drive_synchronously(seal);
			if (seal.result_.error)
				std::rethrow_exception(seal.result_.error);
			auto reopened = io.open_existing("published");
			char buffer[32]{};
			auto count = reopened->read_at(0, buffer);
			if (std::string_view(buffer, count) != "immutable image")
				throw std::runtime_error("published contents differ");
			request.kind = c::Kind::Open;
			request.source = "../escape";
			OnePrimitive invalid(io, request);
			c::drive_synchronously(invalid);
			if (!invalid.result_.error || !invalid.done() || invalid.in_flight())
				throw std::runtime_error("invalid request not completed authentically");
		}
		std::filesystem::remove(directory / "owner.lock");
		{
			auto io = std::make_shared<c::PosixIO>(directory);
			auto owner = std::shared_ptr<c::OwnerLock>(io->acquire_owner(true));
			auto bytes = std::make_shared<const std::string>(65537, 'x');
			c::Token token;
			token.operation = 2;
			c::ImagePreparation job(io, owner, token, "prepared-descriptor", bytes, 65537);
			job.submitted();
			auto original = kronuz::journal::detail::execute_primitive(job.io(), job.request());
			c::Result stale;
			stale.token = job.request().token;
			++stale.token.step;
			if (job.complete(std::move(stale)) || !job.in_flight() || job.done())
				throw std::runtime_error("stale completion consumed original obligation");
			if (!job.complete(std::move(original)))
				throw std::runtime_error("original completion rejected");
			c::drive_synchronously(job);
			if (job.error())
				std::rethrow_exception(job.error());
			c::ImagePublication publish(job.prepared(), "selected", token);
			publish.submitted();
			auto selected = kronuz::journal::detail::execute_primitive(publish.io(), publish.request());
			if (!publish.uncertain() || publish.done())
				throw std::runtime_error("early durable publication");
			publish.complete(std::move(selected));
			if (!publish.uncertain() || publish.done())
				throw std::runtime_error("rename acknowledged before barrier");
			c::drive_synchronously(publish);
			if (publish.error())
				std::rethrow_exception(publish.error());
			if (publish.uncertain() || io->open_existing("selected")->size() != bytes->size())
				throw std::runtime_error("sealed image publication differs");
			bool duplicate_rejected = false;
			try {
				c::ImagePublication duplicate(job.prepared(), "other", token);
			} catch (const std::invalid_argument &) {
				duplicate_rejected = true;
			}
			if (!duplicate_rejected)
				throw std::runtime_error("prepared descriptor published twice");
			auto native_job =
				std::make_shared<c::ImagePreparation>(io, owner, token, "native-generation", bytes, 65537);
			c::NativeQueue queue;
			while (!native_job->done()) {
				if (!queue.submit(native_job))
					throw std::runtime_error("unexpected native admission refusal");
				while (queue.busy()) {
					if (auto completion = queue.poll()) {
						if (!completion->operation->complete(std::move(completion->completion)))
							throw std::runtime_error("native original not accepted");
					} else
						std::this_thread::sleep_for(std::chrono::milliseconds(1));
				}
			}
			if (native_job->error())
				std::rethrow_exception(native_job->error());
			if (!native_job->prepared())
				throw std::runtime_error("native image did not seal");
		}
		std::filesystem::remove_all(directory);
		std::cout << "completion core passed\n";
		return 0;
	} catch (const std::exception &error) {
		std::cerr << error.what() << '\n';
		return 1;
	}
}
