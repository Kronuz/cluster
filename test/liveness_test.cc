// Smoke test for LivenessTracker: a peer reports alive within the grace
// window from construction even before first contact, dead once that
// window (or a touch()'d peer's own timeout) elapses without a touch(),
// and alive again after a fresh touch().
#include "../liveness.h"

#include <cassert>
#include <cstdio>
#include <thread>
#include <vector>

using namespace std::chrono_literals;

int main() {
	// A short timeout keeps this test fast; it's exercising the logic, not
	// timing precision.
	cluster::LivenessTracker tracker(50ms);

	assert(tracker.is_alive("never-seen"));
	std::puts("a peer never touch()'d is alive within the startup grace window OK");

	tracker.touch("a");
	assert(tracker.is_alive("a"));
	std::puts("a just-touched peer is alive OK");

	std::this_thread::sleep_for(100ms); // well past the 50ms timeout
	assert(!tracker.is_alive("a"));
	// "never-seen" was never touched, so it's now judged against the
	// tracker's OWN construction time -- also past the timeout by now,
	// so it must report dead too, not alive forever.
	assert(!tracker.is_alive("never-seen"));
	std::puts("a touched-then-silent peer, and a never-touched peer past the "
	          "startup window, are both dead OK");

	tracker.touch("a"); // a late heartbeat brings it back
	assert(tracker.is_alive("a"));
	std::puts("a fresh touch() after going dead revives it OK");

	{
		cluster::LivenessTracker fresh(50ms);
		std::vector<std::string> ids = {"a", "b", "c"};
		fresh.touch("a");
		// "a" just touched; "b"/"c" never touched but still inside the
		// startup grace window -- all three alive.
		assert(fresh.count_alive(ids) == 3);

		std::this_thread::sleep_for(100ms);
		fresh.touch("b"); // only b is touched now; a's touch and the
		                  // startup window for c are both long past
		assert(fresh.count_alive(ids) == 1);
	}
	std::puts("count_alive reflects a mix of touched/never-touched/expired peers OK");

	std::puts("all liveness tests passed");
	return 0;
}
