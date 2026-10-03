#include "journal/admission.h"
#include <iostream>
#include <random>
#include <vector>

using namespace kronuz::journal;
namespace {
int checks = 0, failures = 0;
void check(bool value, std::string_view message) { ++checks; if (!value) { ++failures; std::cerr << "FAIL: " << message << '\n'; } }
template <class F> bool throws(F&& function) { try { function(); return false; } catch (const std::exception&) { return true; } }
InventoryStats census(StorageResources used) { InventoryStats result; result.complete = true; result.logical_bytes = used.logical_bytes; result.entries = used.entries; return result; }
const AdmissionLimits limits{{1000, 100}, {100, 10}, {300, 30}, 8};
void bounded(const Admission& admission) {
	auto stats = admission.stats();
	auto total = detail::resources_add(detail::resources_add(stats.used, stats.outstanding), detail::resources_add(stats.control_available, stats.replacement_available));
	check(detail::resources_fit(total, limits.hard) && stats.tickets <= limits.maximum_tickets, "used protected and outstanding capacity obey both hard limits");
}
void pools() {
	Admission ledger(census({100, 10}), limits); check(ledger.stats().normal_ready, "funded startup allows normal admission");
	check(!ledger.reserve(AdmissionClass::Normal, {501, 0}) && !ledger.reserve(AdmissionClass::Normal, {0, 51}), "normal work cannot steal either protected pool");
	auto normal = ledger.reserve(AdmissionClass::Normal, {500, 50}); check(normal.has_value(), "normal may exhaust unprotected capacity");
	auto control = ledger.reserve(AdmissionClass::Control, {100, 10}); auto replacement = ledger.reserve(AdmissionClass::Replacement, {300, 30});
	check(control && replacement && !ledger.reserve(AdmissionClass::Replacement, {1, 1}), "exhausted normal work preserves distinct control and one replacement cycle"); bounded(ledger);
	check(ledger.stats().normal_ready && !ledger.reserve(AdmissionClass::Normal, {1, 1}), "held protection remains funded but leaves no unprotected capacity here");
	normal->mark_started(); normal->settle({500, 50}); control->mark_started(); control->settle({20, 2}); replacement->mark_started(); replacement->settle({100, 10}, {100, 10});
	check(ledger.stats().used == StorageResources{620, 62} && !ledger.stats().normal_ready, "actual additions stay charged and only known removals receive credit"); bounded(ledger);
	ledger.credit_durable_reclaim({120, 12}); check(ledger.stats().normal_ready, "durable deletion refills both protected pools"); bounded(ledger);
	{
		auto permit = ledger.reserve(AdmissionClass::Control, {100, 10});
		ledger.credit_durable_reclaim({100, 10});
		check(ledger.stats().control_available == StorageResources{}, "refill never funds capacity already held by a permit twice"); bounded(ledger);
	}
	check(ledger.stats().control_available == limits.control_pool, "pre-IO cancellation returns its originating protected reservation"); bounded(ledger);
	Admission pressure(census({950, 95}), limits);
	check(!pressure.stats().normal_ready && pressure.stats().control_available == StorageResources{50, 5} && pressure.stats().replacement_available == StorageResources{}, "partial startup funds control first and reports pressure");
	check(!pressure.reserve(AdmissionClass::Normal, {1, 1}) && !pressure.reserve(AdmissionClass::Replacement, {1, 1}), "pressure refuses new normal and unfunded replacement work");
	{ auto partial = pressure.reserve(AdmissionClass::Control, {50, 5}); check(partial.has_value(), "funded partial control permit remains usable"); bounded(pressure); }
	for (auto over : {StorageResources{1001, 0}, StorageResources{0, 101}, StorageResources{std::numeric_limits<std::uint64_t>::max(), 1}}) {
		Admission startup(census(over), limits);
		check(startup.stats().over_limit && !startup.stats().normal_ready && !startup.reserve(AdmissionClass::Control, {0, 1}) && !startup.reserve(AdmissionClass::Replacement, {1, 0}), "above-limit startup cannot make positive reservations in either dimension");
	}
}
void uncertainty() {
	Admission clean(census({100, 10}), limits);
	{ auto ticket = clean.reserve(AdmissionClass::Normal, {10, 1}); }
	check(!clean.stats().tainted && clean.stats().outstanding == StorageResources{}, "unused tickets cancel before IO");
	{ auto ticket = clean.reserve(AdmissionClass::Normal, {10, 1}); ticket->mark_started(); }
	check(clean.stats().tainted && clean.stats().outstanding == StorageResources{10, 1} && !clean.reserve(AdmissionClass::Control, {1, 1}), "lost completion taints accounting and retains uncertain reservation");
	Admission explicit_failure(census({100, 10}), limits); auto failure = explicit_failure.reserve(AdmissionClass::Normal, {10, 1}); failure->abandon();
	check(explicit_failure.stats().tainted, "explicit uncertain failure stops admission");
	Admission invalid(census({100, 10}), limits); auto ticket = invalid.reserve(AdmissionClass::Normal, {10, 1});
	check(throws([&] { ticket->settle({1, 1}); }) && !invalid.stats().tainted, "settlement without starting has no IO accounting effect");
	ticket->mark_started(); check(throws([&] { ticket->settle({11, 1}); }) && invalid.stats().tainted, "over-reservation settlement taints accounting");
	Admission double_credit(census({100, 10}), limits);
	check(throws([&] { double_credit.credit_durable_reclaim({101, 1}); }) && double_credit.stats().tainted, "credit exceeding charged resources fails closed");
	Admission moved(census({100, 10}), limits);
	{
		auto first = moved.reserve(AdmissionClass::Control, {20, 2}); auto second = moved.reserve(AdmissionClass::Normal, {20, 2});
		check(second.has_value(), "held control protection allows normal work from unprotected capacity"); second.reset();
		auto destination = moved.reserve(AdmissionClass::Control, {20, 2}); *destination = std::move(*first);
		check(moved.stats().tickets == 1 && moved.stats().outstanding == StorageResources{20, 2}, "move assignment cancels old pre-IO reservation once");
	}
	check(!moved.stats().tainted && moved.stats().tickets == 0, "moved-from destruction cannot double refund");
	std::optional<StoragePermit> escaped;
	{ Admission temporary(census({100, 10}), limits); escaped = temporary.reserve(AdmissionClass::Normal, {10, 1}); }
	Admission recovered(census({200, 20}), limits);
	escaped->mark_started(); escaped->settle({5, 1});
	check(recovered.stats().used == StorageResources{200, 20} && throws([&] { escaped->mark_started(); }), "late settlement outlives old facade without crediting a recovered session");
	InventoryStats unready; check(throws([&] { Admission rejected(unready, limits); }), "only ready census initializes an accounting session");
	auto invalid_limits = limits; invalid_limits.replacement_pool = {901, 91};
	check(throws([&] { Admission rejected(census({0, 0}), invalid_limits); }), "protected pool sums cannot exceed hard limits");
	auto invalid_count = limits; invalid_count.maximum_tickets = 2;
	check(throws([&] { Admission rejected(census({0, 0}), invalid_count); }), "permit bound must fund a slot for each admission class");
}
void slots() {
	auto small = limits; small.maximum_tickets = 3;
	{
		Admission mixed(census({100, 10}), small);
		auto control = mixed.reserve(AdmissionClass::Control, {1, 1});
		auto replacement = mixed.reserve(AdmissionClass::Replacement, {1, 1});
		auto normal = mixed.reserve(AdmissionClass::Normal, {1, 1});
		check(control && replacement && normal, "occupied protected slots do not block the remaining normal slot");
	}
	for (bool replacement_first : {false, true}) {
		Admission ledger(census({100, 10}), small);
		auto normal = ledger.reserve(AdmissionClass::Normal, {1, 1});
		check(normal && !ledger.reserve(AdmissionClass::Normal, {1, 1}), "normal ticket saturation preserves two protected slots");
		auto first = ledger.reserve(replacement_first ? AdmissionClass::Replacement : AdmissionClass::Control, {1, 1});
		auto second = ledger.reserve(replacement_first ? AdmissionClass::Control : AdmissionClass::Replacement, {1, 1});
		check(first && second && ledger.stats().tickets == 3 && !ledger.reserve(AdmissionClass::Control, {1, 1}), "either protected admission order fits after normal saturation");
		first->mark_started(); first->settle({1, 1}); second.reset(); normal.reset();
		check(ledger.stats().tickets == 0 && !ledger.stats().tainted, "protected completion and cancellation return ticket slots once");
	}
	Admission ledger(census({100, 10}), small);
	auto first = ledger.reserve(AdmissionClass::Control, {1, 1}); auto second = ledger.reserve(AdmissionClass::Control, {1, 1});
	check(first && second && !ledger.reserve(AdmissionClass::Control, {1, 1}), "control ticket saturation preserves replacement's slot");
	auto replacement = ledger.reserve(AdmissionClass::Replacement, {1, 1});
	check(replacement && ledger.stats().tickets == 3, "replacement retains its slot after control saturation"); replacement.reset();
	check(!ledger.reserve(AdmissionClass::Control, {1, 1}) && ledger.reserve(AdmissionClass::Replacement, {1, 1}), "released replacement slot cannot be stolen by another control ticket");
}
void interleaved_replacement() {
	Admission ledger(census({100, 10}), limits);
	auto replacement = ledger.reserve(AdmissionClass::Replacement, {300, 30});
	check(replacement && ledger.stats().normal_ready, "fully reserved replacement remains funded during preparation");
	auto normal = ledger.reserve(AdmissionClass::Normal, {500, 50});
	check(normal && !ledger.reserve(AdmissionClass::Normal, {1, 1}), "interleaved appends consume only remaining unprotected capacity"); bounded(ledger);
	replacement->mark_started(); replacement->settle({300, 30});
	check(!ledger.stats().normal_ready && !ledger.reserve(AdmissionClass::Normal, {1, 1}), "settled replacement pauses normal work when consumed protection cannot refill"); bounded(ledger);
	normal->mark_started(); normal->settle({500, 50});
	ledger.credit_durable_reclaim({300, 30});
	check(ledger.stats().normal_ready && ledger.stats().replacement_available == limits.replacement_pool, "durable cleanup restores replacement protection after interleaving"); bounded(ledger);
}
void control_pack_slots() {
	auto configured = limits; configured.maximum_tickets = 6; configured.control_slots = 3;
	for (bool replacement_first : {false, true}) {
		Admission ledger(census({100, 10}), configured);
		auto normal = ledger.reserve(AdmissionClass::Normal, {1, 1});
		auto second = ledger.reserve(AdmissionClass::Normal, {1, 1});
		check(normal && second && !ledger.reserve(AdmissionClass::Normal, {1, 1}), "normal saturation preserves a complete three-write control pack and replacement");
		std::optional<StoragePermit> replacement;
		if (replacement_first) { replacement = ledger.reserve(AdmissionClass::Replacement, {1, 1}); }
		std::vector<StoragePermit> pack;
		for (unsigned i = 0; i < 3; ++i) {
			auto permit = ledger.reserve(AdmissionClass::Control, {1, 1});
			check(permit.has_value(), "every control continuation fits after normal saturation");
			if (permit) { pack.push_back(std::move(*permit)); }
		}
		if (!replacement_first) { replacement = ledger.reserve(AdmissionClass::Replacement, {1, 1}); }
		check(replacement && ledger.stats().tickets == 6, "either pack/replacement order preserves all protected slots");
		pack.clear();
		check(!ledger.reserve(AdmissionClass::Normal, {1, 1}), "canceled control pack restores protection before new normal admission");
		normal.reset(); second.reset(); replacement.reset();
		check(ledger.stats().tickets == 0 && !ledger.stats().tainted, "pack cancellation releases all counters once");
	}
	for (auto slots : {std::size_t{0}, std::size_t{5}}) {
		configured.control_slots = slots;
		check(throws([&] { Admission invalid(census({0, 0}), configured); }), "invalid protected slot configurations reject before use");
	}
	configured.maximum_tickets = 5; configured.control_slots = 3;
	{
		Admission minimum(census({100, 10}), configured);
		auto replacement = minimum.reserve(AdmissionClass::Replacement, {1, 1});
		auto one = minimum.reserve(AdmissionClass::Control, {1, 1});
		auto two = minimum.reserve(AdmissionClass::Control, {1, 1});
		auto three = minimum.reserve(AdmissionClass::Control, {1, 1});
		auto normal = minimum.reserve(AdmissionClass::Normal, {1, 1});
		check(replacement && one && two && three && normal, "five-slot minimum fits protected pack before normal admission");
		one->mark_started(); one->settle({1, 1});
		check(!minimum.reserve(AdmissionClass::Normal, {1, 1}) && minimum.reserve(AdmissionClass::Control, {1, 1}), "settled control slot remains reusable exclusively by protected work at saturation");
	}
	Admission lifecycle(census({100, 10}), configured);
	auto replacement = lifecycle.reserve(AdmissionClass::Replacement, {1, 1});
	auto first = lifecycle.reserve(AdmissionClass::Control, {1, 1});
	auto second = lifecycle.reserve(AdmissionClass::Control, {1, 1});
	*second = std::move(*first);
	auto normal = lifecycle.reserve(AdmissionClass::Normal, {1, 1});
	check(normal && !lifecycle.reserve(AdmissionClass::Normal, {1, 1}), "move assignment preserves exact remaining control protection");
	second->mark_started(); second->settle({1, 1});
	check(!lifecycle.reserve(AdmissionClass::Normal, {1, 1}), "settlement restores the vacant protected control slot");
	auto control = lifecycle.reserve(AdmissionClass::Control, {1, 1});
	control->mark_started(); control->abandon();
	check(lifecycle.stats().tainted && lifecycle.stats().tickets == 2, "abandonment releases its slot once while fencing uncertain accounting");
}
void schedules() {
	Admission ledger(census({100, 10}), limits); std::mt19937 random(0x504f4f4c); std::vector<StoragePermit> held;
	for (unsigned step = 0; step < 2000; ++step) {
		auto mode = random() % 4;
		if (mode < 2) {
			auto kind = static_cast<AdmissionClass>(random() % 3); auto permit = ledger.reserve(kind, {1 + random() % 20, 1 + random() % 2});
			if (permit) { held.push_back(std::move(*permit)); }
		} else if (mode == 2 && !held.empty()) {
			auto peak = held.back().peak(); held.back().mark_started(); held.back().settle({peak.logical_bytes / 2, peak.entries / 2}); held.pop_back();
		} else {
			auto used = ledger.stats().used; ledger.credit_durable_reclaim({std::min<std::uint64_t>(used.logical_bytes, 10), std::min<std::uint64_t>(used.entries, 1)});
		}
		bounded(ledger); check(!ledger.stats().tainted, "known scheduled outcomes preserve accounting health");
	}
	held.clear(); bounded(ledger);
}
}
int main() {
	try { pools(); uncertainty(); slots(); interleaved_replacement(); control_pack_slots(); schedules(); } catch (const std::exception& error) { check(false, error.what()); }
	std::cout << checks << " admission checks, " << failures << " failures\n"; return failures ? 1 : 0;
}
