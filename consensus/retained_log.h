#pragma once

#include "types.h"
#include <algorithm>
#include <memory>
#include <stdexcept>

namespace cluster::consensus::detail {

// Indexed AVL log with cached subtree accounting. Removing a range only
// detaches roots; bounded retire() turns reclaim their payloads separately.
// Balance depends on subtree heights rather than remotely selected indexes.
class RetainedLog {
	struct Node {
		explicit Node(Entry value) : entry(std::move(value)), bytes(entry.payload.size()) {}
		Entry entry;
		std::unique_ptr<Node> left, right, retired_next;
		std::size_t count = 1, bytes, height = 1;
	};
	using Root = std::unique_ptr<Node>;
	static std::size_t count(const Root &root) noexcept { return root ? root->count : 0; }
	static std::size_t bytes(const Root &root) noexcept { return root ? root->bytes : 0; }
	static std::size_t height(const Root &root) noexcept { return root ? root->height : 0; }
	static void refresh(Node &node) noexcept {
		node.count = 1 + count(node.left) + count(node.right);
		node.bytes = node.entry.payload.size() + bytes(node.left) + bytes(node.right);
		node.height = 1 + std::max(height(node.left), height(node.right));
	}
	static Root rotate_left(Root root) noexcept {
		auto pivot = std::move(root->right);
		root->right = std::move(pivot->left);
		refresh(*root);
		pivot->left = std::move(root);
		refresh(*pivot);
		return pivot;
	}
	static Root rotate_right(Root root) noexcept {
		auto pivot = std::move(root->left);
		root->left = std::move(pivot->right);
		refresh(*root);
		pivot->right = std::move(root);
		refresh(*pivot);
		return pivot;
	}
	static Root balance(Root root) noexcept {
		refresh(*root);
		if (height(root->left) > height(root->right) + 1) {
			if (height(root->left->right) > height(root->left->left)) {
				root->left = rotate_left(std::move(root->left));
			}
			return rotate_right(std::move(root));
		}
		if (height(root->right) > height(root->left) + 1) {
			if (height(root->right->left) > height(root->right->right)) {
				root->right = rotate_right(std::move(root->right));
			}
			return rotate_left(std::move(root));
		}
		return root;
	}
	static std::pair<Root, Root> take_first(Root root) noexcept {
		if (!root->left) {
			auto rest = std::move(root->right);
			refresh(*root);
			return {std::move(root), std::move(rest)};
		}
		auto [first, rest] = take_first(std::move(root->left));
		root->left = std::move(rest);
		return {std::move(first), balance(std::move(root))};
	}
	static Root join(Root left, Root right) noexcept {
		if (!left) {
			return right;
		}
		if (!right) {
			return left;
		}
		if (height(left) > height(right) + 1) {
			left->right = join(std::move(left->right), std::move(right));
			return balance(std::move(left));
		}
		if (height(right) > height(left) + 1) {
			right->left = join(std::move(left), std::move(right->left));
			return balance(std::move(right));
		}
		auto [root, rest] = take_first(std::move(right));
		root->left = std::move(left);
		root->right = std::move(rest);
		return balance(std::move(root));
	}
	static std::pair<Root, Root> split(Root root, std::size_t prefix) noexcept {
		if (!root) {
			return {};
		}
		auto original_left = std::move(root->left), original_right = std::move(root->right);
		refresh(*root);
		if (prefix <= count(original_left)) {
			auto [left, right] = split(std::move(original_left), prefix);
			return {std::move(left),
					join(join(std::move(right), std::move(root)), std::move(original_right))};
		}
		auto [left, right] = split(std::move(original_right), prefix - count(original_left) - 1);
		return {join(join(std::move(original_left), std::move(root)), std::move(left)), std::move(right)};
	}
	void retain_for_retirement(Root root) noexcept {
		if (!root) {
			return;
		}
		retired_count_ += count(root);
		retired_bytes_ += bytes(root);
		push_retired(std::move(root));
	}
	void push_retired(Root root) noexcept {
		if (root) {
			root->retired_next = std::move(retired_);
			retired_ = std::move(root);
		}
	}

  public:
	RetainedLog() = default;
	explicit RetainedLog(std::vector<Entry> entries) {
		for (auto &entry : entries) {
			push_back(std::move(entry));
		}
	}
	RetainedLog(const RetainedLog &) = delete;
	RetainedLog &operator=(const RetainedLog &) = delete;
	~RetainedLog() {
		clear();
		while (retired_) {
			retire(256, std::numeric_limits<std::size_t>::max());
		}
	}
	std::size_t tree_height() const noexcept { return height(root_); }
	std::size_t size() const noexcept { return count(root_); }
	std::size_t payload_bytes() const noexcept { return bytes(root_); }
	std::size_t retired_entries() const noexcept { return retired_count_; }
	std::size_t retired_bytes() const noexcept { return retired_bytes_; }
	std::size_t charged_entries() const noexcept { return size() + retired_count_; }
	std::size_t charged_bytes() const noexcept { return payload_bytes() + retired_bytes_; }
	const Entry &at(std::size_t index) const {
		if (index >= size()) {
			throw std::out_of_range("retained log offset");
		}
		auto *node = root_.get();
		while (node) {
			auto left = count(node->left);
			if (index == left) {
				return node->entry;
			}
			if (index < left) {
				node = node->left.get();
			} else {
				index -= left + 1;
				node = node->right.get();
			}
		}
		throw std::logic_error("retained log accounting mismatch");
	}
	const Entry &operator[](std::size_t index) const { return at(index); }
	std::size_t suffix_bytes(std::size_t first) const {
		if (first > size()) {
			throw std::out_of_range("retained log suffix offset");
		}
		std::size_t prefix = 0;
		auto *node = root_.get();
		while (node && first) {
			auto left = count(node->left);
			if (first <= left) {
				node = node->left.get();
			} else {
				prefix += bytes(node->left) + node->entry.payload.size();
				first -= left + 1;
				node = node->right.get();
			}
		}
		return payload_bytes() - prefix;
	}
	void push_back(Entry entry) {
		// Allocate before moving the live root; allocation failure preserves it.
		auto incoming = std::make_unique<Node>(std::move(entry));
		root_ = join(std::move(root_), std::move(incoming));
	}

	void erase_prefix(std::size_t prefix) {
		if (prefix > size()) {
			throw std::out_of_range("retained log prefix offset");
		}
		auto [removed, live] = split(std::move(root_), prefix);
		root_ = std::move(live);
		retain_for_retirement(std::move(removed));
	}
	void resize(std::size_t retained) {
		if (retained > size()) {
			throw std::out_of_range("retained log truncation offset");
		}
		auto [live, removed] = split(std::move(root_), retained);
		root_ = std::move(live);
		retain_for_retirement(std::move(removed));
	}
	void clear() noexcept { retain_for_retirement(std::move(root_)); }
	std::size_t retire(std::size_t budget, std::size_t byte_budget) noexcept {
		std::size_t reclaimed = 0, reclaimed_bytes = 0;
		while (retired_ && reclaimed < budget) {
			if (retired_->entry.payload.size() > byte_budget - reclaimed_bytes) {
				break;
			}
			auto node = std::move(retired_);
			retired_ = std::move(node->retired_next);
			push_retired(std::move(node->left));
			push_retired(std::move(node->right));
			--retired_count_;
			retired_bytes_ -= node->entry.payload.size();
			reclaimed_bytes += node->entry.payload.size();
			++reclaimed;
		}
		return reclaimed;
	}

  private:
	Root root_, retired_;
	std::size_t retired_count_ = 0, retired_bytes_ = 0;
};

} // namespace cluster::consensus::detail
