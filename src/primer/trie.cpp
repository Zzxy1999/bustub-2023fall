#include "primer/trie.h"
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
  if (root_ == nullptr) {
    return nullptr;
  }

  auto node = root_;
  for (auto c : key) {
    auto iter = node->children_.find(c);
    if (iter != node->children_.end()) {
      node = iter->second;
    } else {
      return nullptr;
    }
  }

  if (node->is_value_node_) {
    if (auto tmp = std::dynamic_pointer_cast<const TrieNodeWithValue<T>>(node)) {
      return tmp->value_.get();
    }
  }
  return nullptr;
}

/**
 * @brief Put TrieNode recursively. If needed, create a new node and copy children
 * @return the new node
 */
template <class T>
auto NPut(std::string_view key, size_t idx, T value, std::shared_ptr<const TrieNode> node)
    -> std::shared_ptr<const TrieNode> {
  if (idx == key.size()) {
    if (node == nullptr) {
      return std::make_shared<const TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value)));
    }

    auto new_node = node->Clone();
    return std::make_shared<const TrieNodeWithValue<T>>(std::move(new_node->children_),
                                                        std::make_shared<T>(std::move(value)));
  }

  if (node == nullptr) {
    node = std::make_shared<const TrieNode>();
  }

  auto iter = node->children_.find(key[idx]);
  auto new_node = node->Clone();
  if (iter == node->children_.end()) {
    new_node->children_[key[idx]] = NPut(key, idx + 1, std::move(value), nullptr);
  } else {
    new_node->children_[key[idx]] = NPut(key, idx + 1, std::move(value), iter->second);
  }

  return {std::shared_ptr<const TrieNode>(std::move(new_node))};
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
  return Trie(NPut<T>(key, 0, std::move(value), root_));
}

/**
 * @brief Remove TrieNode recursively
 * @return the new node
 */
auto NRemove(std::string_view key, size_t idx, std::shared_ptr<const TrieNode> node)
    -> std::pair<bool, std::shared_ptr<const TrieNode>> {
  if (node == nullptr) {
    return {false, nullptr};
  }

  if (idx == key.size() && node->is_value_node_) {
    if (node->children_.empty()) {
      return {true, nullptr};
    }
    return {true, std::make_shared<const TrieNode>(node->children_)};
  }

  auto iter = node->children_.find(key[idx]);
  if (iter == node->children_.end()) {
    return {false, node};
  }
  auto ret = NRemove(key, idx + 1, iter->second);
  if (ret.first) {
    auto new_node = node->Clone();

    if (ret.second == nullptr) {
      new_node->children_.erase(key[idx]);
    } else {
      new_node->children_[key[idx]] = ret.second;
    }

    if (new_node->children_.empty() && !new_node->is_value_node_) {
      return {true, nullptr};
    }

    return {true, std::shared_ptr<const TrieNode>(std::move(new_node))};
  }
  return {false, std::shared_ptr<const TrieNode>(std::move(node))};
}

auto Trie::Remove(std::string_view key) const -> Trie {
  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
  return Trie(std::move(NRemove(key, 0, root_)).second);
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
