//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

LRUKNode::LRUKNode(size_t k, frame_id_t fid, bool is_evictable) : k_(k), fid_(fid), is_evictable_(is_evictable) {}

auto LRUKNode::ReachK() const -> bool { return history_.size() >= k_; }

auto LRUKNode::GetTimestamp() const -> size_t { return history_.size() == 0 ? 0 : history_.back(); }

void LRUKNode::PutTimestamp(size_t st) {
  if (history_.size() == k_) {
    history_.pop_back();
  }
  history_.push_front(st);
}

auto LRUKNode::SetEvictable(bool set_evictable) -> int {
  int ret = 0;
  if (is_evictable_ != set_evictable) {
    ret = set_evictable ? 1 : -1;
  }
  is_evictable_ = set_evictable;
  return ret;
}

auto LRUKNode::GetEvictable() const -> bool { return is_evictable_; }

auto LRUKNode::operator<(const LRUKNode &other) -> bool {
  if (!other.GetEvictable()) {
    return true;
  }
  if (!this->GetEvictable()) {
    return false;
  }
  if (this->ReachK() != other.ReachK()) {
    return !this->ReachK();
  }
  return this->GetTimestamp() < other.GetTimestamp();
}

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> lk(latch_);
  auto victim = node_store_.begin();
  for (auto iter = node_store_.begin(); iter != node_store_.end(); ++iter) {
    if (iter->second < victim->second) {
      victim = iter;
    }
  }
  if (victim != node_store_.end() && victim->second.GetEvictable()) {
    *frame_id = victim->first;
    node_store_.erase(victim);
    --curr_size_;
    return true;
  }
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::lock_guard<std::mutex> lk(latch_);
  auto iter = node_store_.find(frame_id);
  if (iter != node_store_.end()) {
    iter->second.PutTimestamp(current_timestamp_++);
  } else {
    BUSTUB_ASSERT(node_store_.size() < replacer_size_, "replacer: no slot");
    node_store_.insert({frame_id, LRUKNode(k_, frame_id)});
    node_store_.at(frame_id).PutTimestamp(current_timestamp_++);
    ++curr_size_;
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lk(latch_);
  auto iter = node_store_.find(frame_id);
  if (iter != node_store_.end()) {
    switch (iter->second.SetEvictable(set_evictable)) {
      case 1:
        curr_size_ += 1;
        break;
      case -1:
        curr_size_ -= 1;
        break;
      default:
        break;
    }
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lk(latch_);
  auto iter = node_store_.find(frame_id);
  if (iter != node_store_.end()) {
    if (iter->second.GetEvictable()) {
      curr_size_ -= 1;
    }
    node_store_.erase(iter);
  }
}

auto LRUKReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> lk(latch_);
  return curr_size_;
}

}  // namespace bustub
