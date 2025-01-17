//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::lock_guard<std::mutex> lk(latch_);
  frame_id_t frame_id;
  if ((frame_id = ListAlloc()) == -1) {
    if ((frame_id = MapAlloc()) == -1) {
      return nullptr;
    }
  }
  // alloc a page and return
  *page_id = AllocatePage();
  auto page = pages_ + frame_id;
  page->page_id_ = *page_id;
  page->pin_count_ = 1;
  // first page then idx
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  page_table_.insert({*page_id, frame_id});
  ;
  return page;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::lock_guard<std::mutex> lk(latch_);
  frame_id_t frame_id;
  if ((frame_id = MapFetch(page_id)) == -1) {
    if ((frame_id = DiskFetch(page_id)) == -1) {
      return nullptr;
    }
  }
  return pages_ + frame_id;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::lock_guard<std::mutex> lk(latch_);
  auto page = MapFind(page_id);
  if (page == nullptr) {
    return false;
  }
  if (page->GetPinCount() == 0) {
    return false;
  }
  page->is_dirty_ |= is_dirty;
  if (--page->pin_count_ == 0) {
    replacer_->SetEvictable(page - pages_, true);
  }
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lk(latch_);
  auto page = MapFind(page_id);
  if (page == nullptr) {
    return false;
  }
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  disk_scheduler_->Schedule({true, page->GetData(), page_id, std::move(promise)});
  future.get();
  page->is_dirty_ = false;
  return true;
}

void BufferPoolManager::FlushAllPages() {
  std::lock_guard<std::mutex> lk(latch_);
  for (auto p : page_table_) {
    auto page = pages_ + p.second;
    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    disk_scheduler_->Schedule({true, page->GetData(), p.first, std::move(promise)});
    future.get();
    page->is_dirty_ = false;
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lk(latch_);
  auto page = MapFind(page_id);
  if (page == nullptr) {
    return true;
  }

  auto frame_id = page - pages_;
  if (page->GetPinCount() > 0) {
    return false;
  }
  page_table_.erase(page_id);
  replacer_->Remove(frame_id);

  page->is_dirty_ = false;
  page->page_id_ = INVALID_PAGE_ID;
  page->pin_count_ = 0;
  page->ResetMemory();

  free_list_.push_front(frame_id);

  DeallocatePage(page_id);

  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, nullptr}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::ListAlloc() -> frame_id_t {
  if (free_list_.empty()) {
    return -1;
  }
  auto frame_id = free_list_.front();
  free_list_.pop_front();
  return frame_id;
}

auto BufferPoolManager::MapAlloc() -> frame_id_t {
  frame_id_t frame_id;
  if (!replacer_->Evict(&frame_id)) {
    return -1;
  }
  auto page = pages_ + frame_id;
  auto page_id = page->GetPageId();
  page_table_.erase(page_id);

  // safe to unlock, cause no one can attach this page through page_id

  // no need to lock, cause no one hold this page or find this page
  if (page->IsDirty()) {
    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    disk_scheduler_->Schedule({true, page->GetData(), page_id, std::move(promise)});
    future.get();
    page->is_dirty_ = false;
  }
  page->page_id_ = INVALID_PAGE_ID;
  page->ResetMemory();
  // safe, frame_id not in free_list
  return frame_id;
}

auto BufferPoolManager::MapFetch(page_id_t page_id) -> frame_id_t {
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    return -1;
  }
  auto frame_id = iter->second;
  auto page = pages_ + frame_id;
  if (++page->pin_count_ == 1) {
    replacer_->SetEvictable(frame_id, false);
  }
  replacer_->RecordAccess(frame_id);
  // safe, frame must be unevictable, cause pin_count_ always >= 1 until this thread unpin
  return frame_id;
}

auto BufferPoolManager::DiskFetch(page_id_t page_id) -> frame_id_t {
  frame_id_t frame_id;
  if ((frame_id = ListAlloc()) == -1) {
    if ((frame_id = MapAlloc()) == -1) {
      return -1;
    }
  }
  auto page = pages_ + frame_id;
  // safe
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  disk_scheduler_->Schedule({false, page->GetData(), page_id, std::move(promise)});
  future.get();
  page->page_id_ = page_id;
  page->pin_count_ = 1;
  // safe
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  page_table_.insert({page_id, frame_id});
  return frame_id;
}

auto BufferPoolManager::MapFind(page_id_t page_id) -> Page * {
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    return nullptr;
  }
  return pages_ + iter->second;
};

}  // namespace bustub
