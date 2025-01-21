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

  for (size_t i = 0; i < pool_size_ + 1; ++i) {
    req_queue_.emplace_back(std::make_shared<Chan>());
  }

  for (size_t i = 0; i < pool_size_ + 1; ++i) {
    threads_.emplace_back([=] { StartWorkerThread(i); });
  }
}

BufferPoolManager::~BufferPoolManager() {
  for (size_t i = 0; i < pool_size_ + 1; ++i) {
    req_queue_[i]->Put(std::nullopt);
  }

  for (auto &thread : threads_) {
    thread.join();
  }

  delete[] pages_;
}

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  size_t idx = pool_size_;
  auto promise = CreatPromise();
  auto future = promise.get_future();
  req_queue_[idx]->Put(std::optional<BufferPoolReq>({BufferPoolReqType::New, std::move(promise), -1}));
  auto ret = future.get();
  *page_id = ret.page_id_;
  return ret.page_;
}

auto BufferPoolManager::NewPageImpl(page_id_t *page_id) -> Page * {
  frame_id_t frame_id;
  if ((frame_id = ListAlloc()) == -1) {
    if ((frame_id = MapAlloc()) == -1) {
      return nullptr;
    }
  }

  *page_id = AllocatePage();
  auto page = pages_ + frame_id;
  page->page_id_ = *page_id;
  page->pin_count_ = 1;

  MapLock();
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  page_table_.insert({*page_id, frame_id});
  MapUnlock();
  return page;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  size_t idx = page_id % pool_size_;
  auto promise = CreatPromise();
  auto future = promise.get_future();
  req_queue_[idx]->Put(
      std::optional<BufferPoolReq>({BufferPoolReqType::Fetch, std::move(promise), page_id, false, access_type}));
  return future.get().page_;
}

auto BufferPoolManager::FetchPageImpl(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  frame_id_t frame_id;
  if ((frame_id = MapFetch(page_id)) == -1) {
    if ((frame_id = DiskFetch(page_id)) == -1) {
      return nullptr;
    }
  }
  return pages_ + frame_id;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  size_t idx = page_id % pool_size_;
  auto promise = CreatPromise();
  auto future = promise.get_future();
  req_queue_[idx]->Put(
      std::optional<BufferPoolReq>({BufferPoolReqType::Unpin, std::move(promise), page_id, is_dirty, access_type}));
  return future.get().success_;
}

auto BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type)
    -> bool {
  MapLock();
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    MapUnlock();
    return false;
  }
  auto frame_id = iter->second;

  auto page = pages_ + frame_id;
  if (page->GetPinCount() == 0) {
    MapUnlock();
    return false;
  }
  if (--page->pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  page->is_dirty_ |= is_dirty;

  MapUnlock();
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  size_t idx = page_id % pool_size_;
  auto promise = CreatPromise();
  auto future = promise.get_future();
  req_queue_[idx]->Put(std::optional<BufferPoolReq>({BufferPoolReqType::Flush, std::move(promise), page_id}));
  return future.get().success_;
}

auto BufferPoolManager::FlushPageImpl(page_id_t page_id) -> bool {
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
  size_t idx = pool_size_;
  auto promise = CreatPromise();
  auto future = promise.get_future();
  req_queue_[idx]->Put(std::optional<BufferPoolReq>({BufferPoolReqType::FlushAll, std::move(promise)}));
  future.get();
}

void BufferPoolManager::FlushAllPagesImpl() {
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
  size_t idx = page_id % pool_size_;
  auto promise = CreatPromise();
  auto future = promise.get_future();
  req_queue_[idx]->Put(std::optional<BufferPoolReq>({BufferPoolReqType::Delete, std::move(promise), page_id}));
  return future.get().success_;
}

auto BufferPoolManager::DeletePageImpl(page_id_t page_id) -> bool {
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

  list_latch_.lock();
  free_list_.push_front(frame_id);
  list_latch_.unlock();

  DeallocatePage(page_id);

  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, nullptr}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::ListAlloc() -> frame_id_t {
  list_latch_.lock();
  if (free_list_.empty()) {
    list_latch_.unlock();
    return -1;
  }
  auto frame_id = free_list_.front();
  free_list_.pop_front();
  list_latch_.unlock();
  return frame_id;
}

auto BufferPoolManager::MapAlloc() -> frame_id_t {
  frame_id_t frame_id;
  MapLock();
  if (!replacer_->Evict(&frame_id)) {
    MapUnlock();
    return -1;
  }
  auto page = pages_ + frame_id;
  auto page_id = page->GetPageId();
  page_table_.erase(page_id);
  MapUnlock();
  if (page->IsDirty()) {
    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    disk_scheduler_->Schedule({true, page->GetData(), page_id, std::move(promise)});
    future.get();
    page->is_dirty_ = false;
  }
  page->page_id_ = INVALID_PAGE_ID;
  page->ResetMemory();
  // safe, no one hold this page
  return frame_id;
}

auto BufferPoolManager::MapFetch(page_id_t page_id) -> frame_id_t {
  MapLock();
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    MapUnlock();
    return -1;
  }
  auto frame_id = iter->second;
  auto page = pages_ + frame_id;
  if (++page->pin_count_ == 1) {
    replacer_->SetEvictable(frame_id, false);
  }
  replacer_->RecordAccess(frame_id);
  MapUnlock();
  // safe, at least one hold this page
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
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  disk_scheduler_->Schedule({false, page->GetData(), page_id, std::move(promise)});
  future.get();
  page->page_id_ = page_id;
  page->pin_count_ = 1;

  MapLock();
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  page_table_.insert({page_id, frame_id});
  MapUnlock();
  return frame_id;
}

auto BufferPoolManager::MapFind(page_id_t page_id) -> Page * {
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    return nullptr;
  }
  return pages_ + iter->second;
};

void BufferPoolManager::MapLock() { latch_.lock(); }

void BufferPoolManager::MapUnlock() { latch_.unlock(); }

void BufferPoolManager::StartWorkerThread(size_t idx) {
  auto queue = req_queue_[idx];
  std::optional<BufferPoolReq> opt;
  while ((opt = queue->Get()) != std::nullopt) {
    auto req = std::move(opt.value());
    Page *page = nullptr;
    switch (req.type_) {
      case BufferPoolReqType::New:
        page_id_t page_id;
        page = NewPageImpl(&page_id);
        req.callback_.set_value({false, page, page_id});
        break;
      case BufferPoolReqType::Fetch:
        req.callback_.set_value({false, FetchPageImpl(req.page_id_, req.access_type_)});
        break;
      case BufferPoolReqType::Unpin:
        req.callback_.set_value({UnpinPageImpl(req.page_id_, req.is_dirty_, req.access_type_), nullptr});
        break;
      case BufferPoolReqType::Flush:
        req.callback_.set_value({FlushPageImpl(req.page_id_), nullptr});
        break;
      case BufferPoolReqType::FlushAll:
        FlushAllPagesImpl();
        req.callback_.set_value({false, nullptr});
        break;
      case BufferPoolReqType::Delete:
        req.callback_.set_value({DeletePageImpl(req.page_id_), nullptr});
        break;
      default:
        break;
    }
  }
}

}  // namespace bustub
