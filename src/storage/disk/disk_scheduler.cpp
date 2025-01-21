//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_scheduler.cpp
//
// Identification: src/storage/disk/disk_scheduler.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/disk/disk_scheduler.h"
#include "common/exception.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

DiskScheduler::DiskScheduler(DiskManager *disk_manager, size_t threads_n)
    : disk_manager_(disk_manager), threads_n_(threads_n) {
  // TODO(P1): remove this line after you have implemented the disk scheduler API
  // Spawn the background thread
  for (size_t i = 0; i < threads_n_; ++i) {
    request_queue_.emplace_back(std::make_shared<Chan>());
  }

  for (size_t i = 0; i < threads_n_; ++i) {
    threads_.emplace_back([=] { StartWorkerThread(i); });
  }
}

DiskScheduler::~DiskScheduler() {
  // Put a `std::nullopt` in the queue to signal to exit the loop
  for (size_t i = 0; i < threads_n_; ++i) {
    request_queue_[i]->Put(std::nullopt);
  }
  for (auto &thread : threads_) {
    thread.join();
  }
}

void DiskScheduler::Schedule(DiskRequest r) {
  auto idx = r.page_id_ % threads_n_;
  request_queue_[idx]->Put(std::optional<DiskRequest>(std::move(r)));
}

void DiskScheduler::StartWorkerThread(size_t idx) {
  auto queue = request_queue_[idx];
  while (auto opt = queue->Get()) {
    if (!opt.has_value()) {
      break;
    }
    auto req = std::move(opt.value());
    if (req.is_write_) {
      disk_manager_->WritePage(req.page_id_, req.data_);
    } else {
      disk_manager_->ReadPage(req.page_id_, req.data_);
    }
    req.callback_.set_value(true);
  }
}

}  // namespace bustub
