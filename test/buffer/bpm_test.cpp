/**
 * Self Test for BufferPoolManagerTest
 */

#include "buffer/buffer_pool_manager.h"
#include "storage/disk/disk_manager_memory.h"

#include <cstdio>
#include <limits>
#include <random>
#include <string>

#include "gtest/gtest.h"

namespace bustub {

TEST(BufferPoolManagerTest, SimpleTest) {
  const int kPoolSize = 3;

  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_unique<BufferPoolManager>(kPoolSize, disk_manager.get(), 2);

  for (int i = 0; i < kPoolSize; ++i) {
    page_id_t page_id;
    auto p = bpm->NewPage(&page_id);
    EXPECT_EQ(page_id, i);
    snprintf(p->GetData(), BUSTUB_PAGE_SIZE, "Hello-%d", i);
  }

  for (int i = 0; i < kPoolSize; ++i) {
    EXPECT_TRUE(bpm->UnpinPage(i, true));
  }

  for (int i = 0; i < kPoolSize; ++i) {
    EXPECT_FALSE(bpm->UnpinPage(i, false));
  }

  for (int i = 0; i < kPoolSize; ++i) {
    auto p = bpm->FetchPage(i);
    char buf[BUSTUB_PAGE_SIZE];
    sprintf(buf, "Hello-%d", i);
    EXPECT_EQ(0, strcmp(p->GetData(), buf));
  }

  for (int i = 0; i < kPoolSize; ++i) {
    EXPECT_TRUE(bpm->UnpinPage(i, false));
  }

  for (int i = 0; i < kPoolSize; ++i) {
    EXPECT_TRUE(bpm->FlushPage(i));
  }

  // 3 4 5
  for (int i = kPoolSize; i < 2 * kPoolSize; ++i) {
    page_id_t page_id;
    auto p = bpm->NewPage(&page_id);
    EXPECT_EQ(page_id, i);
    snprintf(p->GetData(), BUSTUB_PAGE_SIZE, "Hello-%d", i);
    EXPECT_TRUE(bpm->UnpinPage(i, true));
  }

  for (int i = 0; i < 2 * kPoolSize; ++i) {
    auto p = bpm->FetchPage(i);
    char buf[BUSTUB_PAGE_SIZE];
    sprintf(buf, "Hello-%d", i);
    EXPECT_EQ(0, strcmp(p->GetData(), buf));
    EXPECT_TRUE(bpm->UnpinPage(i, false));
  }

  for (int i = 0; i < 2 * kPoolSize; ++i) {
    EXPECT_TRUE(bpm->DeletePage(i));
  }

  for (int i = 0; i < 2 * kPoolSize; ++i) {
    EXPECT_TRUE(bpm->DeletePage(i));
  }
}

TEST(BufferPoolManagerTest, MultiThreadsTest) {
  const int kPoolSize = 10;
  const int kMul = 1000;
  const int kThreads = 20;

  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_unique<BufferPoolManager>(kPoolSize, disk_manager.get(), 2);

  for (int i = 0; i < kPoolSize * kMul; ++i) {
    page_id_t page_id;
    auto p = bpm->NewPage(&page_id);
    EXPECT_EQ(page_id, i);
    snprintf(p->GetData(), BUSTUB_PAGE_SIZE, "Hello-%d", i);
    bpm->UnpinPage(page_id, true);
  }

  for (int i = 0; i < kPoolSize * kMul; ++i) {
    page_id_t page_id = i;
    auto page = bpm->FetchPage(page_id);
    EXPECT_EQ(page_id, page->GetPageId());
    char buf[BUSTUB_PAGE_SIZE];
    sprintf(buf, "Hello-%d", i);
    EXPECT_EQ(0, strcmp(page->GetData(), buf));
    EXPECT_TRUE(bpm->UnpinPage(page_id, false));
  }

  std::vector<std::thread> threads;
  for (int i = 0; i < kThreads; ++i) {
    threads.emplace_back([&bpm, i] {
      for (int j = i; j < kPoolSize * kMul; j += kThreads) {
        page_id_t page_id = i;
        auto page = bpm->FetchPage(page_id);
        if (page != nullptr) {
          EXPECT_NE(page, nullptr);
          EXPECT_EQ(page_id, page->GetPageId());
          char buf[BUSTUB_PAGE_SIZE];
          sprintf(buf, "Hello-%d", i);
          EXPECT_EQ(0, strcmp(page->GetData(), buf));
          EXPECT_TRUE(bpm->UnpinPage(page_id, false));
        }
      }
    });
  }

  for (auto &thread : threads) {
    thread.join();
  }
}

}  // namespace bustub
