/**
 * Self Test for BufferPoolManagerTest
 */

#include "buffer/buffer_pool_manager.h"

#include <cstdio>
#include <limits>
#include <random>
#include <string>

#include "gtest/gtest.h"

namespace bustub {

TEST(BufferPoolManagerTest, SelfTest) {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManager(3, disk_manager, 2);
  page_id_t tmp;
  auto p0 = bpm->NewPage(&tmp);

  ASSERT_NE(nullptr, p0);
  EXPECT_EQ(0, tmp);

  snprintf(p0->GetData(), BUSTUB_PAGE_SIZE, "Hello");
  EXPECT_EQ(0, strcmp(p0->GetData(), "Hello"));

  auto p1 = bpm->FetchPage(tmp);
  EXPECT_EQ(0, strcmp(p1->GetData(), "Hello"));

  EXPECT_EQ(true, bpm->UnpinPage(tmp, true));
  EXPECT_EQ(true, bpm->UnpinPage(tmp, true));
  bpm->FlushAllPages();

  // 1, 2, 3
  for (int i = 1; i <= 3; ++i) {
    auto p = bpm->NewPage(&tmp);
    EXPECT_EQ(i, tmp);
    snprintf(p->GetData(), BUSTUB_PAGE_SIZE, "Hello%d", i);
    bpm->UnpinPage(tmp, true);
  }

  auto p2 = bpm->FetchPage(0);
  EXPECT_EQ(0, strcmp(p2->GetData(), "Hello"));

  disk_manager->ShutDown();
  remove("test.db");

  delete bpm;
  delete disk_manager;
}

}  // namespace bustub
