/*------------------------------------------------------------------------------
 - Copyright (c) 2024. Websoft research group, Nanjing University.
 -
 - This program is free software: you can redistribute it and/or modify
 - it under the terms of the GNU General Public License as published by
 - the Free Software Foundation, either version 3 of the License, or
 - (at your option) any later version.
 -
 - This program is distributed in the hope that it will be useful,
 - but WITHOUT ANY WARRANTY; without even the implied warranty of
 - MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 - GNU General Public License for more details.
 -
 - You should have received a copy of the GNU General Public License
 - along with this program.  If not, see <https://www.gnu.org/licenses/>.
 -----------------------------------------------------------------------------*/

//
// ACID Issue #1: FlushAllPages WAL Protocol Tests
// Purpose: Verify that FlushAllPages enforces WAL (Write-Ahead Logging)
//

#include <gtest/gtest.h>
#include <filesystem>
#include <ctime>
#include "storage/disk/disk_manager.h"
#include "storage/buffer/buffer_pool_manager.h"
#include "log/log_manager.h"

namespace njudb {

class WALFlushAllPagesTest : public ::testing::Test {
protected:
  void SetUp() override {
    // Create unique test directory
    test_dir_ = "./test_wal_flush_" + std::to_string(std::rand()) + "_" + 
                std::to_string(std::time(nullptr));
    std::filesystem::create_directories(test_dir_);
    
    test_log_path_ = test_dir_ + "/test.log";
    test_tb_path_ = test_dir_ + "/test.tab";
    
    // Create test database file
    DiskManager::CreateFile(test_tb_path_);
  }
  
  void TearDown() override {
    if (std::filesystem::exists(test_dir_)) {
      std::filesystem::remove_all(test_dir_);
    }
  }
  
  std::string test_dir_;
  std::string test_log_path_;
  std::string test_tb_path_;
};

//===--------------------------------------------------------------------===//
// Test 1: FlushAllPages Calls Log Flush
//===--------------------------------------------------------------------===//

TEST_F(WALFlushAllPagesTest, FlushAllPagesEnforcesWAL) {
  DiskManager disk_manager;
  LogManager log_manager(&disk_manager, test_log_path_);
  BufferPoolManager bpm(&disk_manager, &log_manager);
  
  // Open test database file
  file_id_t fid = disk_manager.OpenFile(test_tb_path_);
  
  // Fetch and modify a page (use page 1, not 0 which is file header)
  page_id_t test_page = 1;
  Page *page1 = bpm.FetchPage(fid, test_page);
  ASSERT_NE(page1, nullptr);
  
  // Write some data to the page content (not header)
  char *data = PageContentPtr(page1->GetData());
  strcpy(data, "test data for WAL");
  
  // Simulate logging something
  LogRecord test_record(1, INVALID_LSN, LogRecordType::BEGIN, 
                       ConcurrencyProtocol::TWO_PHASE_LOCKING);
  lsn_t logged_lsn = log_manager.AppendLogRecord(test_record);
  page1->SetLsn(logged_lsn);
  
  // Mark page as dirty
  bpm.UnpinPage(fid, test_page, true);
  
  // FlushAllPages should flush log up to page LSN
  bool success = bpm.FlushAllPages(fid);
  EXPECT_TRUE(success);
  
  // Verify persistent LSN was updated (log was flushed)
  lsn_t persistent_lsn_after = log_manager.GetPersistentLSN();
  EXPECT_GE(persistent_lsn_after, logged_lsn) 
    << "Log should be flushed up to page LSN before writing page";
  
  disk_manager.CloseFile(fid);
}

//===--------------------------------------------------------------------===//
// Test 2: Multiple Dirty Pages WAL Enforcement
//===--------------------------------------------------------------------===//

TEST_F(WALFlushAllPagesTest, MultiplePageWALEnforcement) {
  DiskManager disk_manager;
  LogManager log_manager(&disk_manager, test_log_path_);
  BufferPoolManager bpm(&disk_manager, &log_manager);
  
  file_id_t fid = disk_manager.OpenFile(test_tb_path_);
  
  const int NUM_PAGES = 5;
  std::vector<lsn_t> page_lsns;
  
  // Create multiple dirty pages with different LSNs (start from page 1, not 0)
  for (int i = 1; i <= NUM_PAGES; i++) {
    Page *page = bpm.FetchPage(fid, i);
    ASSERT_NE(page, nullptr);
    
    char *data = PageContentPtr(page->GetData());
    snprintf(data, 256, "page %d data", i);  // Use reasonable buffer size
    
    // Log operation for this page
    LogRecord record(1, INVALID_LSN, LogRecordType::BEGIN,
                    ConcurrencyProtocol::TWO_PHASE_LOCKING);
    lsn_t lsn = log_manager.AppendLogRecord(record);
    page_lsns.push_back(lsn);
    
    page->SetLsn(lsn);
    bpm.UnpinPage(fid, i, true);
  }
  
  // Find maximum LSN among all pages
  lsn_t max_lsn = *std::max_element(page_lsns.begin(), page_lsns.end());
  
  // FlushAllPages should flush log up to maximum page LSN
  bpm.FlushAllPages(fid);
  
  lsn_t persistent_lsn = log_manager.GetPersistentLSN();
  EXPECT_GE(persistent_lsn, max_lsn)
    << "All log records must be persistent before any page is written";
  
  disk_manager.CloseFile(fid);
}

//===--------------------------------------------------------------------===//
// Test 3: FlushAllPages vs FlushPage Consistency
//===--------------------------------------------------------------------===//

TEST_F(WALFlushAllPagesTest, ConsistencyWithFlushPage) {
  DiskManager disk_manager;
  LogManager log_manager(&disk_manager, test_log_path_);
  BufferPoolManager bpm(&disk_manager, &log_manager);
  
  file_id_t fid = disk_manager.OpenFile(test_tb_path_);
  
  // Create two pages (use page 1 and 2, not 0)
  Page *page1 = bpm.FetchPage(fid, 1);
  Page *page2 = bpm.FetchPage(fid, 2);
  
  // Log and modify both pages
  LogRecord record1(1, INVALID_LSN, LogRecordType::BEGIN,
                   ConcurrencyProtocol::TWO_PHASE_LOCKING);
  lsn_t lsn1 = log_manager.AppendLogRecord(record1);
  page1->SetLsn(lsn1);
  strcpy(PageContentPtr(page1->GetData()), "page1 data");
  bpm.UnpinPage(fid, 1, true);
  
  LogRecord record2(2, INVALID_LSN, LogRecordType::BEGIN,
                   ConcurrencyProtocol::TWO_PHASE_LOCKING);
  lsn_t lsn2 = log_manager.AppendLogRecord(record2);
  page2->SetLsn(lsn2);
  strcpy(PageContentPtr(page2->GetData()), "page2 data");
  bpm.UnpinPage(fid, 2, true);
  
  // Flush page1 individually
  bpm.FlushPage(fid, 1);
  lsn_t persistent_after_flush_page = log_manager.GetPersistentLSN();
  EXPECT_GE(persistent_after_flush_page, lsn1);
  
  // Flush all pages
  bpm.FlushAllPages(fid);
  lsn_t persistent_after_flush_all = log_manager.GetPersistentLSN();
  EXPECT_GE(persistent_after_flush_all, lsn2);
  
  // Both methods should enforce WAL equally
  EXPECT_GE(persistent_after_flush_all, std::max(lsn1, lsn2));
  
  disk_manager.CloseFile(fid);
}

//===--------------------------------------------------------------------===//
// Test 4: No-Op When No Dirty Pages
//===--------------------------------------------------------------------===//

TEST_F(WALFlushAllPagesTest, NoOpWhenNoDirtyPages) {
  DiskManager disk_manager;
  LogManager log_manager(&disk_manager, test_log_path_);
  BufferPoolManager bpm(&disk_manager, &log_manager);
  
  file_id_t fid = disk_manager.OpenFile(test_tb_path_);
  
  // Fetch pages but don't mark them dirty (use page 1 and 2)
  bpm.FetchPage(fid, 1);
  bpm.UnpinPage(fid, 1, false); // NOT dirty
  
  bpm.FetchPage(fid, 2);
  bpm.UnpinPage(fid, 2, false); // NOT dirty
  
  // FlushAllPages should succeed without errors
  bool success = bpm.FlushAllPages(fid);
  EXPECT_TRUE(success);
  
  disk_manager.CloseFile(fid);
}

//===--------------------------------------------------------------------===//
// Test 5: WAL Enforcement with INVALID_LSN
//===--------------------------------------------------------------------===//

TEST_F(WALFlushAllPagesTest, HandlesInvalidLSN) {
  DiskManager disk_manager;
  LogManager log_manager(&disk_manager, test_log_path_);
  BufferPoolManager bpm(&disk_manager, &log_manager);
  
  file_id_t fid = disk_manager.OpenFile(test_tb_path_);
  
  // Fetch and modify a page explicitly marked as having no WAL record.
  Page *page = bpm.FetchPage(fid, 1);
  page->SetLsn(INVALID_LSN);
  strcpy(PageContentPtr(page->GetData()), "data without log");
  bpm.UnpinPage(fid, 1, true);
  
  // FlushAllPages should handle INVALID_LSN gracefully (skip log flush)
  bool success = bpm.FlushAllPages(fid);
  EXPECT_TRUE(success) << "Should handle pages with INVALID_LSN";
  
  disk_manager.CloseFile(fid);
}

//===--------------------------------------------------------------------===//
// Test 6: DPT recLSN Tracks the First Dirtying Log Record
//===--------------------------------------------------------------------===//

TEST_F(WALFlushAllPagesTest, DirtyPageTracksFirstRecLSNUntilFlushed) {
  DiskManager disk_manager;
  LogManager log_manager(&disk_manager, test_log_path_);
  BufferPoolManager bpm(&disk_manager, &log_manager);
  file_id_t fid = disk_manager.OpenFile(test_tb_path_);

  LogRecord record1(1, INVALID_LSN, LogRecordType::BEGIN,
                    ConcurrencyProtocol::TWO_PHASE_LOCKING);
  lsn_t first_lsn = log_manager.AppendLogRecord(record1);
  Page *page = bpm.FetchPage(fid, 1);
  page->SetLsn(first_lsn);
  bpm.UnpinPage(fid, 1, true);

  LogRecord record2(1, first_lsn, LogRecordType::COMMIT);
  lsn_t second_lsn = log_manager.AppendLogRecord(record2);
  page = bpm.FetchPage(fid, 1);
  page->SetLsn(second_lsn);
  bpm.UnpinPage(fid, 1, true);

  auto key = std::make_pair(test_tb_path_, static_cast<page_id_t>(1));
  auto dirty_pages = bpm.GetDirtyPages();
  ASSERT_EQ(dirty_pages.count(key), 1);
  EXPECT_EQ(dirty_pages.at(key), first_lsn);

  ASSERT_TRUE(bpm.FlushPage(fid, 1));
  EXPECT_EQ(bpm.GetDirtyPages().count(key), 0);

  LogRecord record3(2, INVALID_LSN, LogRecordType::BEGIN,
                    ConcurrencyProtocol::TWO_PHASE_LOCKING);
  lsn_t third_lsn = log_manager.AppendLogRecord(record3);
  page = bpm.FetchPage(fid, 1);
  page->SetLsn(third_lsn);
  bpm.UnpinPage(fid, 1, true);

  dirty_pages = bpm.GetDirtyPages();
  ASSERT_EQ(dirty_pages.count(key), 1);
  EXPECT_EQ(dirty_pages.at(key), third_lsn);

  disk_manager.CloseFile(fid);
}

//===--------------------------------------------------------------------===//
// Test 7: Eviction Uses the Same WAL-Safe Flush Path
//===--------------------------------------------------------------------===//

TEST_F(WALFlushAllPagesTest, EvictionFlushesWALBeforeDirtyVictim) {
  DiskManager disk_manager;
  LogManager log_manager(&disk_manager, test_log_path_);
  BufferPoolManager bpm(&disk_manager, &log_manager);
  file_id_t fid = disk_manager.OpenFile(test_tb_path_);

  lsn_t first_lsn = INVALID_LSN;
  for (page_id_t pid = 1; pid <= static_cast<page_id_t>(BUFFER_POOL_SIZE); ++pid) {
    LogRecord record(pid, INVALID_LSN, LogRecordType::BEGIN,
                     ConcurrencyProtocol::TWO_PHASE_LOCKING);
    lsn_t lsn = log_manager.AppendLogRecord(record);
    if (first_lsn == INVALID_LSN) {
      first_lsn = lsn;
    }
    Page *page = bpm.FetchPage(fid, pid);
    ASSERT_NE(page, nullptr);
    page->SetLsn(lsn);
    bpm.UnpinPage(fid, pid, true);
  }

  Page *replacement = bpm.FetchPage(fid, BUFFER_POOL_SIZE + 1);
  ASSERT_NE(replacement, nullptr);
  bpm.UnpinPage(fid, BUFFER_POOL_SIZE + 1, false);

  EXPECT_GE(log_manager.GetPersistentLSN(), first_lsn);
  disk_manager.CloseFile(fid);
}

//===--------------------------------------------------------------------===//
// Test 8: Pinned Dirty Pages Are Not Written from an Unstable Snapshot
//===--------------------------------------------------------------------===//

TEST_F(WALFlushAllPagesTest, PinnedDirtyPageIsNotFlushed) {
  DiskManager disk_manager;
  LogManager log_manager(&disk_manager, test_log_path_);
  BufferPoolManager bpm(&disk_manager, &log_manager);
  file_id_t fid = disk_manager.OpenFile(test_tb_path_);

  LogRecord record(1, INVALID_LSN, LogRecordType::BEGIN,
                   ConcurrencyProtocol::TWO_PHASE_LOCKING);
  lsn_t lsn = log_manager.AppendLogRecord(record);
  Page *page = bpm.FetchPage(fid, 1);
  page->SetLsn(lsn);
  bpm.UnpinPage(fid, 1, true);

  ASSERT_NE(bpm.FetchPage(fid, 1), nullptr);
  EXPECT_FALSE(bpm.FlushPage(fid, 1));
  EXPECT_LT(log_manager.GetPersistentLSN(), lsn);

  bpm.UnpinPage(fid, 1, false);
  EXPECT_TRUE(bpm.FlushPage(fid, 1));
  EXPECT_GE(log_manager.GetPersistentLSN(), lsn);

  disk_manager.CloseFile(fid);
}

//===--------------------------------------------------------------------===//
// Test 9: Index Pages Are Not Interpreted as WAL-Managed Table Pages
//===--------------------------------------------------------------------===//

TEST_F(WALFlushAllPagesTest, IndexPagesDoNotInterpretBytesAsPageLSN) {
  DiskManager disk_manager;
  LogManager log_manager(&disk_manager, test_log_path_);
  BufferPoolManager bpm(&disk_manager, &log_manager);
  std::string index_path = test_dir_ + "/test.idx";
  DiskManager::CreateFile(index_path);
  file_id_t fid = disk_manager.OpenFile(index_path);

  Page *page = bpm.FetchPage(fid, 1);
  ASSERT_NE(page, nullptr);
  page->SetLsn(1000000);
  bpm.UnpinPage(fid, 1, true);

  EXPECT_TRUE(bpm.FlushPage(fid, 1));
  EXPECT_EQ(log_manager.GetPersistentLSN(), INVALID_LSN);

  disk_manager.CloseFile(fid);
}

//===--------------------------------------------------------------------===//
// Test 10: Delete Paths Use the Same WAL-Safe Flush Path
//===--------------------------------------------------------------------===//

TEST_F(WALFlushAllPagesTest, DeletePathsEnforceWAL) {
  DiskManager disk_manager;
  LogManager log_manager(&disk_manager, test_log_path_);
  BufferPoolManager bpm(&disk_manager, &log_manager);
  file_id_t fid = disk_manager.OpenFile(test_tb_path_);

  LogRecord record1(1, INVALID_LSN, LogRecordType::BEGIN,
                    ConcurrencyProtocol::TWO_PHASE_LOCKING);
  lsn_t first_lsn = log_manager.AppendLogRecord(record1);
  Page *page = bpm.FetchPage(fid, 1);
  page->SetLsn(first_lsn);
  bpm.UnpinPage(fid, 1, true);

  ASSERT_TRUE(bpm.DeletePage(fid, 1));
  EXPECT_GE(log_manager.GetPersistentLSN(), first_lsn);

  LogRecord record2(2, INVALID_LSN, LogRecordType::BEGIN,
                    ConcurrencyProtocol::TWO_PHASE_LOCKING);
  lsn_t second_lsn = log_manager.AppendLogRecord(record2);
  page = bpm.FetchPage(fid, 2);
  page->SetLsn(second_lsn);
  bpm.UnpinPage(fid, 2, true);

  ASSERT_TRUE(bpm.DeleteAllPages(fid));
  EXPECT_GE(log_manager.GetPersistentLSN(), second_lsn);

  disk_manager.CloseFile(fid);
}

} // namespace njudb

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
