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
// Created by ziqi on 2024/7/17.
//

#include "storage/buffer/buffer_pool_manager.h"
#include "storage/buffer/page_guard.h"
#include "../config.h"

#include <cassert>
#include <cstring>
#include <ctime>
#include <string>
#include <thread>
#include <unordered_map>
#include <filesystem>
#include <vector>
#include <unordered_set>

#include "gtest/gtest.h"

[[maybe_unused]] constexpr int MAX_PAGES = 64;

TEST(PageGuardTest, BasicTest)
{
  wsdb::DiskManager       disk_manager{};
  wsdb::BufferPoolManager buffer_pool_manager(&disk_manager);
  if (!std::filesystem::exists(TEST_DIR))
    std::filesystem::create_directory(TEST_DIR);
  std::filesystem::current_path(TEST_DIR);
  
  try {
    wsdb::DiskManager::CreateFile("test_page_guard.tbl");
  } catch (wsdb::WSDBException_ &e) {
    // destroy and recreate the file
    wsdb::DiskManager::DestroyFile("test_page_guard.tbl");
    wsdb::DiskManager::CreateFile("test_page_guard.tbl");
  }

  SUB_TEST(ReadPageGuard)
  {
    auto fd = disk_manager.OpenFile("test_page_guard.tbl");
    
    // Test basic ReadPageGuard functionality
    {
      auto read_guard = buffer_pool_manager.FetchPageRead(fd, 0);
      ASSERT_NE(read_guard.GetPage(), nullptr);
      ASSERT_NE(read_guard.GetData(), nullptr);
      ASSERT_EQ(read_guard.GetFileId(), fd);
      ASSERT_EQ(read_guard.GetPageId(), 0);
      ASSERT_FALSE(read_guard.IsDirty());
      
      // Test const data access
      const char *data = read_guard.GetData();
      ASSERT_NE(data, nullptr);
    }
    // Page should be automatically unpinned when guard goes out of scope
    
    // Test multiple ReadPageGuards
    for (int i = 0; i < 10; ++i) {
      auto read_guard = buffer_pool_manager.FetchPageRead(fd, i);
      ASSERT_NE(read_guard.GetPage(), nullptr);
      ASSERT_EQ(read_guard.GetFileId(), fd);
      ASSERT_EQ(read_guard.GetPageId(), i);
      ASSERT_FALSE(read_guard.IsDirty());
    }
    
    disk_manager.CloseFile(fd);
  }

  SUB_TEST(WritePageGuard)
  {
    auto fd = disk_manager.OpenFile("test_page_guard.tbl");
    
    // Test basic WritePageGuard functionality
    const char test_data[] = "Hello, Page Guard!";
    {
      auto write_guard = buffer_pool_manager.FetchPageWrite(fd, 0);
      ASSERT_NE(write_guard.GetPage(), nullptr);
      ASSERT_NE(write_guard.GetMutableData(), nullptr);
      ASSERT_EQ(write_guard.GetFileId(), fd);
      ASSERT_EQ(write_guard.GetPageId(), 0);
      ASSERT_TRUE(write_guard.IsDirty());

      // Write to the page
      char *data = write_guard.GetMutableData();
      ASSERT_NE(data, nullptr);
      std::memcpy(data, test_data, sizeof(test_data));
    }
    // Page should be automatically unpinned as dirty when guard goes out of scope
    
    // Verify the data was written
    {
      auto read_guard = buffer_pool_manager.FetchPageRead(fd, 0);
      const char *data = read_guard.GetData();
      ASSERT_NE(data, nullptr);
      ASSERT_EQ(std::memcmp(data, test_data, sizeof(test_data)), 0);
    }
    
    disk_manager.CloseFile(fd);
  }

  SUB_TEST(MoveSemantics)
  {
    auto fd = disk_manager.OpenFile("test_page_guard.tbl");
    
    // Test move constructor
    const char test_data1[] = "Move constructor test";
    {
      auto write_guard1 = buffer_pool_manager.FetchPageWrite(fd, 1);
      char *data = write_guard1.GetMutableData();
      std::memcpy(data, test_data1, sizeof(test_data1));
      
      // Move constructor
      auto write_guard2 = std::move(write_guard1);
      ASSERT_EQ(write_guard1.GetPage(), nullptr);  // Original should be invalidated
      ASSERT_NE(write_guard2.GetPage(), nullptr);  // New should be valid
      ASSERT_EQ(write_guard2.GetFileId(), fd);
      ASSERT_EQ(write_guard2.GetPageId(), 1);
      ASSERT_TRUE(write_guard2.IsDirty());
    }
    
    // Verify data was written
    {
      auto read_guard = buffer_pool_manager.FetchPageRead(fd, 1);
      const char *data = read_guard.GetData();
      ASSERT_EQ(std::memcmp(data, test_data1, sizeof(test_data1)), 0);
    }
    
    // Test move assignment
    const char test_data2[] = "Move assignment test";
    const char test_data3[] = "Second page data";
    {
      auto write_guard1 = buffer_pool_manager.FetchPageWrite(fd, 2);
      char *data1 = write_guard1.GetMutableData();
      std::memcpy(data1, test_data2, sizeof(test_data2));
      
      auto write_guard2 = buffer_pool_manager.FetchPageWrite(fd, 3);
      char *data2 = write_guard2.GetMutableData();
      std::memcpy(data2, test_data3, sizeof(test_data3));
      
      // Move assignment
      write_guard1 = std::move(write_guard2);
      ASSERT_EQ(write_guard2.GetPage(), nullptr);  // Source should be invalidated
      ASSERT_NE(write_guard1.GetPage(), nullptr);  // Target should be valid
      ASSERT_EQ(write_guard1.GetPageId(), 3);     // Should have moved page ID
      ASSERT_TRUE(write_guard1.IsDirty());
    }
    
    // Verify both pages have correct data
    {
      auto read_guard1 = buffer_pool_manager.FetchPageRead(fd, 2);
      const char *data1 = read_guard1.GetData();
      ASSERT_EQ(std::memcmp(data1, test_data2, sizeof(test_data2)), 0);
      
      auto read_guard2 = buffer_pool_manager.FetchPageRead(fd, 3);
      const char *data2 = read_guard2.GetData();
      ASSERT_EQ(std::memcmp(data2, test_data3, sizeof(test_data3)), 0);
    }
    
    disk_manager.CloseFile(fd);
  }

  SUB_TEST(DropTest)
  {
    auto fd = disk_manager.OpenFile("test_page_guard.tbl");
    
    const char test_data[] = "Drop test data";
    {
      auto write_guard = buffer_pool_manager.FetchPageWrite(fd, 4);
      char *data = write_guard.GetMutableData();
      std::memcpy(data, test_data, sizeof(test_data));
      
      // Manually drop the guard
      write_guard.Drop();
      ASSERT_EQ(write_guard.GetPage(), nullptr);  // Should be invalidated
      
      // Calling Drop again should be safe
      write_guard.Drop();
      ASSERT_EQ(write_guard.GetPage(), nullptr);
    }
    
    // Verify data was written (drop should have unpinned as dirty)
    {
      auto read_guard = buffer_pool_manager.FetchPageRead(fd, 4);
      const char *data = read_guard.GetData();
      ASSERT_EQ(std::memcmp(data, test_data, sizeof(test_data)), 0);
    }
    
    disk_manager.CloseFile(fd);
  }

  SUB_TEST(MultiplePages)
  {
    auto fd = disk_manager.OpenFile("test_page_guard.tbl");
    
    // Write data to multiple pages using WritePageGuard
    std::vector<std::string> page_data(MAX_PAGES);
    for (int i = 0; i < MAX_PAGES; ++i) {
      page_data[i] = "Page " + std::to_string(i) + " data";
      auto write_guard = buffer_pool_manager.FetchPageWrite(fd, i);
      ASSERT_NE(write_guard.GetPage(), nullptr);
      ASSERT_EQ(write_guard.GetFileId(), fd);
      ASSERT_EQ(write_guard.GetPageId(), i);
      ASSERT_TRUE(write_guard.IsDirty());
      
      char *data = write_guard.GetMutableData();
      ASSERT_NE(data, nullptr);
      std::memcpy(data, page_data[i].c_str(), page_data[i].size());
    }
    
    // Read data from multiple pages using ReadPageGuard
    for (int i = 0; i < MAX_PAGES; ++i) {
      auto read_guard = buffer_pool_manager.FetchPageRead(fd, i);
      ASSERT_NE(read_guard.GetPage(), nullptr);
      ASSERT_EQ(read_guard.GetFileId(), fd);
      ASSERT_EQ(read_guard.GetPageId(), i);
      ASSERT_FALSE(read_guard.IsDirty());
      
      const char *data = read_guard.GetData();
      ASSERT_NE(data, nullptr);
      ASSERT_EQ(std::memcmp(data, page_data[i].c_str(), page_data[i].size()), 0);
    }
    
    buffer_pool_manager.DeleteAllPages(fd);
    disk_manager.CloseFile(fd);
  }

  SUB_TEST(DirtyFlagTest)
  {
    auto fd = disk_manager.OpenFile("test_page_guard.tbl");
    
    // Test that ReadPageGuard doesn't mark pages as dirty
    {
      auto read_guard = buffer_pool_manager.FetchPageRead(fd, 0);
      ASSERT_FALSE(read_guard.IsDirty());
      
      // ReadPageGuard doesn't have SetDirty method - this is by design
    }
    
    // Test that WritePageGuard marks pages as dirty
    {
      auto write_guard = buffer_pool_manager.FetchPageWrite(fd, 1);
      ASSERT_TRUE(write_guard.IsDirty());
    
      
      write_guard.UnsetDirty();
      ASSERT_FALSE(write_guard.IsDirty());
    }
    
    disk_manager.CloseFile(fd);
  }

  // Clean up
  wsdb::DiskManager::DestroyFile("test_page_guard.tbl");
}

TEST(PageGuardTest, MultiThreadTest)
{
  wsdb::DiskManager       disk_manager{};
  wsdb::BufferPoolManager buffer_pool_manager(&disk_manager);
  if (!std::filesystem::exists(TEST_DIR))
    std::filesystem::create_directory(TEST_DIR);
  std::filesystem::current_path(TEST_DIR);
  
  try {
    wsdb::DiskManager::CreateFile("test_page_guard_mt.tbl");
  } catch (wsdb::WSDBException_ &e) {
    wsdb::DiskManager::DestroyFile("test_page_guard_mt.tbl");
    wsdb::DiskManager::CreateFile("test_page_guard_mt.tbl");
  }

  SUB_TEST(ConcurrentReadWrite)
  {
    auto fd = disk_manager.OpenFile("test_page_guard_mt.tbl");
    
    std::vector<std::thread> threads;
    std::atomic<int> successful_operations(0);
    
    // Launch multiple threads that read and write using page guards
    for (int t = 0; t < 5; ++t) {
      threads.emplace_back([&, t]() {
        for (int i = 0; i < 100; ++i) {
          try {
            // Write operation
            {
              auto write_guard = buffer_pool_manager.FetchPageWrite(fd, i % 32);
              char *data = write_guard.GetMutableData();
              if (data != nullptr) {
                std::string test_data = "Thread " + std::to_string(t) + " Page " + std::to_string(i);
                std::memcpy(data, test_data.c_str(), std::min(test_data.size(), static_cast<size_t>(4096)));
                successful_operations++;
              }
            }
            
            // Read operation
            {
              auto read_guard = buffer_pool_manager.FetchPageRead(fd, i % 32);
              const char *data = read_guard.GetData();
              if (data != nullptr) {
                successful_operations++;
              }
            }
          } catch (wsdb::WSDBException_ &e) {
            if (e.type_ == wsdb::WSDB_NO_FREE_FRAME) {
              std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
          }
        }
      });
    }
    
    for (auto &thread : threads) {
      thread.join();
    }
    
    ASSERT_GT(successful_operations.load(), 0);
    
    buffer_pool_manager.DeleteAllPages(fd);
    disk_manager.CloseFile(fd);
  }

  // Clean up
  wsdb::DiskManager::DestroyFile("test_page_guard_mt.tbl");
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
