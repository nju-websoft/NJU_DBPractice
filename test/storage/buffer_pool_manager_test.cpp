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
#include "storage/buffer/buffer_pool_manager.h"
#include "storage/buffer/replacer/lru_replacer.h"
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

[[maybe_unused]] constexpr int MAX_FILES = 11;
[[maybe_unused]] constexpr int MAX_PAGES = 64;

TEST(BufferPoolManagerTest, SimpleTest)
{
  wsdb::DiskManager       disk_manager{};
  wsdb::BufferPoolManager buffer_pool_manager(&disk_manager);
  if (!std::filesystem::exists(TEST_DIR))
    std::filesystem::create_directory(TEST_DIR);
  std::filesystem::current_path(TEST_DIR);
  try {
    wsdb::DiskManager::CreateFile("test.tbl");
  } catch (wsdb::WSDBException_ &e) {
    // destroy and recreate the file
    wsdb::DiskManager::DestroyFile("test.tbl");
    wsdb::DiskManager::CreateFile("test.tbl");
  }
  SUB_TEST(Basic)
  {
    auto  fd   = disk_manager.OpenFile("test.tbl");
    Page *page = buffer_pool_manager.FetchPage(fd, 0);
    ASSERT_NE(page, nullptr);
    ASSERT_EQ(page->GetFileId(), fd);
    ASSERT_EQ(page->GetPageId(), 0);
    ASSERT_NE(page->GetData(), nullptr);
    buffer_pool_manager.UnpinPage(fd, 0, true);
    buffer_pool_manager.UnpinPage(fd, 0, false);
    auto is_dirty = buffer_pool_manager.GetFrame(fd, 0)->IsDirty();
    ASSERT_EQ(is_dirty, true);
    buffer_pool_manager.DeletePage(fd, 0);

    for (int i = 0; i < MAX_PAGES; ++i) {
      page = buffer_pool_manager.FetchPage(fd, i);
      ASSERT_NE(page, nullptr);
      ASSERT_EQ(page->GetFileId(), fd);
      ASSERT_EQ(page->GetPageId(), i);
      ASSERT_NE(page->GetData(), nullptr);
      buffer_pool_manager.UnpinPage(fd, i, false);
    }
    buffer_pool_manager.DeleteAllPages(fd);
    auto fm = buffer_pool_manager.GetFrame(fd, 0);
    ASSERT_EQ(fm, nullptr);

    /// test buffer pool with write
    std::vector<std::string> page_data(MAX_PAGES);
    for (int i = 0; i < MAX_PAGES; ++i) {
      auto rand_str = std::to_string(rand());
    }
    for (int i = 0; i < MAX_PAGES; ++i) {
      page_data[i] = std::to_string(i);
      page         = buffer_pool_manager.FetchPage(fd, i);
      ASSERT_NE(page, nullptr);
      ASSERT_EQ(page->GetFileId(), fd);
      ASSERT_EQ(page->GetPageId(), i);
      ASSERT_NE(page->GetData(), nullptr);
      memcpy(page->GetData(), page_data[i].c_str(), page_data[i].size());
      buffer_pool_manager.UnpinPage(fd, i, true);
    }
    for (int i = 0; i < MAX_PAGES; ++i) {
      page = buffer_pool_manager.FetchPage(fd, i);
      ASSERT_NE(page, nullptr);
      ASSERT_EQ(page->GetFileId(), fd);
      ASSERT_EQ(page->GetPageId(), i);
      ASSERT_NE(page->GetData(), nullptr);
      ASSERT_EQ(memcmp(page->GetData(), page_data[i].c_str(), page_data[i].size()), 0);
      buffer_pool_manager.UnpinPage(fd, i, false);
    }
    buffer_pool_manager.DeleteAllPages(fd);
    disk_manager.CloseFile(fd);
    wsdb::DiskManager::DestroyFile("test.tbl");
  }

  SUB_TEST(MultiFiles)
  {
    for (int i = 0; i < MAX_FILES; ++i) {
      std::string file_name = "test" + std::to_string(i) + ".tbl";
      try {
        wsdb::DiskManager::CreateFile(file_name);
      } catch (wsdb::WSDBException_ &e) {
        // destroy and recreate the file
        wsdb::DiskManager::DestroyFile(file_name);
        wsdb::DiskManager::CreateFile(file_name);
      }
      auto fd = disk_manager.OpenFile(file_name);
      for (int j = 0; j < MAX_PAGES; ++j) {
        auto page = buffer_pool_manager.FetchPage(fd, j);
        ASSERT_NE(page, nullptr);
        ASSERT_EQ(page->GetFileId(), fd);
        ASSERT_EQ(page->GetPageId(), j);
        ASSERT_NE(page->GetData(), nullptr);
        buffer_pool_manager.UnpinPage(fd, j, false);
      }
      buffer_pool_manager.DeleteAllPages(fd);
      disk_manager.CloseFile(fd);
    }
    // write data to different pages in different files;
    std::vector<std::vector<std::string>> file_page_data(MAX_FILES, std::vector<std::string>(MAX_PAGES));
    for (int i = 0; i < MAX_FILES; ++i) {
      for (int j = 0; j < MAX_PAGES; ++j) {
        file_page_data[i][j] = std::to_string(rand());
      }
    }
    for (int i = 0; i < MAX_FILES; ++i) {
      std::string file_name = "test" + std::to_string(i) + ".tbl";
      auto        fd        = disk_manager.OpenFile(file_name);
      for (int j = 0; j < MAX_PAGES; ++j) {
        auto page = buffer_pool_manager.FetchPage(fd, j);
        ASSERT_NE(page, nullptr);
        ASSERT_EQ(page->GetFileId(), fd);
        ASSERT_EQ(page->GetPageId(), j);
        ASSERT_NE(page->GetData(), nullptr);
        memcpy(page->GetData(), file_page_data[i][j].c_str(), file_page_data[i][j].size());
        buffer_pool_manager.UnpinPage(fd, j, true);
      }
    }
    for (int i = 0; i < MAX_FILES; ++i) {
      std::string file_name = "test" + std::to_string(i) + ".tbl";
      auto        fd        = disk_manager.GetFileId(file_name);
      for (int j = 0; j < MAX_PAGES; ++j) {
        auto page = buffer_pool_manager.FetchPage(fd, j);
        ASSERT_NE(page, nullptr);
        ASSERT_EQ(page->GetFileId(), fd);
        ASSERT_EQ(page->GetPageId(), j);
        ASSERT_NE(page->GetData(), nullptr);
        ASSERT_EQ(memcmp(page->GetData(), file_page_data[i][j].c_str(), file_page_data[i][j].size()), 0);
        buffer_pool_manager.UnpinPage(fd, j, false);
      }
    }
    // close and delete all files
    for (int i = 0; i < MAX_FILES; ++i) {
      std::string file_name = "test" + std::to_string(i) + ".tbl";
      auto        fd        = disk_manager.GetFileId(file_name);
      buffer_pool_manager.DeleteAllPages(fd);
      disk_manager.CloseFile(fd);
      wsdb::DiskManager::DestroyFile(file_name);
    }
  }
}

class Progress
{
public:
  explicit Progress(int total) : total_(total), current_(0) {}

  ~Progress() { std::cout << std::endl; }

  void Step() { current_++; }

  void Show()
  {
    mtx_.lock();
    int progress  = current_ * 100 / total_;
    int bar_width = 70;
    std::cout << fmt::format("[{}{}] {}%\r",
        std::string(progress * bar_width / 100, '='),
        std::string(bar_width - progress * bar_width / 100, ' '),
        progress);
    std::cout.flush();
    mtx_.unlock();
  }

private:
  int              total_;
  std::atomic<int> current_;
  std::mutex       mtx_;
};

TEST(BufferPoolManagerTest, MultiThread)
{
  wsdb::DiskManager       disk_manager{};
  wsdb::BufferPoolManager buffer_pool_manager(&disk_manager);
  if (!std::filesystem::exists(TEST_DIR))
    std::filesystem::create_directory(TEST_DIR);
  std::filesystem::current_path(TEST_DIR);
  SUB_TEST(SingleFile)
  {
    std::cout << "Single file test begin..." << std::endl;
    Progress progress(10000 + 10 * MAX_PAGES + 10 * MAX_PAGES);
    try {
      wsdb::DiskManager::CreateFile("test.tbl");
    } catch (wsdb::WSDBException_ &e) {
      // destroy and recreate the file
      wsdb::DiskManager::DestroyFile("test.tbl");
      wsdb::DiskManager::CreateFile("test.tbl");
    }
    auto                     fd = disk_manager.OpenFile("test.tbl");
    std::vector<std::thread> threads;
    threads.reserve(10);
    for (int i = 0; i < 10; ++i) {
      threads.emplace_back([&progress, &buffer_pool_manager, fd] {
        for (int j = 0; j < 1000; ++j) {
          Page *page = nullptr;
          while (page == nullptr) {
            try {
              page = buffer_pool_manager.FetchPage(fd, j);
            } catch (wsdb::WSDBException_ &e) {
              if (e.type_ == wsdb::WSDB_NO_FREE_FRAME) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
              } else {
                throw;
              }
            }
          }
          ASSERT_NE(page, nullptr);
          ASSERT_EQ(page->GetFileId(), fd);
          ASSERT_EQ(page->GetPageId(), j);
          ASSERT_NE(page->GetData(), nullptr);
          buffer_pool_manager.UnpinPage(fd, j, false);
          progress.Step();
          progress.Show();
        }
      });
    }
    for (auto &thread : threads) {
      thread.join();
    }
    buffer_pool_manager.DeleteAllPages(fd);
    disk_manager.CloseFile(fd);

    /// single file write and read
    fd = disk_manager.OpenFile("test.tbl");
    std::vector<std::string> page_data(MAX_PAGES);
    for (int i = 0; i < MAX_PAGES; ++i) {
      page_data[i] = std::to_string(rand());
    }
    threads.clear();
    threads.reserve(10);
    for (int i = 0; i < 10; ++i) {
      threads.emplace_back([&progress, &buffer_pool_manager, fd, &page_data] {
        for (int j = 0; j < MAX_PAGES; ++j) {
          Page *page = nullptr;
          while (page == nullptr) {
            try {
              page = buffer_pool_manager.FetchPage(fd, j);
            } catch (wsdb::WSDBException_ &e) {
              if (e.type_ == wsdb::WSDB_NO_FREE_FRAME) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
              } else {
                throw;
              }
            }
          }
          ASSERT_EQ(page->GetFileId(), fd);
          ASSERT_EQ(page->GetPageId(), j);
          ASSERT_NE(page->GetData(), nullptr);
          memcpy(page->GetData(), page_data[j].c_str(), page_data[j].size());
          buffer_pool_manager.UnpinPage(fd, j, true);
          progress.Step();
          progress.Show();
        }
      });
    }
    for (auto &thread : threads) {
      thread.join();
    }
    threads.clear();
    threads.reserve(10);
    for (int i = 0; i < 10; ++i) {
      threads.emplace_back([&progress, &buffer_pool_manager, fd, &page_data] {
        for (int j = 0; j < MAX_PAGES; ++j) {
          Page *page = nullptr;
          while (page == nullptr) {
            try {
              page = buffer_pool_manager.FetchPage(fd, j);
            } catch (wsdb::WSDBException_ &e) {
              if (e.type_ == wsdb::WSDB_NO_FREE_FRAME) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
              } else {
                throw;
              }
            }
          }
          ASSERT_EQ(page->GetFileId(), fd);
          ASSERT_EQ(page->GetPageId(), j);
          ASSERT_NE(page->GetData(), nullptr);
          ASSERT_EQ(memcmp(page->GetData(), page_data[j].c_str(), page_data[j].size()), 0);
          buffer_pool_manager.UnpinPage(fd, j, false);
          progress.Step();
          progress.Show();
        }
      });
    }
    for (auto &thread : threads) {
      thread.join();
    }
    buffer_pool_manager.DeleteAllPages(fd);
    disk_manager.CloseFile(fd);
  }
  SUB_TEST(MultiFiles)
  {
    std::cout << "Multi files test begin..." << std::endl;
    Progress progress(10 * 2 * MAX_FILES * 1001 + 10 * 2 * MAX_FILES * 1001);
    for (int i = 0; i < MAX_FILES; ++i) {
      std::string file_name = "test" + std::to_string(i) + ".tbl";
      try {
        wsdb::DiskManager::CreateFile(file_name);
      } catch (wsdb::WSDBException_ &e) {
        // destroy and recreate the file
        wsdb::DiskManager::DestroyFile(file_name);
        wsdb::DiskManager::CreateFile(file_name);
      }
    }
    std::vector<std::thread> threads;
    threads.reserve(10);
    // generate data
    std::vector<std::vector<std::string>> file_page_data(MAX_FILES, std::vector<std::string>(MAX_PAGES));
    for (int i = 0; i < MAX_FILES; ++i) {
      for (int j = 0; j < MAX_PAGES; ++j) {
        file_page_data[i][j] = std::to_string(rand());
      }
    }
    // open all tables
    for (file_id_t i = 0; i < MAX_FILES; ++i) {
      disk_manager.OpenFile("test" + std::to_string(i) + ".tbl");
    }
    // multi thread read and write
    for (int i = 0; i < 10; ++i) {
      threads.emplace_back([&progress, &buffer_pool_manager, &disk_manager, &file_page_data] {
        for (int i = 0; i < 2 * MAX_FILES; ++i) {
          const int   file_name_no = i % MAX_FILES;
          std::string rand_file    = "test" + std::to_string(file_name_no) + ".tbl";
          file_id_t   fd           = disk_manager.GetFileId(rand_file);
          ASSERT_NE(fd, INVALID_FILE_ID);
          for (int j = 0; j < 1001; ++j) {
            Page     *page   = nullptr;
            page_id_t rd_pid = j % MAX_PAGES;
            while (page == nullptr) {
              try {
                page = buffer_pool_manager.FetchPage(fd, rd_pid);
              } catch (wsdb::WSDBException_ &e) {
                if (e.type_ == wsdb::WSDB_NO_FREE_FRAME) {
                  std::this_thread::sleep_for(std::chrono::milliseconds(1));
                } else {
                  throw;
                }
              }
            }
            ASSERT_NE(page, nullptr);
            ASSERT_EQ(page->GetFileId(), fd);
            ASSERT_EQ(page->GetPageId(), rd_pid);
            ASSERT_NE(page->GetData(), nullptr);
            memcpy(page->GetData(),
                file_page_data[file_name_no][rd_pid].c_str(),
                file_page_data[file_name_no][rd_pid].size());
            buffer_pool_manager.UnpinPage(fd, rd_pid, true);
            progress.Step();
            progress.Show();
          }
        }
      });
    }
    for (auto &thread : threads) {
      thread.join();
    }
    threads.clear();
    // read data
    for (int i = 0; i < 10; ++i) {
      threads.emplace_back([&progress, &buffer_pool_manager, &disk_manager, &file_page_data] {
        for (int i = 0; i < 2 * MAX_FILES; ++i) {
          const int   file_name_no = i % MAX_FILES;
          std::string rand_file    = "test" + std::to_string(file_name_no) + ".tbl";
          file_id_t   fd           = disk_manager.GetFileId(rand_file);
          ASSERT_NE(fd, INVALID_FILE_ID);
          for (int j = 0; j < 1001; ++j) {
            Page     *page   = nullptr;
            page_id_t rd_pid = j % MAX_PAGES;
            while (page == nullptr) {
              try {
                page = buffer_pool_manager.FetchPage(fd, rd_pid);
              } catch (wsdb::WSDBException_ &e) {
                if (e.type_ == wsdb::WSDB_NO_FREE_FRAME) {
                  std::this_thread::sleep_for(std::chrono::milliseconds(1));
                } else {
                  throw;
                }
              }
            }
            ASSERT_NE(page, nullptr);
            ASSERT_EQ(page->GetFileId(), fd);
            ASSERT_EQ(page->GetPageId(), rd_pid);
            ASSERT_NE(page->GetData(), nullptr);
            ASSERT_EQ(memcmp(page->GetData(),
                          file_page_data[file_name_no][rd_pid].c_str(),
                          file_page_data[file_name_no][rd_pid].size()),
                0);
            buffer_pool_manager.UnpinPage(fd, rd_pid, false);
            progress.Step();
            progress.Show();
          }
        }
      });
    }
    for (auto &thread : threads) {
      thread.join();
    }
    threads.clear();
    // delete all pages
    for (int i = 0; i < MAX_FILES; ++i) {
      std::string file_name = "test" + std::to_string(i) + ".tbl";
      file_id_t   fd        = disk_manager.GetFileId(file_name);
      buffer_pool_manager.DeleteAllPages(fd);
      disk_manager.CloseFile(fd);
      wsdb::DiskManager::DestroyFile(file_name);
    }
  }
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
