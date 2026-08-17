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
// Unit tests for Log Manager (Phase 2)
//

#include "log/log_manager.h"
#include "log/log_record.h"
#include "storage/disk/disk_manager.h"
#include <gtest/gtest.h>
#include <filesystem>
#include <fstream>
#include <thread>
#include <vector>
#include <atomic>

namespace njudb {

class LogManagerTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create test log file
        test_log_file_ = "test_log.log";
        
        // Remove existing test log file
        if (std::filesystem::exists(test_log_file_)) {
            std::filesystem::remove(test_log_file_);
        }
        
        disk_manager_ = new DiskManager();
    }
    
    void TearDown() override {
        // Clean up test log file
        if (std::filesystem::exists(test_log_file_)) {
            std::filesystem::remove(test_log_file_);
        }
        
        delete disk_manager_;
    }
    
    std::string test_log_file_;
    DiskManager* disk_manager_;
};

//===--------------------------------------------------------------------===//
// Basic Functionality Tests
//===--------------------------------------------------------------------===//

TEST_F(LogManagerTest, ConstructorAndDestructorTest) {
    // Create log manager
    {
        LogManager log_mgr(disk_manager_, test_log_file_);
        
        // Verify log file created
        EXPECT_TRUE(std::filesystem::exists(test_log_file_));
        
        // Verify initial state
        EXPECT_EQ(log_mgr.GetCurrentLSN(), 0);
        EXPECT_EQ(log_mgr.GetPersistentLSN(), -1);
    }
    
    // After destructor, file should still exist
    EXPECT_TRUE(std::filesystem::exists(test_log_file_));
}

TEST_F(LogManagerTest, AppendAndFlushTest) {
    LogManager log_mgr(disk_manager_, test_log_file_);
    
    // Append BEGIN record
    LogRecord begin(1, INVALID_LSN, LogRecordType::BEGIN);
    lsn_t lsn = log_mgr.AppendLogRecord(begin);
    
    EXPECT_EQ(lsn, 0);
    EXPECT_EQ(log_mgr.GetCurrentLSN(), 1);
    
    // Flush
    log_mgr.FlushLog();
    
    EXPECT_EQ(log_mgr.GetPersistentLSN(), 0);
}

TEST_F(LogManagerTest, MultipleAppendsTest) {
    LogManager log_mgr(disk_manager_, test_log_file_);
    
    // Append multiple records
    for (int i = 0; i < 100; i++) {
        LogRecord record(i, i > 0 ? i - 1 : INVALID_LSN, LogRecordType::BEGIN);
        lsn_t lsn = log_mgr.AppendLogRecord(record);
        EXPECT_EQ(lsn, i);
    }
    
    EXPECT_EQ(log_mgr.GetCurrentLSN(), 100);
    
    // Flush and verify
    log_mgr.FlushLog();
    EXPECT_EQ(log_mgr.GetPersistentLSN(), 99);
}

TEST_F(LogManagerTest, FlushUpToTest) {
    LogManager log_mgr(disk_manager_, test_log_file_);
    
    // Append some records
    LogRecord rec1(1, INVALID_LSN, LogRecordType::BEGIN);
    lsn_t lsn1 = log_mgr.AppendLogRecord(rec1);
    
    LogRecord rec2(1, lsn1, LogRecordType::COMMIT);
    lsn_t lsn2 = log_mgr.AppendLogRecord(rec2);
    
    // Flush up to lsn2
    log_mgr.FlushLogUpTo(lsn2);
    
    // Verify both records are persistent
    EXPECT_GE(log_mgr.GetPersistentLSN(), lsn2);
}

//===--------------------------------------------------------------------===//
// Recovery Read Tests
//===--------------------------------------------------------------------===//

TEST_F(LogManagerTest, RecoveryReadTest) {
    // Write logs
    {
        LogManager log_mgr(disk_manager_, test_log_file_);
        
        for (int i = 0; i < 10; i++) {
            LogRecord record(i, INVALID_LSN, LogRecordType::BEGIN);
            log_mgr.AppendLogRecord(record);
        }
        
        log_mgr.FlushLog();
    }
    
    // Read logs
    {
        LogManager log_mgr(disk_manager_, test_log_file_);
        
        // Read specific record
        LogRecord record = log_mgr.ReadLogRecord(5);
        EXPECT_EQ(record.GetLSN(), 5);
        EXPECT_EQ(record.GetTxnId(), 5);
        
        // Read all records from LSN 5
        auto records = log_mgr.GetLogRecordsFrom(5);
        EXPECT_EQ(records.size(), 5);  // LSN 5-9
        
        for (size_t i = 0; i < records.size(); ++i) {
            EXPECT_EQ(records[i].GetLSN(), 5 + i);
        }
    }
}

TEST_F(LogManagerTest, ReadAllRecordsTest) {
    LogManager log_mgr(disk_manager_, test_log_file_);
    
    // Append various types of records
    LogRecord begin(100, INVALID_LSN, LogRecordType::BEGIN);
    log_mgr.AppendLogRecord(begin);
    
    std::string old_data = "OLD";
    std::string new_data = "NEW";
    LogRecord update(100, 0, LogRecordType::UPDATE,
                     1, 42, 100, 3,
                     old_data.c_str(), new_data.c_str());
    log_mgr.AppendLogRecord(update);
    
    LogRecord commit(100, 1, LogRecordType::COMMIT);
    log_mgr.AppendLogRecord(commit);
    
    log_mgr.FlushLog();
    
    // Read all records
    auto records = log_mgr.GetLogRecordsFrom(0);
    EXPECT_EQ(records.size(), 3);
    
    EXPECT_EQ(records[0].GetLogRecordType(), LogRecordType::BEGIN);
    EXPECT_EQ(records[1].GetLogRecordType(), LogRecordType::UPDATE);
    EXPECT_EQ(records[2].GetLogRecordType(), LogRecordType::COMMIT);
}

//===--------------------------------------------------------------------===//
// Concurrency Tests
//===--------------------------------------------------------------------===//

TEST_F(LogManagerTest, ConcurrentAppendsTest) {
    LogManager log_mgr(disk_manager_, test_log_file_);
    
    const int NUM_THREADS = 10;
    const int RECORDS_PER_THREAD = 100;
    
    std::vector<std::thread> threads;
    std::atomic<int> lsn_sum{0};
    
    // Launch threads
    for (int t = 0; t < NUM_THREADS; t++) {
        threads.emplace_back([&, t]() {
            for (int i = 0; i < RECORDS_PER_THREAD; i++) {
                LogRecord record(t * 1000 + i, INVALID_LSN, LogRecordType::BEGIN);
                lsn_t lsn = log_mgr.AppendLogRecord(record);
                lsn_sum.fetch_add(lsn);
            }
        });
    }
    
    // Wait for all threads
    for (auto &thread : threads) {
        thread.join();
    }
    
    // Verify all records written
    EXPECT_EQ(log_mgr.GetCurrentLSN(), NUM_THREADS * RECORDS_PER_THREAD);
    
    log_mgr.FlushLog();
    EXPECT_EQ(log_mgr.GetPersistentLSN(), NUM_THREADS * RECORDS_PER_THREAD - 1);
}

TEST_F(LogManagerTest, ConcurrentAppendsPersistInStrictLSNOrder) {
    constexpr int NUM_THREADS = 8;
    constexpr int RECORDS_PER_THREAD = 200;
    LogManager log_mgr(disk_manager_, test_log_file_, 512);

    std::vector<std::thread> threads;
    for (int t = 0; t < NUM_THREADS; t++) {
        threads.emplace_back([&, t]() {
            for (int i = 0; i < RECORDS_PER_THREAD; i++) {
                LogRecord record(t * RECORDS_PER_THREAD + i, INVALID_LSN, LogRecordType::BEGIN);
                log_mgr.AppendLogRecord(record);
            }
        });
    }
    for (auto &thread : threads) {
        thread.join();
    }
    log_mgr.FlushLog();

    auto records = log_mgr.GetLogRecordsFrom(0);
    ASSERT_EQ(records.size(), NUM_THREADS * RECORDS_PER_THREAD);
    for (size_t i = 0; i < records.size(); i++) {
        EXPECT_EQ(records[i].GetLSN(), static_cast<lsn_t>(i));
    }
}

TEST_F(LogManagerTest, ConcurrentAppendAndFlushTest) {
    LogManager log_mgr(disk_manager_, test_log_file_);
    
    const int NUM_APPEND_THREADS = 5;
    const int NUM_FLUSH_THREADS = 2;
    const int RECORDS_PER_THREAD = 50;
    
    std::vector<std::thread> threads;
    std::atomic<bool> stop{false};
    
    // Append threads
    for (int t = 0; t < NUM_APPEND_THREADS; t++) {
        threads.emplace_back([&, t]() {
            for (int i = 0; i < RECORDS_PER_THREAD; i++) {
                LogRecord record(t, INVALID_LSN, LogRecordType::BEGIN);
                log_mgr.AppendLogRecord(record);
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
        });
    }
    
    // Flush threads
    for (int t = 0; t < NUM_FLUSH_THREADS; t++) {
        threads.emplace_back([&]() {
            while (!stop.load()) {
                log_mgr.FlushLog();
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
        });
    }
    
    // Wait for append threads
    for (int i = 0; i < NUM_APPEND_THREADS; i++) {
        threads[i].join();
    }
    
    // Stop flush threads
    stop.store(true);
    for (size_t i = NUM_APPEND_THREADS; i < threads.size(); i++) {
        threads[i].join();
    }
    
    // Final flush
    log_mgr.FlushLog();
    
    // Verify all records persisted
    EXPECT_EQ(log_mgr.GetCurrentLSN(), NUM_APPEND_THREADS * RECORDS_PER_THREAD);
    EXPECT_EQ(log_mgr.GetPersistentLSN(), NUM_APPEND_THREADS * RECORDS_PER_THREAD - 1);
}

TEST_F(LogManagerTest, PersistentLSNNeverExceedsDurableRecordsDuringConcurrentFlush) {
    LogManager log_mgr(disk_manager_, test_log_file_, 1024);
    std::atomic<bool> append_done{false};
    std::atomic<bool> invariant_holds{true};

    std::thread appender([&]() {
        for (int i = 0; i < 1000; i++) {
            LogRecord record(i, INVALID_LSN, LogRecordType::BEGIN);
            log_mgr.AppendLogRecord(record);
        }
        append_done = true;
    });

    std::thread flusher([&]() {
        while (!append_done.load()) {
            log_mgr.FlushLog();
            auto persistent = log_mgr.GetPersistentLSN();
            auto records = log_mgr.GetLogRecordsFrom(0);
            if (persistent != INVALID_LSN &&
                (records.empty() || records.back().GetLSN() < persistent)) {
                invariant_holds = false;
            }
        }
    });

    appender.join();
    flusher.join();
    log_mgr.FlushLog();
    EXPECT_TRUE(invariant_holds.load());
    EXPECT_EQ(log_mgr.GetPersistentLSN(), 999);
}

//===--------------------------------------------------------------------===//
// Buffer Management Tests
//===--------------------------------------------------------------------===//

TEST_F(LogManagerTest, BufferOverflowTest) {
    // Use small buffer to trigger overflow
    const size_t SMALL_BUFFER_SIZE = 1024;  // 1 KB
    LogManager log_mgr(disk_manager_, test_log_file_, SMALL_BUFFER_SIZE);
    
    // Append records until buffer overflows
    for (int i = 0; i < 100; i++) {
        LogRecord record(i, i > 0 ? i - 1 : INVALID_LSN, LogRecordType::BEGIN);
        lsn_t lsn = log_mgr.AppendLogRecord(record);
        EXPECT_EQ(lsn, i);
    }
    
    // Verify all records written (some should have been automatically flushed)
    EXPECT_EQ(log_mgr.GetCurrentLSN(), 100);
    
    // Final flush
    log_mgr.FlushLog();
    
    // Verify all persisted
    EXPECT_EQ(log_mgr.GetPersistentLSN(), 99);
}

TEST_F(LogManagerTest, LargeRecordTest) {
    LogManager log_mgr(disk_manager_, test_log_file_);
    
    // Create large UPDATE record
    std::string large_old(4096, 'A');
    std::string large_new(4096, 'B');
    
    LogRecord update(1, INVALID_LSN, LogRecordType::UPDATE,
                     1, 42, 0, 4096,
                     large_old.c_str(), large_new.c_str());
    
    lsn_t lsn = log_mgr.AppendLogRecord(update);
    EXPECT_EQ(lsn, 0);
    
    log_mgr.FlushLog();
    
    // Read back and verify
    LogRecord read_record = log_mgr.ReadLogRecord(lsn);
    EXPECT_EQ(read_record.GetLength(), 4096);
    
    const auto& read_old = read_record.GetOldData();
    const auto& read_new = read_record.GetNewData();
    EXPECT_EQ(std::string(read_old.begin(), read_old.end()), large_old);
    EXPECT_EQ(std::string(read_new.begin(), read_new.end()), large_new);
}

TEST_F(LogManagerTest, RecordLargerThanBufferThrowsWithoutConsumingLSN) {
    LogManager log_mgr(disk_manager_, test_log_file_, 128);
    std::string old_data(128, 'A');
    std::string new_data(128, 'B');
    LogRecord update(1, INVALID_LSN, LogRecordType::UPDATE,
                     1, 42, 0, old_data.size(), old_data.data(), new_data.data());

    EXPECT_THROW(log_mgr.AppendLogRecord(update), NJUDBException_);
    EXPECT_EQ(log_mgr.GetCurrentLSN(), 0);
    EXPECT_EQ(log_mgr.GetPersistentLSN(), INVALID_LSN);
}

//===--------------------------------------------------------------------===//
// Persistence and Recovery Tests
//===--------------------------------------------------------------------===//

TEST_F(LogManagerTest, PersistenceTest) {
    lsn_t last_lsn;
    
    // Write logs
    {
        LogManager log_mgr(disk_manager_, test_log_file_);
        
        for (int i = 0; i < 20; i++) {
            LogRecord record(i, i > 0 ? i - 1 : INVALID_LSN, LogRecordType::BEGIN);
            last_lsn = log_mgr.AppendLogRecord(record);
        }
        
        log_mgr.FlushLog();
    }
    
    // Reopen and verify
    {
        LogManager log_mgr(disk_manager_, test_log_file_);
        
        // Next LSN should continue from where we left off
        EXPECT_EQ(log_mgr.GetCurrentLSN(), last_lsn + 1);
        EXPECT_EQ(log_mgr.GetPersistentLSN(), last_lsn);
        
        // Read all records
        auto records = log_mgr.GetLogRecordsFrom(0);
        EXPECT_EQ(records.size(), 20);
        
        // Append more records
        LogRecord new_record(100, last_lsn, LogRecordType::BEGIN);
        lsn_t new_lsn = log_mgr.AppendLogRecord(new_record);
        EXPECT_EQ(new_lsn, last_lsn + 1);
    }
}

TEST_F(LogManagerTest, TransactionChainTest) {
    LogManager log_mgr(disk_manager_, test_log_file_);
    
    txn_id_t txn_id = 42;
    lsn_t prev_lsn = INVALID_LSN;
    
    // BEGIN
    LogRecord begin(txn_id, prev_lsn, LogRecordType::BEGIN);
    prev_lsn = log_mgr.AppendLogRecord(begin);
    
    // UPDATE 1
    std::string old1 = "old1";
    std::string new1 = "new1";
    LogRecord update1(txn_id, prev_lsn, LogRecordType::UPDATE,
                      1, 10, 0, 4, old1.c_str(), new1.c_str());
    prev_lsn = log_mgr.AppendLogRecord(update1);
    
    // UPDATE 2
    std::string old2 = "old2";
    std::string new2 = "new2";
    LogRecord update2(txn_id, prev_lsn, LogRecordType::UPDATE,
                      1, 20, 0, 4, old2.c_str(), new2.c_str());
    prev_lsn = log_mgr.AppendLogRecord(update2);
    
    // COMMIT
    LogRecord commit(txn_id, prev_lsn, LogRecordType::COMMIT);
    prev_lsn = log_mgr.AppendLogRecord(commit);
    
    log_mgr.FlushLog();
    
    // Read and verify chain
    auto records = log_mgr.GetLogRecordsFrom(0);
    EXPECT_EQ(records.size(), 4);
    
    EXPECT_EQ(records[0].GetPrevLSN(), INVALID_LSN);
    EXPECT_EQ(records[1].GetPrevLSN(), records[0].GetLSN());
    EXPECT_EQ(records[2].GetPrevLSN(), records[1].GetLSN());
    EXPECT_EQ(records[3].GetPrevLSN(), records[2].GetLSN());
}

//===--------------------------------------------------------------------===//
// Background Flush Thread Tests
//===--------------------------------------------------------------------===//

TEST_F(LogManagerTest, BackgroundFlushTest) {
    LogManager log_mgr(disk_manager_, test_log_file_);
    
    // Append records without explicit flush
    for (int i = 0; i < 10; i++) {
        LogRecord record(i, INVALID_LSN, LogRecordType::BEGIN);
        log_mgr.AppendLogRecord(record);
    }
    
    // Wait for background flush (100ms interval + some margin)
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    
    // Records should be flushed by background thread
    EXPECT_EQ(log_mgr.GetPersistentLSN(), 9);
}

TEST_F(LogManagerTest, OrdinaryFlushPreservesCheckpointMasterRecord) {
    LogManager log_mgr(disk_manager_, test_log_file_);
    LogRecord first(1, INVALID_LSN, LogRecordType::BEGIN);
    auto checkpoint_lsn = log_mgr.AppendLogRecord(first);
    log_mgr.FlushLog();
    log_mgr.UpdateCheckpointLSN(checkpoint_lsn);

    LogRecord second(2, INVALID_LSN, LogRecordType::BEGIN);
    log_mgr.AppendLogRecord(second);
    log_mgr.FlushLog();

    EXPECT_EQ(log_mgr.GetLastCheckpointLSN(), checkpoint_lsn);
}

TEST_F(LogManagerTest, MasterRecordRejectsNonPersistentCheckpoint) {
    LogManager log_mgr(disk_manager_, test_log_file_);
    LogRecord record(1, INVALID_LSN, LogRecordType::CHECKPOINT_BEGIN);
    auto checkpoint_lsn = log_mgr.AppendLogRecord(record);

    EXPECT_THROW(log_mgr.UpdateCheckpointLSN(checkpoint_lsn), NJUDBException_);
    EXPECT_EQ(log_mgr.GetLastCheckpointLSN(), INVALID_LSN);
}

//===--------------------------------------------------------------------===//
// Edge Cases and Error Handling
//===--------------------------------------------------------------------===//

TEST_F(LogManagerTest, EmptyFlushTest) {
    LogManager log_mgr(disk_manager_, test_log_file_);
    
    // Flush with no records
    log_mgr.FlushLog();
    
    EXPECT_EQ(log_mgr.GetCurrentLSN(), 0);
    EXPECT_EQ(log_mgr.GetPersistentLSN(), -1);
}

TEST_F(LogManagerTest, MultipleFlushTest) {
    LogManager log_mgr(disk_manager_, test_log_file_);
    
    LogRecord record(1, INVALID_LSN, LogRecordType::BEGIN);
    lsn_t lsn = log_mgr.AppendLogRecord(record);
    
    // Flush multiple times
    log_mgr.FlushLog();
    log_mgr.FlushLog();
    log_mgr.FlushLog();
    
    EXPECT_EQ(log_mgr.GetPersistentLSN(), lsn);
}

TEST_F(LogManagerTest, ReadNonExistentLSNTest) {
    LogManager log_mgr(disk_manager_, test_log_file_);
    
    LogRecord record(1, INVALID_LSN, LogRecordType::BEGIN);
    log_mgr.AppendLogRecord(record);
    log_mgr.FlushLog();
    
    // Try to read non-existent LSN
    EXPECT_THROW(log_mgr.ReadLogRecord(999), NJUDBException_);
}

TEST_F(LogManagerTest, GetRecordsFromFutureTest) {
    LogManager log_mgr(disk_manager_, test_log_file_);
    
    for (int i = 0; i < 5; i++) {
        LogRecord record(i, INVALID_LSN, LogRecordType::BEGIN);
        log_mgr.AppendLogRecord(record);
    }
    log_mgr.FlushLog();
    
    // Get records from future LSN (should return empty)
    auto records = log_mgr.GetLogRecordsFrom(100);
    EXPECT_EQ(records.size(), 0);
}

TEST_F(LogManagerTest, FlushUpToRejectsNonExistentLSN) {
    LogManager log_mgr(disk_manager_, test_log_file_);
    EXPECT_THROW(log_mgr.FlushLogUpTo(0), NJUDBException_);
}

TEST_F(LogManagerTest, FlushUpToReturnsOnlyAfterTargetIsReadable) {
    LogManager log_mgr(disk_manager_, test_log_file_);
    LogRecord record(1, INVALID_LSN, LogRecordType::BEGIN);
    auto lsn = log_mgr.AppendLogRecord(record);

    log_mgr.FlushLogUpTo(lsn);

    EXPECT_GE(log_mgr.GetPersistentLSN(), lsn);
    EXPECT_EQ(log_mgr.ReadLogRecord(lsn).GetLSN(), lsn);
}

TEST_F(LogManagerTest, DmlOperationSurvivesFileRoundTrip) {
    LogManager log_mgr(disk_manager_, test_log_file_);
    const std::string data = "record01";

    LogRecord insert(1, INVALID_LSN, LogRecordType::UPDATE,
                     10, 20, 30, data.size(), nullptr, data.data());
    LogRecord deletion(1, INVALID_LSN, LogRecordType::UPDATE,
                       10, 20, 31, data.size(), data.data(), nullptr);
    auto insert_lsn = log_mgr.AppendLogRecord(insert);
    auto delete_lsn = log_mgr.AppendLogRecord(deletion);
    log_mgr.FlushLog();

    auto read_insert = log_mgr.ReadLogRecord(insert_lsn);
    auto read_delete = log_mgr.ReadLogRecord(delete_lsn);
    EXPECT_EQ(read_insert.GetOperationType(), DataOperationType::INSERT);
    EXPECT_TRUE(read_insert.GetOldData().empty());
    EXPECT_EQ(read_delete.GetOperationType(), DataOperationType::DELETE);
    EXPECT_TRUE(read_delete.GetNewData().empty());
}

TEST_F(LogManagerTest, IgnoresIncompleteTailRecord) {
    LogManager log_mgr(disk_manager_, test_log_file_);
    LogRecord record(1, INVALID_LSN, LogRecordType::BEGIN);
    log_mgr.AppendLogRecord(record);
    log_mgr.FlushLog();

    {
        std::ofstream out(test_log_file_, std::ios::binary | std::ios::app);
        lsn_t partial_lsn = 1;
        uint32_t declared_size = 100;
        out.write(reinterpret_cast<const char *>(&partial_lsn), sizeof(partial_lsn));
        out.write(reinterpret_cast<const char *>(&declared_size), sizeof(declared_size));
    }

    auto records = log_mgr.GetLogRecordsFrom(0);
    ASSERT_EQ(records.size(), 1);
    EXPECT_EQ(records.front().GetLSN(), 0);
    EXPECT_THROW(log_mgr.ReadLogRecord(1), NJUDBException_);
}

}  // namespace njudb

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
