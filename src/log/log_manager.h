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
// Created by ziqi on 2024/7/18.
// Lab06 - Phase 2: Log Manager Implementation
//

#ifndef NJUDB_LOG_MANAGER_H
#define NJUDB_LOG_MANAGER_H

#include <mutex>
#include <atomic>
#include <thread>
#include <fstream>
#include "storage/disk/disk_manager.h"
#include "log/log_record.h"

namespace njudb {

/**
 * @brief LogManager manages the write-ahead log
 *
 * Responsibilities:
 *  - Append log records with LSN assignment
 *  - Buffer log records in memory
 *  - Flush log buffer to disk (sync/async)
 *  - Provide recovery interface for reading logs
 *
 * Thread Safety:
 *  - AppendLogRecord() is thread-safe (multiple transactions)
 *  - FlushLog() is thread-safe
 *  - Uses mutex for log buffer access
 */
class LogManager {
public:
    /**
     * @brief Construct a new Log Manager
     * @param disk_manager Disk manager for file I/O
     * @param log_file_name Name of log file (default: "njudb.log")
     * @param log_buffer_size Size of log buffer in bytes (default: 4MB)
     */
    explicit LogManager(DiskManager *disk_manager, 
                       const std::string &log_file_name = "njudb.log",
                       size_t log_buffer_size = LOG_BUFFER_SIZE);
    
    ~LogManager();
    
    // Disable copy and move
    DISABLE_COPY_MOVE_AND_ASSIGN(LogManager)
    
    //===--------------------------------------------------------------------===//
    // Append Operations
    //===--------------------------------------------------------------------===//
    
    /**
     * @brief Append a log record to the log buffer
     * 
     * Steps:
     *  1. Assign LSN (atomically increment next_lsn_)
     *  2. Acquire log buffer latch
     *  3. Serialize log record to buffer
     *  4. Update metadata (buffer offset)
     *  5. Check if buffer is full → trigger flush if needed
     *  6. Release latch
     * 
     * @param log_record Log record to append
     * @return LSN assigned to this log record
     */
    auto AppendLogRecord(LogRecord &log_record) -> lsn_t;
    
    //===--------------------------------------------------------------------===//
    // Flush Operations
    //===--------------------------------------------------------------------===//
    
    /**
     * @brief Flush all log records in buffer to disk
     * 
     * Steps:
     *  1. Acquire flush latch
     *  2. If buffer empty, return immediately
     *  3. Write buffer contents to log file
     *  4. fsync() to ensure persistence
     *  5. Update persistent_lsn_
     *  6. Clear buffer, notify waiting threads
     * 
     * Called by:
     *  - Transaction commit (force-log-at-commit)
     *  - Buffer full during append
     *  - Periodic background flush
     */
    void FlushLog();
    
    /**
     * @brief Flush log up to specified LSN
     * 
     * Used for force-log-at-commit: wait until commit record is on disk
     * 
     * @param lsn LSN that must be persisted
     */
    void FlushLogUpTo(lsn_t lsn);
    
    //===--------------------------------------------------------------------===//
    // Recovery Interface
    //===--------------------------------------------------------------------===//
    
    /**
     * @brief Read log record at specified LSN
     * @param lsn LSN to read
     * @return Log record at that LSN
     */
    auto ReadLogRecord(lsn_t lsn) -> LogRecord;
    
    /**
     * @brief Get iterator for all log records from specified LSN
     * @param start_lsn Starting LSN (inclusive)
     * @return Vector of log records
     */
    auto GetLogRecordsFrom(lsn_t start_lsn) -> std::vector<LogRecord>;
    
    //===--------------------------------------------------------------------===//
    // Status and Metadata
    //===--------------------------------------------------------------------===//
    
    /**
     * @brief Get current LSN (next LSN to be assigned)
     * @return Current LSN value
     */
    auto GetCurrentLSN() const -> lsn_t { return next_lsn_.load(); }
    
    /**
     * @brief Get persistent LSN (highest LSN guaranteed on disk)
     * @return Persistent LSN value
     */
    auto GetPersistentLSN() const -> lsn_t { return persistent_lsn_.load(); }
    
    /**
     * @brief Get log file name
     */
    auto GetLogFileName() const -> std::string { return log_file_name_; }
    
    /**
     * @brief Get last checkpoint LSN from master record
     * @return Last checkpoint LSN or INVALID_LSN if none
     */
    auto GetLastCheckpointLSN() const -> lsn_t;
    
    /**
     * @brief Update master record with new checkpoint LSN
     * @param checkpoint_lsn LSN of the checkpoint to record
     */
    void UpdateCheckpointLSN(lsn_t checkpoint_lsn);
    
    //===--------------------------------------------------------------------===//
    // Background Flush Thread
    //===--------------------------------------------------------------------===//
    
    /**
     * @brief Start background flush thread
     * 
     * Periodically flushes log buffer every flush_interval_ms milliseconds
     */
    void StartFlushThread();
    
    /**
     * @brief Stop background flush thread
     */
    void StopFlushThread();

private:
    //===--------------------------------------------------------------------===//
    // Internal Helper Methods
    //===--------------------------------------------------------------------===//
    
    /**
     * @brief Background thread function for periodic log flush
     */
    void FlushThreadLoop();
    
    /**
     * @brief Initialize log file (create or open existing)
     */
    void InitializeLogFile();
    
    /**
     * @brief Write buffer contents to disk
     * @param buffer Buffer to write
     * @param size Size of data in buffer
     * @param batch_max_lsn Maximum LSN contained in this buffer
     */
    void WriteToLogFile(const char* buffer, size_t size, lsn_t batch_max_lsn);

    /**
     * @brief Flush userspace buffers and fsync the actual log file.
     */
    void SyncLogFile();

private:
    //===--------------------------------------------------------------------===//
    // Configuration
    //===--------------------------------------------------------------------===//
    
    static constexpr size_t LOG_BUFFER_SIZE = 4 * 1024 * 1024;  // 4 MB
    static constexpr int FLUSH_INTERVAL_MS = 100;                // 100 ms
    
    //===--------------------------------------------------------------------===//
    // Core State
    //===--------------------------------------------------------------------===//
    
    [[maybe_unused]] DiskManager *disk_manager_;  // Reserved for future use
    std::string log_file_name_;       // Log file path
    std::fstream log_file_;           // Log file stream
    LogFileHeader log_file_header_;   // Current persisted header, including master record
    
    // LSN management
    std::atomic<lsn_t> next_lsn_{0};       // Next LSN to assign
    std::atomic<lsn_t> persistent_lsn_{-1}; // Highest LSN on disk
    
    //===--------------------------------------------------------------------===//
    // Log Buffer (Double Buffering)
    //===--------------------------------------------------------------------===//
    
    char *log_buffer_;                // Active buffer (write here)
    size_t log_buffer_size_;          // Buffer size
    size_t log_buffer_offset_{0};     // Current offset in buffer
    lsn_t log_buffer_max_lsn_{INVALID_LSN}; // Maximum LSN in active buffer
    
    char *flush_buffer_;              // Buffer being flushed
    size_t flush_buffer_size_;        // Size of flush buffer
    
    //===--------------------------------------------------------------------===//
    // Synchronization
    //===--------------------------------------------------------------------===//
    
    std::mutex latch_;                // Protects log buffer
    mutable std::mutex flush_latch_;  // Protects flush operation (mutable for const methods)
    //===--------------------------------------------------------------------===//
    // Background Flush Thread
    //===--------------------------------------------------------------------===//
    
    std::thread flush_thread_;
    std::atomic<bool> flush_thread_running_{false};
};

}  // namespace njudb

#endif  // NJUDB_LOG_MANAGER_H
