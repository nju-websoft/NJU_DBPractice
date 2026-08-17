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

#include "log_manager.h"
#include <cerrno>
#include <filesystem>
#include <cstring>
#include <fcntl.h>
#include <unistd.h>

namespace njudb {

//===--------------------------------------------------------------------===//
// Constructor and Initialization
//===--------------------------------------------------------------------===//

LogManager::LogManager(DiskManager *disk_manager, 
                       const std::string &log_file_name,
                       size_t log_buffer_size)
    : disk_manager_(disk_manager),
      log_file_name_(log_file_name),
      log_buffer_size_(log_buffer_size),
      flush_buffer_size_(log_buffer_size) {
    
    // Allocate log buffers
    log_buffer_ = new char[log_buffer_size_];
    flush_buffer_ = new char[flush_buffer_size_];
    
    // Initialize buffers to zero
    memset(log_buffer_, 0, log_buffer_size_);
    memset(flush_buffer_, 0, flush_buffer_size_);
    
    // Initialize log file
    InitializeLogFile();
    
    // Start background flush thread
    StartFlushThread();
}

LogManager::~LogManager() {
    // Stop flush thread
    StopFlushThread();
    
    // Final flush
    FlushLog();
    
    // Close log file
    if (log_file_.is_open()) {
        log_file_.close();
    }
    
    // Free buffers
    delete[] log_buffer_;
    delete[] flush_buffer_;
}

void LogManager::InitializeLogFile() {
  NJUDB_STUDENT_TODO(l6, t2);
}

//===--------------------------------------------------------------------===//
// Append Operations
//===--------------------------------------------------------------------===//

auto LogManager::AppendLogRecord(LogRecord &log_record) -> lsn_t {
  NJUDB_STUDENT_TODO(l6, t2);
}

//===--------------------------------------------------------------------===//
// Flush Operations
//===--------------------------------------------------------------------===//

void LogManager::FlushLog() {
  NJUDB_STUDENT_TODO(l6, t2);
}

void LogManager::FlushLogUpTo(lsn_t lsn) {
  NJUDB_STUDENT_TODO(l6, t2);
}

void LogManager::WriteToLogFile(const char* buffer, size_t size, lsn_t batch_max_lsn) {
  NJUDB_STUDENT_TODO(l6, t2);
}

void LogManager::SyncLogFile() {
    log_file_.flush();
    if (log_file_.fail()) {
        NJUDB_THROW(NJUDB_FILE_WRITE_ERROR, "Failed to flush log file: " + log_file_name_);
    }

    int fd = ::open(log_file_name_.c_str(), O_RDWR);
    if (fd == -1) {
        NJUDB_THROW(NJUDB_FILE_OPEN_ERROR,
                    fmt::format("Failed to open log file for fsync: {}", std::strerror(errno)));
    }
    if (::fsync(fd) != 0) {
        int sync_errno = errno;
        ::close(fd);
        NJUDB_THROW(NJUDB_FILE_WRITE_ERROR,
                    fmt::format("Failed to fsync log file: {}", std::strerror(sync_errno)));
    }
    ::close(fd);
}

//===--------------------------------------------------------------------===//
// Background Flush Thread
//===--------------------------------------------------------------------===//

void LogManager::StartFlushThread() {
    flush_thread_running_ = true;
    flush_thread_ = std::thread(&LogManager::FlushThreadLoop, this);
}

void LogManager::StopFlushThread() {
    if (flush_thread_running_) {
        flush_thread_running_ = false;
        
        if (flush_thread_.joinable()) {
            flush_thread_.join();
        }
    }
}

void LogManager::FlushThreadLoop() {
    while (flush_thread_running_) {
        std::this_thread::sleep_for(std::chrono::milliseconds(FLUSH_INTERVAL_MS));
        FlushLog();
    }
}

//===--------------------------------------------------------------------===//
// Recovery Read Interface
//===--------------------------------------------------------------------===//

auto LogManager::ReadLogRecord(lsn_t lsn) -> LogRecord {
  NJUDB_STUDENT_TODO(l6, t2);
}

auto LogManager::GetLogRecordsFrom(lsn_t start_lsn) -> std::vector<LogRecord> {
  NJUDB_STUDENT_TODO(l6, t2);
}

//===--------------------------------------------------------------------===//
// Master Record (Checkpoint LSN)
//===--------------------------------------------------------------------===//

auto LogManager::GetLastCheckpointLSN() const -> lsn_t {
    // Read log file header to get master record
    std::lock_guard<std::mutex> lock(flush_latch_);
    
    std::ifstream log_in(log_file_name_, std::ios::binary);
    if (!log_in.is_open()) {
        return INVALID_LSN;
    }
    
    // Read header
    char buffer[4096];
    log_in.read(buffer, sizeof(LogFileHeader));
    
    if (log_in.eof() || log_in.fail()) {
        log_in.close();
        return INVALID_LSN;
    }
    
    LogFileHeader header = LogFileHeader::DeserializeFrom(buffer);
    log_in.close();
    
    return header.last_checkpoint_lsn_;
}

void LogManager::UpdateCheckpointLSN(lsn_t checkpoint_lsn) {
    // Update master record in log file header
    std::lock_guard<std::mutex> lock(flush_latch_);
    if (checkpoint_lsn != INVALID_LSN && checkpoint_lsn > persistent_lsn_.load()) {
        NJUDB_THROW(NJUDB_LOG_INVALID_RECORD,
                    fmt::format("Checkpoint LSN {} is not persistent", checkpoint_lsn));
    }
    
    LogFileHeader header = log_file_header_;
    header.last_checkpoint_lsn_ = checkpoint_lsn;
    
    // Write back to beginning of file
    char buffer[4096];
    log_file_.seekp(0, std::ios::beg);
    header.SerializeTo(buffer);
    log_file_.write(buffer, sizeof(LogFileHeader));
    if (log_file_.fail()) {
        NJUDB_THROW(NJUDB_FILE_WRITE_ERROR,
                    "Failed to update checkpoint LSN: " + log_file_name_);
    }
    log_file_.seekp(0, std::ios::end);
    SyncLogFile();
    log_file_header_ = header;
}

}  // namespace njudb
