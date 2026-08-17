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
// Created for Lab06 - Phase 1: Log Record Infrastructure
//

#ifndef NJUDB_LOG_RECORD_H
#define NJUDB_LOG_RECORD_H

#include <cstring>
#include <sstream>
#include <string>
#include <vector>
#include "common/types.h"
#include "common/rid.h"
#include "../../common/error.h"
#include "concurrency/concurrency_protocol.h"

namespace njudb {

// Forward declaration
class DiskManager;

/**
 * @brief Log record types for Write-Ahead Logging
 */
enum class LogRecordType : uint8_t {
  INVALID = 0,
  
  // Transaction lifecycle
  BEGIN = 1,    // Transaction start
  COMMIT = 2,   // Transaction commit
  ABORT = 3,    // Transaction abort
  
  // Data modification (physical logging)
  UPDATE = 10,  // Page update (includes INSERT/DELETE)
  
  // Compensation (for undo during recovery)
  CLR = 20,     // Compensation Log Record
  
  // Checkpointing
  CHECKPOINT_BEGIN = 30,
  CHECKPOINT_END = 31
};

/**
 * @brief Physical tuple operation represented by an UPDATE or CLR record.
 *
 * UPDATE records store the original operation. CLR records store the
 * compensation operation that must be redone after a repeated crash.
 */
enum class DataOperationType : uint8_t {
  INVALID = 0,
  INSERT = 1,
  UPDATE = 2,
  DELETE = 3
};

/**
 * @brief Log record for Write-Ahead Logging
 * 
 * Supports different log record types:
 *  - BEGIN, COMMIT, ABORT: Transaction lifecycle (header only)
 *  - UPDATE: Page modifications with before/after images
 *  - CLR: Compensation during undo (redo-only)
 *  - CHECKPOINT: System snapshots
 */
class LogRecord {
public:
  //===--------------------------------------------------------------------===//
  // Constructors
  //===--------------------------------------------------------------------===//
  
  /**
   * @brief Constructor for BEGIN (with protocol info)
   */
  LogRecord(txn_id_t txn_id, lsn_t prev_lsn, LogRecordType type, ConcurrencyProtocol protocol)
      : txn_id_(txn_id), prev_lsn_(prev_lsn), log_record_type_(type), protocol_(protocol) {
    size_ = GetSize();
  }
  
  /**
   * @brief Constructor for COMMIT, ABORT, CHECKPOINT_BEGIN, or BEGIN with default protocol
   * If type is BEGIN, uses default 2PL protocol for backward compatibility
   */
  LogRecord(txn_id_t txn_id, lsn_t prev_lsn, LogRecordType type)
      : txn_id_(txn_id), prev_lsn_(prev_lsn), log_record_type_(type) {
    // For BEGIN records, default to 2PL for backward compatibility
    if (type == LogRecordType::BEGIN) {
      protocol_ = ConcurrencyProtocol::TWO_PHASE_LOCKING;
    }
    size_ = GetSize();
  }
  
  /**
   * @brief Constructor for CHECKPOINT_END (with checkpoint data)
   */
  LogRecord(txn_id_t txn_id, lsn_t prev_lsn, LogRecordType type,
            const std::vector<char>& checkpoint_data)
      : txn_id_(txn_id), prev_lsn_(prev_lsn), log_record_type_(type),
        checkpoint_data_(checkpoint_data) {
    size_ = GetSize();
  }
  
  /**
   * @brief Constructor for UPDATE (with page modification)
   * @param disk_manager DiskManager to resolve file_id to filename (for stable logging)
   */
  LogRecord(txn_id_t txn_id, lsn_t prev_lsn, LogRecordType type,
            file_id_t file_id, page_id_t page_id, uint32_t offset, uint32_t length,
            const char* old_data, const char* new_data, DiskManager* disk_manager = nullptr,
            ConcurrencyProtocol protocol = ConcurrencyProtocol::TWO_PHASE_LOCKING);
  
  /**
   * @brief Constructor for CLR (compensation during undo)
   * @param disk_manager DiskManager to resolve file_id to filename (for stable logging)
   */
  LogRecord(txn_id_t txn_id, lsn_t prev_lsn, LogRecordType type,
            lsn_t undo_next_lsn, file_id_t file_id, page_id_t page_id,
            uint32_t offset, uint32_t length, const char* redo_data,
            DiskManager* disk_manager = nullptr);

  /**
   * @brief Constructor for CLR with an explicit compensation operation.
   */
  LogRecord(txn_id_t txn_id, lsn_t prev_lsn, LogRecordType type,
            lsn_t undo_next_lsn, file_id_t file_id, page_id_t page_id,
            uint32_t offset, uint32_t length, const char* redo_data,
            DataOperationType operation, DiskManager* disk_manager = nullptr);
  
  // Default constructor
  LogRecord() = default;
  
  //===--------------------------------------------------------------------===//
  // Accessors
  //===--------------------------------------------------------------------===//
  
  auto GetTxnId() const -> txn_id_t { return txn_id_; }
  auto GetPrevLSN() const -> lsn_t { return prev_lsn_; }
  auto GetLogRecordType() const -> LogRecordType { return log_record_type_; }
  auto GetLSN() const -> lsn_t { return lsn_; }
  void SetLSN(lsn_t lsn) { lsn_ = lsn; }
  
  // For UPDATE records
  auto GetFileId() const -> file_id_t { return file_id_; }
  auto GetFileName() const -> const std::string& { return file_name_; }
  auto GetPageId() const -> page_id_t { return page_id_; }
  auto GetOffset() const -> uint32_t { return offset_; }
  auto GetLength() const -> uint32_t { return length_; }
  auto GetOperationType() const -> DataOperationType { return operation_type_; }
  auto GetOldData() const -> const std::vector<char>& { return old_data_; }
  auto GetNewData() const -> const std::vector<char>& { return new_data_; }
  
  /**
   * @brief Resolve filename to current file_id using DiskManager
   * This is critical for recovery after system restart when file descriptors change
   */
  auto ResolveFileId(DiskManager* disk_manager) const -> file_id_t;
  
  /**
   * @brief Extract table name from filename
   * Filename format: {db_name}/{table_name}.tab
   * Returns just the table name part
   */
  auto GetTableName() const -> std::string;
  
  // For CLR records
  auto GetUndoNextLSN() const -> lsn_t { return undo_next_lsn_; }
  
  // For BEGIN and UPDATE records
  auto GetProtocol() const -> ConcurrencyProtocol { return protocol_; }
  
  // For CHECKPOINT_END records
  auto GetCheckpointData() const -> const std::vector<char>& { return checkpoint_data_; }
  
  //===--------------------------------------------------------------------===//
  // Serialization
  //===--------------------------------------------------------------------===//
  
  /**
   * @brief Serialize log record to buffer
   * @param[out] buffer Output buffer (must be at least GetSize() bytes)
   * @return Number of bytes written
   */
  auto SerializeTo(char* buffer) const -> size_t;
  
  /**
   * @brief Deserialize log record from buffer
   * @param[in] buffer Input buffer
   * @return Deserialized log record
   */
  static auto DeserializeFrom(const char* buffer, size_t buffer_size) -> LogRecord;
  
  /**
   * @brief Calculate serialized size of this log record
   * @return Size in bytes
   */
  auto GetSize() const -> size_t;
  
  /**
   * @brief Get human-readable string representation (for debugging)
   */
  auto ToString() const -> std::string;

  static constexpr uint32_t MIN_RECORD_SIZE = 17;
  static constexpr uint32_t MAX_RECORD_SIZE = 64 * 1024 * 1024;

private:
  //===--------------------------------------------------------------------===//
  // Common fields (all log records)
  //===--------------------------------------------------------------------===//
  
  lsn_t lsn_{INVALID_LSN};              // Log Sequence Number (assigned by LogManager)
  uint32_t size_{0};                     // Total size of this log record
  txn_id_t txn_id_{INVALID_TXN_ID};     // Transaction ID
  lsn_t prev_lsn_{INVALID_LSN};         // Previous LSN for this transaction (log chain)
  LogRecordType log_record_type_{LogRecordType::INVALID};
  
  //===--------------------------------------------------------------------===//
  // UPDATE-specific fields
  //===--------------------------------------------------------------------===//
  
  file_id_t file_id_{INVALID_FILE_ID};  // File (table) modified (runtime, unstable)
  std::string file_name_;                // Filename (persistent, stable across restarts)
  page_id_t page_id_{INVALID_PAGE_ID};  // Page modified
  uint32_t offset_{0};                   // Offset within page
  uint32_t length_{0};                   // Length of modified region
  DataOperationType operation_type_{DataOperationType::INVALID};
  std::vector<char> old_data_;           // Before image
  std::vector<char> new_data_;           // After image
  
  //===--------------------------------------------------------------------===//
  // CLR-specific fields
  //===--------------------------------------------------------------------===//
  
  lsn_t undo_next_lsn_{INVALID_LSN};    // Next LSN to undo (skip already undone)
  
  //===--------------------------------------------------------------------===//
  // BEGIN-specific fields
  //===--------------------------------------------------------------------===//
  
  ConcurrencyProtocol protocol_{ConcurrencyProtocol::TWO_PHASE_LOCKING};  // Transaction protocol
  
  //===--------------------------------------------------------------------===//
  // CHECKPOINT_END-specific fields
  //===--------------------------------------------------------------------===//
  
  std::vector<char> checkpoint_data_;   // Serialized checkpoint data
};

/**
 * @brief Log file header structure
 * 
 * Stored at the beginning of log file (first 4KB)
 * Contains master record with last checkpoint LSN for fast recovery startup
 */
struct LogFileHeader {
  static constexpr uint32_t MAGIC_NUMBER = 0x4E4A5544;  // "NJUD" (NJUDB)
  static constexpr uint32_t VERSION = 3;
  static constexpr size_t HEADER_SIZE = 4096;  // 4KB page size
  
  uint32_t magic_number_{MAGIC_NUMBER};
  uint32_t version_{VERSION};
  lsn_t first_lsn_{0};          // First LSN in this log file
  lsn_t last_lsn_{INVALID_LSN}; // Last LSN written
  uint32_t num_records_{0};     // Total number of log records
  lsn_t last_checkpoint_lsn_{INVALID_LSN};  // MASTER RECORD: Last checkpoint LSN
  char padding_[4072];          // Pad to 4096 bytes (4096 - 24 = 4072)
  
  /**
   * @brief Serialize header to buffer
   */
  void SerializeTo(char* buffer) const {
    memcpy(buffer, this, sizeof(LogFileHeader));
  }
  
  /**
   * @brief Deserialize header from buffer
   */
  static auto DeserializeFrom(const char* buffer) -> LogFileHeader {
    LogFileHeader header;
    memcpy(&header, buffer, sizeof(LogFileHeader));
    
    // Validate
    if (header.magic_number_ != MAGIC_NUMBER) {
      NJUDB_THROW(NJUDB_LOG_INVALID_HEADER, "Invalid log file: bad magic number");
    }
    if (header.version_ != VERSION) {
      NJUDB_THROW(NJUDB_LOG_INVALID_HEADER, "Incompatible log file version");
    }
    
    return header;
  }
};

// Ensure header is exactly 4096 bytes
static_assert(sizeof(LogFileHeader) == 4096, 
              "LogFileHeader must be exactly 4096 bytes");

}  // namespace njudb

#endif  // NJUDB_LOG_RECORD_H
