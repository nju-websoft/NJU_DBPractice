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

#include "log/log_record.h"
#include "storage/disk/disk_manager.h"
#include "../../common/micro.h"
#include <fmt/format.h>

namespace njudb {

//===--------------------------------------------------------------------===//
// Constructor implementations
//===--------------------------------------------------------------------===//

LogRecord::LogRecord(txn_id_t txn_id, lsn_t prev_lsn, LogRecordType type,
                     file_id_t file_id, page_id_t page_id, uint32_t offset, uint32_t length,
                     const char* old_data, const char* new_data, DiskManager* disk_manager,
                     ConcurrencyProtocol protocol)
    : txn_id_(txn_id), prev_lsn_(prev_lsn), log_record_type_(type),
      file_id_(file_id), page_id_(page_id), offset_(offset), length_(length),
      protocol_(protocol) {
  if (old_data == nullptr && new_data != nullptr) {
    operation_type_ = DataOperationType::INSERT;
  } else if (old_data != nullptr && new_data == nullptr) {
    operation_type_ = DataOperationType::DELETE;
  } else {
    operation_type_ = DataOperationType::UPDATE;
  }
  // Store filename for stability across restarts (CRITICAL FIX)
  if (disk_manager != nullptr) {
    file_name_ = disk_manager->GetFileName(file_id);
  }
  // Deep copy data
  if (old_data != nullptr && length > 0) {
    old_data_.assign(old_data, old_data + length);
  }
  if (new_data != nullptr && length > 0) {
    new_data_.assign(new_data, new_data + length);
  }
  size_ = GetSize();
}

LogRecord::LogRecord(txn_id_t txn_id, lsn_t prev_lsn, LogRecordType type,
                     lsn_t undo_next_lsn, file_id_t file_id, page_id_t page_id,
                     uint32_t offset, uint32_t length, const char* redo_data,
                     DiskManager* disk_manager)
    : LogRecord(txn_id, prev_lsn, type, undo_next_lsn, file_id, page_id, offset,
                length, redo_data,
                redo_data == nullptr ? DataOperationType::DELETE : DataOperationType::UPDATE,
                disk_manager) {}

LogRecord::LogRecord(txn_id_t txn_id, lsn_t prev_lsn, LogRecordType type,
                     lsn_t undo_next_lsn, file_id_t file_id, page_id_t page_id,
                     uint32_t offset, uint32_t length, const char* redo_data,
                     DataOperationType operation, DiskManager* disk_manager)
    : txn_id_(txn_id), prev_lsn_(prev_lsn), log_record_type_(type),
      file_id_(file_id), page_id_(page_id), offset_(offset), length_(length),
      operation_type_(operation), undo_next_lsn_(undo_next_lsn) {
  // Store filename for stability across restarts (CRITICAL FIX)
  if (disk_manager != nullptr) {
    file_name_ = disk_manager->GetFileName(file_id);
  }
  // CLR stores redo data (old_data from UPDATE for redo during recovery)
  if (redo_data != nullptr && length > 0) {
    new_data_.assign(redo_data, redo_data + length);
  }
  size_ = GetSize();
}

//===--------------------------------------------------------------------===//
// Serialization format:
//   [lsn: 4 bytes]
//   [size: 4 bytes]
//   [txn_id: 4 bytes]
//   [prev_lsn: 4 bytes]
//   [log_record_type: 1 byte]
//   
//   For UPDATE records, additionally:
//   [file_id: 4 bytes]
//   [page_id: 4 bytes]
//   [offset: 4 bytes]
//   [operation: 1 byte]
//   [protocol: 1 byte]
//   [old_size: 4 bytes]
//   [new_size: 4 bytes]
//   [old_data: old_size bytes]
//   [new_data: new_size bytes]
//   
//   For CLR records, additionally:
//   [undo_next_lsn: 4 bytes]
//   [file_id: 4 bytes]
//   [page_id: 4 bytes]
//   [offset: 4 bytes]
//   [operation: 1 byte]
//   [redo_size: 4 bytes]
//   [new_data: redo_size bytes] (redo data)
//===--------------------------------------------------------------------===//

auto LogRecord::GetSize() const -> size_t {
  NJUDB_STUDENT_TODO(l6, t1);
}

auto LogRecord::SerializeTo(char* buffer) const -> size_t {
  NJUDB_STUDENT_TODO(l6, t1);
}

auto LogRecord::DeserializeFrom(const char* buffer, size_t buffer_size) -> LogRecord {
  NJUDB_STUDENT_TODO(l6, t1);
}

auto LogRecord::ToString() const -> std::string {
  std::string type_str;
  switch (log_record_type_) {
    case LogRecordType::INVALID:       type_str = "INVALID"; break;
    case LogRecordType::BEGIN:         type_str = "BEGIN"; break;
    case LogRecordType::COMMIT:        type_str = "COMMIT"; break;
    case LogRecordType::ABORT:         type_str = "ABORT"; break;
    case LogRecordType::UPDATE:        type_str = "UPDATE"; break;
    case LogRecordType::CLR:           type_str = "CLR"; break;
    case LogRecordType::CHECKPOINT_BEGIN: type_str = "CHECKPOINT_BEGIN"; break;
    case LogRecordType::CHECKPOINT_END:   type_str = "CHECKPOINT_END"; break;
    default:                           type_str = "UNKNOWN"; break;
  }
  
  std::string result = fmt::format(
      "LogRecord[LSN={}, Type={}, TxnId={}, PrevLSN={}, Size={}",
      lsn_, type_str, txn_id_, prev_lsn_, size_);
  
  switch (log_record_type_) {
    case LogRecordType::BEGIN:
      result += fmt::format(", Protocol={}", 
                           protocol_ == ConcurrencyProtocol::TWO_PHASE_LOCKING ? "2PL" : "MVCC");
      break;
      
    case LogRecordType::CHECKPOINT_END:
      result += fmt::format(", CheckpointDataSize={}", checkpoint_data_.size());
      break;
      
    case LogRecordType::UPDATE:
      result += fmt::format(", FileId={}, PageId={}, Offset={}, Length={}, Operation={}, Protocol={}",
                           file_id_, page_id_, offset_, length_, static_cast<int>(operation_type_),
                           protocol_ == ConcurrencyProtocol::TWO_PHASE_LOCKING ? "2PL" : "MVCC");
      break;
      
    case LogRecordType::CLR:
      result += fmt::format(", UndoNextLSN={}, FileId={}, PageId={}, Offset={}, Length={}, Operation={}",
                           undo_next_lsn_, file_id_, page_id_, offset_, length_, static_cast<int>(operation_type_));
      break;
      
    default:
      // No additional fields to print
      break;
  }
  
  result += "]";
  return result;
}

auto LogRecord::ResolveFileId(DiskManager* disk_manager) const -> file_id_t {
  // CRITICAL: Resolve filename to current file_id
  // This handles the case where file descriptors change after system restart
  if (file_name_.empty()) {
    // Fallback to stored file_id (for backward compatibility or in-memory recovery)
    return file_id_;
  }
  
  // Use DiskManager to get current file_id for this filename
  file_id_t current_fid = disk_manager->GetFileId(file_name_);
  if (current_fid == INVALID_FILE_ID) {
    // File not currently open - this shouldn't happen during recovery
    // because DatabaseHandle.Open() should have opened all tables
    // Try to open it (may fail if filename format doesn't match)
    try {
      current_fid = disk_manager->OpenFile(file_name_);
    } catch (...) {
      // If open fails, throw a more informative error
      throw std::runtime_error(fmt::format(
          "Failed to resolve file_id for filename '{}'. "
          "File not found in DiskManager and cannot be opened.", file_name_));
    }
  }
  
  return current_fid;
}

auto LogRecord::GetTableName() const -> std::string {
  // Extract table name from filename
  // Filename format: {db_name}/{table_name}.tab (or .idx for indexes)
  // Example: "./test_recovery_xxx/crash_test_table.tab" -> "crash_test_table"
  
  if (file_name_.empty()) {
    return "";
  }
  
  // Use OBJNAME_FROM_FILENAME macro which does: std::filesystem::path(filename).stem().string()
  return OBJNAME_FROM_FILENAME(file_name_);
}

}  // namespace njudb
