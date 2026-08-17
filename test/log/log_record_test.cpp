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
// Unit tests for Log Record Infrastructure (Phase 1)
//

#include "log/log_record.h"
#include <gtest/gtest.h>
#include <memory>
#include <vector>

namespace njudb {

class LogRecordTest : public ::testing::Test {
protected:
  void SetUp() override {
    // Setup test data
    txn_id_ = 100;
    prev_lsn_ = 50;
    file_id_ = 1;
    page_id_ = 42;
    offset_ = 100;
    length_ = 8;
    
    // Create sample data
    old_data_ = "OLD_DATA";
    new_data_ = "NEW_DATA";
  }
  
  txn_id_t txn_id_;
  lsn_t prev_lsn_;
  file_id_t file_id_;
  page_id_t page_id_;
  uint32_t offset_;
  uint32_t length_;
  std::string old_data_;
  std::string new_data_;
};

//===--------------------------------------------------------------------===//
// Transaction Lifecycle Records Tests
//===--------------------------------------------------------------------===//

TEST_F(LogRecordTest, BeginRecordSerializationTest) {
  // Create BEGIN record
  LogRecord begin_record(txn_id_, prev_lsn_, LogRecordType::BEGIN);
  begin_record.SetLSN(1000);
  
  // Verify basic properties
  EXPECT_EQ(begin_record.GetTxnId(), txn_id_);
  EXPECT_EQ(begin_record.GetPrevLSN(), prev_lsn_);
  EXPECT_EQ(begin_record.GetLogRecordType(), LogRecordType::BEGIN);
  EXPECT_EQ(begin_record.GetLSN(), 1000);
  EXPECT_EQ(begin_record.GetSize(), 18);  // Header (17) + protocol (1)
  
  // Serialize
  std::vector<char> buffer(begin_record.GetSize());
  size_t written = begin_record.SerializeTo(buffer.data());
  EXPECT_EQ(written, begin_record.GetSize());
  
  // Deserialize
  LogRecord deserialized = LogRecord::DeserializeFrom(buffer.data(), buffer.size());
  EXPECT_EQ(deserialized.GetLSN(), begin_record.GetLSN());
  EXPECT_EQ(deserialized.GetTxnId(), begin_record.GetTxnId());
  EXPECT_EQ(deserialized.GetPrevLSN(), begin_record.GetPrevLSN());
  EXPECT_EQ(deserialized.GetLogRecordType(), begin_record.GetLogRecordType());
  EXPECT_EQ(deserialized.GetSize(), begin_record.GetSize());
  
  // Verify ToString
  std::string str = begin_record.ToString();
  EXPECT_NE(str.find("BEGIN"), std::string::npos);
  EXPECT_NE(str.find("LSN=1000"), std::string::npos);
}

TEST_F(LogRecordTest, CommitRecordSerializationTest) {
  // Create COMMIT record
  LogRecord commit_record(txn_id_, prev_lsn_, LogRecordType::COMMIT);
  commit_record.SetLSN(2000);
  
  // Verify properties
  EXPECT_EQ(commit_record.GetLogRecordType(), LogRecordType::COMMIT);
  EXPECT_EQ(commit_record.GetSize(), 17);
  
  // Serialize and deserialize
  std::vector<char> buffer(commit_record.GetSize());
  commit_record.SerializeTo(buffer.data());
  LogRecord deserialized = LogRecord::DeserializeFrom(buffer.data(), buffer.size());
  
  EXPECT_EQ(deserialized.GetLSN(), commit_record.GetLSN());
  EXPECT_EQ(deserialized.GetLogRecordType(), LogRecordType::COMMIT);
  
  // Verify ToString
  std::string str = commit_record.ToString();
  EXPECT_NE(str.find("COMMIT"), std::string::npos);
}

TEST_F(LogRecordTest, AbortRecordSerializationTest) {
  // Create ABORT record
  LogRecord abort_record(txn_id_, prev_lsn_, LogRecordType::ABORT);
  abort_record.SetLSN(3000);
  
  // Verify properties
  EXPECT_EQ(abort_record.GetLogRecordType(), LogRecordType::ABORT);
  EXPECT_EQ(abort_record.GetSize(), 17);
  
  // Serialize and deserialize
  std::vector<char> buffer(abort_record.GetSize());
  abort_record.SerializeTo(buffer.data());
  LogRecord deserialized = LogRecord::DeserializeFrom(buffer.data(), buffer.size());
  
  EXPECT_EQ(deserialized.GetLSN(), abort_record.GetLSN());
  EXPECT_EQ(deserialized.GetLogRecordType(), LogRecordType::ABORT);
  
  // Verify ToString
  std::string str = abort_record.ToString();
  EXPECT_NE(str.find("ABORT"), std::string::npos);
}

//===--------------------------------------------------------------------===//
// UPDATE Record Tests
//===--------------------------------------------------------------------===//

TEST_F(LogRecordTest, UpdateRecordSerializationTest) {
  // Create UPDATE record
  LogRecord update_record(
      txn_id_, prev_lsn_, LogRecordType::UPDATE,
      file_id_, page_id_, offset_, length_,
      old_data_.c_str(), new_data_.c_str()
  );
  update_record.SetLSN(4000);
  
  // Verify properties
  EXPECT_EQ(update_record.GetTxnId(), txn_id_);
  EXPECT_EQ(update_record.GetPrevLSN(), prev_lsn_);
  EXPECT_EQ(update_record.GetLogRecordType(), LogRecordType::UPDATE);
  EXPECT_EQ(update_record.GetLSN(), 4000);
  EXPECT_EQ(update_record.GetFileId(), file_id_);
  EXPECT_EQ(update_record.GetPageId(), page_id_);
  EXPECT_EQ(update_record.GetOffset(), offset_);
  EXPECT_EQ(update_record.GetLength(), length_);
  EXPECT_EQ(update_record.GetOperationType(), DataOperationType::UPDATE);
  
  // Verify data
  const auto& old_data = update_record.GetOldData();
  const auto& new_data = update_record.GetNewData();
  EXPECT_EQ(old_data.size(), length_);
  EXPECT_EQ(new_data.size(), length_);
  EXPECT_EQ(std::string(old_data.begin(), old_data.end()), old_data_);
  EXPECT_EQ(std::string(new_data.begin(), new_data.end()), new_data_);
  
  EXPECT_EQ(update_record.GetSize(), 59);
  
  // Serialize
  std::vector<char> buffer(update_record.GetSize());
  size_t written = update_record.SerializeTo(buffer.data());
  EXPECT_EQ(written, update_record.GetSize());
  
  // Deserialize
  LogRecord deserialized = LogRecord::DeserializeFrom(buffer.data(), buffer.size());
  EXPECT_EQ(deserialized.GetLSN(), update_record.GetLSN());
  EXPECT_EQ(deserialized.GetTxnId(), update_record.GetTxnId());
  EXPECT_EQ(deserialized.GetPrevLSN(), update_record.GetPrevLSN());
  EXPECT_EQ(deserialized.GetLogRecordType(), LogRecordType::UPDATE);
  EXPECT_EQ(deserialized.GetFileId(), file_id_);
  EXPECT_EQ(deserialized.GetPageId(), page_id_);
  EXPECT_EQ(deserialized.GetOffset(), offset_);
  EXPECT_EQ(deserialized.GetLength(), length_);
  EXPECT_EQ(deserialized.GetOperationType(), DataOperationType::UPDATE);
  
  // Verify deserialized data
  const auto& des_old = deserialized.GetOldData();
  const auto& des_new = deserialized.GetNewData();
  EXPECT_EQ(std::string(des_old.begin(), des_old.end()), old_data_);
  EXPECT_EQ(std::string(des_new.begin(), des_new.end()), new_data_);
  
  // Verify ToString
  std::string str = update_record.ToString();
  EXPECT_NE(str.find("UPDATE"), std::string::npos);
  EXPECT_NE(str.find("FileId="), std::string::npos);
  EXPECT_NE(str.find("PageId="), std::string::npos);
}

TEST_F(LogRecordTest, UpdateRecordEmptyDataTest) {
  // Create UPDATE record with empty data (edge case)
  LogRecord update_record(
      txn_id_, prev_lsn_, LogRecordType::UPDATE,
      file_id_, page_id_, offset_, 0,
      nullptr, nullptr
  );
  update_record.SetLSN(5000);
  
  EXPECT_EQ(update_record.GetLength(), 0);
  EXPECT_TRUE(update_record.GetOldData().empty());
  EXPECT_TRUE(update_record.GetNewData().empty());
  
  EXPECT_EQ(update_record.GetSize(), 43);
  
  // Serialize and deserialize
  std::vector<char> buffer(update_record.GetSize());
  update_record.SerializeTo(buffer.data());
  LogRecord deserialized = LogRecord::DeserializeFrom(buffer.data(), buffer.size());
  
  EXPECT_EQ(deserialized.GetLength(), 0);
  EXPECT_TRUE(deserialized.GetOldData().empty());
  EXPECT_TRUE(deserialized.GetNewData().empty());
}

TEST_F(LogRecordTest, UpdateRecordLargeDataTest) {
  // Create UPDATE record with larger data
  std::string large_old(256, 'A');
  std::string large_new(256, 'B');
  
  LogRecord update_record(
      txn_id_, prev_lsn_, LogRecordType::UPDATE,
      file_id_, page_id_, offset_, 256,
      large_old.c_str(), large_new.c_str()
  );
  update_record.SetLSN(6000);
  
  EXPECT_EQ(update_record.GetLength(), 256);
  
  EXPECT_EQ(update_record.GetSize(), 555);
  
  // Serialize and deserialize
  std::vector<char> buffer(update_record.GetSize());
  update_record.SerializeTo(buffer.data());
  LogRecord deserialized = LogRecord::DeserializeFrom(buffer.data(), buffer.size());
  
  const auto& des_old = deserialized.GetOldData();
  const auto& des_new = deserialized.GetNewData();
  EXPECT_EQ(std::string(des_old.begin(), des_old.end()), large_old);
  EXPECT_EQ(std::string(des_new.begin(), des_new.end()), large_new);
}

TEST_F(LogRecordTest, InsertAndDeletePreserveOperationAndMissingImage) {
  LogRecord insert(txn_id_, prev_lsn_, LogRecordType::UPDATE,
                   file_id_, page_id_, offset_, length_, nullptr, new_data_.data(), nullptr,
                   ConcurrencyProtocol::MVCC);
  insert.SetLSN(6100);
  std::vector<char> insert_buffer(insert.GetSize());
  insert.SerializeTo(insert_buffer.data());

  auto insert_round_trip = LogRecord::DeserializeFrom(insert_buffer.data(), insert_buffer.size());
  EXPECT_EQ(insert_round_trip.GetOperationType(), DataOperationType::INSERT);
  EXPECT_EQ(insert_round_trip.GetProtocol(), ConcurrencyProtocol::MVCC);
  EXPECT_TRUE(insert_round_trip.GetOldData().empty());
  EXPECT_EQ(std::string(insert_round_trip.GetNewData().begin(), insert_round_trip.GetNewData().end()), new_data_);

  LogRecord deletion(txn_id_, prev_lsn_, LogRecordType::UPDATE,
                     file_id_, page_id_, offset_, length_, old_data_.data(), nullptr);
  deletion.SetLSN(6101);
  std::vector<char> delete_buffer(deletion.GetSize());
  deletion.SerializeTo(delete_buffer.data());

  auto delete_round_trip = LogRecord::DeserializeFrom(delete_buffer.data(), delete_buffer.size());
  EXPECT_EQ(delete_round_trip.GetOperationType(), DataOperationType::DELETE);
  EXPECT_EQ(std::string(delete_round_trip.GetOldData().begin(), delete_round_trip.GetOldData().end()), old_data_);
  EXPECT_TRUE(delete_round_trip.GetNewData().empty());
}

TEST_F(LogRecordTest, CLRPreservesCompensationOperation) {
  LogRecord clr(txn_id_, prev_lsn_, LogRecordType::CLR, 123,
                file_id_, page_id_, offset_, length_, old_data_.data(),
                DataOperationType::INSERT);
  clr.SetLSN(6200);
  std::vector<char> buffer(clr.GetSize());
  clr.SerializeTo(buffer.data());

  auto round_trip = LogRecord::DeserializeFrom(buffer.data(), buffer.size());
  EXPECT_EQ(round_trip.GetOperationType(), DataOperationType::INSERT);
  EXPECT_EQ(round_trip.GetUndoNextLSN(), 123);
  EXPECT_EQ(std::string(round_trip.GetNewData().begin(), round_trip.GetNewData().end()), old_data_);
}

TEST_F(LogRecordTest, RejectsTruncatedAndCorruptedRecords) {
  LogRecord update(txn_id_, prev_lsn_, LogRecordType::UPDATE,
                   file_id_, page_id_, offset_, length_, old_data_.data(), new_data_.data());
  update.SetLSN(6300);
  std::vector<char> buffer(update.GetSize());
  update.SerializeTo(buffer.data());

  for (size_t size = 0; size < buffer.size(); ++size) {
    EXPECT_THROW(LogRecord::DeserializeFrom(buffer.data(), size), NJUDBException_);
  }

  auto invalid_size = buffer;
  uint32_t oversized = LogRecord::MAX_RECORD_SIZE + 1;
  memcpy(invalid_size.data() + sizeof(lsn_t), &oversized, sizeof(oversized));
  EXPECT_THROW(LogRecord::DeserializeFrom(invalid_size.data(), invalid_size.size()), NJUDBException_);

  auto invalid_operation = buffer;
  constexpr size_t operation_offset =
      17 + sizeof(file_id_t) + sizeof(uint32_t) + sizeof(page_id_t) + sizeof(uint32_t);
  invalid_operation[operation_offset] = static_cast<char>(DataOperationType::INVALID);
  EXPECT_THROW(LogRecord::DeserializeFrom(invalid_operation.data(), invalid_operation.size()), NJUDBException_);

  auto invalid_protocol = buffer;
  constexpr size_t protocol_offset = operation_offset + sizeof(DataOperationType);
  invalid_protocol[protocol_offset] = static_cast<char>(0xFF);
  EXPECT_THROW(LogRecord::DeserializeFrom(invalid_protocol.data(), invalid_protocol.size()), NJUDBException_);
}

//===--------------------------------------------------------------------===//
// CLR Record Tests
//===--------------------------------------------------------------------===//

TEST_F(LogRecordTest, CLRRecordSerializationTest) {
  lsn_t undo_next_lsn = 3500;
  
  // Create CLR record (compensation for undo)
  LogRecord clr_record(
      txn_id_, prev_lsn_, LogRecordType::CLR,
      undo_next_lsn, file_id_, page_id_, offset_, length_,
      old_data_.c_str()  // CLR redoes the old value (undo of UPDATE)
  );
  clr_record.SetLSN(7000);
  
  // Verify properties
  EXPECT_EQ(clr_record.GetLogRecordType(), LogRecordType::CLR);
  EXPECT_EQ(clr_record.GetUndoNextLSN(), undo_next_lsn);
  EXPECT_EQ(clr_record.GetFileId(), file_id_);
  EXPECT_EQ(clr_record.GetPageId(), page_id_);
  EXPECT_EQ(clr_record.GetOffset(), offset_);
  EXPECT_EQ(clr_record.GetLength(), length_);
  EXPECT_EQ(clr_record.GetOperationType(), DataOperationType::UPDATE);
  
  // CLR stores redo data in new_data_
  const auto& redo_data = clr_record.GetNewData();
  EXPECT_EQ(std::string(redo_data.begin(), redo_data.end()), old_data_);
  
  EXPECT_EQ(clr_record.GetSize(), 50);
  
  // Serialize
  std::vector<char> buffer(clr_record.GetSize());
  size_t written = clr_record.SerializeTo(buffer.data());
  EXPECT_EQ(written, clr_record.GetSize());
  
  // Deserialize
  LogRecord deserialized = LogRecord::DeserializeFrom(buffer.data(), buffer.size());
  EXPECT_EQ(deserialized.GetLSN(), clr_record.GetLSN());
  EXPECT_EQ(deserialized.GetLogRecordType(), LogRecordType::CLR);
  EXPECT_EQ(deserialized.GetUndoNextLSN(), undo_next_lsn);
  EXPECT_EQ(deserialized.GetFileId(), file_id_);
  EXPECT_EQ(deserialized.GetPageId(), page_id_);
  EXPECT_EQ(deserialized.GetOperationType(), DataOperationType::UPDATE);
  
  // Verify deserialized redo data
  const auto& des_redo = deserialized.GetNewData();
  EXPECT_EQ(std::string(des_redo.begin(), des_redo.end()), old_data_);
  
  // Verify ToString
  std::string str = clr_record.ToString();
  EXPECT_NE(str.find("CLR"), std::string::npos);
  EXPECT_NE(str.find("UndoNextLSN="), std::string::npos);
}

//===--------------------------------------------------------------------===//
// Checkpoint Record Tests
//===--------------------------------------------------------------------===//

TEST_F(LogRecordTest, CheckpointRecordSerializationTest) {
  // Create CHECKPOINT_BEGIN record
  LogRecord checkpoint_begin(txn_id_, prev_lsn_, LogRecordType::CHECKPOINT_BEGIN);
  checkpoint_begin.SetLSN(8000);
  
  EXPECT_EQ(checkpoint_begin.GetLogRecordType(), LogRecordType::CHECKPOINT_BEGIN);
  EXPECT_EQ(checkpoint_begin.GetSize(), 17);
  
  // Serialize and deserialize
  std::vector<char> buffer(checkpoint_begin.GetSize());
  checkpoint_begin.SerializeTo(buffer.data());
  LogRecord deserialized = LogRecord::DeserializeFrom(buffer.data(), buffer.size());
  
  EXPECT_EQ(deserialized.GetLogRecordType(), LogRecordType::CHECKPOINT_BEGIN);
  
  // Create CHECKPOINT_END record
  LogRecord checkpoint_end(txn_id_, prev_lsn_, LogRecordType::CHECKPOINT_END);
  checkpoint_end.SetLSN(9000);
  
  EXPECT_EQ(checkpoint_end.GetLogRecordType(), LogRecordType::CHECKPOINT_END);
  
  // Verify ToString
  std::string str_begin = checkpoint_begin.ToString();
  EXPECT_NE(str_begin.find("CHECKPOINT_BEGIN"), std::string::npos);
  
  std::string str_end = checkpoint_end.ToString();
  EXPECT_NE(str_end.find("CHECKPOINT_END"), std::string::npos);
}

//===--------------------------------------------------------------------===//
// Log File Header Tests
//===--------------------------------------------------------------------===//

TEST_F(LogRecordTest, LogFileHeaderSerializationTest) {
  // Create log file header
  LogFileHeader header;
  header.first_lsn_ = 1000;
  header.last_lsn_ = 9999;
  header.num_records_ = 42;
  
  // Verify size
  EXPECT_EQ(sizeof(LogFileHeader), 4096);
  
  // Serialize
  std::vector<char> buffer(sizeof(LogFileHeader));
  header.SerializeTo(buffer.data());
  
  // Deserialize
  LogFileHeader deserialized = LogFileHeader::DeserializeFrom(buffer.data());
  
  // Verify fields
  EXPECT_EQ(deserialized.magic_number_, LogFileHeader::MAGIC_NUMBER);
  EXPECT_EQ(deserialized.version_, LogFileHeader::VERSION);
  EXPECT_EQ(deserialized.first_lsn_, 1000);
  EXPECT_EQ(deserialized.last_lsn_, 9999);
  EXPECT_EQ(deserialized.num_records_, 42);
}

TEST_F(LogRecordTest, LogFileHeaderDefaultsTest) {
  LogFileHeader header;
  
  // Verify defaults
  EXPECT_EQ(header.magic_number_, LogFileHeader::MAGIC_NUMBER);
  EXPECT_EQ(header.version_, LogFileHeader::VERSION);
  EXPECT_EQ(header.first_lsn_, 0);
  EXPECT_EQ(header.last_lsn_, INVALID_LSN);
  EXPECT_EQ(header.num_records_, 0);
}

TEST_F(LogRecordTest, RejectsIncompatibleLogFileVersion) {
  LogFileHeader header;
  header.version_ = LogFileHeader::VERSION - 1;
  std::vector<char> buffer(sizeof(LogFileHeader));
  header.SerializeTo(buffer.data());

  EXPECT_THROW(LogFileHeader::DeserializeFrom(buffer.data()), NJUDBException_);
}

//===--------------------------------------------------------------------===//
// Edge Cases and Error Handling
//===--------------------------------------------------------------------===//

TEST_F(LogRecordTest, MultipleRecordSerializationTest) {
  // Create a sequence of log records (simulating a transaction)
  std::vector<LogRecord> records;
  
  // BEGIN
  records.emplace_back(txn_id_, INVALID_LSN, LogRecordType::BEGIN);
  records.back().SetLSN(1000);
  
  // UPDATE 1
  records.emplace_back(txn_id_, 1000, LogRecordType::UPDATE,
                       file_id_, page_id_, offset_, length_,
                       old_data_.c_str(), new_data_.c_str());
  records.back().SetLSN(1001);
  
  // UPDATE 2
  records.emplace_back(txn_id_, 1001, LogRecordType::UPDATE,
                       file_id_, page_id_ + 1, offset_, length_,
                       new_data_.c_str(), old_data_.c_str());
  records.back().SetLSN(1002);
  
  // COMMIT
  records.emplace_back(txn_id_, 1002, LogRecordType::COMMIT);
  records.back().SetLSN(1003);
  
  // Serialize all records
  std::vector<char> buffer(1024);
  size_t offset = 0;
  
  for (const auto& record : records) {
    size_t written = record.SerializeTo(buffer.data() + offset);
    offset += written;
  }
  
  // Deserialize all records
  size_t pos = 0;
  for (size_t i = 0; i < records.size(); ++i) {
    LogRecord deserialized = LogRecord::DeserializeFrom(buffer.data() + pos, records[i].GetSize());
    
    EXPECT_EQ(deserialized.GetLSN(), records[i].GetLSN());
    EXPECT_EQ(deserialized.GetTxnId(), records[i].GetTxnId());
    EXPECT_EQ(deserialized.GetPrevLSN(), records[i].GetPrevLSN());
    EXPECT_EQ(deserialized.GetLogRecordType(), records[i].GetLogRecordType());
    
    pos += deserialized.GetSize();
  }
}

TEST_F(LogRecordTest, LogRecordChainTest) {
  // Test log record chaining (prev_lsn links)
  lsn_t current_lsn = 1000;
  lsn_t prev_lsn = INVALID_LSN;
  
  // Create a chain of 5 records
  for (int i = 0; i < 5; ++i) {
    LogRecord record(txn_id_, prev_lsn, LogRecordType::UPDATE,
                     file_id_, page_id_, offset_, length_,
                     old_data_.c_str(), new_data_.c_str());
    record.SetLSN(current_lsn);
    
    // Verify chain
    EXPECT_EQ(record.GetPrevLSN(), prev_lsn);
    
    // Serialize and deserialize
    std::vector<char> buffer(record.GetSize());
    record.SerializeTo(buffer.data());
    LogRecord deserialized = LogRecord::DeserializeFrom(buffer.data(), buffer.size());
    
    EXPECT_EQ(deserialized.GetPrevLSN(), prev_lsn);
    
    // Update for next iteration
    prev_lsn = current_lsn;
    current_lsn += 1;
  }
}

}  // namespace njudb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
