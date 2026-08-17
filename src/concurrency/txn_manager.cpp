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
// Created by ziqi on 2024/7/19.
//

#include "txn_manager.h"
#include "timestamp_oracle.h"
#include "lock_manager.h"
#include "undo_record.h"
#include "system/handle/database_handle.h"
#include "../common/error.h"

#include <cstring>

namespace njudb {

namespace {

void RollbackMVCCWrites(Transaction *txn, DatabaseHandle *db_handle)
{
  NJUDB_STUDENT_TODO(l5_bonus, b3);
}

void AppendCLRForUndo(LogManager *log_manager, Transaction *txn,
                      DatabaseHandle *db_handle, const TableWriteRecord &write_record)
{
  if (log_manager == nullptr || db_handle == nullptr) {
    return;
  }

  auto *table_handle = db_handle->GetTable(write_record.table_id_);
  if (table_handle == nullptr || table_handle->GetLogManager() == nullptr) {
    return;
  }

  const char *redo_data = nullptr;
  uint32_t length = 0;
  std::unique_ptr<char[]> combined_data;
  if (write_record.old_tuple_ != nullptr) {
    const auto nullmap_size = table_handle->GetTableHeader().nullmap_size_;
    const auto data_size = write_record.old_tuple_->GetSchema()->GetRecordLength();
    length = nullmap_size + data_size;
    combined_data = std::make_unique<char[]>(length);
    std::memcpy(combined_data.get(), write_record.old_tuple_->GetNullMap(), nullmap_size);
    std::memcpy(combined_data.get() + nullmap_size, write_record.old_tuple_->GetData(), data_size);
    redo_data = combined_data.get();
  }

  lsn_t undo_next_lsn = INVALID_LSN;
  auto compensation_operation = DataOperationType::UPDATE;
  if (write_record.log_lsn_ != INVALID_LSN) {
    log_manager->FlushLogUpTo(write_record.log_lsn_);
    const auto original_record = log_manager->ReadLogRecord(write_record.log_lsn_);
    undo_next_lsn = original_record.GetPrevLSN();
    switch (original_record.GetOperationType()) {
      case DataOperationType::INSERT:
        compensation_operation = DataOperationType::DELETE;
        break;
      case DataOperationType::DELETE:
        compensation_operation = DataOperationType::INSERT;
        break;
      case DataOperationType::UPDATE:
        compensation_operation = DataOperationType::UPDATE;
        break;
      default:
        NJUDB_THROW(NJUDB_LOG_INVALID_RECORD, "Invalid operation in transaction log chain");
    }
  }

  LogRecord clr_record(txn->GetTxnId(), txn->GetPrevLSN(), LogRecordType::CLR,
      undo_next_lsn, write_record.table_id_, write_record.rid_.PageID(),
      write_record.rid_.SlotID(), length, redo_data, compensation_operation,
      db_handle->GetDiskManager());
  txn->SetPrevLSN(log_manager->AppendLogRecord(clr_record));
}

}  // namespace

void TxnManager::Begin(txn_id_t txn_id, IsolationLevel isolation_level, ConcurrencyProtocol protocol)
{
  NJUDB_ASSERT(txn_id != INVALID_TXN_ID, "Invalid txn_id");

  std::lock_guard<std::mutex> lock(latch_);
  NJUDB_ASSERT(tid_to_ts_.find(txn_id) == tid_to_ts_.end(), "txn_id already exists");

  auto txn = std::make_shared<Transaction>(txn_id, isolation_level, protocol);
  // Required: initialize the transaction lifecycle state.
  NJUDB_STUDENT_TODO(l5, t4);

  if (protocol == ConcurrencyProtocol::MVCC) {
    // Bonus: obtain/register the snapshot timestamp and initialize MVCC timestamps.
    NJUDB_STUDENT_TODO(l5_bonus, b3);
  } else {
    // Required: assign the timestamp used by 2PL deadlock prevention.
    NJUDB_STUDENT_TODO(l5, t4);
  }

  if (log_manager_ != nullptr) {
    LogRecord begin_record(txn_id, INVALID_LSN, LogRecordType::BEGIN, protocol);
    txn->SetPrevLSN(log_manager_->AppendLogRecord(begin_record));
  }

  tid_to_ts_.emplace(txn_id, std::move(txn));
}

void TxnManager::Commit(txn_id_t txn_id)
{
  auto txn_handle = ClaimTransactionForEnd(txn_id);
  if (txn_handle == nullptr) {
    return;
  }
  auto finish_guard = std::unique_ptr<void, std::function<void(void *)>>(
      reinterpret_cast<void *>(1), [this, txn_id](void *) { FinishTransaction(txn_id); });
  auto *txn = txn_handle.get();

  if (log_manager_ != nullptr) {
    LogRecord commit_record(txn_id, txn->GetPrevLSN(), LogRecordType::COMMIT);
    txn->SetPrevLSN(log_manager_->AppendLogRecord(commit_record));
    log_manager_->FlushLog();
  }

  if (txn->GetProtocol() == ConcurrencyProtocol::MVCC) {
    // Bonus: choose the commit timestamp, publish versions and retire the snapshot.
    NJUDB_STUDENT_TODO(l5_bonus, b3);
  } else {
    // Required: strict 2PL releases every lock only at transaction end.
    NJUDB_STUDENT_TODO(l5, t4);
  }

  // Required: finish the lifecycle transition only after protocol work succeeds.
  NJUDB_STUDENT_TODO(l5, t4);
}

void TxnManager::Abort(txn_id_t txn_id, DatabaseHandle *db_handle)
{
  auto txn_handle = ClaimTransactionForEnd(txn_id);
  if (txn_handle == nullptr) {
    return;
  }
  auto finish_guard = std::unique_ptr<void, std::function<void(void *)>>(
      reinterpret_cast<void *>(1), [this, txn_id](void *) { FinishTransaction(txn_id); });
  auto *txn = txn_handle.get();

  // Required: reject an already committed transaction and enter ABORTED state.
  NJUDB_STUDENT_TODO(l5, t4);

  if (txn->GetProtocol() == ConcurrencyProtocol::MVCC) {
    RollbackMVCCWrites(txn, db_handle);
    // Bonus: invalidate this transaction's versions and retire its snapshot.
    NJUDB_STUDENT_TODO(l5_bonus, b3);

    if (log_manager_ != nullptr) {
      LogRecord abort_record(txn_id, txn->GetPrevLSN(), LogRecordType::ABORT);
      txn->SetPrevLSN(log_manager_->AppendLogRecord(abort_record));
      log_manager_->FlushLog();
    }
    return;
  }

  // Index entries are not logged separately; restore them before table tuples.
  auto &index_write_set = txn->GetIndexWriteSet();
  for (auto it = index_write_set.rbegin(); it != index_write_set.rend(); ++it) {
    NJUDB_STUDENT_TODO(l5, t4);
  }

  auto &write_set = txn->GetWriteSet();
  for (auto it = write_set.rbegin(); it != write_set.rend(); ++it) {
    auto &write_record = *it;
    // Required: apply the physical inverse operation to the table tuple.
    NJUDB_STUDENT_TODO(l5, t4);

    if (log_manager_ != nullptr) {
      AppendCLRForUndo(log_manager_, txn, db_handle, write_record);
    }
  }

  if (log_manager_ != nullptr) {
    LogRecord abort_record(txn_id, txn->GetPrevLSN(), LogRecordType::ABORT);
    txn->SetPrevLSN(log_manager_->AppendLogRecord(abort_record));
    log_manager_->FlushLog();
  }

  // Required: strict 2PL releases locks only after rollback is complete.
  NJUDB_STUDENT_TODO(l5, t4);
}

void TxnManager::SetTransaction(Transaction *txn) {}

auto TxnManager::GetTransaction(txn_id_t txn_id) -> Transaction *
{
  auto txn = GetTransactionShared(txn_id);
  return txn.get();
}

auto TxnManager::GetTransactionShared(txn_id_t txn_id) -> std::shared_ptr<Transaction>
{
  std::lock_guard<std::mutex> lock(latch_);
  auto it = tid_to_ts_.find(txn_id);
  return it == tid_to_ts_.end() ? nullptr : it->second;
}

auto TxnManager::ClaimTransactionForEnd(txn_id_t txn_id) -> std::shared_ptr<Transaction>
{
  std::lock_guard<std::mutex> lock(latch_);
  auto it = tid_to_ts_.find(txn_id);
  if (it == tid_to_ts_.end() || ending_txns_.find(txn_id) != ending_txns_.end()) {
    return nullptr;
  }

  ending_txns_.insert(txn_id);
  return it->second;
}

void TxnManager::FinishTransaction(txn_id_t txn_id)
{
  std::lock_guard<std::mutex> lock(latch_);
  tid_to_ts_.erase(txn_id);
  ending_txns_.erase(txn_id);
}

auto TxnManager::GetActiveTransactions() -> std::vector<Transaction *>
{
  std::lock_guard<std::mutex> lock(latch_);
  std::vector<Transaction *> active_txns;
  active_txns.reserve(tid_to_ts_.size());
  
  for (auto &[txn_id, txn_ptr] : tid_to_ts_) {
    active_txns.push_back(txn_ptr.get());
  }
  
  return active_txns;
}

}  // namespace njudb
