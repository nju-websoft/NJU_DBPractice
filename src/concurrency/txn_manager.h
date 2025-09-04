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

#ifndef NJUDB_TXN_MANAGER_H
#define NJUDB_TXN_MANAGER_H

#include <atomic>
#include <mutex>
#include <unordered_map>

#include "common/types.h"
#include "log/log_manager.h"

namespace njudb {

enum class TxnState
{
  INVALID   = 0,
  GROWING   = 1,
  SHIRNKING = 2,
  COMMITTED = 3,
  ABORTED   = 4
};

class Transaction
{
public:
  Transaction() = default;
  explicit Transaction(txn_id_t txn_id) : txn_id_(txn_id) {}
  virtual ~Transaction() = default;

  [[nodiscard]] auto GetTxnId() const -> txn_id_t { return txn_id_; }

  [[nodiscard]] auto GetState() const -> TxnState { return state_; }

  void SetState(TxnState state) { state_ = state; }

  [[nodiscard]] auto IsExplicit() const -> bool { return is_explcit_; }

private:
  txn_id_t txn_id_{INVALID_TXN_ID};
  TxnState state_{TxnState::INVALID};
  // whether the transaction is explicit, i.e. started by begin command.
  bool is_explcit_{false};
};

class TxnManager
{
public:
  TxnManager() = delete;
  explicit TxnManager(LogManager *log_manager) : log_manager_(log_manager) {}
  virtual ~TxnManager() = default;

  void Begin(txn_id_t txn_id);

  void Commit(txn_id_t txn_id);

  void Abort(txn_id_t txn_id);

  void SetTransaction(Transaction *txn);

private:
  std::atomic<txn_id_t>                                      next_tid_{0};
  std::atomic<txn_id_t>                                      next_ts_{0};
  std::unordered_map<txn_id_t, std::unique_ptr<Transaction>> tid_to_ts_;

  LogManager *log_manager_;

  std::mutex latch_;
};
}  // namespace njudb

#endif  // NJUDB_TXN_MANAGER_H
