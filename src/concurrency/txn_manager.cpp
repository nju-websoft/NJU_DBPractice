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
#include "../common/error.h"

namespace njudb {

void TxnManager::Begin(txn_id_t txn_id)
{
  NJUDB_ASSERT(txn_id != INVALID_TXN_ID, "Invalid txn_id");
  NJUDB_ASSERT(tid_to_ts_.find(txn_id) == tid_to_ts_.end(), "txn_id already exists");
  // create a new transaction and set its state to growing
  std::lock_guard<std::mutex> lock(latch_);
  auto                        txn = std::make_unique<Transaction>(txn_id);
  txn->SetState(TxnState::GROWING);
  tid_to_ts_.emplace(txn_id, std::move(txn));
}

void TxnManager::Commit(txn_id_t txn_id) {}

void TxnManager::Abort(txn_id_t txn_id) {}

void TxnManager::SetTransaction(Transaction *txn) {}

}  // namespace njudb