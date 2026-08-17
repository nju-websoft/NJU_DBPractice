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
//

#include "executor_seqscan.h"
#include "concurrency/lock_manager.h"
#include "concurrency/version_manager.h"
#include "concurrency/concurrency_protocol.h"

namespace njudb {

SeqScanExecutor::SeqScanExecutor(TableHandle *tab, Context *context) 
    : AbstractExecutor(Basic), tab_(tab), context_(context) {}

void SeqScanExecutor::Init()
{
  rid_ = tab_->GetFirstRID();
  NJUDB_STUDENT_TODO(l2, t1);
}

void SeqScanExecutor::Next() { NJUDB_STUDENT_TODO(l2, t1); }

void SeqScanExecutor::LoadCurrentVisibleRecord()
{
  while (!IsEnd()) {
    try {
      record_ = tab_->GetRecord(rid_, context_ != nullptr ? context_->txn : nullptr);
      return;
    } catch (const NJUDBException_ &e) {
      if (e.type_ != NJUDB_RECORD_MISS) {
        throw;
      }
      rid_ = tab_->GetNextRID(rid_);
    }
  }

  record_ = nullptr;
}

auto SeqScanExecutor::IsEnd() const -> bool { NJUDB_STUDENT_TODO(l2, t1); }

auto SeqScanExecutor::GetOutSchema() const -> const RecordSchema * { return &tab_->GetSchema(); }
}  // namespace njudb
