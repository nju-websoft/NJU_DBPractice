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

namespace njudb {

SeqScanExecutor::SeqScanExecutor(TableHandle *tab) : AbstractExecutor(Basic), tab_(tab) {}

void SeqScanExecutor::Init()
{
  rid_ = tab_->GetFirstRID();
  
  NJUDB_STUDENT_TODO(l2, t1);
}

void SeqScanExecutor::Next() { NJUDB_STUDENT_TODO(l2, t1); }

auto SeqScanExecutor::IsEnd() const -> bool { NJUDB_STUDENT_TODO(l2, t1); }

auto SeqScanExecutor::GetOutSchema() const -> const RecordSchema * { return &tab_->GetSchema(); }
}  // namespace njudb