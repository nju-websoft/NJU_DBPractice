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
// Lab06 Phase 4: ARIES Recovery Implementation
//

#include "recovery.h"
#include "checkpoint_manager.h"
#include "concurrency/txn_manager.h"
#include "system/handle/table_handle.h"
#include <algorithm>
#include <csignal>
#include <cstring>
#include <cstdlib>
#include <fmt/format.h>
#include <filesystem>
#include <iostream>
#include <queue>
#include <unordered_set>

namespace njudb {

namespace {

constexpr const char *RECOVERY_CRASHPOINT_ENV = "NJUDB_TEST_RECOVERY_CRASHPOINT";

void MaybeCrashDuringRecovery(const char *point)
{
  const char *target = std::getenv(RECOVERY_CRASHPOINT_ENV);
  if (target != nullptr && std::string(target) == point) {
    std::raise(SIGKILL);
  }
}

auto CompensationOperation(DataOperationType operation) -> DataOperationType {
  switch (operation) {
    case DataOperationType::INSERT:
      return DataOperationType::DELETE;
    case DataOperationType::DELETE:
      return DataOperationType::INSERT;
    case DataOperationType::UPDATE:
      return DataOperationType::UPDATE;
    default:
      NJUDB_THROW(NJUDB_LOG_INVALID_RECORD, "Cannot compensate invalid data operation");
  }
}

}  // namespace

void Recovery::SetDBHandle(DatabaseHandle *db_hdl) {
  db_hdls_.clear();
  if (db_hdl != nullptr) {
    db_hdls_[db_hdl->GetName()] = db_hdl;
  }
}

void Recovery::SetDBHandles(
    const std::unordered_map<std::string, std::unique_ptr<DatabaseHandle>> &db_hdls) {
  db_hdls_.clear();
  for (const auto &[name, db_hdl] : db_hdls) {
    db_hdls_[name] = db_hdl.get();
  }
}

auto Recovery::ResolveTable(const LogRecord &log_record) -> TableHandle * {
  const auto file_path = std::filesystem::path(log_record.GetFileName()).lexically_normal();
  const auto parent = file_path.parent_path();
  const auto table_name = file_path.stem().string();

  for (const auto &[name, db_hdl] : db_hdls_) {
    const auto db_path = std::filesystem::path(name).lexically_normal();
    if (db_path == parent || db_path.filename() == parent.filename()) {
      auto *table = db_hdl->GetTable(table_name);
      NJUDB_ASSERT(table != nullptr, fmt::format("Table '{}' must exist in database '{}'", table_name, name));
      return table;
    }
  }

  if (log_record.GetFileName().empty() && db_hdls_.size() == 1) {
    auto *table = db_hdls_.begin()->second->GetTable(table_name);
    NJUDB_ASSERT(table != nullptr, fmt::format("Table '{}' must exist during recovery", table_name));
    return table;
  }
  NJUDB_THROW(NJUDB_TABLE_MISS, fmt::format("Cannot route recovery log for '{}'", log_record.GetFileName()));
}

void Recovery::Recover()
{
  NJUDB_STUDENT_TODO(l6, t5);
}

void Recovery::AnalyzeLog()
{
  NJUDB_STUDENT_TODO(l6, t5);
}

void Recovery::Redo()
{
  NJUDB_STUDENT_TODO(l6, t5);
}

void Recovery::Undo()
{
  NJUDB_STUDENT_TODO(l6, t5);
}

auto Recovery::FindLastCheckpoint() -> lsn_t
{
  NJUDB_STUDENT_TODO(l6, t5);
}

void Recovery::ApplyLogRecord(const LogRecord &log_record)
{
  NJUDB_STUDENT_TODO(l6, t5);
}

auto Recovery::UndoLogRecord(const LogRecord &log_record, txn_id_t txn_id) -> lsn_t
{
  NJUDB_STUDENT_TODO(l6, t5);
}

}  // namespace njudb
