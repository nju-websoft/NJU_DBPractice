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
// Created by ziqi on 2024/7/28.
//

#ifndef NJUDB_DATABASE_HANDLE_H
#define NJUDB_DATABASE_HANDLE_H

#include <mutex>
#include <thread>
#include "storage/disk/disk_manager.h"
#include "storage/buffer/buffer_pool_manager.h"
#include "system/table/table_manager.h"
#include "system/index/index_manager.h"
#include "concurrency/concurrency_protocol.h"

namespace njudb {

// Forward declaration
class LockManager;
class VersionManager;
class LogManager;

class DatabaseHandle
{
public:
  DatabaseHandle() = delete;

  DatabaseHandle(std::string db_name, DiskManager *disk_manager, TableManager *tbl_mgr, IndexManager *idx_mgr,
      ConcurrencyProtocol protocol = ConcurrencyProtocol::TWO_PHASE_LOCKING);

  void Open();

  void Close();

  void Acquire();

  void Release();

  void FlushMeta();

  /** Rebuild all derived indexes from the final physical table state. */
  void RebuildIndexes();

  [[nodiscard]] auto IsIndexRebuildRequired() const -> bool;

  void CreateTable(const std::string &tab_name, const RecordSchema &rec_schema, StorageModel storage_model);

  void DropTable(const std::string &tab_name);

  void CreateIndex(const std::string &idx_name, const std::string &tab_name, const RecordSchema &key_schema, IndexType idx_type);

  void DropIndex(const std::string &idx_name, const std::string &tab_name);

  [[nodiscard]] auto GetName() const -> std::string { return db_name_; }

  [[nodiscard]] auto GetConcurrencyProtocol() const -> ConcurrencyProtocol { return protocol_; }

  auto GetTable(const std::string &tab_name) -> TableHandle *;

  auto GetTable(table_id_t tid) -> TableHandle *;

  auto GetIndexNum(table_id_t tid) -> size_t;

  auto GetIndex(idx_id_t iid) -> IndexHandle *;

  auto GetIndexes(table_id_t tid) -> std::list<IndexHandle *>;

  auto GetIndexes(const std::string &tab_name) -> std::list<IndexHandle *>;

  auto GetAllTables() -> std::unordered_map<table_id_t, std::unique_ptr<TableHandle>> & { return tables_; }

  void SetLockManager(LockManager *lock_manager) { lock_manager_ = lock_manager; }

  void SetVersionManager(VersionManager *version_manager) { version_manager_ = version_manager; }

  void SetLogManager(LogManager *log_manager);

  /** Keep runtime table/index handles stable while background MVCC GC may reference them. */
  void SetKeepOpenWhenIdle(bool keep_open) { keep_open_when_idle_ = keep_open; }
  
  auto GetDiskManager() const -> DiskManager* { return disk_manager_; }

  ~DatabaseHandle() = default;

private:
  struct IndexDescriptor
  {
    std::string table_name_;
    std::string index_name_;
    IndexType index_type_;
    std::unique_ptr<RecordSchema> key_schema_;
  };

  void OpenUnlocked();

  void CloseUnlocked();

  void MarkIndexRebuildRequired() const;

  void ClearIndexRebuildRequired() const;

  void SyncDatabaseDirectory() const;

  auto SnapshotIndexDescriptors() const -> std::vector<IndexDescriptor>;

  void OpenIndexesFromDescriptors(const std::vector<IndexDescriptor> &descriptors);

  // used to determine when to close db
  int ref_cnt_;
  bool is_open_{false};
  std::mutex lifecycle_latch_;

  std::string db_name_;
  const ConcurrencyProtocol protocol_;

  DiskManager *disk_manager_;

  TableManager *tbl_mgr_;
  IndexManager *idx_mgr_;

  std::unordered_map<table_id_t, std::unique_ptr<TableHandle>> tables_;
  std::unordered_map<idx_id_t, std::unique_ptr<IndexHandle>>   indexes_;
  std::unordered_map<table_id_t, std::list<idx_id_t>>          tab_idx_map_;

  LockManager *lock_manager_{nullptr};
  VersionManager *version_manager_{nullptr};
  LogManager *log_manager_{nullptr};
  bool keep_open_when_idle_{false};
};
}  // namespace njudb

#endif  // NJUDB_DATABASE_HANDLE_H
