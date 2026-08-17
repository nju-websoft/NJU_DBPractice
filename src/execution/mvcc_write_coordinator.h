#ifndef NJUDB_MVCC_WRITE_COORDINATOR_H
#define NJUDB_MVCC_WRITE_COORDINATOR_H

#include <list>

#include "concurrency/transaction.h"
#include "concurrency/version_manager.h"
#include "system/handle/index_handle.h"
#include "system/handle/table_handle.h"

namespace njudb {

/**
 * Coordinates one MVCC tuple write across the version chain, table page, and indexes.
 *
 * Each method either completes all three parts and records them in the transaction,
 * or restores the state that existed before the call and rethrows the failure.
 */
class MVCCWriteCoordinator
{
public:
  static auto Insert(
      TableHandle *table, const std::list<IndexHandle *> &indexes, Record &record,
      Transaction *txn, VersionManager *version_manager) -> RID;

  static void Update(
      TableHandle *table, const std::list<IndexHandle *> &indexes, const Record &old_record,
      const Record &new_record, Transaction *txn, VersionManager *version_manager);

  static void Delete(
      TableHandle *table, const std::list<IndexHandle *> &indexes, const Record &old_record,
      Transaction *txn, VersionManager *version_manager);
};

}  // namespace njudb

#endif  // NJUDB_MVCC_WRITE_COORDINATOR_H
