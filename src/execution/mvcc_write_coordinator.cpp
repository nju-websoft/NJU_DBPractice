#include "mvcc_write_coordinator.h"

#include <vector>

#include "../../common/error.h"
#include "concurrency/mvcc_write_context.h"

namespace njudb {

namespace {

void ValidateInputs(Transaction *txn, VersionManager *version_manager)
{
  NJUDB_ASSERT(txn != nullptr, "MVCC write requires a transaction");
  NJUDB_ASSERT(version_manager != nullptr, "MVCC write requires a VersionManager");
  if (txn->GetProtocol() != ConcurrencyProtocol::MVCC) {
    NJUDB_THROW(NJUDB_UNSUPPORTED_OP, "MVCC write coordinator requires an MVCC transaction");
  }
}

void RestoreBookkeeping(Transaction *txn, size_t write_set_size, size_t index_write_set_size)
{
  txn->TruncateWriteSet(write_set_size);
  txn->TruncateIndexWriteSet(index_write_set_size);
}

}  // namespace

auto MVCCWriteCoordinator::Insert(
    TableHandle *table, const std::list<IndexHandle *> &indexes, Record &record,
    Transaction *txn, VersionManager *version_manager) -> RID
{
  NJUDB_STUDENT_TODO(l5_bonus, b4);
}

void MVCCWriteCoordinator::Update(
    TableHandle *table, const std::list<IndexHandle *> &indexes, const Record &old_record,
    const Record &new_record, Transaction *txn, VersionManager *version_manager)
{
  NJUDB_STUDENT_TODO(l5_bonus, b4);
}

void MVCCWriteCoordinator::Delete(
    TableHandle *table, const std::list<IndexHandle *> &indexes, const Record &old_record,
    Transaction *txn, VersionManager *version_manager)
{
  NJUDB_STUDENT_TODO(l5_bonus, b4);
}

}  // namespace njudb
