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

#ifndef NJUDB_TABLE_HANDLE_H
#define NJUDB_TABLE_HANDLE_H
#include <utility>

#include "../../../common/micro.h"
#include "common/page.h"
#include "storage/storage.h"
#include "page_handle.h"

namespace njudb {

/**
 * Table descriptor in memory, including the column schema of the table
 */
class TableHandle
{
public:
  TableHandle() = delete;

  TableHandle(DiskManager *disk_manager, BufferPoolManager *buffer_pool_manager, table_id_t table_id, TableHeader &hdr,
      RecordSchemaUptr &schema, StorageModel storage_model);

  /**
   * Get a record by rid
   * 1. fetch the page handle by rid
   * 2. check if there is a record in the slot using bitmap, if not, unpin the page and throw NJUDB_RECORD_MISS
   * 3. read the record from the slot using page handle
   * 4. unpin the page
   * @param rid
   * @return record
   */
  auto GetRecord(const RID &rid) -> RecordUptr;

  /**
   * Get a chunk in page using record schema indicating which columns should be loaded
   * @param pid
   * @param chunk_schema
   * @return
   */
  auto GetChunk(page_id_t pid, const RecordSchema *chunk_schema) -> ChunkUptr;

  /**
   * Insert a record into the table
   * 1. create a page handle using CreatePageHandle
   * 2. get an empty slot in the page
   * 3. write the record into the slot
   * 4. update the bitmap and the number of records in the page header
   * 5. if the page is full after inserting the record, update the first free page id in the file header and set the
   * next page id of the current page
   * 6. unpin the page
   * @param record
   * @return rid of the inserted record
   */
  auto InsertRecord(const Record &record) -> RID;

  /**
   * Insert a record into the table given rid
   * 1. if rid is invalid, unpin the page and throw NJUDB_PAGE_MISS
   * 2. fetch the page handle and check the bitmap, if the slot is not empty, throw NJUDB_RECORD_EXISTS
   * 3. do the rest of the steps in InsertRecord 3-6
   * @param rid
   * @param record
   */
  void InsertRecord(const RID &rid, const Record &record);

  /**
   * Delete the record by rid
   * 1. if the slot is empty, unpin the page and throw NJUDB_RECORD_MISS
   * 2. update the bitmap and the number of records in the page header
   * 3. if the page is not full after deleting the record, update the first free page id in the file header and the next
   * page id in the page header
   * 4. unpin the page
   * @param rid
   */
  void DeleteRecord(const RID &rid);

  /**
   * Update the record by rid
   * 1. if the slot is empty, unpin the page and throw NJUDB_RECORD_MISS
   * 2. write slot
   * 3. unpin the page
   * @param rid
   * @param record
   */
  void UpdateRecord(const RID &rid, const Record &record);

  [[nodiscard]] auto GetTableId() const -> table_id_t;

  [[nodiscard]] auto GetTableHeader() const -> const TableHeader &;

  [[nodiscard]] auto GetSchema() const -> const RecordSchema &;

  [[nodiscard]] auto GetTableName() const -> std::string;

  [[nodiscard]] auto GetStorageModel() const -> StorageModel;

  [[nodiscard]] auto GetFirstRID() -> RID;

  [[nodiscard]] auto GetNextRID(const RID &rid) -> RID;

  [[nodiscard]] auto HasField(const std::string &field_name) const -> bool;

private:
  /**
   * Fetch the page handle by page id
   * @param page_id
   * @return
   */
  auto FetchPageHandle(page_id_t page_id) -> PageHandleUptr;

  /**
   * Create a page handle that has at least one empty slot
   * @return
   */
  auto CreatePageHandle() -> PageHandleUptr;

  /**
   * Create a fresh new page handle
   * @return
   */
  auto CreateNewPageHandle() -> PageHandleUptr;

  /**
   * Wrap the page handle according to the storage model
   * @param page
   * @return
   */
  auto WrapPageHandle(Page *page) -> PageHandleUptr;

private:
  TableHeader tab_hdr_;
  table_id_t  table_id_;

  DiskManager       *disk_manager_;
  BufferPoolManager *buffer_pool_manager_;

  RecordSchemaUptr schema_;
  StorageModel     storage_model_;

  /// field below is available when storage model is pax
  // field offsets is the offset of each field stored in page
  // pax model is stored like below, field_offset can be calculated by Record Schema
  // | page header | bitmap |
  // slot memery begins
  // | nullmap_1, nullmap_2, ... , nullmap_n|
  // | field_1_1, field_1_2, ... , field_1_n |
  // | field_2_1, field_2_2, ... , field_2_n |
  // ...
  // | field_m_1, field_m_2, ... , field_m_n |
  std::vector<size_t> field_offset_;
};

DEFINE_UNIQUE_PTR(TableHandle);

}  // namespace njudb

#endif  // NJUDB_TABLE_HANDLE_H
