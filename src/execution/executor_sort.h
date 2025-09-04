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
// Created by ziqi on 2024/8/5.
//

/**
 * @brief Sort the records returned by the child executor
 *
 */

#ifndef NJUDB_EXECUTOR_SORT_H
#define NJUDB_EXECUTOR_SORT_H
#include <functional>
#include <fstream>
#include <utility>
#include "executor_abstract.h"

namespace njudb {

class SortExecutor : public AbstractExecutor
{
public:
  SortExecutor(AbstractExecutorUptr child, RecordSchemaUptr key_schema, bool is_desc);

  ~SortExecutor() override;

  void Init() override;

  void Next() override;

  [[nodiscard]] auto IsEnd() const -> bool override;

  [[nodiscard]] auto GetOutSchema() const -> const RecordSchema * override;

private:
  /// @brief  Sort heap node for merge sort algorithm, ignore it in l2.t1
  class SortHeapNode
  {
  public:
    SortHeapNode() = delete;

    SortHeapNode(std::shared_ptr<std::ifstream> file_handle, const RecordSchema *schema, size_t rec_idx)
        : file_handle_(std::move(file_handle)), schema_(schema), rec_idx_(rec_idx), record_(nullptr)
    {}

    SortHeapNode(const SortHeapNode &other)
    {
      file_handle_ = other.file_handle_;
      schema_      = other.schema_;
      rec_idx_     = other.rec_idx_;
      record_      = std::make_unique<Record>(*other.record_);
    }

    SortHeapNode(SortHeapNode &&other) noexcept
    {
      file_handle_ = std::move(other.file_handle_);
      schema_      = other.schema_;
      rec_idx_     = other.rec_idx_;
      record_      = std::move(other.record_);
    }

    auto operator=(const SortHeapNode &other) -> SortHeapNode &
    {
      if (this == &other) {
        return *this;
      }
      file_handle_ = other.file_handle_;
      schema_      = other.schema_;
      rec_idx_     = other.rec_idx_;
      record_      = std::make_unique<Record>(*other.record_);
      return *this;
    }

    /**
     * Load the next record from the file
     * @param max_rec_num
     * @return true if the record is loaded successfully, false if the file is end or idx exceeds the max record number
     */
    auto LoadNextRecord(size_t max_rec_num) -> bool
    {
      NJUDB_ASSERT(file_handle_ != nullptr, "file_handle_ is nullptr");
      NJUDB_ASSERT(file_handle_->is_open(), "file_handle_ is not open");

      NJUDB_STUDENT_TODO(l2, f1);
    }

    [[nodiscard]] auto GetRecord() const -> const RecordUptr & { return record_; }

    void CloseFile()
    {
      NJUDB_ASSERT(file_handle_ != nullptr, "file_handle_ is nullptr");
      file_handle_->close();
      file_handle_ = nullptr;
    }

  private:
    std::shared_ptr<std::ifstream> file_handle_;
    const RecordSchema            *schema_;   // schema of the record
    size_t                         rec_idx_;  // index of the record in the file
    RecordUptr                     record_;   // record
  };

private:
  [[nodiscard]] inline auto GetSortFileName(size_t file_group, size_t file_idx) const -> std::string;

  [[nodiscard]] inline auto Compare(const Record &lhs, const Record &rhs) const -> bool;

  void SortBuffer();

  void DumpBufferToFile(size_t file_idx);

  void LoadMergeResult();

  inline void Merge();

private:
  AbstractExecutorUptr    child_;
  RecordSchemaUptr        key_schema_;
  std::vector<RecordUptr> sort_buffer_;
  size_t                  buf_idx_;
  bool                    is_desc_;
  bool                    is_sorted_;
  // use for merge sort, if you want to use merge sort, set it to true if the record number is larger than max_rec_num_
  bool        is_merge_sort_;
  size_t      max_rec_num_;
  size_t      tmp_file_num_;
  std::string merge_result_file_;
  // we use file stream instead of disk manager to obtain faster sort speed;
  std::unique_ptr<std::ifstream> merge_result_file_handle_;
};

}  // namespace njudb

#endif  // NJUDB_EXECUTOR_SORT_H
