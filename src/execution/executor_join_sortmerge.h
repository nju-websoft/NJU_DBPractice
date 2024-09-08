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
// Created by ziqi on 2024/8/7.
//

/**
 * @brief Join the two ordered tables by sort-merge join
 * 
 */

#ifndef WSDB_EXECUTOR_JOIN_SORTMERGE_H
#define WSDB_EXECUTOR_JOIN_SORTMERGE_H

#include "executor_join.h"

namespace wsdb {
class SortMergeJoinExecutor : public JoinExecutor
{
public:
  SortMergeJoinExecutor(JoinType join_type, AbstractExecutorUptr left, AbstractExecutorUptr right,
      RecordSchemaUptr left_key_schema, RecordSchemaUptr right_key_schema);

private:
  void InitInnerJoin() override;

  void NextInnerJoin() override;

  [[nodiscard]] auto IsEndInnerJoin() const -> bool override;

  void InitOuterJoin() override;

  void NextOuterJoin() override;

  [[nodiscard]] auto IsEndOuterJoin() const -> bool override;

  [[nodiscard]] auto Compare(const Record &left, const Record &right) const -> int;

private:
  RecordSchemaUptr left_key_schema_;
  RecordSchemaUptr right_key_schema_;

  // temporarily store record from the left executor
  RecordUptr left_rec_;
  // buffer to store equal values in right executor
  size_t                               right_idx_{0};
  std::vector<std::shared_ptr<Record>> right_buffer_;
};
}  // namespace wsdb

#endif  // WSDB_EXECUTOR_JOIN_SORTMERGE_H
