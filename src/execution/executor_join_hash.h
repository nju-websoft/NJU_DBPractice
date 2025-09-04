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
// Created by ziqi on 2024/8/9.
//

#ifndef NJUDB_EXECUTOR_JOIN_HASH_H
#define NJUDB_EXECUTOR_JOIN_HASH_H

#include "executor_join.h"
#include "common/bloom_filter.h"
#include <unordered_map>
#include <vector>
#include <memory>

namespace njudb {

/**
 * Hash Join Executor implementing grace hash join algorithm
 * Features:
 * - Uses smaller relation as build side (typically right side)
 * - Optional bloom filter for early pruning
 * - Supports both inner and outer joins
 * - ONLY supports equi-join conditions (equality predicates)
 * 
 * NOTE: Like sort-merge join, hash join requires:
 * 1. Key schemas that define which fields to use for joining
 * 2. The join conditions must be equality conditions on these key fields
 * 3. Hash join cannot handle non-equality conditions (>, <, !=, etc.)
 */
class HashJoinExecutor : public JoinExecutor
{
public:
  HashJoinExecutor(JoinType join_type, AbstractExecutorUptr left, AbstractExecutorUptr right, 
                   RecordSchemaUptr left_key_schema, RecordSchemaUptr right_key_schema, ConditionVec conditions, bool use_bloom_filter = true);

private:
  void InitInnerJoin() override;
  void NextInnerJoin() override;
  [[nodiscard]] auto IsEndInnerJoin() const -> bool override;

  void InitOuterJoin() override;
  void NextOuterJoin() override;
  [[nodiscard]] auto IsEndOuterJoin() const -> bool override;

  // Hash join specific methods
  void BuildHashTable();

private:
  // Key schemas for extracting join keys (like sort-merge join)
  RecordSchemaUptr left_key_schema_;
  RecordSchemaUptr right_key_schema_;
  
  // Hash table: hash_value -> vector of records with that hash
  std::unordered_map<size_t, std::vector<std::shared_ptr<Record>>> hash_table_;
  
  // Bloom filter for early pruning (optional)
  std::unique_ptr<BloomFilter> bloom_filter_;
  bool use_bloom_filter_;
  
  // Probing state
  bool is_probing_;
  std::vector<std::shared_ptr<Record>>::iterator current_match_iter_;
  std::vector<std::shared_ptr<Record>>::iterator current_match_end_;
  RecordUptr current_left_record_;
  
  // For outer join: track if current left record found any matches
  bool current_left_has_match_;
  bool need_output_null_match_;
  
  // Statistics (for debugging/optimization)
  size_t build_records_;
  size_t probe_records_;
};

}  // namespace njudb

#endif  // NJUDB_EXECUTOR_JOIN_HASH_H
