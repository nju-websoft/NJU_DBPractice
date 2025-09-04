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

#ifndef NJUDB_CONFIG_H
#define NJUDB_CONFIG_H
#include <string>
/// storage
constexpr size_t  PAGE_SIZE        = 4096;
constexpr size_t  BUFFER_POOL_SIZE = 8;
const std::string REPLACER         = "LRUReplacer";
// enable this to use LRUKReplacer
const size_t REPLACER_LRU_K = 10;
/// system
constexpr size_t MAX_REC_SIZE = 1024;
/// executor
// 64MB, used for sort executor's buffer
constexpr size_t SORT_BUFFER_SIZE = 64 * 1024 * 1024;
// 10-way merge sort, max tmp file to use in merge sort
constexpr size_t SORT_WAY_NUM = 10;

const std::string DB_SUFFIX  = ".db";
const std::string TAB_SUFFIX = ".tab";
const std::string IDX_SUFFIX = ".idx";
const std::string TMP_SUFFIX = ".tmp";

const std::string DB_DIR  = "db";
const std::string TAB_DIR = "tab";
const std::string IDX_DIR = "idx";
const std::string TMP_DIR = ".tmp";

// Working directory will be written by cmake
const std::string DATA_DIR = "./data";

#endif  // NJUDB_CONFIG_H