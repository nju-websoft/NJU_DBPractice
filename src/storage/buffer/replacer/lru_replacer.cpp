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
// Created by ziqi on 2024/7/17.
//

#include "lru_replacer.h"
#include "common/config.h"
#include "../common/error.h"
namespace njudb {

LRUReplacer::LRUReplacer() : cur_size_(0), max_size_(BUFFER_POOL_SIZE) {}

auto LRUReplacer::Victim(frame_id_t *frame_id) -> bool { NJUDB_STUDENT_TODO(l1, t1); }

void LRUReplacer::Pin(frame_id_t frame_id) { NJUDB_STUDENT_TODO(l1, t1); }

void LRUReplacer::Unpin(frame_id_t frame_id) { NJUDB_STUDENT_TODO(l1, t1); }

auto LRUReplacer::Size() -> size_t { NJUDB_STUDENT_TODO(l1, t1); }

}  // namespace njudb
