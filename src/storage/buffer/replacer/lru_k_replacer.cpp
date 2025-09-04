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

#include "lru_k_replacer.h"
#include "common/config.h"
#include "../common/error.h"

namespace njudb {

LRUKReplacer::LRUKReplacer(size_t k) : max_size_(BUFFER_POOL_SIZE), k_(k) {}

auto LRUKReplacer::Victim(frame_id_t *frame_id) -> bool { NJUDB_STUDENT_TODO(l1, f1); }

void LRUKReplacer::Pin(frame_id_t frame_id) { NJUDB_STUDENT_TODO(l1, f1); }

void LRUKReplacer::Unpin(frame_id_t frame_id) { NJUDB_STUDENT_TODO(l1, f1); }

auto LRUKReplacer::Size() -> size_t { NJUDB_STUDENT_TODO(l1, f1); }

}  // namespace njudb
