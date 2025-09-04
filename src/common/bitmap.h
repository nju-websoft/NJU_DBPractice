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

#ifndef NJUDB_BITMAP_H
#define NJUDB_BITMAP_H

#include <cstring>
#include "../../common/error.h"
#include "../../common/micro.h"

namespace njudb {
#define BITMAP_WIDTH 8
#define BITMAP_SIZE(bit_num) ((bit_num + BITMAP_WIDTH - 1) / BITMAP_WIDTH)

class BitMap
{

public:
  BitMap()  = delete;
  ~BitMap() = delete;
  DISABLE_COPY_MOVE_AND_ASSIGN(BitMap);

  static void SetBit(char *bitmap, size_t bit_idx, bool value)
  {
    if (value) {
      bitmap[bit_idx / BITMAP_WIDTH] |= (1 << (bit_idx % BITMAP_WIDTH));
    } else {
      bitmap[bit_idx / BITMAP_WIDTH] &= ~(1 << (bit_idx % BITMAP_WIDTH));
    }
  }

  static auto GetBit(const char *bitmap, size_t bit_idx) -> bool
  {
    return (bitmap[bit_idx / BITMAP_WIDTH] & (1 << (bit_idx % BITMAP_WIDTH))) != 0;
  }

  static void Clear(char *bitmap, size_t bit_num) { memset(bitmap, 0, BITMAP_SIZE(bit_num)); }

  static void Set(char *bitmap, size_t bit_num) { memset(bitmap, 0xff, BITMAP_SIZE(bit_num)); }

  static auto FindFirst(const char *bitmap, size_t bit_num, size_t start, bool value) -> size_t
  {
    for (size_t i = start; i < bit_num; i++) {
      if (GetBit(bitmap, i) == value) {
        return i;
      }
    }
    return bit_num;
  }
};
}  // namespace njudb

#endif  // NJUDB_BITMAP_H
