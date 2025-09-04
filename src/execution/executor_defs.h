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

#ifndef NJUDB_EXECUTOR_DEFS_H
#define NJUDB_EXECUTOR_DEFS_H

#include "executor_aggregate.h"
#include "executor_ddl.h"
#include "executor_delete.h"
#include "executor_filter.h"
#include "executor_idxscan.h"
#include "executor_insert.h"
#include "executor_join_nestedloop.h"
#include "executor_join_hash.h"
#include "executor_join_sortmerge.h"
#include "executor_limit.h"
#include "executor_projection.h"
#include "executor_seqscan.h"
#include "executor_sort.h"
#include "executor_update.h"

#endif  // NJUDB_EXECUTOR_DEFS_H
