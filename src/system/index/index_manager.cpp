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
// Created by ziqi on 2024/7/28.
//

#include "index_manager.h"

namespace wsdb {
void IndexManager::CreateIndex(const std::string &db_name, const std::string &index_name,
    const wsdb::RecordSchema &schema, wsdb::IndexType index_type)
{
  throw WSDBException(WSDB_NOT_IMPLEMENTED, Q(IndexManager), Q(CreateIndex()));
}

void IndexManager::DropIndex(const std::string &db_name, const std::string &index_name)
{
  throw WSDBException(WSDB_NOT_IMPLEMENTED, Q(IndexManager), Q(DropIndex()));
}
IndexHandleUptr IndexManager::OpenIndex(const std::string &db_name, const std::string &index_name, IndexType index_type)
{
  throw WSDBException(WSDB_NOT_IMPLEMENTED, Q(IndexManager), Q(OpenIndex()));
}

void IndexManager::CloseIndex(const IndexHandle &index_handle)
{
  throw WSDBException(WSDB_NOT_IMPLEMENTED, Q(IndexManager), Q(CloseIndex()));
}

}  // namespace wsdb
