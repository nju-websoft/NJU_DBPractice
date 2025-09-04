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
// Created by ziqi on 2024/8/25.
//

#include "parser.h"
#include "def.h"
#include "../common/error.h"

namespace njudb {

std::shared_ptr<ast::TreeNode> Parser::Parse(const std::string &sql)
{
  auto buf = yy_scan_string(sql.c_str());
  if (yyparse() != 0) {
    yy_delete_buffer(buf);
    NJUDB_THROW(NJUDB_INVALID_SQL, sql);
  }
  auto ret = ast::njudb_ast_;
  yy_delete_buffer(buf);
  return ret;
}

}  // namespace njudb