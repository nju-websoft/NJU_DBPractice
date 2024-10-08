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

#ifndef WSDB_PARSER_H
#define WSDB_PARSER_H

#include "ast.h"
#include "../common/micro.h"

namespace wsdb {
class Parser
{
public:
  Parser() = default;

  DISABLE_COPY_MOVE_AND_ASSIGN(Parser)

  [[nodiscard]] static auto Parse(const std::string &sql) -> std::shared_ptr<ast::TreeNode>;
};
}  // namespace wsdb

#endif  // WSDB_PARSER_H
