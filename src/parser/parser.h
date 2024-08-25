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
  Parser () = default;

  DISABLE_COPY_MOVE_AND_ASSIGN(Parser)

  [[nodiscard]] static auto Parse(const std::string &sql) -> std::shared_ptr<ast::TreeNode>;
};
}  // namespace wsdb

#endif  // WSDB_PARSER_H
