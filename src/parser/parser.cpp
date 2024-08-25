//
// Created by ziqi on 2024/8/25.
//

#include "parser.h"
#include "def.h"
#include "../common/error.h"

namespace wsdb {

std::shared_ptr<ast::TreeNode> Parser::Parse(const std::string &sql)
{
  auto buf = yy_scan_string(sql.c_str());
  if (yyparse() != 0) {
    yy_delete_buffer(buf);
    throw WSDBException(WSDB_INVALID_SQL, Q(Parser), Q(Parse), sql);
  }
  auto ret = ast::wsdb_ast_;
  yy_delete_buffer(buf);
  return ret;
}

}  // namespace wsdb