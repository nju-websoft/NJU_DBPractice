%{
/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "ast.h"
#include "yacc.tab.h"
#include <iostream>
#include <memory>

int yylex(YYSTYPE *yylval, YYLTYPE *yylloc);

void yyerror(YYLTYPE *locp, const char* s) {
    std::cerr << "Parser Error at line " << locp->first_line << " column " << locp->first_column << ": " << s << std::endl;
}

using namespace njudb;
using namespace ast;
%}

// enable location in error handler
%locations
// request a pure (reentrant) parser
%define api.pure full
// enable verbose syntax error message
%define parse.error verbose

// keywords
%token EXPLAIN SHOW TABLES CREATE TABLE DROP DESC INSERT INTO VALUES DELETE FROM OPEN DATABASE ON ASC AS ORDER GROUP BY SUM AVG MAX MIN COUNT IN STATIC_CHECKPOINT USING LOOP MERGE INDEX_BPTREE HASH_KWD
WHERE HAVING UPDATE SET SELECT INT CHAR FLOAT BOOL INDEX AND JOIN INNER OUTER EXIT HELP TXN_BEGIN TXN_COMMIT TXN_ABORT TXN_ROLLBACK ORDER_BY ENABLE_NESTLOOP ENABLE_SORTMERGE STORAGE PAX NARY LIMIT
// non-keywords
%token LEQ NEQ GEQ T_EOF

// type-specific tokens
%token <sv_str> IDENTIFIER VALUE_STRING
%token <sv_int> VALUE_INT
%token <sv_float> VALUE_FLOAT
%token <sv_bool> VALUE_BOOL

// specify types for non-terminal symbol
%type <sv_node> stmt dbStmt ddl dml txnStmt indexStmt logStmt table
%type <sv_sel> selectStmt
%type <sv_field> field
%type <sv_fields> fieldList
%type <sv_type_len> type
%type <sv_comp_op> op
%type <sv_storage_model> optStorageModel
%type <sv_int> optLimit
%type <sv_expr> expr
%type <sv_val> value
%type <sv_vals> valueList
%type <sv_str> tbName colName optAlias
%type <sv_strs> colNameList
%type <sv_node_arr> tableList
%type <sv_col> col aggCol
%type <sv_cols> colList selector colListWithoutAlias
%type <sv_set_clause> setClause
%type <sv_set_clauses> setClauses
%type <sv_cond> condition conditionAgg
%type <sv_conds> whereClause optWhereClause havingClause optHavingClause
%type <sv_groupby> optGroupByClause
%type <sv_join_strategy> optUsingJoinClause
%type <sv_index_type> optUsingIndexClause
%type <sv_orderby>  order_clause opt_order_clause
%type <sv_orderby_dir> opt_asc_desc

%%
start:
        stmt ';'
    {
        njudb_ast_ = $1;
        YYACCEPT;
    }
    |
        EXPLAIN stmt ';'
    {
        njudb_ast_ = std::make_shared<Explain>($2);
        YYACCEPT;
    }
    |   HELP
    {
        njudb_ast_ = std::make_shared<Help>();
        YYACCEPT;
    }
    |   EXIT
    {
        njudb_ast_ = nullptr;
        YYACCEPT;
    }
    |   T_EOF
    {
        njudb_ast_ = nullptr;
        YYACCEPT;
    }
    ;

stmt:
        dbStmt
    |   ddl
    |   dml
    |   txnStmt
    |   indexStmt
    |   logStmt
    |   /*empty*/ { $$ = nullptr; }
    ;

txnStmt:
        TXN_BEGIN
    {
        $$ = std::make_shared<TxnBegin>();
    }
    |   TXN_COMMIT
    {
        $$ = std::make_shared<TxnCommit>();
    }
    |   TXN_ABORT
    {
        $$ = std::make_shared<TxnAbort>();
    }
    | TXN_ROLLBACK
    {
        $$ = std::make_shared<TxnRollback>();
    }
    ;

logStmt:
        CREATE STATIC_CHECKPOINT
    {
        $$ = std::make_shared<LogStaticCheckpoint>();
    }

dbStmt:
        SHOW TABLES
    {
        $$ = std::make_shared<ShowTables>();
    }
    | CREATE DATABASE IDENTIFIER
    {
        $$ = std::make_shared<CreateDatabase>($3);
    }
    | OPEN DATABASE IDENTIFIER
    {
        $$ = std::make_shared<OpenDatabase>($3);
    }
    ;

indexStmt:
        SHOW INDEX ON tbName
    {
        $$ = std::make_shared<ShowIndexes>($4);
    }
    |   SHOW INDEX
    {
        $$ = std::make_shared<ShowIndexes>("");
    }

ddl:
        CREATE TABLE tbName '(' fieldList ')' optStorageModel
    {
        $$ = std::make_shared<CreateTable>($3, $5, $7);
    }
    |   DROP TABLE tbName
    {
        $$ = std::make_shared<DropTable>($3);
    }
    |   DESC tbName
    {
        $$ = std::make_shared<DescTable>($2);
    }
    |   CREATE INDEX tbName ON tbName '(' colNameList ')' optUsingIndexClause
    {
        $$ = std::make_shared<CreateIndex>($3, $5, $7, $9);
    }
    |   DROP INDEX tbName ON tbName
    {
        $$ = std::make_shared<DropIndex>($5, $3);
    }
    ;

optStorageModel:
    /* epsilon */ { $$ = NARY_MODEL; }
    | STORAGE '=' NARY
    { $$ = NARY_MODEL; }
    | STORAGE '=' PAX
    { $$ = PAX_MODEL; }
    ;

dml:
        INSERT INTO tbName VALUES '(' valueList ')'
    {
        $$ = std::make_shared<InsertStmt>($3, $6);
    }
    |   DELETE FROM tbName optWhereClause
    {
        $$ = std::make_shared<DeleteStmt>($3, $4);
    }
    |   UPDATE tbName SET setClauses optWhereClause
    {
        $$ = std::make_shared<UpdateStmt>($2, $4, $5);
    }
    |   selectStmt
    {
        $$ = $1;
    }
    ;

selectStmt:
        SELECT selector FROM tableList optWhereClause optGroupByClause optHavingClause optUsingJoinClause opt_order_clause optLimit
    {
        $$ = std::make_shared<SelectStmt>($2, $4, $5, $6, $7, $8, $9, $10);
    }
    ;

optLimit:
    LIMIT VALUE_INT
    {
        $$ = $2;
    }
    | /* epsilon */ { $$ = -1; }
    ;

fieldList:
        field
    {
        $$ = std::vector<std::shared_ptr<Field>>{$1};
    }
    |   fieldList ',' field
    {
        $$.push_back($3);
    }
    ;

colNameList:
        colName
    {
        $$ = std::vector<std::string>{$1};
    }
    | colNameList ',' colName
    {
        $$.push_back($3);
    }
    ;

field:
        colName type
    {
        $$ = std::make_shared<ColDef>($1, $2);
    }
    ;

type:
        INT
    {
        $$ = std::make_shared<TypeLen>(TYPE_INT, sizeof(int));
    }
    |   BOOL
    {
        $$ = std::make_shared<TypeLen>(TYPE_BOOL, sizeof(bool));
    }
    |   CHAR '(' VALUE_INT ')'
    {
        $$ = std::make_shared<TypeLen>(TYPE_STRING, $3);
    }
    |   FLOAT
    {
        $$ = std::make_shared<TypeLen>(TYPE_FLOAT, sizeof(float));
    }
    ;

valueList:
        value
    {
        $$ = std::vector<std::shared_ptr<Value>>{$1};
    }
    |   valueList ',' value
    {
        $$.push_back($3);
    }
    ;

value:
        VALUE_INT
    {
        $$ = std::make_shared<IntLit>($1);
    }
    |   VALUE_FLOAT
    {
        $$ = std::make_shared<FloatLit>($1);
    }
    |   VALUE_STRING
    {
        $$ = std::make_shared<StringLit>($1);
    }
    |   VALUE_BOOL
    {
        $$ = std::make_shared<BoolLit>($1);
    }
    | /* epsilon */
    {
        $$ = std::make_shared<NullLit>();
    }
    ;

colListWithoutAlias:
        col
    {
        $$ = std::vector<std::shared_ptr<Col>>{$1};
    }
    |   colListWithoutAlias ',' col
    {
        $$.push_back($3);
    }
    ;

optGroupByClause:
        /* epsilon */ { /* ignore*/ }
    |   GROUP BY colListWithoutAlias
    {
        $$ = std::make_shared<GroupBy>($3);
    }
    ;

condition:
        col op expr
    {
        $$ = std::make_shared<BinaryExpr>($1, $2, $3);
    }
    |   col op '(' selectStmt ')'
    {
        $$ = std::make_shared<BinaryExpr>($1, $2, $4);
    }
    | col IN '(' selectStmt ')'
    {
        $$ = std::make_shared<BinaryExpr>($1, OP_IN, $4);
    }
    | col IN '(' valueList ')'
    {
        auto arr = std::make_shared<ArrLit>($4);
        $$ = std::make_shared<BinaryExpr>($1, OP_IN, arr);
    }
    ;

optWhereClause:
        /* epsilon */ { /* ignore*/ }
    |   WHERE whereClause
    {
        $$ = $2;
    }
    ;

optUsingIndexClause:
    /* epsilon */ { $$ = BPTREE; }
    | USING INDEX_BPTREE { $$ = BPTREE; }
    | USING HASH_KWD { $$ = HASH; }

optUsingJoinClause:
    /* epsilon */ {$$ = NESTED_LOOP;}
    |   USING LOOP
    {   $$ = NESTED_LOOP;  }
    |   USING MERGE
    {   $$ = SORT_MERGE;  }
    |   USING HASH_KWD
    {   $$ = HASH_JOIN;  }

conditionAgg:
        aggCol op value
    {
        $$ = std::make_shared<BinaryExpr>($1, $2, $3);
    }
    ;


optHavingClause:
        /* epsilon */ { /* ignore*/ }
    |   HAVING havingClause
    {
        $$ = $2;
    }

havingClause:
     condition
    {
        $$ = std::vector<std::shared_ptr<BinaryExpr>>{$1};
    }
    | conditionAgg
    {
        $$ = std::vector<std::shared_ptr<BinaryExpr>>{$1};
    }
    |   havingClause AND condition
    {
        $$.push_back($3);
    }
    |   havingClause AND conditionAgg
    {
        $$.push_back($3);
    }
    ;


whereClause:
        condition
    {
        $$ = std::vector<std::shared_ptr<BinaryExpr>>{$1};
    }
    |   whereClause AND condition
    {
        $$.push_back($3);
    }
    ;

col:
        tbName '.' colName
    {
        $$ = std::make_shared<Col>($1, $3);
    }
    |   colName
    {
        $$ = std::make_shared<Col>("", $1);
    }
    ;

aggCol:
        COUNT '(' col ')'
    {
        $$ = std::make_shared<AggCol>($3, AGG_COUNT);
    }
    |   SUM '(' col ')'
    {
        $$ = std::make_shared<AggCol>($3, AGG_SUM);
    }
    |   AVG '(' col ')'
    {
        $$ = std::make_shared<AggCol>($3, AGG_AVG);
    }
    |   MAX '(' col ')'
    {
        $$ = std::make_shared<AggCol>($3, AGG_MAX);
    }
    |   MIN '(' col ')'
    {
        $$ = std::make_shared<AggCol>($3, AGG_MIN);
    }
    |  COUNT '(' '*'')'
    {
        auto col = std::make_shared<Col>("", "*");
        $$ = std::make_shared<AggCol>(col, AGG_COUNT_STAR);
    }
    ;

colList:
        col optAlias
    {
        $$ = std::vector<std::shared_ptr<Col>>{$1};
        $$[0]->setAlias($2);
    }
    |   aggCol optAlias
    {
        $$ = std::vector<std::shared_ptr<Col>>{$1};
        $$[0]->setAlias($2);
    }
    |   colList ',' col optAlias
    {
        $$.push_back($3);
        $$.back()->setAlias($4);
    }
    |   colList ',' aggCol optAlias
    {
        $$.push_back($3);
        $$.back()->setAlias($4);
    }
    ;

optAlias:
    AS colName
    {
        $$ = $2;
    }
    |   /* epsilon */ { $$ = ""; }
    ;

op:
        '='
    {
        $$ = OP_EQ;
    }
    |   '<'
    {
        $$ = OP_LT;
    }
    |   '>'
    {
        $$ = OP_GT;
    }
    |   NEQ
    {
        $$ = OP_NE;
    }
    |   LEQ
    {
        $$ = OP_LE;
    }
    |   GEQ
    {
        $$ = OP_GE;
    }
    ;

expr:
        value
    {
        $$ = std::static_pointer_cast<Expr>($1);
    }
    |   col
    {
        $$ = std::static_pointer_cast<Expr>($1);
    }
    ;

setClauses:
        setClause
    {
        $$ = std::vector<std::shared_ptr<SetClause>>{$1};
    }
    |   setClauses ',' setClause
    {
        $$.push_back($3);
    }
    ;

setClause:
        colName '=' value
    {
        $$ = std::make_shared<SetClause>($1, $3);
    }
    ;

selector:
        '*'
    {
        $$ = {};
    }
    |   colList
    ;

table:
        tbName
    {
        $$ = std::make_shared<ExplicitTable>($1);
    }
    |   '(' selectStmt ')'
    {
        $$ = $2;
    }
    |  tbName JOIN tbName
    {
        $$ = std::make_shared<JoinExpr>($1, $3, INNER_JOIN);
    }
    |  tbName INNER JOIN tbName
    {
        $$ = std::make_shared<JoinExpr>($1, $4, INNER_JOIN);
    }
    |  tbName OUTER JOIN tbName
    {
        $$ = std::make_shared<JoinExpr>($1, $4, OUTER_JOIN);
    }
    ;

tableList:
        table
    {
        $$ = std::vector<std::shared_ptr<TreeNode>>{$1};
    }
    |   tableList ',' table
    {
        $$.push_back($3);
    }
    ;

opt_order_clause:
    ORDER BY order_clause
    {
        $$ = $3;
    }
    |   /* epsilon */ { /* ignore*/ }
    ;

order_clause:
      opt_asc_desc colListWithoutAlias
    {
        $$ = std::make_shared<OrderBy>($1, $2);
    }
    ;

opt_asc_desc:
       ASC          { $$ = OrderBy_ASC;     }
    |  DESC         { $$ = OrderBy_DESC;    }
    |               { $$ = OrderBy_ASC; }
    ;

tbName: IDENTIFIER;

colName: IDENTIFIER;
%%
