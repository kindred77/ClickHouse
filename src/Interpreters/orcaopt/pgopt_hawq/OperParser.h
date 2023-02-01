#pragma once

#include <Interpreters/orcaopt/pgopt_hawq/parser_common.h>

#include <Interpreters/orcaopt/pgopt_hawq/CoerceParser.h>
#include <Interpreters/orcaopt/pgopt_hawq/FuncParser.h>

namespace DB
{

class OperParser
{
private:
	CoerceParser coerce_parser;
	FuncParser func_parser;
public:
	explicit OperParser();

    Operator right_oper(PGParseState * pstate, duckdb_libpgquery::PGList * op,
		Oid arg, bool noError, int location);

    Operator left_oper(PGParseState * pstate, duckdb_libpgquery::PGList * op,
		Oid arg, bool noError, int location);

    Operator oper(PGParseState * pstate, duckdb_libpgquery::PGList * opname,
		Oid ltypeId, Oid rtypeId, bool noError, int location);

    duckdb_libpgquery::PGExpr * make_op_expr(PGParseState * pstate, Operator op,
		duckdb_libpgquery::PGNode * ltree, duckdb_libpgquery::PGNode * rtree,
		Oid ltypeId, Oid rtypeId);

    duckdb_libpgquery::PGExpr * make_op(PGParseState * pstate, duckdb_libpgquery::PGList * opname,
		duckdb_libpgquery::PGNode * ltree, duckdb_libpgquery::PGNode * rtree, int location);

    duckdb_libpgquery::PGExpr * make_scalar_array_op(PGParseState * pstate, duckdb_libpgquery::PGList * opname,
		bool useOr, duckdb_libpgquery::PGNode * ltree, duckdb_libpgquery::PGNode * rtree,
		int location);
};

}