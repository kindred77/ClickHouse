#pragma once

#include <Interpreters/orcaopt/parser_common.h>

namespace DB
{

class CoerceParser;
class CoerceParser;
using CoerceParserPtr = std::unique_ptr<CoerceParser>;
using FuncParserPtr = std::unique_ptr<CoerceParser>;

class OperParser
{
private:
	CoerceParserPtr coerce_parser_ptr;
	FuncParserPtr func_parser_ptr;
public:
	explicit OperParser();

    FuncDetailCode oper_select_candidate(int nargs, Oid * input_typeids, FuncCandidateList candidates,
		Oid * operOid) /* output argument */;

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
