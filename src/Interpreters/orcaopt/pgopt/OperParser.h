#pragma once

#include <Interpreters/orcaopt/pgopt/parser_common.h>
#include <Interpreters/orcaopt/pgopt/FuncParser.h>
#include <Interpreters/orcaopt/pgopt/NodeParser.h>

namespace DB
{

class OperParser
{
private:
    FuncParser func_parser;
	NodeParser node_parser;
public:
	explicit OperParser();

    void
    get_sort_group_operators(Oid argtype,
						 bool needLT, bool needEQ, bool needGT,
						 Oid *ltOpr, Oid *eqOpr, Oid *gtOpr,
						 bool *isHashable);
	
	HeapTuple
	oper(PGParseState *pstate, duckdb_libpgquery::PGList *opname, Oid ltypeId, Oid rtypeId,
		bool noError, int location);

	duckdb_libpgquery::PGExpr *
	make_op(PGParseState *pstate, duckdb_libpgquery::PGList *opname, duckdb_libpgquery::PGNode *ltree, duckdb_libpgquery::PGNode *rtree,
		int location);
	
	Operator
	right_oper(PGParseState *pstate, duckdb_libpgquery::PGList *op, Oid arg, bool noError, int location);

	Operator
	left_oper(PGParseState *pstate, duckdb_libpgquery::PGList *op, Oid arg, bool noError, int location);

	duckdb_libpgquery::PGExpr *
	make_scalar_array_op(PGParseState *pstate, duckdb_libpgquery::PGList *opname,
					 bool useOr,
					 duckdb_libpgquery::PGNode *ltree, duckdb_libpgquery::PGNode *rtree,
					 int location);
	
	Oid
	binary_oper_exact(duckdb_libpgquery::PGList *opname, Oid arg1, Oid arg2);

	FuncDetailCode
	oper_select_candidate(int nargs,
					  Oid *input_typeids,
					  FuncCandidateList candidates,
					  Oid *operOid)		/* output argument */;
};

}