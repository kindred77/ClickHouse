#pragma once

#include <Interpreters/orcaopt/pgoptnew/parser_common.h>
#include <Interpreters/orcaopt/pgoptnew/FuncParser.h>
#include <Interpreters/orcaopt/pgoptnew/NodeParser.h>
#include <Interpreters/orcaopt/pgoptnew/CoerceParser.h>

namespace DB
{

class OperParser
{
private:
    FuncParser func_parser;
	NodeParser node_parser;
	CoerceParser coerce_parser;
public:
	explicit OperParser();

	Oid
	compatible_oper_opid(duckdb_libpgquery::PGList *op, Oid arg1, Oid arg2, bool noError);

	Operator
	compatible_oper(PGParseState *pstate, duckdb_libpgquery::PGList *op, Oid arg1, Oid arg2,
				bool noError, int location);

	void
	get_sort_group_operators(Oid argtype,
						 bool needLT, bool needEQ, bool needGT,
						 Oid *ltOpr, Oid *eqOpr, Oid *gtOpr,
						 bool *isHashable);

	bool
	make_oper_cache_key(PGParseState *pstate, OprCacheKey *key, duckdb_libpgquery::PGList *opname,
					Oid ltypeId, Oid rtypeId, int location);

	void
	make_oper_cache_entry(OprCacheKey *key, Oid opr_oid);

	Oid
	find_oper_cache_entry(OprCacheKey *key);

	Oid
	binary_oper_exact(duckdb_libpgquery::PGList *opname, Oid arg1, Oid arg2);

	FuncDetailCode
	oper_select_candidate(int nargs,
					  Oid *input_typeids,
					  FuncCandidateList candidates,
					  Oid *operOid); /* output argument */

	const char *
	op_signature_string(duckdb_libpgquery::PGList *op, char oprkind, Oid arg1, Oid arg2);
	
	Operator
	right_oper(PGParseState *pstate, duckdb_libpgquery::PGList *op,
			Oid arg, bool noError, int location);

	Operator
	left_oper(PGParseState *pstate, duckdb_libpgquery::PGList *op,
			Oid arg, bool noError, int location);

	Operator
	oper(PGParseState *pstate, duckdb_libpgquery::PGList *opname,
		Oid ltypeId, Oid rtypeId,bool noError, int location);

	duckdb_libpgquery::PGExpr *
	make_op(PGParseState *pstate, duckdb_libpgquery::PGList *opname, 
		duckdb_libpgquery::PGNode *ltree, duckdb_libpgquery::PGNode *rtree,
		duckdb_libpgquery::PGNode *last_srf, int location);

	Oid
	oprid(Operator op);

	void
	op_error(PGParseState *pstate, duckdb_libpgquery::PGList *op, char oprkind,
		 Oid arg1, Oid arg2,
		 FuncDetailCode fdresult, int location);

	duckdb_libpgquery::PGExpr *
	make_scalar_array_op(PGParseState *pstate, duckdb_libpgquery::PGList *opname,
					 bool useOr,
					 duckdb_libpgquery::PGNode *ltree, duckdb_libpgquery::PGNode *rtree,
					 int location);
};

}