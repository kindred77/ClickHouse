#pragma once

#include <Interpreters/orcaopt/parser_common.h>

namespace DB
{
class FuncParser;
class NodeParser;
class CoerceParser;
class OperProvider;
class TypeProvider;
class ProcProvider;

using FuncParserPtr = std::shared_ptr<FuncParser>;
using NodeParserPtr = std::shared_ptr<NodeParser>;
using CoerceParserPtr = std::shared_ptr<CoerceParser>;
using OperProviderPtr = std::shared_ptr<OperProvider>;
using TypeProviderPtr = std::shared_ptr<TypeProvider>;
using ProcProviderPtr = std::shared_ptr<ProcProvider>;

class OperParser
{
private:
    FuncParserPtr func_parser;
	NodeParserPtr node_parser;
	CoerceParserPtr coerce_parser;

	OperProviderPtr oper_provider;
	TypeProviderPtr type_provider;
	ProcProviderPtr proc_provider;
public:
	explicit OperParser();

	// Oid
	// compatible_oper_opid(duckdb_libpgquery::PGList *op, Oid arg1, Oid arg2, bool noError);

	PGOperatorPtr
	compatible_oper(PGParseState *pstate, duckdb_libpgquery::PGList *op, Oid arg1, Oid arg2,
				bool noError, int location);

	void
	get_sort_group_operators(Oid argtype,
						 bool needLT, bool needEQ, bool needGT,
						 Oid *ltOpr, Oid *eqOpr, Oid *gtOpr,
						 bool *isHashable);

	//bool
	//make_oper_cache_key(PGParseState *pstate, OprCacheKey *key, duckdb_libpgquery::PGList *opname,
	//				Oid ltypeId, Oid rtypeId, int location);

	//void
	//make_oper_cache_entry(OprCacheKey *key, Oid opr_oid);

	// Oid
	// find_oper_cache_entry(OprCacheKey *key);

	Oid
	binary_oper_exact(duckdb_libpgquery::PGList *opname, Oid arg1, Oid arg2);

	FuncDetailCode
	oper_select_candidate(int nargs,
					  Oid *input_typeids,
					  FuncCandidateListPtr & candidates,
					  Oid *operOid); /* output argument */

	std::string
	op_signature_string(duckdb_libpgquery::PGList *op, char oprkind, Oid arg1, Oid arg2);
	
	PGOperatorPtr
	right_oper(PGParseState *pstate, duckdb_libpgquery::PGList *op,
			Oid arg, bool noError, int location);

	PGOperatorPtr
	left_oper(PGParseState *pstate, duckdb_libpgquery::PGList *op,
			Oid arg, bool noError, int location);

	PGOperatorPtr
	oper(PGParseState *pstate, duckdb_libpgquery::PGList *opname,
		Oid ltypeId, Oid rtypeId,bool noError, int location);

    duckdb_libpgquery::PGExpr * make_op(PGParseState * pstate, duckdb_libpgquery::PGList * opname,
		duckdb_libpgquery::PGNode * ltree, duckdb_libpgquery::PGNode * rtree, int location);

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
