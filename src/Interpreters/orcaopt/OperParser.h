#pragma once

#include <common/parser_common.hpp>

namespace DB
{
// class FuncParser;
// class NodeParser;
// class CoerceParser;

// using FuncParserPtr = std::shared_ptr<FuncParser>;
// using NodeParserPtr = std::shared_ptr<NodeParser>;
// using CoerceParserPtr = std::shared_ptr<CoerceParser>;

// class Context;
// using ContextPtr = std::shared_ptr<const Context>;

class OperParser
{
private:
    // FuncParserPtr func_parser;
	// NodeParserPtr node_parser;
	// CoerceParserPtr coerce_parser;

	// ContextPtr context;
public:
	//explicit OperParser(const ContextPtr& context_);

	// Oid
	// compatible_oper_opid(duckdb_libpgquery::PGList *op, Oid arg1, Oid arg2, bool noError);

	static duckdb_libpgquery::PGOperatorPtr
	compatible_oper(duckdb_libpgquery::PGParseState *pstate, duckdb_libpgquery::PGList *op, duckdb_libpgquery::PGOid arg1, duckdb_libpgquery::PGOid arg2,
				bool noError, int location);

	static void
	get_sort_group_operators(duckdb_libpgquery::PGOid argtype,
						 bool needLT, bool needEQ, bool needGT,
						 duckdb_libpgquery::PGOid *ltOpr, duckdb_libpgquery::PGOid *eqOpr, duckdb_libpgquery::PGOid *gtOpr,
						 bool *isHashable);

	//bool
	//make_oper_cache_key(PGParseState *pstate, OprCacheKey *key, duckdb_libpgquery::PGList *opname,
	//				Oid ltypeId, Oid rtypeId, int location);

	//void
	//make_oper_cache_entry(OprCacheKey *key, Oid opr_oid);

	// Oid
	// find_oper_cache_entry(OprCacheKey *key);

	static duckdb_libpgquery::PGOid
	binary_oper_exact(duckdb_libpgquery::PGList *opname, duckdb_libpgquery::PGOid arg1, duckdb_libpgquery::PGOid arg2);

	static duckdb_libpgquery::FuncDetailCode
	oper_select_candidate(int nargs,
					  duckdb_libpgquery::PGOid *input_typeids,
					  duckdb_libpgquery::FuncCandidateListPtr & candidates,
					  duckdb_libpgquery::PGOid *operOid); /* output argument */

	static std::string
	op_signature_string(duckdb_libpgquery::PGList *op, char oprkind, duckdb_libpgquery::PGOid arg1, duckdb_libpgquery::PGOid arg2);
	
	static duckdb_libpgquery::PGOperatorPtr
	right_oper(duckdb_libpgquery::PGParseState *pstate, duckdb_libpgquery::PGList *op,
			duckdb_libpgquery::PGOid arg, bool noError, int location);

	static duckdb_libpgquery::PGOperatorPtr
	left_oper(duckdb_libpgquery::PGParseState *pstate, duckdb_libpgquery::PGList *op,
			duckdb_libpgquery::PGOid arg, bool noError, int location);

	static duckdb_libpgquery::PGOperatorPtr
	oper(duckdb_libpgquery::PGParseState *pstate, duckdb_libpgquery::PGList *opname,
		duckdb_libpgquery::PGOid ltypeId, duckdb_libpgquery::PGOid rtypeId,bool noError, int location);

    static duckdb_libpgquery::PGExpr * make_op(duckdb_libpgquery::PGParseState * pstate, duckdb_libpgquery::PGList * opname,
		duckdb_libpgquery::PGNode * ltree, duckdb_libpgquery::PGNode * rtree, int location);

    static void
	op_error(duckdb_libpgquery::PGParseState *pstate, duckdb_libpgquery::PGList *op, char oprkind,
		 duckdb_libpgquery::PGOid arg1, duckdb_libpgquery::PGOid arg2,
		 duckdb_libpgquery::FuncDetailCode fdresult, int location);

	static duckdb_libpgquery::PGExpr *
	make_scalar_array_op(duckdb_libpgquery::PGParseState *pstate, duckdb_libpgquery::PGList *opname,
					 bool useOr,
					 duckdb_libpgquery::PGNode *ltree, duckdb_libpgquery::PGNode *rtree,
					 int location);
};

}
