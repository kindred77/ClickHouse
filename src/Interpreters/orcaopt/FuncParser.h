#pragma once

#include <common/parser_common.hpp>

#include <Interpreters/orcaopt/Parser.h>

namespace DB
{

// class RelationParser;
// class CoerceParser;
// class ExprParser;
// class TargetParser;
// class NodeParser;
// class TypeParser;
// class AggParser;
// class ClauseParser;

// using RelationParserPtr = std::shared_ptr<RelationParser>;
// using CoerceParserPtr = std::shared_ptr<CoerceParser>;
// using ExprParserPtr = std::shared_ptr<ExprParser>;
// using TargetParserPtr = std::shared_ptr<TargetParser>;
// using NodeParserPtr = std::shared_ptr<NodeParser>;
// using TypeParserPtr = std::shared_ptr<TypeParser>;
// using AggParserPtr = std::shared_ptr<AggParser>;
// using ClauseParserPtr = std::shared_ptr<ClauseParser>;

// class Context;
// using ContextPtr = std::shared_ptr<const Context>;

class FuncParser : public Parser
{
private:
    // CoerceParserPtr coerce_parser;
    // AggParserPtr agg_parser;
    // ClauseParserPtr clause_parser;
    // NodeParserPtr node_parser;
    // TypeParserPtr type_parser;
    // RelationParserPtr relation_parser;
	// TargetParserPtr target_parser;
    // ExprParserPtr expr_parser;

	// ContextPtr context;
public:
	//explicit FuncParser(const ContextPtr& context_);

    static duckdb_libpgquery::PGNode * ParseFuncOrColumn(duckdb_libpgquery::PGParseState * pstate, duckdb_libpgquery::PGList * funcname, 
		duckdb_libpgquery::PGList * fargs, duckdb_libpgquery::PGFuncCall * fn, int location);

    static duckdb_libpgquery::PGNode *
    ParseComplexProjection(duckdb_libpgquery::PGParseState *pstate, const char *funcname, duckdb_libpgquery::PGNode *first_arg,
					   int location);

    static int
    func_match_argtypes(int nargs,
					duckdb_libpgquery::PGOid *input_typeids,
					duckdb_libpgquery::FuncCandidateListPtr raw_candidates,
					duckdb_libpgquery::FuncCandidateListPtr & candidates);	/* return value */

    static duckdb_libpgquery::FuncCandidateListPtr
    func_select_candidate(int nargs,
					  duckdb_libpgquery::PGOid *input_typeids,
					  duckdb_libpgquery::FuncCandidateListPtr & candidates);

    static duckdb_libpgquery::PGOid
    FuncNameAsType(duckdb_libpgquery::PGList *funcname);

    static std::string
    funcname_signature_string(const char *funcname, int nargs,
						  duckdb_libpgquery::PGList *argnames, const duckdb_libpgquery::PGOid *argtypes);

    static std::string
    func_signature_string(duckdb_libpgquery::PGList *funcname, int nargs,
					  duckdb_libpgquery::PGList *argnames, const duckdb_libpgquery::PGOid *argtypes);

    static duckdb_libpgquery::FuncDetailCode
    func_get_detail(duckdb_libpgquery::PGList *funcname,
				duckdb_libpgquery::PGList *fargs,
				duckdb_libpgquery::PGList *fargnames,
				int nargs,
				duckdb_libpgquery::PGOid *argtypes,
				bool expand_variadic,
				bool expand_defaults,
				duckdb_libpgquery::PGOid *funcid,	/* return value */
				duckdb_libpgquery::PGOid *rettype,	/* return value */
				bool *retset,	/* return value */
				int *nvargs,	/* return value */
				duckdb_libpgquery::PGOid *vatype,	/* return value */
				duckdb_libpgquery::PGOid **true_typeids, /* return value */
				duckdb_libpgquery::PGList **argdefaults) /* optional return value */;

    static void
    unify_hypothetical_args(duckdb_libpgquery::PGParseState *pstate,
						duckdb_libpgquery::PGList *fargs,
						int numAggregatedArgs,
						duckdb_libpgquery::PGOid *actual_arg_types,
						duckdb_libpgquery::PGOid *declared_arg_types);

    static void
    make_fn_arguments(duckdb_libpgquery::PGParseState *pstate,
				  duckdb_libpgquery::PGList *fargs,
				  duckdb_libpgquery::PGOid *actual_arg_types,
				  duckdb_libpgquery::PGOid *declared_arg_types);

    static void check_srf_call_placement(duckdb_libpgquery::PGParseState * pstate, int location);
};

}
