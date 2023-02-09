#pragma once

#include <Interpreters/orcaopt/pgoptnew/parser_common.h>

namespace DB
{

class RelationParser;
class CoerceParser;
class ExprParser;
class TargetParser;
class NodeParser;
class TypeParser;
class AggParser;
class ClauseParser;

using RelationParserPtr = std::unique_ptr<RelationParser>;
using CoerceParserPtr = std::unique_ptr<CoerceParser>;
using ExprParserPtr = std::unique_ptr<ExprParser>;
using TargetParserPtr = std::unique_ptr<TargetParser>;
using NodeParserPtr = std::unique_ptr<NodeParser>;
using TypeParserPtr = std::unique_ptr<TypeParser>;
using AggParserPtr = std::unique_ptr<AggParser>;
using ClauseParserPtr = std::unique_ptr<ClauseParser>;

class FuncParser
{
private:
    CoerceParserPtr coerce_parser;
    AggParserPtr agg_parser;
    ClauseParserPtr clause_parser;
    NodeParserPtr node_parser;
    TypeParserPtr type_parser;
    RelationParserPtr relation_parser;
	TargetParserPtr target_parser;
    ExprParserPtr expr_parser;
public:
	explicit FuncParser();

    duckdb_libpgquery::PGNode *
    ParseFuncOrColumn(PGParseState *pstate, duckdb_libpgquery::PGList *funcname, duckdb_libpgquery::PGList *fargs,
				  duckdb_libpgquery::PGNode *last_srf, duckdb_libpgquery::PGFuncCall *fn, bool proc_call, int location);
    
    duckdb_libpgquery::PGNode *
    ParseComplexProjection(PGParseState *pstate, const char *funcname, duckdb_libpgquery::PGNode *first_arg,
					   int location);

    int
    func_match_argtypes(int nargs,
					Oid *input_typeids,
					FuncCandidateList raw_candidates,
					FuncCandidateList *candidates);	/* return value */

    FuncCandidateList
    func_select_candidate(int nargs,
					  Oid *input_typeids,
					  FuncCandidateList candidates);

    Oid
    FuncNameAsType(duckdb_libpgquery::PGList *funcname);

    char *
    funcname_signature_string(const char *funcname, int nargs,
						  duckdb_libpgquery::PGList *argnames, const Oid *argtypes);

    char *
    func_signature_string(duckdb_libpgquery::PGList *funcname, int nargs,
					  duckdb_libpgquery::PGList *argnames, const Oid *argtypes);

    FuncDetailCode
    func_get_detail(duckdb_libpgquery::PGList *funcname,
				duckdb_libpgquery::PGList *fargs,
				duckdb_libpgquery::PGList *fargnames,
				int nargs,
				Oid *argtypes,
				bool expand_variadic,
				bool expand_defaults,
				Oid *funcid,	/* return value */
				Oid *rettype,	/* return value */
				bool *retset,	/* return value */
				int *nvargs,	/* return value */
				Oid *vatype,	/* return value */
				Oid **true_typeids, /* return value */
				duckdb_libpgquery::PGList **argdefaults) /* optional return value */;

    void
    unify_hypothetical_args(PGParseState *pstate,
						duckdb_libpgquery::PGList *fargs,
						int numAggregatedArgs,
						Oid *actual_arg_types,
						Oid *declared_arg_types);

    void
    make_fn_arguments(PGParseState *pstate,
				  duckdb_libpgquery::PGList *fargs,
				  Oid *actual_arg_types,
				  Oid *declared_arg_types);

    void
    check_srf_call_placement(PGParseState *pstate, duckdb_libpgquery::PGNode *last_srf, int location);

	void 
	parseCheckTableFunctions(PGParseState *pstate, duckdb_libpgquery::PGQuery *qry);
};

}
