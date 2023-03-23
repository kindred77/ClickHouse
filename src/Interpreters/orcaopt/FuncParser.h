#pragma once

#include <Interpreters/orcaopt/parser_common.h>

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
class TypeProvider;
class ProcProvider;
class AggProvider;
class FunctionProvider;

using RelationParserPtr = std::shared_ptr<RelationParser>;
using CoerceParserPtr = std::shared_ptr<CoerceParser>;
using ExprParserPtr = std::shared_ptr<ExprParser>;
using TargetParserPtr = std::shared_ptr<TargetParser>;
using NodeParserPtr = std::shared_ptr<NodeParser>;
using TypeParserPtr = std::shared_ptr<TypeParser>;
using AggParserPtr = std::shared_ptr<AggParser>;
using ClauseParserPtr = std::shared_ptr<ClauseParser>;
using TypeProviderPtr = std::shared_ptr<TypeProvider>;
using ProcProviderPtr = std::shared_ptr<ProcProvider>;
using AggProviderPtr = std::shared_ptr<AggProvider>;
using FunctionProviderPtr = std::shared_ptr<FunctionProvider>;

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
	TypeProviderPtr type_provider;
	ProcProviderPtr proc_provider;
	AggProviderPtr agg_provider;
	FunctionProviderPtr func_provider;
public:
	explicit FuncParser();

    duckdb_libpgquery::PGNode * ParseFuncOrColumn(PGParseState * pstate, duckdb_libpgquery::PGList * funcname, 
		duckdb_libpgquery::PGList * fargs, duckdb_libpgquery::PGFuncCall * fn, int location);

    duckdb_libpgquery::PGNode *
    ParseComplexProjection(PGParseState *pstate, const char *funcname, duckdb_libpgquery::PGNode *first_arg,
					   int location);

    int
    func_match_argtypes(int nargs,
					Oid *input_typeids,
					FuncCandidateListPtr raw_candidates,
					FuncCandidateListPtr & candidates);	/* return value */

    FuncCandidateListPtr
    func_select_candidate(int nargs,
					  Oid *input_typeids,
					  FuncCandidateListPtr & candidates);

    Oid
    FuncNameAsType(duckdb_libpgquery::PGList *funcname);

    std::string
    funcname_signature_string(const char *funcname, int nargs,
						  duckdb_libpgquery::PGList *argnames, const Oid *argtypes);

    std::string
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

    void check_srf_call_placement(PGParseState * pstate, int location);
};

}
