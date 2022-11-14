#pragma once

#include <Interpreters/orcaopt/pgopt/parser_common.h>
#include <Interpreters/orcaopt/pgopt/CoerceParser.h>
#include <Interpreters/orcaopt/pgopt/AggParser.h>
#include <Interpreters/orcaopt/pgopt/ClauseParser.h>
#include <Interpreters/orcaopt/pgopt/NodeParser.h>
#include <Interpreters/orcaopt/pgopt/TypeParser.h>
#include <Interpreters/orcaopt/pgopt/RelationParser.h>

namespace DB
{

class FuncParser
{
private:
    CoerceParser coerce_parser;
    AggParser agg_parser;
    ClauseParser clause_parser;
    NodeParser node_parser;
    TypeParser type_parser;
    RelationParser relation_parser;
public:
	explicit FuncParser();

    duckdb_libpgquery::PGNode *
    ParseFuncOrColumn(PGParseState *pstate, duckdb_libpgquery::PGList *funcname,
                    duckdb_libpgquery::PGList *fargs,
				    duckdb_libpgquery::PGFuncCall *fn, int location);

    void
    make_fn_arguments(PGParseState *pstate,
				  duckdb_libpgquery::PGList *fargs,
				  Oid *actual_arg_types,
				  Oid *declared_arg_types);

    duckdb_libpgquery::PGNode *
    ParseComplexProjection(PGParseState *pstate, char *funcname, duckdb_libpgquery::PGNode *first_arg,
					   int location);

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
				Oid **true_typeids,		/* return value */
				duckdb_libpgquery::PGList **argdefaults)		/* optional return value */;
    
    int
    func_match_argtypes(int nargs,
					Oid *input_typeids,
					FuncCandidateList raw_candidates,
					FuncCandidateList *candidates)		/* return value */;

    Oid
    FuncNameAsType(duckdb_libpgquery::PGList *funcname);

	FuncCandidateList
	func_select_candidate(int nargs,
					  Oid *input_typeids,
					  FuncCandidateList candidates);
};

}