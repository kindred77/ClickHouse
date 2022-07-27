#pragma once

#include <Interpreters/orcaopt/pgopt/parser_common.h>
#include <Interpreters/orcaopt/pgopt/CoerceParser.h>
#include <Interpreters/orcaopt/pgopt/AggParser.h>
#include <Interpreters/orcaopt/pgopt/ClauseParser.h>

namespace DB
{

class FuncParser
{
private:
    CoerceParser coerce_parser;
    AggParser agg_parser;
    ClauseParser clause_parser;
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
};

}