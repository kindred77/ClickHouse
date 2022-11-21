#pragma once

#include <Interpreters/orcaopt/pgoptnew/parser_common.h>
#include <Interpreters/orcaopt/pgoptnew/CoerceParser.h>
#include <Interpreters/orcaopt/pgoptnew/AggParser.h>
#include <Interpreters/orcaopt/pgoptnew/ClauseParser.h>
#include <Interpreters/orcaopt/pgoptnew/NodeParser.h>
#include <Interpreters/orcaopt/pgoptnew/TypeParser.h>
#include <Interpreters/orcaopt/pgoptnew/RelationParser.h>
#include <Interpreters/orcaopt/pgoptnew/TargetParser.h>

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
	TargetParser target_parser;
public:
	explicit FuncParser();

    duckdb_libpgquery::PGNode *
    ParseFuncOrColumn(PGParseState *pstate, duckdb_libpgquery::PGList *funcname, duckdb_libpgquery::PGList *fargs,
				  duckdb_libpgquery::PGNode *last_srf, duckdb_libpgquery::PGFuncCall *fn, bool proc_call, int location);
};

}