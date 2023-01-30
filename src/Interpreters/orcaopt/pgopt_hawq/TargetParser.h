#pragma once

#include <Interpreters/orcaopt/pgopt_hawq/parser_common.h>

#include <Interpreters/orcaopt/pgopt_hawq/RelationParser.h>
#include <Interpreters/orcaopt/pgopt_hawq/CTEParser.h>

namespace DB
{

class TargetParser
{
private:
    RelationParser relation_parser;
    CTEParser cte_parser;
public:
	explicit TargetParser();

    int FigureColnameInternal(duckdb_libpgquery::PGNode * node, char ** name);

    char * FigureColname(duckdb_libpgquery::PGNode * node);

    duckdb_libpgquery::PGTargetEntry * transformTargetEntry(PGParseState * pstate,
        duckdb_libpgquery::PGNode * node, duckdb_libpgquery::PGNode * expr,
        char * colname, bool resjunk);

    duckdb_libpgquery::PGList * transformTargetList(PGParseState * pstate,
        duckdb_libpgquery::PGList * targetlist);

    void markTargetListOrigin(PGParseState *pstate, duckdb_libpgquery::PGTargetEntry *tle,
					 duckdb_libpgquery::PGVar *var, int levelsup);

    void markTargetListOrigins(PGParseState *pstate, duckdb_libpgquery::PGList *targetlist);
};

}