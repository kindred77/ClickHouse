#pragma once

#include <Interpreters/orcaopt/parser_common.h>

#include <Interpreters/orcaopt/RelationParser.h>
#include <Interpreters/orcaopt/CTEParser.h>
#include <Interpreters/orcaopt/ExprParser.h>

namespace DB
{

class TargetParser
{
private:
    RelationParserPtr relation_parser_ptr;
    CTEParserPtr cte_parser_ptr;
    ExprParserPtr expr_parser_ptr;
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

    TupleDesc expandRecordVariable(PGParseState * pstate, duckdb_libpgquery::PGVar * var, int levelsup);

    duckdb_libpgquery::PGList * ExpandIndirectionStar(PGParseState * pstate, duckdb_libpgquery::PGAIndirection * ind, bool targetlist);

    duckdb_libpgquery::PGList * ExpandColumnRefStar(PGParseState * pstate, duckdb_libpgquery::PGColumnRef * cref, bool targetlist);

    duckdb_libpgquery::PGList * ExpandAllTables(PGParseState * pstate);

    duckdb_libpgquery::PGList * transformExpressionList(PGParseState * pstate, duckdb_libpgquery::PGList * exprlist);
};
}
