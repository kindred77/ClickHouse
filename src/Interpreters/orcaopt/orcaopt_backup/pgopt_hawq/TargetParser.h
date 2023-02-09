#pragma once

#include <Interpreters/orcaopt/parser_common.h>

namespace DB
{

class RelationParser;
class CTEParser;
class ExprParser;
using RelationParserPtr = std::unique_ptr<RelationParser>;
using CTEParserPtr = std::unique_ptr<CTEParser>;
using ExprParserPtr = std::unique_ptr<ExprParser>;

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
        duckdb_libpgquery::PGList * targetlist, PGParseExprKind exprKind);

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
