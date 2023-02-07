#pragma once

#include <Interpreters/orcaopt/pgoptnew/parser_common.h>
#include <Interpreters/orcaopt/pgoptnew/RelationParser.h>
#include <Interpreters/orcaopt/pgoptnew/ExprParser.h>
#include <Interpreters/orcaopt/pgoptnew/NodeParser.h>
#include <Interpreters/orcaopt/pgoptnew/CoerceParser.h>

namespace DB
{

class TargetParser
{
private:
    RelationParser relation_parser;
    ExprParser expr_parser;
    NodeParser node_parser;
    CoerceParser coerce_parser;
public:
	explicit TargetParser();

    duckdb_libpgquery::PGList *
    transformTargetList(PGParseState *pstate, duckdb_libpgquery::PGList *targetlist,
					PGParseExprKind exprKind);
    
    duckdb_libpgquery::PGTargetEntry *
    transformTargetEntry(PGParseState *pstate,
					 duckdb_libpgquery::PGNode *node,
					 duckdb_libpgquery::PGNode *expr,
					 PGParseExprKind exprKind,
					 char *colname,
					 bool resjunk);
    
    int
    FigureColnameInternal(duckdb_libpgquery::PGNode *node, char **name);

    char *
    FigureColname(duckdb_libpgquery::PGNode *node);

    duckdb_libpgquery::PGList *
    ExpandAllTables(PGParseState *pstate, int location);

    duckdb_libpgquery::PGList *
    ExpandColumnRefStar(PGParseState *pstate, duckdb_libpgquery::PGColumnRef *cref,
					bool make_target_entry);
    
    duckdb_libpgquery::PGList *
    ExpandRowReference(PGParseState *pstate, duckdb_libpgquery::PGNode *expr,
				   bool make_target_entry);

    duckdb_libpgquery::PGList *
    ExpandSingleTable(PGParseState *pstate, duckdb_libpgquery::PGRangeTblEntry *rte,
				  int location, bool make_target_entry);

    duckdb_libpgquery::PGList *
    ExpandIndirectionStar(PGParseState *pstate, duckdb_libpgquery::PGAIndirection *ind,
					  bool make_target_entry, PGParseExprKind exprKind);

    TupleDesc
    expandRecordVariable(PGParseState *pstate, duckdb_libpgquery::PGVar *var, int levelsup);

    duckdb_libpgquery::PGList *
    transformExpressionList(PGParseState *pstate, duckdb_libpgquery::PGList *exprlist,
						PGParseExprKind exprKind, bool allowDefault);

    void
    resolveTargetListUnknowns(PGParseState *pstate, duckdb_libpgquery::PGList *targetlist);
};

}