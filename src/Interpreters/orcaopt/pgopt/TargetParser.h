#pragma once

#include <Interpreters/orcaopt/pgopt/parser_common.h>
#include <Interpreters/orcaopt/pgopt/RelationParser.h>
#include <Interpreters/orcaopt/pgopt/ExprParser.h>

namespace DB
{

class TargetParser
{
private:
    RelationParser relation_parser;
    ExprParser expr_parser;
public:
	explicit TargetParser();

    duckdb_libpgquery::PGList * transformTargetList(PGParseState *pstate, duckdb_libpgquery::PGList *targetlist,
        PGParseExprKind exprKind);
    
    duckdb_libpgquery::PGList *
    ExpandColumnRefStar(PGParseState *pstate, duckdb_libpgquery::PGColumnRef *cref,
					bool make_target_entry);
    
    duckdb_libpgquery::PGList *
    ExpandSingleTable(PGParseState *pstate, duckdb_libpgquery::PGRangeTblEntry *rte,
				  int location, bool make_target_entry);

    void markTargetListOrigins(PGParseState *pstate, duckdb_libpgquery::PGList *targetlist);

    void markTargetListOrigin(PGParseState *pstate, duckdb_libpgquery::PGTargetEntry *tle,
					 duckdb_libpgquery::PGVar *var, int levelsup);
    
    duckdb_libpgquery::PGTargetEntry *
    transformTargetEntry(PGParseState *pstate,
					 duckdb_libpgquery::PGNode *node,
					 duckdb_libpgquery::PGNode *expr,
					 PGParseExprKind exprKind,
					 char *colname,
					 bool resjunk);
    
    char * FigureColname(duckdb_libpgquery::PGNode *node);

    int FigureColnameInternal(duckdb_libpgquery::PGNode *node, char **name);

    duckdb_libpgquery::PGList *
    transformExpressionList(PGParseState *pstate, duckdb_libpgquery::PGList *exprlist,
						PGParseExprKind exprKind);
};

}