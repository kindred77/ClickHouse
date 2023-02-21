#pragma once

#include <Interpreters/orcaopt/parser_common.h>

namespace DB
{
class RelationParser;
class ExprParser;
class NodeParser;
class CoerceParser;
class TypeProvider;
class RelationProvider;

using RelationParserPtr = std::unique_ptr<RelationParser>;
using ExprParserPtr = std::unique_ptr<ExprParser>;
using NodeParserPtr = std::unique_ptr<NodeParser>;
using CoerceParserPtr = std::unique_ptr<CoerceParser>;
using TypeProviderPtr = std::unique_ptr<TypeProvider>;
using RelationProviderPtr = std::unique_ptr<RelationProvider>;

class TargetParser
{
private:
    RelationParserPtr relation_parser;
    ExprParserPtr expr_parser;
    NodeParserPtr node_parser;
    CoerceParserPtr coerce_parser;
    TypeProviderPtr type_provider;
    RelationProviderPtr relation_provider;
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
    FigureColnameInternal(duckdb_libpgquery::PGNode *node, std::string & name);

    std::string
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

    PGTupleDescPtr
    expandRecordVariable(PGParseState *pstate, duckdb_libpgquery::PGVar *var, int levelsup);

    duckdb_libpgquery::PGList *
    transformExpressionList(PGParseState *pstate, duckdb_libpgquery::PGList *exprlist,
						PGParseExprKind exprKind);

    void
    resolveTargetListUnknowns(PGParseState *pstate, duckdb_libpgquery::PGList *targetlist);
};

}
