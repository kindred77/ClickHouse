#pragma once

#include <common/parser_common.hpp>

namespace DB
{
class RelationParser;
class ExprParser;
class NodeParser;
class CoerceParser;
class TypeProvider;
class RelationProvider;

using RelationParserPtr = std::shared_ptr<RelationParser>;
using ExprParserPtr = std::shared_ptr<ExprParser>;
using NodeParserPtr = std::shared_ptr<NodeParser>;
using CoerceParserPtr = std::shared_ptr<CoerceParser>;
using TypeProviderPtr = std::shared_ptr<TypeProvider>;
using RelationProviderPtr = std::shared_ptr<RelationProvider>;

class Context;
using ContextPtr = std::shared_ptr<const Context>;

class TargetParser
{
private:
    RelationParserPtr relation_parser;
    ExprParserPtr expr_parser;
    NodeParserPtr node_parser;
    CoerceParserPtr coerce_parser;
    TypeProviderPtr type_provider;
    RelationProviderPtr relation_provider;

    ContextPtr context;
public:
	explicit TargetParser(const ContextPtr& context_);

    duckdb_libpgquery::PGList *
    transformTargetList(duckdb_libpgquery::PGParseState *pstate, duckdb_libpgquery::PGList *targetlist,
					duckdb_libpgquery::PGParseExprKind exprKind);
    
    duckdb_libpgquery::PGTargetEntry *
    transformTargetEntry(duckdb_libpgquery::PGParseState *pstate,
					 duckdb_libpgquery::PGNode *node,
					 duckdb_libpgquery::PGNode *expr,
					 duckdb_libpgquery::PGParseExprKind exprKind,
					 char *colname,
					 bool resjunk);
    
    int
    FigureColnameInternal(duckdb_libpgquery::PGNode *node, std::string & name);

    std::string
    FigureColname(duckdb_libpgquery::PGNode *node);

    duckdb_libpgquery::PGList *
    ExpandAllTables(duckdb_libpgquery::PGParseState *pstate, int location);

    duckdb_libpgquery::PGList *
    ExpandColumnRefStar(duckdb_libpgquery::PGParseState *pstate, duckdb_libpgquery::PGColumnRef *cref,
					bool make_target_entry);
    
    duckdb_libpgquery::PGList *
    ExpandRowReference(duckdb_libpgquery::PGParseState *pstate, duckdb_libpgquery::PGNode *expr,
				   bool make_target_entry);

    duckdb_libpgquery::PGList *
    ExpandSingleTable(duckdb_libpgquery::PGParseState *pstate, duckdb_libpgquery::PGRangeTblEntry *rte,
				  int location, bool make_target_entry);

    duckdb_libpgquery::PGList *
    ExpandIndirectionStar(duckdb_libpgquery::PGParseState *pstate, duckdb_libpgquery::PGAIndirection *ind,
					  bool make_target_entry, duckdb_libpgquery::PGParseExprKind exprKind);

    duckdb_libpgquery::PGTupleDescPtr
    expandRecordVariable(duckdb_libpgquery::PGParseState *pstate, duckdb_libpgquery::PGVar *var, int levelsup);

    duckdb_libpgquery::PGList *
    transformExpressionList(duckdb_libpgquery::PGParseState *pstate, duckdb_libpgquery::PGList *exprlist,
						duckdb_libpgquery::PGParseExprKind exprKind);

    void
    resolveTargetListUnknowns(duckdb_libpgquery::PGParseState *pstate, duckdb_libpgquery::PGList *targetlist);
};

}
