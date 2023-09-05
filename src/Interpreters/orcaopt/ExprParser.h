#pragma once

#include <common/parser_common.hpp>

namespace DB
{

class RelationParser;
class SelectParser;
class CoerceParser;
class ExprParser;
class TargetParser;
class OperParser;
class NodeParser;
class TypeParser;
class FuncParser;
class AggParser;
class TypeProvider;
class RelationProvider;

using RelationParserPtr = std::shared_ptr<RelationParser>;
using SelectParserPtr = std::shared_ptr<SelectParser>;
using CoerceParserPtr = std::shared_ptr<CoerceParser>;
using ExprParserPtr = std::shared_ptr<ExprParser>;
using TargetParserPtr = std::shared_ptr<TargetParser>;
using OperParserPtr = std::shared_ptr<OperParser>;
using NodeParserPtr = std::shared_ptr<NodeParser>;
using TypeParserPtr = std::shared_ptr<TypeParser>;
using FuncParserPtr = std::shared_ptr<FuncParser>;
using AggParserPtr = std::shared_ptr<AggParser>;
using TypeProviderPtr = std::shared_ptr<TypeProvider>;
using RelationProviderPtr = std::shared_ptr<RelationProvider>;

class Context;
using ContextPtr = std::shared_ptr<const Context>;

class ExprParser
{
private:
    RelationParserPtr relation_parser;
    CoerceParserPtr coerce_parser;
    SelectParserPtr select_parser;
    TargetParserPtr target_parser;
    TypeParserPtr type_parser;
    FuncParserPtr func_parser;
    OperParserPtr oper_parser;
    //CollationParserPtr collation_parser;
    NodeParserPtr node_parser;
    AggParserPtr agg_parser;
    TypeProviderPtr type_provider;
    RelationProviderPtr relation_provider;

    bool		operator_precedence_warning = false;
    bool		Transform_null_equals = false;

    ContextPtr context;
public:
	explicit ExprParser(const ContextPtr& context_);

    bool
    exprIsNullConstant(duckdb_libpgquery::PGNode *arg);

    duckdb_libpgquery::PGNode *
    transformExpr(duckdb_libpgquery::PGParseState *pstate, duckdb_libpgquery::PGNode *expr, duckdb_libpgquery::PGParseExprKind exprKind);

    duckdb_libpgquery::PGNode *
    transformExprRecurse(duckdb_libpgquery::PGParseState *pstate, duckdb_libpgquery::PGNode *expr);

    duckdb_libpgquery::PGNode *
    transformWholeRowRef(duckdb_libpgquery::PGParseState *pstate, duckdb_libpgquery::PGRangeTblEntry *rte, int location);

    duckdb_libpgquery::PGNode *
    transformColumnRef(duckdb_libpgquery::PGParseState *pstate, duckdb_libpgquery::PGColumnRef *cref);

    duckdb_libpgquery::PGNode *
    transformParamRef(duckdb_libpgquery::PGParseState *pstate, duckdb_libpgquery::PGParamRef *pref);

    duckdb_libpgquery::PGNode *
    transformIndirection(duckdb_libpgquery::PGParseState *pstate, duckdb_libpgquery::PGNode *basenode, duckdb_libpgquery::PGList *indirection);

    duckdb_libpgquery::PGNode *
    transformArrayExpr(duckdb_libpgquery::PGParseState *pstate, duckdb_libpgquery::PGAArrayExpr *a,
				   duckdb_libpgquery::PGOid array_type, duckdb_libpgquery::PGOid element_type, duckdb_libpgquery::int32 typmod);
    
    duckdb_libpgquery::PGNode *
    transformTypeCast(duckdb_libpgquery::PGParseState *pstate, duckdb_libpgquery::PGTypeCast *tc);

    duckdb_libpgquery::PGNode *
    transformCollateClause(duckdb_libpgquery::PGParseState *pstate, duckdb_libpgquery::PGCollateClause *c);

    duckdb_libpgquery::PGNode *
    transformAExprOp(duckdb_libpgquery::PGParseState *pstate, duckdb_libpgquery::PGAExpr *a);

    duckdb_libpgquery::PGNode *
    transformAExprOpAny(duckdb_libpgquery::PGParseState *pstate, duckdb_libpgquery::PGAExpr *a);

    duckdb_libpgquery::PGNode *
    transformAExprOpAll(duckdb_libpgquery::PGParseState *pstate, duckdb_libpgquery::PGAExpr *a);

    duckdb_libpgquery::PGNode *
    transformAExprDistinct(duckdb_libpgquery::PGParseState *pstate, duckdb_libpgquery::PGAExpr *a);

    duckdb_libpgquery::PGNode *
    transformAExprNullIf(duckdb_libpgquery::PGParseState *pstate, duckdb_libpgquery::PGAExpr *a);

    duckdb_libpgquery::PGNode *
    transformAExprOf(duckdb_libpgquery::PGParseState *pstate, duckdb_libpgquery::PGAExpr *a);

    duckdb_libpgquery::PGNode *
    transformAExprIn(duckdb_libpgquery::PGParseState *pstate, duckdb_libpgquery::PGAExpr *a);

    duckdb_libpgquery::PGNode *
    transformAExprBetween(duckdb_libpgquery::PGParseState *pstate, duckdb_libpgquery::PGAExpr *a);

    duckdb_libpgquery::PGNode *
    transformBoolExpr(duckdb_libpgquery::PGParseState *pstate, duckdb_libpgquery::PGBoolExpr *a);

    duckdb_libpgquery::PGNode *
    transformFuncCall(duckdb_libpgquery::PGParseState *pstate, duckdb_libpgquery::PGFuncCall *fn);

    duckdb_libpgquery::PGNode *
    transformRowExpr(duckdb_libpgquery::PGParseState *pstate, duckdb_libpgquery::PGRowExpr *r);

    duckdb_libpgquery::PGNode *
    transformSubLink(duckdb_libpgquery::PGParseState *pstate, duckdb_libpgquery::PGSubLink *sublink);

    duckdb_libpgquery::PGNode *
    transformCaseExpr(duckdb_libpgquery::PGParseState *pstate, duckdb_libpgquery::PGCaseExpr *c);

    duckdb_libpgquery::PGNode *
    transformCoalesceExpr(duckdb_libpgquery::PGParseState *pstate, duckdb_libpgquery::PGCoalesceExpr *c);

    duckdb_libpgquery::PGNode *
    transformMinMaxExpr(duckdb_libpgquery::PGParseState *pstate, duckdb_libpgquery::PGMinMaxExpr *m);

    duckdb_libpgquery::PGNode *
    transformBooleanTest(duckdb_libpgquery::PGParseState *pstate, duckdb_libpgquery::PGBooleanTest *b);

    duckdb_libpgquery::PGNode *
    transformCurrentOfExpr(duckdb_libpgquery::PGParseState *pstate, duckdb_libpgquery::PGCurrentOfExpr *cexpr);

    duckdb_libpgquery::PGNode * transformGroupingFunc(duckdb_libpgquery::PGParseState * pstate, duckdb_libpgquery::PGGroupingFunc * gf);

    int
    operator_precedence_group(duckdb_libpgquery::PGNode *node, const char **nodename);

    void
    emit_precedence_warnings(duckdb_libpgquery::PGParseState *pstate,
						 int opgroup, const char *opname,
						 duckdb_libpgquery::PGNode *lchild, duckdb_libpgquery::PGNode *rchild,
						 int location);
    
    duckdb_libpgquery::PGNode *
    make_nulltest_from_distinct(duckdb_libpgquery::PGParseState *pstate, duckdb_libpgquery::PGAExpr *distincta, duckdb_libpgquery::PGNode *arg);

    const char *
    ParseExprKindName(duckdb_libpgquery::PGParseExprKind exprKind);

    bool
    isWhenIsNotDistinctFromExpr(duckdb_libpgquery::PGNode *warg);

    void
    unknown_attribute(duckdb_libpgquery::PGParseState *pstate, duckdb_libpgquery::PGNode *relref, const char *attname,
				  int location);

    duckdb_libpgquery::PGExpr *
    make_distinct_op(duckdb_libpgquery::PGParseState *pstate, duckdb_libpgquery::PGList *opname,
                duckdb_libpgquery::PGNode *ltree, duckdb_libpgquery::PGNode *rtree,
				int location);

    duckdb_libpgquery::PGNode *
    make_row_distinct_op(duckdb_libpgquery::PGParseState *pstate, duckdb_libpgquery::PGList *opname,
					 duckdb_libpgquery::PGRowExpr *lrow, duckdb_libpgquery::PGRowExpr *rrow,
					 int location);
};

}
