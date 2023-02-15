#pragma once

#include <Interpreters/orcaopt/parser_common.h>

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

using RelationParserPtr = std::unique_ptr<RelationParser>;
using SelectParserPtr = std::unique_ptr<SelectParser>;
using CoerceParserPtr = std::unique_ptr<CoerceParser>;
using ExprParserPtr = std::unique_ptr<ExprParser>;
using TargetParserPtr = std::unique_ptr<TargetParser>;
using OperParserPtr = std::unique_ptr<OperParser>;
using NodeParserPtr = std::unique_ptr<NodeParser>;
using TypeParserPtr = std::unique_ptr<TypeParser>;
using FuncParserPtr = std::unique_ptr<FuncParser>;
using AggParserPtr = std::unique_ptr<AggParser>;
using TypeProviderPtr = std::unique_ptr<TypeProvider>;
using RelationProviderPtr = std::unique_ptr<RelationProvider>;

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
public:
	explicit ExprParser();

    bool
    exprIsNullConstant(duckdb_libpgquery::PGNode *arg);

    duckdb_libpgquery::PGNode *
    transformExpr(PGParseState *pstate, duckdb_libpgquery::PGNode *expr, PGParseExprKind exprKind);

    duckdb_libpgquery::PGNode *
    transformExprRecurse(PGParseState *pstate, duckdb_libpgquery::PGNode *expr);

    duckdb_libpgquery::PGNode *
    transformWholeRowRef(PGParseState *pstate, duckdb_libpgquery::PGRangeTblEntry *rte, int location);

    duckdb_libpgquery::PGNode *
    transformColumnRef(PGParseState *pstate, duckdb_libpgquery::PGColumnRef *cref);

    duckdb_libpgquery::PGNode *
    transformParamRef(PGParseState *pstate, duckdb_libpgquery::PGParamRef *pref);

    duckdb_libpgquery::PGNode *
    transformIndirection(PGParseState *pstate, duckdb_libpgquery::PGNode *basenode, duckdb_libpgquery::PGList *indirection);

    duckdb_libpgquery::PGNode *
    transformArrayExpr(PGParseState *pstate, duckdb_libpgquery::PGAArrayExpr *a,
				   Oid array_type, Oid element_type, int32 typmod);
    
    duckdb_libpgquery::PGNode *
    transformTypeCast(PGParseState *pstate, duckdb_libpgquery::PGTypeCast *tc);

    duckdb_libpgquery::PGNode *
    transformCollateClause(PGParseState *pstate, duckdb_libpgquery::PGCollateClause *c);

    duckdb_libpgquery::PGNode *
    transformAExprOp(PGParseState *pstate, duckdb_libpgquery::PGAExpr *a);

    duckdb_libpgquery::PGNode *
    transformAExprOpAny(PGParseState *pstate, duckdb_libpgquery::PGAExpr *a);

    duckdb_libpgquery::PGNode *
    transformAExprOpAll(PGParseState *pstate, duckdb_libpgquery::PGAExpr *a);

    duckdb_libpgquery::PGNode *
    transformAExprDistinct(PGParseState *pstate, duckdb_libpgquery::PGAExpr *a);

    duckdb_libpgquery::PGNode *
    transformAExprNullIf(PGParseState *pstate, duckdb_libpgquery::PGAExpr *a);

    duckdb_libpgquery::PGNode *
    transformAExprOf(PGParseState *pstate, duckdb_libpgquery::PGAExpr *a);

    duckdb_libpgquery::PGNode *
    transformAExprIn(PGParseState *pstate, duckdb_libpgquery::PGAExpr *a);

    duckdb_libpgquery::PGNode *
    transformAExprBetween(PGParseState *pstate, duckdb_libpgquery::PGAExpr *a);

    duckdb_libpgquery::PGNode *
    transformBoolExpr(PGParseState *pstate, duckdb_libpgquery::PGBoolExpr *a);

    duckdb_libpgquery::PGNode *
    transformFuncCall(PGParseState *pstate, duckdb_libpgquery::PGFuncCall *fn);

    duckdb_libpgquery::PGNode *
    transformMultiAssignRef(PGParseState *pstate, duckdb_libpgquery::PGMultiAssignRef *maref);

    duckdb_libpgquery::PGNode *
    transformRowExpr(PGParseState *pstate, duckdb_libpgquery::PGRowExpr *r, bool allowDefault);

    duckdb_libpgquery::PGNode *
    transformSubLink(PGParseState *pstate, duckdb_libpgquery::PGSubLink *sublink);

    duckdb_libpgquery::PGNode *
    transformCaseExpr(PGParseState *pstate, duckdb_libpgquery::PGCaseExpr *c);

    duckdb_libpgquery::PGNode *
    transformCoalesceExpr(PGParseState *pstate, duckdb_libpgquery::PGCoalesceExpr *c);

    duckdb_libpgquery::PGNode *
    transformMinMaxExpr(PGParseState *pstate, duckdb_libpgquery::PGMinMaxExpr *m);

    duckdb_libpgquery::PGNode *
    transformSQLValueFunction(PGParseState *pstate, duckdb_libpgquery::PGSQLValueFunction *svf);

    duckdb_libpgquery::PGNode *
    transformBooleanTest(PGParseState *pstate, duckdb_libpgquery::PGBooleanTest *b);

    duckdb_libpgquery::PGNode *
    transformCurrentOfExpr(PGParseState *pstate, duckdb_libpgquery::PGCurrentOfExpr *cexpr);

    int
    operator_precedence_group(duckdb_libpgquery::PGNode *node, const char **nodename);

    void
    emit_precedence_warnings(PGParseState *pstate,
						 int opgroup, const char *opname,
						 duckdb_libpgquery::PGNode *lchild, duckdb_libpgquery::PGNode *rchild,
						 int location);
    
    duckdb_libpgquery::PGNode *
    make_nulltest_from_distinct(PGParseState *pstate, duckdb_libpgquery::PGAExpr *distincta, duckdb_libpgquery::PGNode *arg);

    char *
    ParseExprKindName(PGParseExprKind exprKind);

    bool
    isWhenIsNotDistinctFromExpr(duckdb_libpgquery::PGNode *warg);

    void
    unknown_attribute(PGParseState *pstate, duckdb_libpgquery::PGNode *relref, const char *attname,
				  int location);

    duckdb_libpgquery::PGExpr *
    make_distinct_op(PGParseState *pstate, duckdb_libpgquery::PGList *opname,
                duckdb_libpgquery::PGNode *ltree, duckdb_libpgquery::PGNode *rtree,
				int location);

    duckdb_libpgquery::PGNode *
    make_row_distinct_op(PGParseState *pstate, duckdb_libpgquery::PGList *opname,
					 duckdb_libpgquery::PGRowExpr *lrow, duckdb_libpgquery::PGRowExpr *rrow,
					 int location);
};

}
