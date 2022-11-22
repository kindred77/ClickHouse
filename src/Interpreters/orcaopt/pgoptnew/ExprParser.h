#pragma once

#include <Interpreters/orcaopt/pgoptnew/parser_common.h>
#include <Interpreters/orcaopt/pgoptnew/RelationParser.h>
#include <Interpreters/orcaopt/pgoptnew/CoerceParser.h>
#include <Interpreters/orcaopt/pgoptnew/SelectParser.h>
#include <Interpreters/orcaopt/pgoptnew/TargetParser.h>
#include <Interpreters/orcaopt/pgoptnew/TypeParser.h>
#include <Interpreters/orcaopt/pgoptnew/FuncParser.h>
#include <Interpreters/orcaopt/pgoptnew/OperParser.h>
#include <Interpreters/orcaopt/pgoptnew/CollationParser.h>
#include <Interpreters/orcaopt/pgoptnew/NodeParser.h>
#include <Interpreters/orcaopt/pgoptnew/AggParser.h>

namespace DB
{

class ExprParser
{
private:
    RelationParser relation_parser;
    CoerceParser coerce_parser;
    SelectParser select_parser;
    TargetParser target_parser;
    TypeParser type_parser;
    FuncParser func_parser;
    OperParser oper_parser;
    CollationParser collation_parser;
    NodeParser node_parser;
    AggParser agg_parser;

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
    transformIndirection(PGParseState *pstate, duckdb_libpgquery::PGAIndirection *ind);

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

    duckdb_libpgquery::PGNode *
    make_row_comparison_op(PGParseState *pstate, duckdb_libpgquery::PGList *opname,
					   duckdb_libpgquery::PGList *largs, duckdb_libpgquery::PGList *rargs,
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