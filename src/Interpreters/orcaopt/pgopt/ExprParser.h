#pragma once

#include <Interpreters/orcaopt/pgopt/parser_common.h>
#include <Interpreters/orcaopt/pgopt/RelationParser.h>
#include <Interpreters/orcaopt/pgopt/CoerceParser.h>
#include <Interpreters/orcaopt/pgopt/SelectParser.h>
#include <Interpreters/orcaopt/pgopt/TargetParser.h>
#include <Interpreters/orcaopt/pgopt/TypeParser.h>

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
public:
	explicit ExprParser();

    duckdb_libpgquery::PGNode *
    transformExpr(PGParseState *pstate, duckdb_libpgquery::PGNode *expr, PGParseExprKind exprKind);

    duckdb_libpgquery::PGNode *
    transformExprRecurse(PGParseState *pstate, duckdb_libpgquery::PGNode *expr);

    duckdb_libpgquery::PGNode *
    transformColumnRef(PGParseState *pstate, duckdb_libpgquery::PGColumnRef *cref);

    duckdb_libpgquery::PGNode *
    transformAExprOp(PGParseState *pstate, duckdb_libpgquery::PGAExpr *a);

    duckdb_libpgquery::PGNode *
    transformParamRef(PGParseState *pstate, duckdb_libpgquery::PGParamRef *pref);

    duckdb_libpgquery::PGNode *
    transformIndirection(PGParseState *pstate, duckdb_libpgquery::PGNode *basenode, duckdb_libpgquery::PGList *indirection);

    duckdb_libpgquery::PGNode *
    transformArrayExpr(PGParseState *pstate, PGAArrayExpr *a,
				   Oid array_type, Oid element_type, int32 typmod);
    
    duckdb_libpgquery::PGNode *
    transformTypeCast(PGParseState *pstate, PGTypeCast *tc);

    duckdb_libpgquery::PGNode *
    transformCollateClause(PGParseState *pstate, PGCollateClause *c);

    duckdb_libpgquery::PGNode *
    transformAExprOpAny(PGParseState *pstate, PGAExpr *a);

    duckdb_libpgquery::PGNode *
    transformAExprOpAll(PGParseState *pstate, PGAExpr *a);

    duckdb_libpgquery::PGNode *
    transformAExprDistinct(PGParseState *pstate, PGAExpr *a);

    duckdb_libpgquery::PGNode *
    transformAExprNullIf(PGParseState *pstate, PGAExpr *a);

    duckdb_libpgquery::PGNode *
    transformAExprOf(PGParseState *pstate, PGAExpr *a);

    duckdb_libpgquery::PGNode *
    transformAExprIn(PGParseState *pstate, PGAExpr *a);

    duckdb_libpgquery::PGNode *
    transformBoolExpr(PGParseState *pstate, PGBoolExpr *a);

    duckdb_libpgquery::PGNode *
    transformFuncCall(PGParseState *pstate, PGFuncCall *fn);

    duckdb_libpgquery::PGNode *
    transformSubLink(PGParseState *pstate, PGSubLink *sublink);

    duckdb_libpgquery::PGNode *
    transformCaseExpr(PGParseState *pstate, PGCaseExpr *c);

    duckdb_libpgquery::PGNode *
    transformRowExpr(PGParseState *pstate, PGRowExpr *r);

    duckdb_libpgquery::PGNode *
    transformWholeRowRef(PGParseState *pstate, PGRangeTblEntry *rte, int location);

    duckdb_libpgquery::PGArrayRef *
    transformArraySubscripts(PGParseState *pstate,
						 duckdb_libpgquery::PGNode *arrayBase,
						 Oid arrayType,
						 Oid elementType,
						 int32 arrayTypMod,
						 duckdb_libpgquery::PGList *indirection,
						 duckdb_libpgquery::PGNode *assignFrom);

    Oid transformArrayType(Oid *arrayType, int32 *arrayTypmod);

    duckdb_libpgquery::PGConst *
    make_const(PGParseState *pstate, duckdb_libpgquery::PGValue *value, int location);

    duckdb_libpgquery::PGNode *
    transformCoalesceExpr(PGParseState *pstate, duckdb_libpgquery::PGCoalesceExpr *c);

    duckdb_libpgquery::PGNode *
    transformMinMaxExpr(PGParseState *pstate, duckdb_libpgquery::PGMinMaxExpr *m);

    duckdb_libpgquery::PGNode *
    transformBooleanTest(PGParseState *pstate, duckdb_libpgquery::PGBooleanTest *b);

    duckdb_libpgquery::PGNode *
    transformCurrentOfExpr(PGParseState *pstate, duckdb_libpgquery::PGCurrentOfExpr *cexpr);

    duckdb_libpgquery::PGNode *
    transformGroupingFunc(PGParseState *pstate, duckdb_libpgquery::PGGroupingFunc *gf);
};

}