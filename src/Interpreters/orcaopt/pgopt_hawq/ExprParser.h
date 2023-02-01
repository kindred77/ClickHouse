#pragma once

#include <Interpreters/orcaopt/pgopt_hawq/parser_common.h>
#include <Interpreters/orcaopt/pgopt_hawq/RelationParser.h>
#include <Interpreters/orcaopt/pgopt_hawq/OperParser.h>
#include <Interpreters/orcaopt/pgopt_hawq/NodeParser.h>
#include <Interpreters/orcaopt/pgopt_hawq/TypeParser.h>
#include <Interpreters/orcaopt/pgopt_hawq/CoerceParser.h>
#include <Interpreters/orcaopt/pgopt_hawq/FuncParser.h>
#include <Interpreters/orcaopt/pgopt_hawq/SelectParser.h>
#include <Interpreters/orcaopt/pgopt_hawq/TargetParser.h>

namespace DB
{

class ExprParser
{
private:
    RelationParser relation_parser;
    OperParser oper_parser;
    NodeParser node_parser;
    TypeParser type_parser;
    CoerceParser coerce_parser;
    FuncParser func_parser;
    SelectParser select_parser;
    TargetParser target_parser;
    bool		operator_precedence_warning = false;
    bool		Transform_null_equals = false;
public:
	explicit ExprParser();

    bool exprIsNullConstant(duckdb_libpgquery::PGNode * arg);

    duckdb_libpgquery::PGNode * typecast_expression(PGParseState * pstate,
        duckdb_libpgquery::PGNode * expr, duckdb_libpgquery::PGTypeName * typname);

    duckdb_libpgquery::PGNode * make_row_comparison_op(PGParseState * pstate,
        duckdb_libpgquery::PGList * opname, duckdb_libpgquery::PGList * largs,
        duckdb_libpgquery::PGList * rargs, int location);

    duckdb_libpgquery::PGNode * transformExpr(PGParseState * pstate, duckdb_libpgquery::PGNode * expr);

    duckdb_libpgquery::PGNode * transformParamRef(PGParseState * pstate,
        duckdb_libpgquery::PGParamRef * pref);

    duckdb_libpgquery::PGNode * transformAExprOp(PGParseState * pstate,
        duckdb_libpgquery::PGAExpr * a);

    duckdb_libpgquery::PGNode * transformAExprAnd(PGParseState * pstate, duckdb_libpgquery::PGAExpr * a);
    
    duckdb_libpgquery::PGNode * transformAExprOr(PGParseState * pstate, duckdb_libpgquery::PGAExpr * a);

    duckdb_libpgquery::PGNode * transformAExprNot(PGParseState * pstate, duckdb_libpgquery::PGAExpr * a);

    duckdb_libpgquery::PGNode * transformAExprOpAny(PGParseState * pstate, duckdb_libpgquery::PGAExpr * a);

    duckdb_libpgquery::PGNode * transformAExprOpAll(PGParseState * pstate, duckdb_libpgquery::PGAExpr * a);

    duckdb_libpgquery::PGNode * make_row_distinct_op(PGParseState * pstate, duckdb_libpgquery::PGList * opname,
        duckdb_libpgquery::PGRowExpr * lrow, duckdb_libpgquery::PGRowExpr * rrow, int location);

    duckdb_libpgquery::PGExpr * make_distinct_op(PGParseState * pstate, duckdb_libpgquery::PGList * opname,
        duckdb_libpgquery::PGNode * ltree, duckdb_libpgquery::PGNode * rtree, int location);

    duckdb_libpgquery::PGNode * transformAExprDistinct(PGParseState * pstate, duckdb_libpgquery::PGAExpr * a);

    duckdb_libpgquery::PGNode * transformAExprNullIf(PGParseState * pstate, duckdb_libpgquery::PGAExpr * a);

    duckdb_libpgquery::PGNode * transformAExprOf(PGParseState * pstate, duckdb_libpgquery::PGAExpr * a);

    duckdb_libpgquery::PGNode * transformAExprIn(PGParseState * pstate, duckdb_libpgquery::PGAExpr * a);

    duckdb_libpgquery::PGNode * transformFuncCall(PGParseState * pstate, duckdb_libpgquery::PGFuncCall * fn);

    duckdb_libpgquery::PGNode * transformSubLink(PGParseState * pstate, duckdb_libpgquery::PGSubLink * sublink);

    bool isWhenIsNotDistinctFromExpr(duckdb_libpgquery::PGNode * warg);

    duckdb_libpgquery::PGNode * transformCaseExpr(PGParseState * pstate, duckdb_libpgquery::PGCaseExpr * c);

    duckdb_libpgquery::PGNode * transformArrayExpr(PGParseState * pstate, duckdb_libpgquery::PGArrayExpr * a);

    duckdb_libpgquery::PGNode * transformRowExpr(PGParseState * pstate, duckdb_libpgquery::PGRowExpr * r);

    duckdb_libpgquery::PGNode * transformCoalesceExpr(PGParseState * pstate, duckdb_libpgquery::PGCoalesceExpr * c);

    duckdb_libpgquery::PGNode * transformMinMaxExpr(PGParseState * pstate, duckdb_libpgquery::PGMinMaxExpr * m);

    duckdb_libpgquery::PGNode * transformBooleanTest(PGParseState * pstate, duckdb_libpgquery::PGBooleanTest * b);

    duckdb_libpgquery::PGNode * transformGroupingFunc(PGParseState * pstate, duckdb_libpgquery::PGGroupingFunc * gf);

    duckdb_libpgquery::PGNode * transformColumnRef(PGParseState * pstate,
        duckdb_libpgquery::PGColumnRef * cref);

    duckdb_libpgquery::PGNode * transformWholeRowRef(PGParseState * pstate, char * catalogname,
        char * schemaname, char * relname, int location);
};

}