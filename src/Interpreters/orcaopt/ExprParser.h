#pragma once

#include <Interpreters/orcaopt/parser_common.h>

namespace DB
{

class RelationParser;
class OperParser;
class NodeParser;
class TypeParser;
class CoerceParser;
class FuncParser;
class SelectParser;
class TargetParser;

using RelationParserPtr = std::unique_ptr<RelationParser>;
using OperParserPtr = std::unique_ptr<OperParser>;
using NodeParserPtr = std::unique_ptr<NodeParser>;
using TypeParserPtr = std::unique_ptr<TypeParser>;
using CoerceParserPtr = std::unique_ptr<CoerceParser>;
using FuncParserPtr = std::unique_ptr<FuncParser>;
using SelectParserPtr = std::unique_ptr<SelectParser>;
using TargetParserPtr = std::unique_ptr<TargetParser>;

class ExprParser
{
private:
    RelationParserPtr relation_parser_ptr;
    OperParserPtr oper_parser_ptr;
    NodeParserPtr node_parser_ptr;
    TypeParserPtr type_parser_ptr;
    CoerceParserPtr coerce_parser_ptr;
    FuncParserPtr func_parser_ptr;
    SelectParserPtr select_parser_ptr;
    TargetParserPtr target_parser_ptr;
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

    const char * ParseExprKindName(PGParseExprKind exprKind);
};

}
