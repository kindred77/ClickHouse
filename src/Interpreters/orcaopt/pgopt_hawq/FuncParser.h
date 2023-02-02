#pragma once

#include <Interpreters/orcaopt/pgopt_hawq/parser_common.h>

#include <Interpreters/orcaopt/pgopt_hawq/CoerceParser.h>

namespace DB
{

class FuncParser
{
private:
    CoerceParser coerce_parser;
public:
	explicit FuncParser();

    bool typeInheritsFrom(Oid subclassTypeId, Oid superclassTypeId);

    void make_fn_arguments(PGParseState * pstate, duckdb_libpgquery::PGList * fargs,
        Oid * actual_arg_types, Oid * declared_arg_types);

    duckdb_libpgquery::PGNode * ParseFuncOrColumn(
        PGParseState * pstate,
        duckdb_libpgquery::PGList * funcname,
        duckdb_libpgquery::PGList * fargs,
        duckdb_libpgquery::PGList * agg_order,
        bool agg_star,
        bool agg_distinct,
        bool func_variadic,
        bool is_column,
        WindowSpec * over,
        int location,
        duckdb_libpgquery::PGNode * agg_filter);

    int func_match_argtypes(
        int nargs, Oid * input_typeids, FuncCandidateList raw_candidates, FuncCandidateList * candidates) /* return value */;

    FuncCandidateList func_select_candidate(int nargs, Oid * input_typeids, FuncCandidateList candidates);
};

}