#pragma once

#include <Interpreters/orcaopt/pgoptnew/parser_common.h>
#include <Interpreters/orcaopt/pgoptnew/ClauseParser.h>
#include <Interpreters/orcaopt/pgoptnew/NodeParser.h>
#include <Interpreters/orcaopt/pgoptnew/ExprParser.h>

namespace DB
{

struct check_agg_arguments_context
{
	PGParseState *pstate;
	int			min_varlevel;
	int			min_agglevel;
	int			sublevels_up;
};

extern bool
check_agg_arguments_walker(duckdb_libpgquery::PGNode *node,
						   check_agg_arguments_context *context);

class AggParser
{
private:
    ClauseParser clause_parser;
	NodeParser node_parser;
	ExprParser expr_parser;
public:
	explicit AggParser();

	duckdb_libpgquery::PGNode *
	transformGroupingFunc(PGParseState *pstate, duckdb_libpgquery::PGGroupingFunc *p);

	int
	check_agg_arguments(PGParseState *pstate,
					duckdb_libpgquery::PGList *directargs,
					duckdb_libpgquery::PGList *args,
					duckdb_libpgquery::PGExpr *filter);

	void
	check_agglevels_and_constraints(PGParseState *pstate, duckdb_libpgquery::PGNode *expr);

	void
	transformAggregateCall(PGParseState *pstate, duckdb_libpgquery::PGAggref *agg,
					   duckdb_libpgquery::PGList *args,
					   duckdb_libpgquery::PGList *aggorder, bool agg_distinct);

	void
	transformWindowFuncCall(PGParseState *pstate, duckdb_libpgquery::PGWindowFunc *wfunc,
						duckdb_libpgquery::PGWindowDef *windef);

	void
	parseCheckAggregates(PGParseState *pstate, duckdb_libpgquery::PGQuery *qry);
};

}