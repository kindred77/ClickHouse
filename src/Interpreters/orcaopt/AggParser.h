#pragma once

#include <common/parser_common.hpp>

#define AGGKIND_IS_ORDERED_SET(kind)  ((kind) != AGGKIND_NORMAL)

namespace DB
{

extern int
cmp_list_len_asc(const void *a, const void *b);

class ClauseParser;
class NodeParser;
class ExprParser;

using ClauseParserPtr = std::shared_ptr<ClauseParser>;
using NodeParserPtr = std::shared_ptr<NodeParser>;
using ExprParserPtr = std::shared_ptr<ExprParser>;

class Context;
using ContextPtr = std::shared_ptr<const Context>;

class AggParser
{
private:
    ClauseParserPtr clause_parser;
	NodeParserPtr node_parser;
	ExprParserPtr expr_parser;
	ContextPtr context;
public:
	explicit AggParser(const ContextPtr& context_);

	duckdb_libpgquery::PGNode *
	transformGroupingFunc(duckdb_libpgquery::PGParseState *pstate, duckdb_libpgquery::PGGroupingFunc *p);

	int
	check_agg_arguments(duckdb_libpgquery::PGParseState *pstate,
					duckdb_libpgquery::PGList *directargs,
					duckdb_libpgquery::PGList *args,
					duckdb_libpgquery::PGExpr *filter);

	void
	check_agglevels_and_constraints(duckdb_libpgquery::PGParseState *pstate, duckdb_libpgquery::PGNode *expr);

    void transformAggregateCall(duckdb_libpgquery::PGParseState * pstate,
		duckdb_libpgquery::PGAggref * agg, 
		duckdb_libpgquery::PGList * args, 
		duckdb_libpgquery::PGList * aggorder, bool agg_distinct);

    void
	transformWindowFuncCall(duckdb_libpgquery::PGParseState *pstate, duckdb_libpgquery::PGWindowFunc *wfunc,
						duckdb_libpgquery::PGWindowDef *windef);

	void parseCheckAggregates(duckdb_libpgquery::PGParseState *pstate, duckdb_libpgquery::PGQuery *qry);

	duckdb_libpgquery::PGList *
	expand_groupingset_node(duckdb_libpgquery::PGGroupingSet *gs);

	duckdb_libpgquery::PGList *
	expand_grouping_sets(duckdb_libpgquery::PGList *groupingSets, int limit);

    duckdb_libpgquery::PGList * get_groupclause_exprs(duckdb_libpgquery::PGNode * grpcl,
		duckdb_libpgquery::PGList * targetList);

    void check_ungrouped_columns(
        duckdb_libpgquery::PGNode * node, duckdb_libpgquery::PGParseState * pstate, duckdb_libpgquery::PGQuery * qry,
		duckdb_libpgquery::PGList * groupClauses, bool have_non_var_grouping, 
		duckdb_libpgquery::PGList ** func_grouped_rels);

    bool checkExprHasGroupExtFuncs(duckdb_libpgquery::PGNode * node);
};

}
