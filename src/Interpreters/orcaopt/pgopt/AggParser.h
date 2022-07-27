#pragma once

#include <Interpreters/orcaopt/pgopt/parser_common.h>
#include <Interpreters/orcaopt/pgopt/ClauseParser.h>

namespace DB
{

struct check_agg_arguments_context
{
	PGParseState *pstate;
	int			min_varlevel;
	int			min_agglevel;
	int			sublevels_up;
};

class AggParser
{
private:
    ClauseParser clause_parser;
public:
	explicit AggParser();

    void
    transformWindowFuncCall(PGParseState *pstate, duckdb_libpgquery::PGWindowFunc *wfunc,
						duckdb_libpgquery::PGWindowDef *windef);

    void
    transformAggregateCall(PGParseState *pstate, duckdb_libpgquery::PGAggref *agg,
					   duckdb_libpgquery::PGList *args, duckdb_libpgquery::PGList *aggorder, bool agg_distinct);

    int
    check_agg_arguments(PGParseState *pstate,
					duckdb_libpgquery::PGList *directargs,
					duckdb_libpgquery::PGList *args,
					duckdb_libpgquery::PGExpr *filter);
};

}