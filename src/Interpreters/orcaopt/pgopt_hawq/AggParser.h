#pragma once

#include <Interpreters/orcaopt/pgopt_hawq/parser_common.h>

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

extern int
cmp_list_len_asc(const void *a, const void *b);

class AggParser
{
private:

public:
	explicit AggParser();

    void transformWindowSpec(PGParseState * pstate, WindowSpec * spec);

    void transformWindowSpecExprs(PGParseState * pstate);
};

}