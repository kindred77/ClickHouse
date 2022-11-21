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

class AggParser
{
private:
    ClauseParser clause_parser;
	NodeParser node_parser;
	ExprParser expr_parser;
public:
	explicit AggParser();

};

}