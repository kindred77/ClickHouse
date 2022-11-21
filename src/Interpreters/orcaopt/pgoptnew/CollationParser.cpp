#include <Interpreters/orcaopt/pgoptnew/CollationParser.h>

using namespace duckdb_libpgquery;

namespace DB
{

void
CollationParser::assign_expr_collations(PGParseState *pstate, PGNode *expr)
{
    assign_collations_context context;

	/* initialize context for tree walk */
	context.pstate = pstate;
	context.collation = InvalidOid;
	context.strength = COLLATE_NONE;
	context.location = -1;

	/* and away we go */
	(void) assign_collations_walker(expr, &context);
};

}