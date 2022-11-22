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

void
CollationParser::assign_query_collations(PGParseState *pstate, PGQuery *query)
{
	/*
	 * We just use query_tree_walker() to visit all the contained expressions.
	 * We can skip the rangetable and CTE subqueries, though, since RTEs and
	 * subqueries had better have been processed already (else Vars referring
	 * to them would not get created with the right collation).
	 */
	(void) query_tree_walker(query,
							 assign_query_collations_walker,
							 (void *) pstate,
							 QTW_IGNORE_RANGE_TABLE |
							 QTW_IGNORE_CTE_SUBQUERIES);
};

}