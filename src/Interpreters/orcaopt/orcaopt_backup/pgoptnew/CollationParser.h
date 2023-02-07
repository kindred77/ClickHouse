#pragma once

#include <Interpreters/orcaopt/pgoptnew/parser_common.h>
#include <Interpreters/orcaopt/pgoptnew/walkers.h>

namespace DB
{

extern void
pg_assign_ordered_set_collations(duckdb_libpgquery::PGAggref *aggref,
							  assign_collations_context *loccontext);

extern void
pg_merge_collation_state(Oid collation,
					  CollateStrength strength,
					  int location,
					  Oid collation2,
					  int location2,
					  assign_collations_context *context);

extern void
pg_assign_hypothetical_collations(duckdb_libpgquery::PGAggref *aggref,
							   assign_collations_context *loccontext);

extern void
pg_assign_aggregate_collations(duckdb_libpgquery::PGAggref *aggref,
							assign_collations_context *loccontext);

extern bool
pg_assign_collations_walker(duckdb_libpgquery::PGNode *node, assign_collations_context *context);

extern void
pg_assign_expr_collations(PGParseState *pstate, duckdb_libpgquery::PGNode *expr);

extern void
pg_assign_list_collations(PGParseState *pstate, duckdb_libpgquery::PGList *exprs);

extern void
pg_assign_query_collations(PGParseState *pstate, duckdb_libpgquery::PGQuery *query);

extern bool
pg_assign_query_collations_walker(duckdb_libpgquery::PGNode *node, PGParseState *pstate);

class CollationParser
{
private:
    

public:
	explicit CollationParser();

};

}