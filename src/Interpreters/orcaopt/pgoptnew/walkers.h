#pragma once

#include <Interpreters/orcaopt/pgoptnew/parser_common.h>

typedef struct
{
	int			sublevels_up;
} contain_aggs_of_level_context;

typedef struct
{
	PGParseState *pstate;			/* parse state (for error reporting) */
	Oid			collation;		/* OID of current collation, if any */
	CollateStrength strength;	/* strength of current collation choice */
	int			location;		/* location of expr that set collation */
	/* Remaining fields are only valid when strength == COLLATE_CONFLICT */
	Oid			collation2;		/* OID of conflicting collation */
	int			location2;		/* location of expr that set collation2 */
} assign_collations_context;

typedef bool (*walker_func) (duckdb_libpgquery::PGNode *node, assign_collations_context *context);

extern bool
pg_expression_tree_walker(duckdb_libpgquery::PGNode *node,
					   walker_func walker,
					   void *context);

extern bool
pg_range_table_walker(duckdb_libpgquery::PGList *rtable,
				   walker_func walker,
				   void *context,
				   int flags);

extern bool
pg_query_tree_walker(duckdb_libpgquery::PGQuery *query,
				  walker_func walker,
				  void *context,
				  int flags);

extern bool
pg_contain_aggs_of_level_walker(duckdb_libpgquery::PGNode *node,
							 contain_aggs_of_level_context *context);

extern bool
pg_query_or_expression_tree_walker(duckdb_libpgquery::PGNode *node,
								walker_func walker,
								void *context,
								int flags);

extern bool
pg_contain_aggs_of_level(duckdb_libpgquery::PGNode *node, int levelsup);

extern bool
pg_contain_windowfuncs_walker(duckdb_libpgquery::PGNode *node, void *context);

extern bool
pg_contain_windowfuncs(duckdb_libpgquery::PGNode *node);

extern bool
pg_query_or_expression_tree_walker(duckdb_libpgquery::PGNode *node,
								walker_func walker,
								void *context,
								int flags);
