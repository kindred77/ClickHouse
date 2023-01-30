#pragma once

#include <Interpreters/orcaopt/pgopt_hawq/parser_common.h>

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

typedef struct
{
	int			agg_location;
	int			sublevels_up;
} locate_agg_of_level_context;

typedef struct
{
	int			var_location;
	int			sublevels_up;
} locate_var_of_level_context;

typedef struct
{
	int			win_location;
} locate_windowfunc_context;

typedef struct
{
	duckdb_libpgquery::PGQuery	   *query;			/* outer Query */
	int			sublevels_up;
	bool		possible_sublink;	/* could aliases include a SubLink? */
	bool		inserted_sublink;	/* have we inserted a SubLink? */
} flatten_join_alias_vars_context;

typedef struct
{
	int			delta_sublevels_up;
	int			min_sublevels_up;

	/*
	 * MPP-19436: when a query mixes window function with group by or aggregates,
	 * then a transformation will turn the original structure Q into an outer
	 * query Q' and an inner query Q''
	 * Q ->  Q'
	 *       |
	 *       |->Q''
	 * All the structures will be copied from Q to Q'' except ctelists.
	 * This causes Q'', and its inner structures, to have dangling cte references, since
	 * ctelists are kept in Q'. In such a case, we need to ignore min_sublevels_up and
	 * increment by delta_sublevels_up.
	 *
	 */
	bool		        ignore_min_sublevels_up;
} IncrementVarSublevelsUp_context;

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

bool
pg_contain_windowfuncs_walker(duckdb_libpgquery::PGNode *node, void *context);

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

extern bool
locate_agg_of_level_walker(duckdb_libpgquery::PGNode *node,
		locate_agg_of_level_context *context);

extern int
pg_locate_agg_of_level(duckdb_libpgquery::PGNode *node, int levelsup);

extern int
pg_locate_var_of_level(duckdb_libpgquery::PGNode *node, int levelsup);

extern bool
pg_locate_var_of_level_walker(duckdb_libpgquery::PGNode *node,
				locate_var_of_level_context *context);

extern bool
pg_contain_windowfuncs(duckdb_libpgquery::PGNode *node);

extern bool
pg_contain_windowfuncs_walker(duckdb_libpgquery::PGNode *node, void *context);

extern bool
pg_locate_windowfunc_walker(duckdb_libpgquery::PGNode *node, locate_windowfunc_context *context);

extern int
pg_locate_windowfunc(duckdb_libpgquery::PGNode *node);

extern bool
PGIncrementVarSublevelsUp_walker(duckdb_libpgquery::PGNode *node,
							   IncrementVarSublevelsUp_context *context);

extern void
PGIncrementVarSublevelsUp(duckdb_libpgquery::PGNode *node, int delta_sublevels_up,
						int min_sublevels_up);

extern duckdb_libpgquery::PGList *
pg_range_table_mutator(duckdb_libpgquery::PGList *rtable,
					duckdb_libpgquery::PGNode *(*mutator) (duckdb_libpgquery::PGNode *node, void *context),
					void *context,
					int flags);

extern duckdb_libpgquery::PGQuery *
pg_query_tree_mutator(duckdb_libpgquery::PGQuery *query,
				   duckdb_libpgquery::PGNode *(*mutator) (duckdb_libpgquery::PGNode *node, void *context),
				   void *context,
				   int flags);

extern duckdb_libpgquery::PGNode *
pg_expression_tree_mutator(duckdb_libpgquery::PGNode *node,
						duckdb_libpgquery::PGNode *(*mutator) (duckdb_libpgquery::PGNode *node, void *context),
						void *context);

extern duckdb_libpgquery::PGNode *
pg_flatten_join_alias_vars_mutator(duckdb_libpgquery::PGNode *node,
			flatten_join_alias_vars_context *context);

extern duckdb_libpgquery::PGNode *
pg_flatten_join_alias_vars(duckdb_libpgquery::PGQuery *query, duckdb_libpgquery::PGNode *node);
