#pragma once

#include "common/common_macro.hpp"
#include "common/common_def.hpp"

namespace duckdb_libpgquery {

extern duckdb_libpgquery::PGTargetEntry *
tlist_member(duckdb_libpgquery::PGNode *node, duckdb_libpgquery::PGList *targetlist);

struct contain_aggs_of_level_context
{
	int			sublevels_up;
};

struct assign_collations_context
{
	PGParseState *pstate;			/* parse state (for error reporting) */
	PGOid			collation;		/* OID of current collation, if any */
	CollateStrength strength;	/* strength of current collation choice */
	int			location;		/* location of expr that set collation */
	/* Remaining fields are only valid when strength == COLLATE_CONFLICT */
	PGOid			collation2;		/* OID of conflicting collation */
	int			location2;		/* location of expr that set collation2 */
};

struct locate_agg_of_level_context
{
	int			agg_location;
	int			sublevels_up;
};

struct locate_var_of_level_context
{
	int			var_location;
	int			sublevels_up;
};

typedef struct
{
	int			win_location;
} locate_windowfunc_context;

struct flatten_join_alias_vars_context
{
    PGPlannerInfo * root;
    int sublevels_up;
    bool possible_sublink; /* could aliases include a SubLink? */
    bool inserted_sublink; /* have we inserted a SubLink? */
    duckdb_libpgquery::PGNode ** root_parse_rtable_arrray; /* array form of root->parse->rtable */
};

struct IncrementVarSublevelsUp_context
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
};

typedef bool (*walker_func) (duckdb_libpgquery::PGNode *node, void *context);

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

extern duckdb_libpgquery::PGNode * pg_query_or_expression_tree_mutator(
	duckdb_libpgquery::PGNode * node, 
	duckdb_libpgquery::PGNode * (*mutator)(duckdb_libpgquery::PGNode *node, void *context),
	void * context, int flags);

extern bool
checkExprHasSubLink(duckdb_libpgquery::PGNode *node);

extern bool
checkExprHasSubLink_walker(duckdb_libpgquery::PGNode *node, void *context);

extern duckdb_libpgquery::PGNode *
pg_flatten_join_alias_vars_mutator(duckdb_libpgquery::PGNode *node,
			void *context);

extern duckdb_libpgquery::PGQuery *
pg_flatten_join_alias_var_optimizer(duckdb_libpgquery::PGQuery *query, int queryLevel);

extern duckdb_libpgquery::PGNode **
rtable_to_array(duckdb_libpgquery::PGList *rtable);

extern duckdb_libpgquery::PGNode *
pg_flatten_join_alias_vars(PGPlannerInfo *root, duckdb_libpgquery::PGNode *node);


typedef bool (*Cdb_walk_vars_callback_Aggref)(duckdb_libpgquery::PGAggref * aggref, void * context, int sublevelsup);
typedef bool (*Cdb_walk_vars_callback_Var)(duckdb_libpgquery::PGVar * var, void * context, int sublevelsup);
typedef bool (*Cdb_walk_vars_callback_CurrentOf)(duckdb_libpgquery::PGCurrentOfExpr * expr, void * context, int sublevelsup);

struct Cdb_walk_vars_context
{
    Cdb_walk_vars_callback_Var callback_var;
    Cdb_walk_vars_callback_Aggref callback_aggref;
    Cdb_walk_vars_callback_CurrentOf callback_currentof;
    void * context;
    int sublevelsup;
};

extern bool cdb_walk_vars_walker(duckdb_libpgquery::PGNode * node, void * wvwcontext);

extern bool cdb_walk_vars(
    duckdb_libpgquery::PGNode * node,
    Cdb_walk_vars_callback_Var callback_var,
    Cdb_walk_vars_callback_Aggref callback_aggref,
    Cdb_walk_vars_callback_CurrentOf callback_currentof,
    void * context,
    int levelsup);

extern bool
contain_vars_of_level_walker(duckdb_libpgquery::PGNode *node, int *sublevels_up);

extern bool
contain_vars_of_level(duckdb_libpgquery::PGNode *node, int levelsup);

struct winref_check_ctx
{
    PGParseState * pstate;
    PGIndex winref;
    bool has_order;
    bool has_frame;
};

/*
 * winref_checkspec_walker
 */
extern bool winref_checkspec_walker(duckdb_libpgquery::PGNode * node, void * ctx);

/*
 * winref_checkspec
 *
 * See if any WindowFuncss using this spec are DISTINCT qualified.
 *
 * In addition, we're going to check winrequireorder / winallowframe.
 * You might want to do it in ParseFuncOrColumn,
 * but we need to do this here after all the transformations
 * (especially parent inheritance) was done.
 */
extern bool winref_checkspec(PGParseState * pstate, duckdb_libpgquery::PGList * targetlist, PGIndex winref, bool has_order, bool has_frame);

struct check_table_func_context
{
	duckdb_libpgquery::PGNode *parent;
};

extern void 
parseCheckTableFunctions(PGParseState *pstate, duckdb_libpgquery::PGQuery *qry);

extern bool 
checkTableFunctions_walker(duckdb_libpgquery::PGNode *node, check_table_func_context *context);

struct grouping_rewrite_ctx
{
	duckdb_libpgquery::PGList *grp_tles;
	PGParseState *pstate;
};

extern bool
pg_grouping_rewrite_walker(duckdb_libpgquery::PGNode *node, void *context);

struct check_ungrouped_columns_context
{
	PGParseState *pstate;
	duckdb_libpgquery::PGQuery	   *qry;
	duckdb_libpgquery::PGList	   *groupClauses;
	bool		have_non_var_grouping;
	duckdb_libpgquery::PGList	  **func_grouped_rels;
	int			sublevels_up;
	bool		in_agg_direct_args;
};

extern bool
pg_check_ungrouped_columns_walker(duckdb_libpgquery::PGNode *node,
							   check_ungrouped_columns_context *context);

struct checkHasGroupExtFuncs_context
{
	int sublevels_up;
};

extern bool
pg_checkExprHasGroupExtFuncs_walker(duckdb_libpgquery::PGNode *node, checkHasGroupExtFuncs_context *context);


extern void pg_get_sortgroupclauses_tles_recurse(duckdb_libpgquery::PGList * clauses, duckdb_libpgquery::PGList * targetList,
	duckdb_libpgquery::PGList ** tles, duckdb_libpgquery::PGList ** sortops,
	duckdb_libpgquery::PGList ** eqops);

extern void
pg_get_sortgroupclauses_tles(duckdb_libpgquery::PGList *clauses, duckdb_libpgquery::PGList *targetList,
						  duckdb_libpgquery::PGList **tles, duckdb_libpgquery::PGList **sortops, duckdb_libpgquery::PGList **eqops);

struct maxSortGroupRef_context
{
	PGIndex maxsgr;
	bool include_orderedagg;
};

extern bool maxSortGroupRef_walker(duckdb_libpgquery::PGNode *node, maxSortGroupRef_context *cxt);

extern PGIndex maxSortGroupRef(duckdb_libpgquery::PGList *targetlist, bool include_orderedagg);

extern char * generate_positional_name(PGAttrNumber attrno);

extern duckdb_libpgquery::PGList*
generate_alternate_vars(duckdb_libpgquery::PGVar *invar, grouped_window_ctx *ctx);

extern duckdb_libpgquery::PGVar * var_for_gw_expr(grouped_window_ctx * ctx, duckdb_libpgquery::PGNode * expr, bool force);

extern duckdb_libpgquery::PGNode* grouped_window_mutator(duckdb_libpgquery::PGNode *node, void *context);

extern void IncrementVarSublevelsUpInTransformGroupedWindows(duckdb_libpgquery::PGNode * node, int delta_sublevels_up, int min_sublevels_up);

/**
 * These are helpers to find node in queries
 */
struct pg_find_nodes_context
{
	std::vector<duckdb_libpgquery::PGNodeTag> nodeTags;
	int foundNode;
};

extern bool
pg_find_nodes_walker(duckdb_libpgquery::PGNode *node, pg_find_nodes_context *context);

}
