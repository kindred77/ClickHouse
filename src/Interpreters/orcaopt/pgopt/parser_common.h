#pragma once

#ifdef __clang__
#pragma clang diagnostic ignored "-Wno-old-style-cast"
#else
#pragma GCC diagnostic ignored "-Wold-style-cast"
#endif

#include <nodes/parsenodes.hpp>
#include <nodes/makefuncs.hpp>
#include <nodes/nodeFuncs.hpp>
#include <nodes/nodes.hpp>
#include <nodes/pg_list.hpp>
#include <nodes/bitmapset.hpp>
#include <pg_functions.hpp>
#include <access/attnum.hpp>

#include <Common/Exception.h>

#include<string.h>

enum class PGParseExprKind
{
	EXPR_KIND_NONE = 0,			/* "not in an expression" */
	EXPR_KIND_OTHER,			/* reserved for extensions */
	EXPR_KIND_JOIN_ON,			/* JOIN ON */
	EXPR_KIND_JOIN_USING,		/* JOIN USING */
	EXPR_KIND_FROM_SUBSELECT,	/* sub-SELECT in FROM clause */
	EXPR_KIND_FROM_FUNCTION,	/* function in FROM clause */
	EXPR_KIND_WHERE,			/* WHERE */
	EXPR_KIND_HAVING,			/* HAVING */
	EXPR_KIND_FILTER,			/* FILTER */
	EXPR_KIND_WINDOW_PARTITION, /* window definition PARTITION BY */
	EXPR_KIND_WINDOW_ORDER,		/* window definition ORDER BY */
	EXPR_KIND_WINDOW_FRAME_RANGE,	/* window frame clause with RANGE */
	EXPR_KIND_WINDOW_FRAME_ROWS,	/* window frame clause with ROWS */
	EXPR_KIND_SELECT_TARGET,	/* SELECT target list item */
	EXPR_KIND_INSERT_TARGET,	/* INSERT target list item */
	EXPR_KIND_UPDATE_SOURCE,	/* UPDATE assignment source item */
	EXPR_KIND_UPDATE_TARGET,	/* UPDATE assignment target item */
	EXPR_KIND_GROUP_BY,			/* GROUP BY */
	EXPR_KIND_ORDER_BY,			/* ORDER BY */
	EXPR_KIND_DISTINCT_ON,		/* DISTINCT ON */
	EXPR_KIND_LIMIT,			/* LIMIT */
	EXPR_KIND_OFFSET,			/* OFFSET */
	EXPR_KIND_RETURNING,		/* RETURNING */
	EXPR_KIND_VALUES,			/* VALUES */
	EXPR_KIND_CHECK_CONSTRAINT, /* CHECK constraint for a table */
	EXPR_KIND_DOMAIN_CHECK,		/* CHECK constraint for a domain */
	EXPR_KIND_COLUMN_DEFAULT,	/* default value for a table column */
	EXPR_KIND_FUNCTION_DEFAULT, /* default parameter value for function */
	EXPR_KIND_INDEX_EXPRESSION, /* index expression */
	EXPR_KIND_INDEX_PREDICATE,	/* index predicate */
	EXPR_KIND_ALTER_COL_TRANSFORM,	/* transform expr in ALTER COLUMN TYPE */
	EXPR_KIND_EXECUTE_PARAMETER,	/* parameter value in EXECUTE */
	EXPR_KIND_TRIGGER_WHEN,		/* WHEN condition in CREATE TRIGGER */
	EXPR_KIND_PARTITION_EXPRESSION, /* PARTITION BY expression */

	/* GPDB additions */
	EXPR_KIND_SCATTER_BY		/* SCATTER BY expression */
};

struct PGParseState
{
	struct PGParseState *parentParseState;		/* stack link */
	const char *p_sourcetext;	/* source text, or NULL if not available */
	duckdb_libpgquery::PGList	   *p_rtable;		/* range table so far */
	duckdb_libpgquery::PGList	   *p_joinexprs;	/* JoinExprs for RTE_JOIN p_rtable entries */
	duckdb_libpgquery::PGList	   *p_joinlist;		/* join items so far (will become FromExpr
								 * node's fromlist) */
	duckdb_libpgquery::PGList	   *p_namespace;	/* currently-referenceable RTEs (List of
								 * ParseNamespaceItem) */
	bool		p_lateral_active;		/* p_lateral_only items visible? */
	duckdb_libpgquery::PGList	   *p_ctenamespace; /* current namespace for common table exprs */
	duckdb_libpgquery::PGList	   *p_future_ctes;	/* common table exprs not yet in namespace */
	duckdb_libpgquery::PGCommonTableExpr *p_parent_cte;		/* this query's containing CTE */
	duckdb_libpgquery::PGList	   *p_windowdefs;	/* raw representations of window clauses */
	PGParseExprKind p_expr_kind;	/* what kind of expression we're parsing */
	int			p_next_resno;	/* next targetlist resno to assign */
	duckdb_libpgquery::PGList	   *p_locking_clause;		/* raw FOR UPDATE/FOR SHARE info */
	duckdb_libpgquery::PGNode	   *p_value_substitute;		/* what to replace VALUE with, if any */
	bool		p_hasAggs;
	bool		p_hasWindowFuncs;
	bool		p_hasSubLinks;
	bool		p_hasModifyingCTE;
	bool		p_is_insert;
	bool		p_is_update;
	duckdb_libpgquery::PGLockingClause *p_lockclause_from_parent;
	Relation	p_target_relation;
	duckdb_libpgquery::PGRangeTblEntry *p_target_rangetblentry;

	struct HTAB *p_namecache;  /* parse state object name cache */
	bool        p_hasTblValueExpr;
	bool        p_hasDynamicFunction; /* function w/unstable return type */
	bool		p_hasFuncsWithExecRestrictions; /* function with EXECUTE ON MASTER / ALL SEGMENTS */

	duckdb_libpgquery::PGList	   *p_grp_tles;

	/*
	 * Optional hook functions for parser callbacks.  These are null unless
	 * set up by the caller of make_parsestate.
	 */
	//PreParseColumnRefHook p_pre_columnref_hook;
	//PostParseColumnRefHook p_post_columnref_hook;
	//ParseParamRefHook p_paramref_hook;
	//CoerceParamHook p_coerce_param_hook;
	void	   *p_ref_hook_state;		/* common passthrough link for above */
};

struct PGParseNamespaceItem
{
	duckdb_libpgquery::PGRangeTblEntry *p_rte;		/* The relation's rangetable entry */
	bool		p_rel_visible;	/* Relation name is visible? */
	bool		p_cols_visible; /* Column names visible as unqualified refs? */
	bool		p_lateral_only; /* Is only visible to LATERAL expressions? */
	bool		p_lateral_ok;	/* If so, does join type allow use? */
};

typedef PGIndex Index;

typedef PGOid Oid;

struct PGColumn
{
	Oid oid;
	String name;
	NameAndTypePair name_and_type;
	Oid type_oid;
	Oid type_modifier_oid;
	Oid collation_oid;
};

struct PGRelation
{
	Oid oid;
	std::vector<PGColumn> columns;
};

enum class InhOption
{
	INH_NO,						/* Do NOT scan child tables */
	INH_YES,					/* DO scan child tables */
	INH_DEFAULT					/* Use current SQL_inheritance option */
};

bool		SQL_inheritance = true;

typedef signed int int32;

#define ERROR		20

#define AGGKIND_NORMAL			'n'
#define AGGKIND_ORDERED_SET		'o'
#define AGGKIND_HYPOTHETICAL	'h'

//#define InvalidOid		((Oid) 0)
#define UNKNOWNOID		705
#define TEXTOID			25
#define INT8OID			20
#define INT4OID			23
#define BOOLOID         16

#define rt_fetch(rangetable_index, rangetable) \
	((duckdb_libpgquery::PGRangeTblEntry *) list_nth(rangetable, (rangetable_index)-1))


PGParseState *
make_parsestate(PGParseState *parentParseState)
{
	auto pstate = new PGParseState();

	pstate->parentParseState = parentParseState;

	/* Fill in fields that don't start at null/false/zero */
	pstate->p_next_resno = 1;

	if (parentParseState)
	{
		pstate->p_sourcetext = parentParseState->p_sourcetext;
		/* all hooks are copied from parent */
		//pstate->p_pre_columnref_hook = parentParseState->p_pre_columnref_hook;
		//pstate->p_post_columnref_hook = parentParseState->p_post_columnref_hook;
		//pstate->p_paramref_hook = parentParseState->p_paramref_hook;
		//pstate->p_coerce_param_hook = parentParseState->p_coerce_param_hook;
		pstate->p_ref_hook_state = parentParseState->p_ref_hook_state;
	}

	return pstate;
};

void
free_parsestate(PGParseState *pstate)
{
	delete pstate;
	pstate = NULL;
};

duckdb_libpgquery::PGTargetEntry *
get_sortgroupref_tle(Index sortref, duckdb_libpgquery::PGList *targetList)
{
	duckdb_libpgquery::PGListCell   *l;

	foreach(l, targetList)
	{
		duckdb_libpgquery::PGTargetEntry *tle = (duckdb_libpgquery::PGTargetEntry *) lfirst(l);

		if (tle->ressortgroupref == sortref)
			return tle;
	}

	/*
	 * XXX: we probably should catch this earlier, but we have a
	 * few queries in the regression suite that hit this.
	 */
	duckdb_libpgquery::ereport(ERROR,
			(duckdb_libpgquery::errcode(duckdb_libpgquery::PG_ERRCODE_SYNTAX_ERROR),
			 duckdb_libpgquery::errmsg("ORDER/GROUP BY expression not found in targetlist")));
	return NULL;				/* keep compiler quiet */
};

duckdb_libpgquery::PGTargetEntry *
get_sortgroupclause_tle(duckdb_libpgquery::PGSortGroupClause *sgClause,
						duckdb_libpgquery::PGList *targetList)
{
	return get_sortgroupref_tle(sgClause->tleSortGroupRef, targetList);
};

duckdb_libpgquery::PGNode *
get_sortgroupclause_expr(duckdb_libpgquery::PGSortGroupClause *sgClause,
	duckdb_libpgquery::PGList *targetList)
{
	duckdb_libpgquery::PGTargetEntry *tle = get_sortgroupclause_tle(sgClause, targetList);

	return (duckdb_libpgquery::PGNode *) tle->expr;
};

bool
contain_vars_of_level_walker(duckdb_libpgquery::PGNode *node, int *sublevels_up);

bool
contain_vars_of_level(duckdb_libpgquery::PGNode *node, int levelsup)
{
	int			sublevels_up = levelsup;

	return query_or_expression_tree_walker(node,
										   contain_vars_of_level_walker,
										   (void *) &sublevels_up,
										   0);
};

bool
contain_vars_of_level_walker(duckdb_libpgquery::PGNode *node, int *sublevels_up)
{
	using duckdb_libpgquery::PGVar;
	using duckdb_libpgquery::PGCurrentOfExpr;
	//using duckdb_libpgquery::PGPlaceHolderVar;
	using duckdb_libpgquery::PGQuery;
	if (node == NULL)
		return false;
	if (IsA(node, PGVar))
	{
		if (((PGVar *) node)->varlevelsup == *sublevels_up)
			return true;		/* abort tree traversal and return true */
		return false;
	}
	if (IsA(node, PGCurrentOfExpr))
	{
		if (*sublevels_up == 0)
			return true;
		return false;
	}
	// if (IsA(node, PGPlaceHolderVar))
	// {
	// 	if (((PGPlaceHolderVar *) node)->phlevelsup == *sublevels_up)
	// 		return true;		/* abort the tree traversal and return true */
	// 	/* else fall through to check the contained expr */
	// }
	if (IsA(node, PGQuery))
	{
		/* Recurse into subselects */
		bool		result;

		(*sublevels_up)++;
		result = query_tree_walker((PGQuery *) node,
								   contain_vars_of_level_walker,
								   (void *) sublevels_up,
								   0);
		(*sublevels_up)--;
		return result;
	}
	return expression_tree_walker(node,
								  contain_vars_of_level_walker,
								  (void *) sublevels_up);
};
