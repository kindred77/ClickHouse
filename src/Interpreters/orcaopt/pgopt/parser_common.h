#include <nodes/parsenodes.hpp>
#include <nodes/makefuncs.hpp>
#include <nodes/nodefuncs.hpp>
#include <nodes/nodes.hpp>
#include <nodes/pg_list.hpp>
#include <nodes/bitmapset.hpp>
#include <pg_functions.hpp>
#include <access/attnum.hpp>

typedef enum PGParseExprKind
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
} PGParseExprKind;

typedef struct PGParseState
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
} PGParseState;

typedef struct PGParseNamespaceItem
{
	duckdb_libpgquery::PGRangeTblEntry *p_rte;		/* The relation's rangetable entry */
	bool		p_rel_visible;	/* Relation name is visible? */
	bool		p_cols_visible; /* Column names visible as unqualified refs? */
	bool		p_lateral_only; /* Is only visible to LATERAL expressions? */
	bool		p_lateral_ok;	/* If so, does join type allow use? */
} PGParseNamespaceItem;

typedef unsigned int Index;

typedef unsigned int Oid;

typedef enum InhOption
{
	INH_NO,						/* Do NOT scan child tables */
	INH_YES,					/* DO scan child tables */
	INH_DEFAULT					/* Use current SQL_inheritance option */
} InhOption;

bool		SQL_inheritance = true;

typedef signed int int32;

#define ERROR		20

#define rt_fetch(rangetable_index, rangetable) \
	((duckdb_libpgquery::PGRangeTblEntry *) list_nth(rangetable, (rangetable_index)-1))