#pragma once

#ifdef __clang__
#pragma clang diagnostic ignored "-Wold-style-cast"
#pragma clang diagnostic ignored "-Wzero-as-null-pointer-constant"
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
//#include <c.h>
//#include <funcapi.h>
#include <Common/Exception.h>

#include<string.h>

typedef enum 
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

struct PGParseState
{
	struct ParseState *parentParseState;	/* stack link */
	const char *p_sourcetext;	/* source text, or NULL if not available */
	duckdb_libpgquery::PGList	   *p_rtable;		/* range table so far */
	duckdb_libpgquery::PGList	   *p_joinexprs;	/* JoinExprs for RTE_JOIN p_rtable entries */
	duckdb_libpgquery::PGList	   *p_joinlist;		/* join items so far (will become FromExpr
								 * node's fromlist) */
	duckdb_libpgquery::PGList	   *p_namespace;	/* currently-referenceable RTEs (List of
								 * ParseNamespaceItem) */
	bool		p_lateral_active;	/* p_lateral_only items visible? */
	duckdb_libpgquery::PGList	   *p_ctenamespace; /* current namespace for common table exprs */
	duckdb_libpgquery::PGList	   *p_future_ctes;	/* common table exprs not yet in namespace */
	duckdb_libpgquery::PGCommonTableExpr *p_parent_cte;	/* this query's containing CTE */
	Relation	p_target_relation;	/* INSERT/UPDATE/DELETE target rel */
	duckdb_libpgquery::PGRangeTblEntry *p_target_rangetblentry;	/* target rel's RTE */
	bool		p_is_insert;	/* process assignment like INSERT not UPDATE */
	duckdb_libpgquery::PGList	   *p_windowdefs;	/* raw representations of window clauses */
	PGParseExprKind p_expr_kind;	/* what kind of expression we're parsing */
	int			p_next_resno;	/* next targetlist resno to assign */
	duckdb_libpgquery::PGList	   *p_multiassign_exprs;	/* junk tlist entries for multiassign */
	duckdb_libpgquery::PGList	   *p_locking_clause;	/* raw FOR UPDATE/FOR SHARE info */
	bool		p_locked_from_parent;	/* parent has marked this subquery
										 * with FOR UPDATE/FOR SHARE */
	bool		p_resolve_unknowns; /* resolve unknown-type SELECT outputs as
									 * type text */

	//QueryEnvironment *p_queryEnv;	/* curr env, incl refs to enclosing env */

	/* Flags telling about things found in the query: */
	bool		p_hasAggs;
	bool		p_hasWindowFuncs;
	bool		p_hasTargetSRFs;
	bool		p_hasSubLinks;
	bool		p_hasModifyingCTE;
	duckdb_libpgquery::PGNode	   *p_last_srf;		/* most recent set-returning func/op found */
	bool        p_is_on_conflict_update;
	bool        p_canOptSelectLockingClause; /* Whether can do some optimization on select with locking clause */
	duckdb_libpgquery::PGLockingClause *p_lockclause_from_parent;

	struct HTAB *p_namecache;  /* parse state object name cache */
	bool        p_hasTblValueExpr;
	bool        p_hasDynamicFunction; /* function w/unstable return type */
	bool		p_hasFuncsWithExecRestrictions; /* function with EXECUTE ON MASTER / ALL SEGMENTS */

	duckdb_libpgquery::PGList	   *p_grp_tles;

	/*
	 * Optional hook functions for parser callbacks.  These are null unless
	 * set up by the caller of make_parsestate.
	 */
	// PreParseColumnRefHook p_pre_columnref_hook;
	// PostParseColumnRefHook p_post_columnref_hook;
	// ParseParamRefHook p_paramref_hook;
	// CoerceParamHook p_coerce_param_hook;
	void	   *p_ref_hook_state;	/* common passthrough link for above */
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

typedef enum
{
	INH_NO,						/* Do NOT scan child tables */
	INH_YES,					/* DO scan child tables */
	INH_DEFAULT					/* Use current SQL_inheritance option */
} InhOption;

typedef enum
{
	TYPEFUNC_SCALAR,			/* scalar result type */
	TYPEFUNC_COMPOSITE,			/* determinable rowtype result */
	TYPEFUNC_RECORD,			/* indeterminate rowtype result */
	TYPEFUNC_OTHER				/* bogus type, eg pseudotype */
} TypeFuncClass;

/* Result codes for func_get_detail */
typedef enum 
{
	FUNCDETAIL_NOTFOUND,		/* no matching function */
	FUNCDETAIL_MULTIPLE,		/* too many matching functions */
	FUNCDETAIL_NORMAL,			/* found a matching regular function */
	FUNCDETAIL_PROCEDURE,		/* found a matching procedure */
	FUNCDETAIL_AGGREGATE,		/* found a matching aggregate function */
	FUNCDETAIL_WINDOWFUNC,		/* found a matching window function */
	FUNCDETAIL_COERCION			/* it's a type coercion request */
} FuncDetailCode;

typedef struct _FuncCandidateList
{
	struct _FuncCandidateList *next;
	int			pathpos;		/* for internal use of namespace lookup */
	Oid			oid;			/* the function or operator's OID */
	int			nargs;			/* number of arg types returned */
	int			nvargs;			/* number of args to become variadic array */
	int			ndargs;			/* number of defaulted args */
	int		   *argnumbers;		/* args' positional indexes, if named call */
	Oid			args[1];		/* arg types --- VARIABLE LENGTH ARRAY */
}	*FuncCandidateList;	/* VARIABLE LENGTH STRUCT */

/*
 * Collation strength (the SQL standard calls this "derivation").  Order is
 * chosen to allow comparisons to work usefully.  Note: the standard doesn't
 * seem to distinguish between NONE and CONFLICT.
 */
typedef enum
{
	COLLATE_NONE,				/* expression is of a noncollatable datatype */
	COLLATE_IMPLICIT,			/* collation was derived implicitly */
	COLLATE_CONFLICT,			/* we had a conflict of implicit collations */
	COLLATE_EXPLICIT			/* collation was derived explicitly */
} CollateStrength;

typedef signed int int32;

/*
 * Arrays are varlena objects, so must meet the varlena convention that
 * the first int32 of the object contains the total object size in bytes.
 * Be sure to use VARSIZE() and SET_VARSIZE() to access it, though!
 *
 * CAUTION: if you change the header for ordinary arrays you will also
 * need to change the headers for oidvector and int2vector!
 */
typedef struct ArrayType
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	int			ndim;			/* # of dimensions */
	int32		dataoffset;		/* offset to data, or 0 if no bitmap */
	Oid			elemtype;		/* element type OID */
} ArrayType;

typedef struct
{
	int			distance;		/* Weighted distance (lowest so far) */
	duckdb_libpgquery::PGRangeTblEntry *rfirst;		/* RTE of first */
	duckdb_libpgquery::PGAttrNumber	first;			/* Closest attribute so far */
	duckdb_libpgquery::PGRangeTblEntry *rsecond;		/* RTE of second */
	duckdb_libpgquery::PGAttrNumber	second;			/* Second closest attribute so far */
} FuzzyAttrMatchState;

bool		SQL_inheritance = true;

typedef signed short int16;
typedef long int int64;
typedef int64 Datum;
typedef char TYPCATEGORY;

static inline int32 DatumGetInt32(Datum d) { return (int32) d; } 
static inline Datum Int32GetDatum(int32 i32) { return (Datum) i32; } 
static inline Datum Int64GetDatum(int64 i64) { return (Datum) i64; } 

#define PointerGetDatum(X) ((Datum) (X))
static inline Datum CStringGetDatum(const char *p) { return PointerGetDatum(p); }
static inline Datum ObjectIdGetDatum(Oid oid) { return (Datum) oid; } 

#define FLOAT8PASSBYVAL true

#define MAX_FUZZY_DISTANCE				3
#define MaxTupleAttributeNumber 1664	/* 8 * 208 */

#define ERROR		20

#define AGGKIND_NORMAL			'n'
#define AGGKIND_ORDERED_SET		'o'
#define AGGKIND_HYPOTHETICAL	'h'

//#define InvalidOid		((Oid) 0)
#define UNKNOWNOID		705
#define TEXTOID			25
#define OIDOID			26
#define INT8OID			20
#define INT2VECTOROID	22
#define INT4OID			23
#define BOOLOID         16
#define OIDVECTOROID	30
#define INT2ARRAYOID		1005
#define OIDARRAYOID			1028
#define VOIDOID			2278
#define RECORDOID		2249
#define NUMERICOID		1700
#define INTERVALOID		1186
#define CSTRINGOID		2275
#define RECORDARRAYOID	2287
#define BITOID	 1560
#define ANYOID			2276
#define ANYARRAYOID		2277
#define ANYELEMENTOID	2283
#define ANYNONARRAYOID	2776
#define ANYENUMOID		3500
#define ANYRANGEOID		3831
#define ANYTABLEOID     7053

/* Is a type OID a polymorphic pseudotype?	(Beware of multiple evaluation) */
#define IsPolymorphicType(typid)  \
	((typid) == ANYELEMENTOID || \
	 (typid) == ANYARRAYOID || \
	 (typid) == ANYNONARRAYOID || \
	 (typid) == ANYENUMOID || \
	 (typid) == ANYRANGEOID)

#define OidIsValid(objectId)  ((bool) ((objectId) != InvalidOid))

#define rt_fetch(rangetable_index, rangetable) \
	((duckdb_libpgquery::PGRangeTblEntry *) list_nth(rangetable, (rangetable_index)-1))

/*
 * macros
 */
#define  TYPTYPE_BASE		'b' /* base type (ordinary scalar type) */
#define  TYPTYPE_COMPOSITE	'c' /* composite (e.g., table's rowtype) */
#define  TYPTYPE_DOMAIN		'd' /* domain over another type */
#define  TYPTYPE_ENUM		'e' /* enumerated type */
#define  TYPTYPE_PSEUDO		'p' /* pseudo-type */
#define  TYPTYPE_RANGE		'r' /* range type */

#define  TYPCATEGORY_INVALID	'\0'	/* not an allowed category */
#define  TYPCATEGORY_ARRAY		'A'
#define  TYPCATEGORY_BOOLEAN	'B'
#define  TYPCATEGORY_COMPOSITE	'C'
#define  TYPCATEGORY_DATETIME	'D'
#define  TYPCATEGORY_ENUM		'E'
#define  TYPCATEGORY_GEOMETRIC	'G'
#define  TYPCATEGORY_NETWORK	'I'		/* think INET */
#define  TYPCATEGORY_NUMERIC	'N'
#define  TYPCATEGORY_PSEUDOTYPE 'P'
#define  TYPCATEGORY_RANGE		'R'
#define  TYPCATEGORY_STRING		'S'
#define  TYPCATEGORY_TIMESPAN	'T'
#define  TYPCATEGORY_USER		'U'
#define  TYPCATEGORY_BITSTRING	'V'		/* er ... "varbit"? */
#define  TYPCATEGORY_UNKNOWN	'X'

/* Get a bit mask of the bits set in non-long aligned addresses */
#define LONG_ALIGN_MASK (sizeof(long) - 1)
#define MEMSET_LOOP_LIMIT 1024
/*
 * MemSet
 *	Exactly the same as standard library function memset(), but considerably
 *	faster for zeroing small word-aligned structures (such as parsetree nodes).
 *	This has to be a macro because the main point is to avoid function-call
 *	overhead.   However, we have also found that the loop is faster than
 *	native libc memset() on some platforms, even those with assembler
 *	memset() functions.  More research needs to be done, perhaps with
 *	MEMSET_LOOP_LIMIT tests in configure.
 */
#define MemSet(start, val, len) \
	do \
	{ \
		/* must be void* because we don't know if it is integer aligned yet */ \
		void   *_vstart = (void *) (start); \
		int		_val = (val); \
		size_t	_len = (len); \
\
		if ((((uintptr_t) _vstart) & LONG_ALIGN_MASK) == 0 && \
			(_len & LONG_ALIGN_MASK) == 0 && \
			_val == 0 && \
			_len <= MEMSET_LOOP_LIMIT && \
			/* \
			 *	If MEMSET_LOOP_LIMIT == 0, optimizer should find \
			 *	the whole "if" false at compile time. \
			 */ \
			MEMSET_LOOP_LIMIT != 0) \
		{ \
			long *_start = (long *) _vstart; \
			long *_stop = (long *) ((char *) _start + _len); \
			while (_start < _stop) \
				*_start++ = 0; \
		} \
		else \
			memset(_vstart, _val, _len); \
	} while (0)

#define TypeSupportsDescribe(typid)  \
	((typid) == RECORDOID)

#define Min(x, y)		((x) < (y) ? (x) : (y))

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
