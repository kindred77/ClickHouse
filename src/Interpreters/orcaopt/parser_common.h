#pragma once

#ifdef __clang__
#pragma clang diagnostic ignored "-Wold-style-cast"
#pragma clang diagnostic ignored "-Wzero-as-null-pointer-constant"
#pragma clang diagnostic ignored "-Wreserved-identifier"
#pragma clang diagnostic ignored "-Wcast-align"
#pragma clang diagnostic ignored "-Wcomma"
#else
#pragma GCC diagnostic ignored "-Wold-style-cast"
#endif

#include <nodes/parsenodes.hpp>
#include <nodes/makefuncs.hpp>
#include <nodes/nodeFuncs.hpp>
#include <nodes/nodes.hpp>
#include <nodes/primnodes.hpp>
#include <nodes/pg_list.hpp>
#include <nodes/bitmapset.hpp>
#include <nodes/lockoptions.hpp>
#include <nodes/value.hpp>
#include <pg_functions.hpp>
#include <access/attnum.hpp>
//#include <c.h>
//#include <funcapi.h>
#include <Common/Exception.h>

#include<string.h>

/* class RelationParser;
class CTEParser;
class ExprParser;
class ClauseParser;
class AggParser;
class TargetParser;
class NodeParser;
class CoerceParser;
class FuncParser;
class OperParser;
class TypeParser;
class SelectParser;

using RelationParserPtr = std::unique_ptr<RelationParser>;
using CTEParserPtr = std::unique_ptr<CTEParser>;
using ExprParserPtr = std::unique_ptr<ExprParser>;
using ClauseParserPtr = std::unique_ptr<ClauseParser>;
using AggParserPtr = std::unique_ptr<AggParser>;
using TargetParserPtr = std::unique_ptr<TargetParser>;
using NodeParserPtr = std::unique_ptr<NodeParser>;
using CoerceParserPtr = std::unique_ptr<CoerceParser>;
using FuncParserPtr = std::unique_ptr<FuncParser>;
using OperParserPtr = std::unique_ptr<OperParser>;
using TypeParserPtr = std::unique_ptr<TypeParser>;
using SelectParserPtr = std::unique_ptr<SelectParser>; */

/*
 * Symbolic values for prokind column
 */
#define PG_PROKIND_FUNCTION 'f'
#define PG_PROKIND_AGGREGATE 'a'
#define PG_PROKIND_WINDOW 'w'
#define PG_PROKIND_PROCEDURE 'p'

bool		SQL_inheritance = true;

typedef unsigned char uint8;
typedef signed short int16;
typedef unsigned short uint16;	/* == 16 bits */
typedef long int int64;
typedef signed int int32;
typedef unsigned int uint32;	/* == 32 bits */
typedef int32 int4;
typedef int16 int2;
typedef float float4;

typedef int64 Datum;
typedef char TYPCATEGORY;
typedef char *Pointer;

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
	EXPR_KIND_WINDOW_FRAME_GROUPS,	/* window frame clause with GROUPS */
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
	EXPR_KIND_VALUES_SINGLE,	/* single-row VALUES (in INSERT only) */
	EXPR_KIND_CHECK_CONSTRAINT, /* CHECK constraint for a table */
	EXPR_KIND_DOMAIN_CHECK,		/* CHECK constraint for a domain */
	EXPR_KIND_COLUMN_DEFAULT,	/* default value for a table column */
	EXPR_KIND_FUNCTION_DEFAULT, /* default parameter value for function */
	EXPR_KIND_INDEX_EXPRESSION, /* index expression */
	EXPR_KIND_INDEX_PREDICATE,	/* index predicate */
	EXPR_KIND_ALTER_COL_TRANSFORM,	/* transform expr in ALTER COLUMN TYPE */
	EXPR_KIND_EXECUTE_PARAMETER,	/* parameter value in EXECUTE */
	EXPR_KIND_TRIGGER_WHEN,		/* WHEN condition in CREATE TRIGGER */
	EXPR_KIND_POLICY,			/* USING or WITH CHECK expr in policy */
	EXPR_KIND_PARTITION_BOUND,	/* partition bound expression */
	EXPR_KIND_PARTITION_EXPRESSION, /* PARTITION BY expression */
	EXPR_KIND_CALL_ARGUMENT,	/* procedure argument in CALL */
	EXPR_KIND_COPY_WHERE,		/* WHERE condition in COPY FROM */
	EXPR_KIND_GENERATED_COLUMN, /* generation expression for a column */

	/* GPDB additions */
	EXPR_KIND_SCATTER_BY		/* SCATTER BY expression */
} PGParseExprKind;

struct ParseStateBreadCrumb
{
    duckdb_libpgquery::PGNode * node;
    struct ParseStateBreadCrumb * pop;
};

struct PGParseState
{
	struct PGParseState *parentParseState;	/* stack link */
	//for oldversion
	ParseStateBreadCrumb    p_breadcrumb;       /* top of err location stack */

	const char *p_sourcetext;	/* source text, or NULL if not available */
	duckdb_libpgquery::PGList	   *p_rtable;		/* range table so far */
	duckdb_libpgquery::PGList	   *p_joinexprs;	/* JoinExprs for RTE_JOIN p_rtable entries */
	duckdb_libpgquery::PGList	   *p_joinlist;		/* join items so far (will become FromExpr
								 * node's fromlist) */
	//for oldversion
	duckdb_libpgquery::PGList	   *p_relnamespace; /* current namespace for relations */
	duckdb_libpgquery::PGList	   *p_varnamespace; /* current namespace for columns */

	duckdb_libpgquery::PGList	   *p_namespace;	/* currently-referenceable RTEs (List of
								 * ParseNamespaceItem) */
	bool		p_lateral_active;	/* p_lateral_only items visible? */
	duckdb_libpgquery::PGList	   *p_ctenamespace; /* current namespace for common table exprs */
	duckdb_libpgquery::PGList	   *p_future_ctes;	/* common table exprs not yet in namespace */
	duckdb_libpgquery::PGCommonTableExpr *p_parent_cte;	/* this query's containing CTE */
	//Relation	p_target_relation;	/* INSERT/UPDATE/DELETE target rel */
	duckdb_libpgquery::PGRangeTblEntry *p_target_rangetblentry;	/* target rel's RTE */
	bool		p_is_insert;	/* process assignment like INSERT not UPDATE */
	duckdb_libpgquery::PGList	   *p_windowdefs;	/* raw representations of window clauses */
	PGParseExprKind p_expr_kind;	/* what kind of expression we're parsing */
	int			p_next_resno;	/* next targetlist resno to assign */
	duckdb_libpgquery::PGList	   *p_multiassign_exprs;	/* junk tlist entries for multiassign */
	duckdb_libpgquery::PGNode	   *p_value_substitute;		/* what to replace VALUE with, if any */
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
    duckdb_libpgquery::PGRangeTblEntry * p_rte; /* The relation's rangetable entry */
    bool p_rel_visible; /* Relation name is visible? */
    bool p_cols_visible; /* Column names visible as unqualified refs? */
    bool p_lateral_only; /* Is only visible to LATERAL expressions? */
    bool p_lateral_ok; /* If so, does join type allow use? */
};

/* Enumeration of contexts in which a self-reference is disallowed */
typedef enum
{
    RECURSION_OK,
    RECURSION_NONRECURSIVETERM, /* inside the left-hand term */
    RECURSION_SUBLINK, /* inside a sublink */
    RECURSION_OUTERJOIN, /* inside nullable side of an outer join */
    RECURSION_INTERSECT, /* underneath INTERSECT (ALL) */
    RECURSION_EXCEPT /* underneath EXCEPT (ALL) */
} RecursionContext;

/*
 * For WITH RECURSIVE, we have to find an ordering of the clause members
 * with no forward references, and determine which members are recursive
 * (i.e., self-referential).  It is convenient to do this with an array
 * of CteItems instead of a list of CommonTableExprs.
 */
struct PGCteItem
{
    duckdb_libpgquery::PGCommonTableExpr * cte; /* One CTE to examine */
    int id; /* Its ID number for dependencies */
    duckdb_libpgquery::PGBitmapset * depends_on; /* CTEs depended on (not including self) */
};

/* CteState is what we need to pass around in the tree walkers */
struct PGCteState
{
    /* global state: */
    PGParseState * pstate; /* global parse state */
    PGCteItem * items; /* array of CTEs and extra data */
    int numitems; /* number of CTEs */
    /* working state during a tree walk: */
    int curitem; /* index of item currently being examined */
    duckdb_libpgquery::PGList * innerwiths; /* list of lists of CommonTableExpr */
    /* working state for checkWellFormedRecursion walk only: */
    int selfrefcount; /* number of self-references detected */
    RecursionContext context; /* context to allow or disallow self-ref */
};

/*
 * This enum represents the different strengths of FOR UPDATE/SHARE clauses.
 * The ordering here is important, because the highest numerical value takes
 * precedence when a RTE is specified multiple ways.  See applyLockingClause.
 */
// typedef enum LockClauseStrength
// {
// 	LCS_NONE,					/* no such clause - only used in PlanRowMark */
// 	LCS_FORKEYSHARE,			/* FOR KEY SHARE */
// 	LCS_FORSHARE,				/* FOR SHARE */
// 	LCS_FORNOKEYUPDATE,			/* FOR NO KEY UPDATE */
// 	LCS_FORUPDATE				/* FOR UPDATE */
// } LockClauseStrength;

typedef enum {
	ERRCODE_SYNTAX_ERROR,
	ERRCODE_FEATURE_NOT_SUPPORTED,
	ERRCODE_INVALID_PARAMETER_VALUE,
	ERRCODE_WINDOWING_ERROR,
	ERRCODE_RESERVED_NAME,
	ERRCODE_INVALID_ESCAPE_SEQUENCE,
	ERRCODE_NONSTANDARD_USE_OF_ESCAPE_CHARACTER,
	ERRCODE_NAME_TOO_LONG,
	ERRCODE_DATATYPE_MISMATCH,
	ERRCODE_DUPLICATE_COLUMN,
	ERRCODE_AMBIGUOUS_COLUMN,
	ERRCODE_UNDEFINED_COLUMN,
	ERRCODE_GROUPING_ERROR,
	ERRCODE_INVALID_COLUMN_REFERENCE,
	ERRCODE_WRONG_OBJECT_TYPE,
	ERRCODE_TOO_MANY_ARGUMENTS,
	ERRCODE_UNDEFINED_FUNCTION,
	ERRCODE_UNDEFINED_OBJECT,
	ERRCODE_CANNOT_COERCE,
	ERRCODE_UNDEFINED_PARAMETER,
	ERRCODE_INDETERMINATE_DATATYPE,
	ERRCODE_TOO_MANY_COLUMNS,
	ERRCODE_AMBIGUOUS_FUNCTION,
	ERRCODE_INVALID_FUNCTION_DEFINITION,
	ERRCODE_UNDEFINED_TABLE,
	ERRCODE_DUPLICATE_ALIAS,
	ERRCODE_AMBIGUOUS_ALIAS,
	ERRCODE_PROGRAM_LIMIT_EXCEEDED,
	ERRCODE_STATEMENT_TOO_COMPLEX,
	ERRCODE_INVALID_RECURSION,

	ERRCODE_COLLATION_MISMATCH,
	ERRCODE_DATA_EXCEPTION,
	ERRCODE_QUERY_CANCELED,
    ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
	ERRCODE_INVALID_TEXT_REPRESENTATION
} PGPostgresParserErrors;

typedef struct ErrorContextCallback
{
	struct ErrorContextCallback *previous;
	void		(*callback) (void *arg);
	void	   *arg;
} ErrorContextCallback;

/* Support for parser_errposition_callback function */
typedef struct PGParseCallbackState
{
	PGParseState *pstate;
	int			location;
	ErrorContextCallback errcallback;
} PGParseCallbackState;

typedef PGIndex Index;

typedef PGOid Oid;

typedef duckdb_libpgquery::PGBitmapset *PGRelids;

// struct PGColumn
// {
// 	Oid oid;
// 	String name;
// 	NameAndTypePair name_and_type;
// 	Oid type_oid;
// 	Oid type_modifier_oid;
// 	Oid collation_oid;
// };

// struct PGRelation
// {
// 	Oid oid;
// 	std::vector<PGColumn> columns;
// };

#define NAMEDATALEN 64

struct NameData
{
    char data[NAMEDATALEN];
};
typedef NameData * Name;

struct Form_pg_attribute
{
    Oid attrelid; /* OID of relation containing this attribute */
    NameData attname; /* name of attribute */

    /*
	 * atttypid is the OID of the instance in Catalog Class pg_type that
	 * defines the data type of this attribute (e.g. int4).  Information in
	 * that instance is redundant with the attlen, attbyval, and attalign
	 * attributes of this instance, so they had better match or Postgres will
	 * fail.
	 */
    Oid atttypid;

    /*
	 * attstattarget is the target number of statistics datapoints to collect
	 * during VACUUM ANALYZE of this column.  A zero here indicates that we do
	 * not wish to collect any stats about this column. A "-1" here indicates
	 * that no value has been explicitly set for this column, so ANALYZE
	 * should use the default setting.
	 */
    int4 attstattarget;

    /*
	 * attlen is a copy of the typlen field from pg_type for this attribute.
	 * See atttypid comments above.
	 */
    int2 attlen;

    /*
	 * attnum is the "attribute number" for the attribute:	A value that
	 * uniquely identifies this attribute within its class. For user
	 * attributes, Attribute numbers are greater than 0 and not greater than
	 * the number of attributes in the class. I.e. if the Class pg_class says
	 * that Class XYZ has 10 attributes, then the user attribute numbers in
	 * Class pg_attribute must be 1-10.
	 *
	 * System attributes have attribute numbers less than 0 that are unique
	 * within the class, but not constrained to any particular range.
	 *
	 * Note that (attnum - 1) is often used as the index to an array.
	 */
    int2 attnum;

    /*
	 * attndims is the declared number of dimensions, if an array type,
	 * otherwise zero.
	 */
    int4 attndims;

    /*
	 * fastgetattr() uses attcacheoff to cache byte offsets of attributes in
	 * heap tuples.  The value actually stored in pg_attribute (-1) indicates
	 * no cached value.  But when we copy these tuples into a tuple
	 * descriptor, we may then update attcacheoff in the copies. This speeds
	 * up the attribute walking process.
	 */
    int4 attcacheoff;

    /*
	 * atttypmod records type-specific data supplied at table creation time
	 * (for example, the max length of a varchar field).  It is passed to
	 * type-specific input and output functions as the third argument. The
	 * value will generally be -1 for types that do not need typmod.
	 */
    int4 atttypmod;

    /*
	 * attbyval is a copy of the typbyval field from pg_type for this
	 * attribute.  See atttypid comments above.
	 */
    bool attbyval;

    /*----------
	 * attstorage tells for VARLENA attributes, what the heap access
	 * methods can do to it if a given tuple doesn't fit into a page.
	 * Possible values are
	 *		'p': Value must be stored plain always
	 *		'e': Value can be stored in "secondary" relation (if relation
	 *			 has one, see pg_class.reltoastrelid)
	 *		'm': Value can be stored compressed inline
	 *		'x': Value can be stored compressed inline or in "secondary"
	 * Note that 'm' fields can also be moved out to secondary storage,
	 * but only as a last resort ('e' and 'x' fields are moved first).
	 *----------
	 */
    char attstorage;

    /*
	 * attalign is a copy of the typalign field from pg_type for this
	 * attribute.  See atttypid comments above.
	 */
    char attalign;

    /* This flag represents the "NOT NULL" constraint */
    bool attnotnull;

    /* Has DEFAULT value or not */
    bool atthasdef;

    /* Is dropped (ie, logically invisible) or not */
    bool attisdropped;

    /* Has a local definition (hence, do not drop when attinhcount is 0) */
    bool attislocal;

    /* Number of times inherited from direct parent relation(s) */
    int4 attinhcount;

    /*
	 * VARIABLE LENGTH FIELDS start here.  These fields may be NULL, too.
	 *
	 * NOTE: the following fields are not present in tuple descriptors!
	 */

    /* Column-level access permissions */
};

struct TupleDesc
{
    int natts; /* number of attributes in the tuple */
    Form_pg_attribute * attrs;
    /* attrs[N] is a pointer to the description of Attribute Number N+1 */
    //TupleConstr *constr;		/* constraints, or NULL if none */
    Oid tdtypeid; /* composite type ID for tuple type */
    int32 tdtypmod; /* typmod for tuple type */
    bool tdhasoid; /* tuple has oid attribute in its header */
    int tdrefcount; /* reference count, or -1 if not counting */
};

struct Form_pg_operator
{
	Oid oid;
    NameData oprname; /* name of operator */
    Oid oprnamespace; /* OID of namespace containing this oper */
    Oid oprowner; /* operator owner */
    char oprkind; /* 'l', 'r', or 'b' */
    bool oprcanhash; /* can be used in hash join? */
    Oid oprleft; /* left arg type, or 0 if 'l' oprkind */
    Oid oprright; /* right arg type, or 0 if 'r' oprkind */
    Oid oprresult; /* result datatype */
    Oid oprcom; /* OID of commutator oper, or 0 if none */
    Oid oprnegate; /* OID of negator oper, or 0 if none */
    Oid oprlsortop; /* OID of left sortop, if mergejoinable */
    Oid oprrsortop; /* OID of right sortop, if mergejoinable */
    Oid oprltcmpop; /* OID of "l<r" oper, if mergejoinable */
    Oid oprgtcmpop; /* OID of "l>r" oper, if mergejoinable */
    Oid oprcode; /* OID of underlying function */
    Oid oprrest; /* OID of restriction estimator, or 0 */
    Oid oprjoin; /* OID of join estimator, or 0 */
};

using PGOperatorPtr = std::shared_ptr<Form_pg_operator>;

struct Form_pg_type
{
	Oid oid;
    NameData typname; /* type name */
    Oid typnamespace; /* OID of namespace containing this type */
    Oid typowner; /* type owner */

    /*
	 * For a fixed-size type, typlen is the number of bytes we use to
	 * represent a value of this type, e.g. 4 for an int4.  But for a
	 * variable-length type, typlen is negative.  We use -1 to indicate a
	 * "varlena" type (one that has a length word), -2 to indicate a
	 * null-terminated C string.
	 */
    int16 typlen;

    /*
	 * typbyval determines whether internal Postgres routines pass a value of
	 * this type by value or by reference.  typbyval had better be FALSE if
	 * the length is not 1, 2, or 4 (or 8 on 8-byte-Datum machines).
	 * Variable-length types are always passed by reference. Note that
	 * typbyval can be false even if the length would allow pass-by-value;
	 * this is currently true for type float4, for example.
	 */
    bool typbyval;

    /*
	 * typtype is 'b' for a base type, 'c' for a composite type (e.g., a
	 * table's rowtype), 'd' for a domain, 'e' for an enum type, 'p' for a
	 * pseudo-type, or 'r' for a range type. (Use the TYPTYPE macros below.)
	 *
	 * If typtype is 'c', typrelid is the OID of the class' entry in pg_class.
	 */
    char typtype;

    /*
	 * typcategory and typispreferred help the parser distinguish preferred
	 * and non-preferred coercions.  The category can be any single ASCII
	 * character (but not \0).  The categories used for built-in types are
	 * identified by the TYPCATEGORY macros below.
	 */
    char typcategory; /* arbitrary type classification */

    bool typispreferred; /* is type "preferred" within its category? */

    /*
	 * If typisdefined is false, the entry is only a placeholder (forward
	 * reference).  We know the type name, but not yet anything else about it.
	 */
    bool typisdefined;

    char typdelim; /* delimiter for arrays of this type */

    Oid typrelid; /* 0 if not a composite type */

    /*
	 * If typelem is not 0 then it identifies another row in pg_type. The
	 * current type can then be subscripted like an array yielding values of
	 * type typelem. A non-zero typelem does not guarantee this type to be a
	 * "real" array type; some ordinary fixed-length types can also be
	 * subscripted (e.g., name, point). Variable-length types can *not* be
	 * turned into pseudo-arrays like that. Hence, the way to determine
	 * whether a type is a "true" array type is if:
	 *
	 * typelem != 0 and typlen == -1.
	 */
    Oid typelem;

    /*
	 * If there is a "true" array type having this type as element type,
	 * typarray links to it.  Zero if no associated "true" array type.
	 */
    Oid typarray;

    /*
	 * I/O conversion procedures for the datatype.
	 */
    Oid typinput; /* text format (required) */
    Oid typoutput;
    Oid typreceive; /* binary format (optional) */
    Oid typsend;

    /*
	 * I/O functions for optional type modifiers.
	 */
    Oid typmodin;
    Oid typmodout;

    /*
	 * Custom ANALYZE procedure for the datatype (0 selects the default).
	 */
    Oid typanalyze;

    /* ----------------
	 * typalign is the alignment required when storing a value of this
	 * type.  It applies to storage on disk as well as most
	 * representations of the value inside Postgres.  When multiple values
	 * are stored consecutively, such as in the representation of a
	 * complete row on disk, padding is inserted before a datum of this
	 * type so that it begins on the specified boundary.  The alignment
	 * reference is the beginning of the first datum in the sequence.
	 *
	 * 'c' = CHAR alignment, ie no alignment needed.
	 * 's' = SHORT alignment (2 bytes on most machines).
	 * 'i' = INT alignment (4 bytes on most machines).
	 * 'd' = DOUBLE alignment (8 bytes on many machines, but by no means all).
	 *
	 * See include/access/tupmacs.h for the macros that compute these
	 * alignment requirements.  Note also that we allow the nominal alignment
	 * to be violated when storing "packed" varlenas; the TOAST mechanism
	 * takes care of hiding that from most code.
	 *
	 * NOTE: for types used in system tables, it is critical that the
	 * size and alignment defined in pg_type agree with the way that the
	 * compiler will lay out the field in a struct representing a table row.
	 * ----------------
	 */
    char typalign;

    /* ----------------
	 * typstorage tells if the type is prepared for toasting and what
	 * the default strategy for attributes of this type should be.
	 *
	 * 'p' PLAIN	  type not prepared for toasting
	 * 'e' EXTERNAL   external storage possible, don't try to compress
	 * 'x' EXTENDED   try to compress and store external if required
	 * 'm' MAIN		  like 'x' but try to keep in main tuple
	 * ----------------
	 */
    char typstorage;

    /*
	 * This flag represents a "NOT NULL" constraint against this datatype.
	 *
	 * If true, the attnotnull column for a corresponding table column using
	 * this datatype will always enforce the NOT NULL constraint.
	 *
	 * Used primarily for domain types.
	 */
    bool typnotnull;

    /*
	 * Domains use typbasetype to show the base (or domain) type that the
	 * domain is based on.  Zero if the type is not a domain.
	 */
    Oid typbasetype;

    /*
	 * Domains use typtypmod to record the typmod to be applied to their base
	 * type (-1 if base type does not use a typmod).  -1 if this type is not a
	 * domain.
	 */
    int32 typtypmod;

    /*
	 * typndims is the declared number of dimensions for an array domain type
	 * (i.e., typbasetype is an array type).  Otherwise zero.
	 */
    int32 typndims;

    /*
	 * Collation: 0 if type cannot use collations, DEFAULT_COLLATION_OID for
	 * collatable base types, possibly other OID for domains
	 */
    Oid typcollation;
};

using PGTypePtr = std::shared_ptr<Form_pg_type>;

struct Form_pg_class
{
    NameData relname; /* class name */
    Oid relnamespace; /* OID of namespace containing this class */
    Oid reltype; /* OID of entry in pg_type for table's
								 * implicit row type */
    Oid reloftype; /* OID of entry in pg_type for underlying
								 * composite type */
    Oid relowner; /* class owner */
    Oid relam; /* index access method; 0 if not an index */
    Oid relfilenode; /* identifier of physical storage file */

    /* relfilenode == 0 means it is a "mapped" relation, see relmapper.c */
    Oid reltablespace; /* identifier of table space for relation */
    int32 relpages; /* # of blocks (not always up-to-date) */
    float4 reltuples; /* # of tuples (not always up-to-date) */
    int32 relallvisible; /* # of all-visible blocks (not always
								 * up-to-date) */
    Oid reltoastrelid; /* OID of toast table; 0 if none */
    bool relhasindex; /* T if has (or has had) any indexes */
    bool relisshared; /* T if shared across databases */
    char relpersistence; /* see RELPERSISTENCE_xxx constants below */
    char relkind; /* see RELKIND_xxx constants below */
    char relstorage; /* see RELSTORAGE_xxx constants below */
    int16 relnatts; /* number of user attributes */

    /*
	 * Class pg_attribute must contain exactly "relnatts" user attributes
	 * (with attnums ranging from 1 to relnatts) for this class.  It may also
	 * contain entries with negative attnums for system attributes.
	 */
    int16 relchecks; /* # of CHECK constraints for class */
    bool relhasoids; /* T if we generate OIDs for rows of rel */
    bool relhaspkey; /* has (or has had) PRIMARY KEY index */
    bool relhasrules; /* has (or has had) any rules */
    bool relhastriggers; /* has (or has had) any TRIGGERs */
    bool relhassubclass; /* has (or has had) derived classes */
    bool relispopulated; /* matview currently holds query results */
    char relreplident; /* see REPLICA_IDENTITY_xxx constants  */
    uint32 relfrozenxid; /* all Xids < this are frozen in this rel */
    uint32 relminmxid; /* all multixacts in this rel are >= this.
								 * this is really a MultiXactId */
};

using PGClassPtr = std::shared_ptr<Form_pg_class>;

struct oidvector
{
    int32 vl_len_; /* these fields must match ArrayType! */
    int ndim; /* always 1 for oidvector */
    int32 dataoffset; /* always 0 for oidvector */
    Oid elemtype;
    int dim1;
    int lbound1;
    Oid values[1]; /* VARIABLE LENGTH ARRAY */
};

struct Form_pg_proc
{
    NameData proname; /* procedure name */
    Oid pronamespace; /* OID of namespace containing this proc */
    Oid proowner; /* procedure owner */
    Oid prolang; /* OID of pg_language entry */
    float4 procost; /* estimated execution cost */
    float4 prorows; /* estimated # of rows out (if proretset) */
    Oid provariadic; /* element type of variadic array, or 0 */
    Oid protransform; /* transforms calls to it during planning */
    bool proisagg; /* is it an aggregate? */
    bool proiswindow; /* is it a window function? */
    bool prosecdef; /* security definer */
    bool proleakproof; /* is it a leak-proof function? */
    bool proisstrict; /* strict with respect to NULLs? */
    bool proretset; /* returns a set? */
    char provolatile; /* see PROVOLATILE_ categories below */
    int16 pronargs; /* number of arguments */
    int16 pronargdefaults; /* number of arguments with defaults */
    Oid prorettype; /* OID of result type */

    /*
	 * variable-length fields start here, but we allow direct access to
	 * proargtypes
	 */
    oidvector proargtypes; /* parameter types (excludes OUT params) */
};

using PGProcPtr = std::shared_ptr<Form_pg_proc>;

struct Form_pg_cast
{
    Oid castsource; /* source datatype for cast */
    Oid casttarget; /* destination datatype for cast */
    Oid castfunc; /* cast function; 0 = binary coercible */
    char castcontext; /* contexts in which cast can be used */
    char castmethod; /* cast method */
};

using PGCastPtr = std::shared_ptr<Form_pg_cast>;

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

#define DEFAULT_COLLATION_OID	100

/*
 * Arrays are varlena objects, so must meet the varlena convention that
 * the first int32 of the object contains the total object size in bytes.
 * Be sure to use VARSIZE() and SET_VARSIZE() to access it, though!
 *
 * CAUTION: if you change the header for ordinary arrays you will also
 * need to change the headers for oidvector and int2vector!
 */
struct ArrayType
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	int			ndim;			/* # of dimensions */
	int32		dataoffset;		/* offset to data, or 0 if no bitmap */
	Oid			elemtype;		/* element type OID */
};

struct FuzzyAttrMatchState
{
	int			distance;		/* Weighted distance (lowest so far) */
	duckdb_libpgquery::PGRangeTblEntry *rfirst;		/* RTE of first */
	PGAttrNumber	first;			/* Closest attribute so far */
	duckdb_libpgquery::PGRangeTblEntry *rsecond;		/* RTE of second */
	PGAttrNumber	second;			/* Second closest attribute so far */
};

#define NAMEDATALEN 64
#define MAX_CACHED_PATH_LEN		16

struct OprCacheKey
{
	char		oprname[NAMEDATALEN];
	Oid			left_arg;		/* Left input OID, or 0 if prefix op */
	Oid			right_arg;		/* Right input OID, or 0 if postfix op */
	Oid			search_path[MAX_CACHED_PATH_LEN];
};

static inline Datum BoolGetDatum(bool b) { return (b ? 1 : 0); } 

static inline int32 DatumGetInt32(Datum d) { return (int32) d; };
static inline Datum Int32GetDatum(int32 i32) { return (Datum) i32; };
static inline Datum Int64GetDatum(int64 i64) { return (Datum) i64; };

#define DatumGetPointer(X) ((Pointer) (X))
#define PointerGetDatum(X) ((Datum) (X))
#define PointerIsValid(pointer) ((const void*)(pointer) != NULL)

static inline Datum CStringGetDatum(const char *p) { return PointerGetDatum(p); };
static inline Datum ObjectIdGetDatum(Oid oid) { return (Datum) oid; } ;
static inline char *DatumGetCString(Datum d) { return (char* ) DatumGetPointer(d); };

static const int oldprecedence_l[] = {
	0, 10, 10, 3, 2, 8, 4, 5, 6, 4, 5, 6, 7, 8, 9
};
static const int oldprecedence_r[] = {
	0, 10, 10, 3, 2, 8, 4, 5, 6, 1, 1, 1, 7, 8, 9
};

#define FLOAT8PASSBYVAL true

#define MAX_FUZZY_DISTANCE				3
#define MaxTupleAttributeNumber 1664	/* 8 * 208 */

/* Error level codes */
#define DEBUG5		10			/* Debugging messages, in categories of
								 * decreasing detail. */
#define DEBUG4		11
#define DEBUG3		12
#define DEBUG2		13
#define DEBUG1		14			/* used by GUC debug_* variables */
#define LOG			15			/* Server operational messages; sent only to
								 * server log by default. */
#define LOG_SERVER_ONLY 16		/* Same as LOG for server reporting, but never
								 * sent to client. */
#define COMMERROR	LOG_SERVER_ONLY /* Client communication problems; same as
									 * LOG for server reporting, but never
									 * sent to client. */
#define INFO		17			/* Messages specifically requested by user (eg
								 * VACUUM VERBOSE output); always sent to
								 * client regardless of client_min_messages,
								 * but by default not sent to server log. */
#define NOTICE		18			/* Helpful messages to users about query
								 * operation; sent to client and not to server
								 * log by default. */
#define WARNING		19			/* Warnings.  NOTICE is for expected messages
								 * like implicit sequence creation by SERIAL.
								 * WARNING is for unexpected messages. */
#define ERROR		20			/* user error - abort transaction; return to
								 * known state */

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

#define PREC_GROUP_POSTFIX_IS	1	/* postfix IS tests (NullTest, etc) */
#define PREC_GROUP_INFIX_IS		2	/* infix IS (IS DISTINCT FROM, etc) */
#define PREC_GROUP_LESS			3	/* < > */
#define PREC_GROUP_EQUAL		4	/* = */
#define PREC_GROUP_LESS_EQUAL	5	/* <= >= <> */
#define PREC_GROUP_LIKE			6	/* LIKE ILIKE SIMILAR */
#define PREC_GROUP_BETWEEN		7	/* BETWEEN */
#define PREC_GROUP_IN			8	/* IN */
#define PREC_GROUP_NOT_LIKE		9	/* NOT LIKE/ILIKE/SIMILAR */
#define PREC_GROUP_NOT_BETWEEN	10	/* NOT BETWEEN */
#define PREC_GROUP_NOT_IN		11	/* NOT IN */
#define PREC_GROUP_POSTFIX_OP	12	/* generic postfix operators */
#define PREC_GROUP_INFIX_OP		13	/* generic infix operators */
#define PREC_GROUP_PREFIX_OP	14	/* generic prefix operators */

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
#define Max(x, y)		((x) > (y) ? (x) : (y))

#define NameStr(name)	((name).data)

Oid			MyDatabaseId = InvalidOid;

size_t
strlcpy(char *dst, const char *src, size_t siz)
{
	char	   *d = dst;
	const char *s = src;
	size_t		n = siz;

	/* Copy as many bytes as will fit */
	if (n != 0)
	{
		while (--n != 0)
		{
			if ((*d++ = *s++) == '\0')
				break;
		}
	}

	/* Not enough room in dst, add NUL and traverse rest of src */
	if (n == 0)
	{
		if (siz != 0)
			*d = '\0';			/* NUL-terminate dst */
		while (*s++)
			;
	}

	return (s - src - 1);		/* count does not include NUL */
}

/*
 * count_nonjunk_tlist_entries
 *		What it says ...
 */
int
count_nonjunk_tlist_entries(duckdb_libpgquery::PGList *tlist)
{
	int			len = 0;
    duckdb_libpgquery::PGListCell * l;

    foreach(l, tlist)
	{
		duckdb_libpgquery::PGTargetEntry *tle = (duckdb_libpgquery::PGTargetEntry *) lfirst(l);

		if (!tle->resjunk)
			len++;
	}
	return len;
}

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

duckdb_libpgquery::PGValue * makeString(char * str)
{
	using duckdb_libpgquery::PGValue;
	using duckdb_libpgquery::T_PGValue;
    PGValue * v = makeNode(PGValue);

    v->type = duckdb_libpgquery::T_PGString;
    v->val.str = str;
    return v;
};

int
parser_errposition(PGParseState *pstate, int location)
{
	int			pos;

	/* No-op if location was not provided */
	if (location < 0)
		return 0;
	/* Can't do anything if source text is not available */
	if (pstate == NULL || pstate->p_sourcetext == NULL)
		return 0;
	/* Convert offset to character number */
	pos = duckdb_libpgquery::pg_mbstrlen_with_len(pstate->p_sourcetext, location) + 1;
	/* And pass it to the ereport mechanism */
	return duckdb_libpgquery::errposition(pos);
};

void
free_parsestate(PGParseState *pstate)
{
	delete pstate;
	pstate = NULL;
};

/* Global variables */
ErrorContextCallback *error_context_stack = NULL;

static void pcb_error_callback(void * arg)
{
    PGParseCallbackState * pcbstate = (PGParseCallbackState *)arg;

	//TODO
    //if (geterrcode() != ERRCODE_QUERY_CANCELED)
        (void)parser_errposition(pcbstate->pstate, pcbstate->location);
};

void
setup_parser_errposition_callback(PGParseCallbackState *pcbstate,
								  PGParseState *pstate, int location)
{
	/* Setup error traceback support for ereport() */
	pcbstate->pstate = pstate;
	pcbstate->location = location;
	pcbstate->errcallback.callback = pcb_error_callback;
	pcbstate->errcallback.arg = (void *) pcbstate;
	pcbstate->errcallback.previous = error_context_stack;
	error_context_stack = &pcbstate->errcallback;
};

void
cancel_parser_errposition_callback(PGParseCallbackState *pcbstate)
{
	/* Pop the error context stack */
	error_context_stack = pcbstate->errcallback.previous;
};

duckdb_libpgquery::PGTargetEntry * get_sortgroupref_tle(Index sortref, duckdb_libpgquery::PGList * targetList)
{
	using duckdb_libpgquery::errcode;
	using duckdb_libpgquery::ereport;
	using duckdb_libpgquery::errmsg;

    duckdb_libpgquery::PGListCell * l;

    foreach (l, targetList)
    {
        duckdb_libpgquery::PGTargetEntry * tle = (duckdb_libpgquery::PGTargetEntry *)lfirst(l);

        if (tle->ressortgroupref == sortref)
            return tle;
    }

    /*
	 * XXX: we probably should catch this earlier, but we have a
	 * few queries in the regression suite that hit this.
	 */
    ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("ORDER/GROUP BY expression not found in targetlist")));
    return NULL; /* keep compiler quiet */
};

duckdb_libpgquery::PGTargetEntry * get_sortgroupclause_tle(
	duckdb_libpgquery::PGSortGroupClause * sgClause, 
	duckdb_libpgquery::PGList * targetList)
{
    return get_sortgroupref_tle(sgClause->tleSortGroupRef, targetList);
};

duckdb_libpgquery::PGNode *
get_sortgroupclause_expr(duckdb_libpgquery::PGSortGroupClause * sgClause, duckdb_libpgquery::PGList * targetList)
{
    duckdb_libpgquery::PGTargetEntry * tle = get_sortgroupclause_tle(sgClause, targetList);

    return (duckdb_libpgquery::PGNode *)tle->expr;
};

/*
 * DeconstructQualifiedName
 *		Given a possibly-qualified name expressed as a list of String nodes,
 *		extract the schema name and object name.
 *
 * *nspname_p is set to NULL if there is no explicit schema name.
 */
void DeconstructQualifiedName(duckdb_libpgquery::PGList * names, char ** nspname_p, char ** objname_p)
{
	using duckdb_libpgquery::ereport;
	using duckdb_libpgquery::errcode;
	using duckdb_libpgquery::errmsg;
	using duckdb_libpgquery::PGValue;

    //char * catalogname;
    char * schemaname = NULL;
    char * objname = NULL;

    switch (list_length(names))
    {
        case 1:
            objname = strVal(linitial(names));
            break;
        case 2:
            schemaname = strVal(linitial(names));
            objname = strVal(lsecond(names));
            break;
        // case 3:
        //     catalogname = strVal(linitial(names));
        //     schemaname = strVal(lsecond(names));
        //     objname = strVal(lthird(names));

        //     /*
		// 	 * We check the catalog name and then ignore it.
		// 	 */
        //     if (strcmp(catalogname, get_database_name(MyDatabaseId)) != 0)
        //         ereport(
        //             ERROR,
        //             (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
        //              errmsg("cross-database references are not implemented: %s", NameListToString(names))));
        //     break;
        default:
            ereport(
                ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR), errmsg("improper qualified name (too many dotted names): %s", NameListToString(names))));
            break;
    }

    *nspname_p = schemaname;
    *objname_p = objname;
};

#define INT64CONST(x)  (x##L)

bool scanint8(const char * str, bool errorOK, int64 * result)
{
	using duckdb_libpgquery::ereport;
	using duckdb_libpgquery::errcode;
	using duckdb_libpgquery::errmsg;

    const char * ptr = str;
    int64 tmp = 0;
    int sign = 1;

    /*
	 * Do our own scan, rather than relying on sscanf which might be broken
	 * for long long.
	 */

    /* skip leading spaces */
    while (*ptr && isspace((unsigned char)*ptr))
        ptr++;

    /* handle sign */
    if (*ptr == '-')
    {
        ptr++;

        /*
		 * Do an explicit check for INT64_MIN.  Ugly though this is, it's
		 * cleaner than trying to get the loop below to handle it portably.
		 */
        if (strncmp(ptr, "9223372036854775808", 19) == 0)
        {
            tmp = -INT64CONST(0x7fffffffffffffff) - 1;
            ptr += 19;
            goto gotdigits;
        }
        sign = -1;
    }
    else if (*ptr == '+')
        ptr++;

    /* require at least one digit */
    if (!isdigit((unsigned char)*ptr))
    {
        if (errorOK)
            return false;
        else
            ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION), errmsg("invalid input syntax for integer: \"%s\"", str)));
    }

    /* process digits */
    while (*ptr && isdigit((unsigned char)*ptr))
    {
        int64 newtmp = tmp * 10 + (*ptr++ - '0');

        if ((newtmp / 10) != tmp) /* overflow? */
        {
            if (errorOK)
                return false;
            else
                ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("value \"%s\" is out of range for type bigint", str)));
        }
        tmp = newtmp;
    }

gotdigits:

    /* allow trailing whitespace, but not other trailing chars */
    while (*ptr != '\0' && isspace((unsigned char)*ptr))
        ptr++;

    if (*ptr != '\0')
    {
        if (errorOK)
            return false;
        else
            ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION), errmsg("invalid input syntax for integer: \"%s\"", str)));
    }

    *result = (sign < 0) ? -tmp : tmp;

    return true;
};

// duckdb_libpgquery::PGTargetEntry *
// get_sortgroupref_tle(Index sortref, duckdb_libpgquery::PGList *targetList)
// {
// 	duckdb_libpgquery::PGListCell   *l;

// 	foreach(l, targetList)
// 	{
// 		duckdb_libpgquery::PGTargetEntry *tle = (duckdb_libpgquery::PGTargetEntry *) lfirst(l);

// 		if (tle->ressortgroupref == sortref)
// 			return tle;
// 	}

// 	/*
// 	 * XXX: we probably should catch this earlier, but we have a
// 	 * few queries in the regression suite that hit this.
// 	 */
// 	duckdb_libpgquery::ereport(ERROR,
// 			(duckdb_libpgquery::errcode(ERRCODE_SYNTAX_ERROR),
// 			 duckdb_libpgquery::errmsg("ORDER/GROUP BY expression not found in targetlist")));
// 	return NULL;				/* keep compiler quiet */
// };

// duckdb_libpgquery::PGTargetEntry *
// get_sortgroupclause_tle(duckdb_libpgquery::PGSortGroupClause *sgClause,
// 						duckdb_libpgquery::PGList *targetList)
// {
// 	return get_sortgroupref_tle(sgClause->tleSortGroupRef, targetList);
// };

// duckdb_libpgquery::PGNode *
// get_sortgroupclause_expr(duckdb_libpgquery::PGSortGroupClause *sgClause,
// 	duckdb_libpgquery::PGList *targetList)
// {
// 	duckdb_libpgquery::PGTargetEntry *tle = get_sortgroupclause_tle(sgClause, targetList);

// 	return (duckdb_libpgquery::PGNode *) tle->expr;
// };

// bool
// contain_vars_of_level_walker(duckdb_libpgquery::PGNode *node, int *sublevels_up);

// bool
// contain_vars_of_level(duckdb_libpgquery::PGNode *node, int levelsup)
// {
// 	int			sublevels_up = levelsup;

// 	return query_or_expression_tree_walker(node,
// 										   contain_vars_of_level_walker,
// 										   (void *) &sublevels_up,
// 										   0);
// };

// bool
// contain_vars_of_level_walker(duckdb_libpgquery::PGNode *node, int *sublevels_up)
// {
// 	using duckdb_libpgquery::PGVar;
// 	using duckdb_libpgquery::PGCurrentOfExpr;
// 	//using duckdb_libpgquery::PGPlaceHolderVar;
// 	using duckdb_libpgquery::PGQuery;
// 	if (node == NULL)
// 		return false;
// 	if (IsA(node, PGVar))
// 	{
// 		if (((PGVar *) node)->varlevelsup == *sublevels_up)
// 			return true;		/* abort tree traversal and return true */
// 		return false;
// 	}
// 	if (IsA(node, PGCurrentOfExpr))
// 	{
// 		if (*sublevels_up == 0)
// 			return true;
// 		return false;
// 	}
// 	// if (IsA(node, PGPlaceHolderVar))
// 	// {
// 	// 	if (((PGPlaceHolderVar *) node)->phlevelsup == *sublevels_up)
// 	// 		return true;		/* abort the tree traversal and return true */
// 	// 	/* else fall through to check the contained expr */
// 	// }
// 	if (IsA(node, PGQuery))
// 	{
// 		/* Recurse into subselects */
// 		bool		result;

// 		(*sublevels_up)++;
// 		result = query_tree_walker((PGQuery *) node,
// 								   contain_vars_of_level_walker,
// 								   (void *) sublevels_up,
// 								   0);
// 		(*sublevels_up)--;
// 		return result;
// 	}
// 	return expression_tree_walker(node,
// 								  contain_vars_of_level_walker,
// 								  (void *) sublevels_up);
// };
