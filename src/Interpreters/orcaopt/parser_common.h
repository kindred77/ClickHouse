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

#include <string.h>
#include <boost/algorithm/string.hpp>

#include <Interpreters/orcaopt/parser_common_macro.h>

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
typedef unsigned long int uint64;

typedef int64 Datum;
typedef char TYPCATEGORY;
typedef char *Pointer;

namespace duckdb_libpgquery
{
    typedef DistinctExpr PGDistinctExpr;

    typedef NullIfExpr PGNullIfExpr;
};

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
	ERRCODE_INVALID_TEXT_REPRESENTATION,
	ERRCODE_INTERNAL_ERROR,
	ERRCODE_INVALID_NAME,
	ERRCODE_INVALID_TABLE_DEFINITION
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

// struct NameData
// {
//     char data[NAMEDATALEN];
// };
// typedef NameData * Name;

struct Form_pg_attribute
{
    Oid attrelid; /* OID of relation containing this attribute */
    std::string attname; /* name of attribute */

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

	/* attribute's collation */
	Oid			attcollation;
};

using PGAttrPtr = std::shared_ptr<Form_pg_attribute>;

typedef struct attrDefault
{
    PGAttrNumber adnum;
    std::string adbin; /* nodeToString representation of expr */
} PGAttrDefault;

using PGAttrDefaultPtr = std::shared_ptr<PGAttrDefault>;

typedef struct constrCheck
{
    std::string ccname;
    std::string ccbin; /* nodeToString representation of expr */
    bool ccvalid;
    bool ccnoinherit; /* this is a non-inheritable constraint */
} PGConstrCheck;

using PGConstrCheckPtr = std::shared_ptr<PGConstrCheck>;

/* This structure contains constraints of a tuple */
typedef struct tupleConstr
{
    std::vector<PGAttrDefaultPtr> defval; /* array */
    std::vector<PGConstrCheckPtr> check; /* array */
    uint16 num_defval;
    uint16 num_check;
    bool has_not_null;
} PGTupleConstr;

using PGTupleConstrPtr = std::shared_ptr<PGTupleConstr>;

struct PGTupleDesc
{
    int natts; /* number of attributes in the tuple */
    std::vector<PGAttrPtr> attrs;
    /* attrs[N] is a pointer to the description of Attribute Number N+1 */
    PGTupleConstrPtr constr;		/* constraints, or NULL if none */
    Oid tdtypeid; /* composite type ID for tuple type */
    int32 tdtypmod; /* typmod for tuple type */
    bool tdhasoid; /* tuple has oid attribute in its header */
    int tdrefcount; /* reference count, or -1 if not counting */
};

using PGTupleDescPtr = std::shared_ptr<PGTupleDesc>;

struct Form_pg_operator
{
	Oid oid;
    std::string oprname; /* name of operator */
    Oid oprnamespace; /* OID of namespace containing this oper */
    Oid oprowner; /* operator owner */
    char oprkind; /* 'l', 'r', or 'b' */
    bool oprcanmerge;
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
    std::string typname; /* type name */
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

    Oid lt_opr;
    Oid eq_opr;
    Oid gt_opr;
    Oid hash_proc;
};

using PGTypePtr = std::shared_ptr<Form_pg_type>;

struct Form_pg_class
{
    Oid oid;
    std::string relname; /* class name */
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

// struct oidvector
// {
//     int32 vl_len_; /* these fields must match ArrayType! */
//     int ndim; /* always 1 for oidvector */
//     int32 dataoffset; /* always 0 for oidvector */
//     Oid elemtype;
//     int dim1;
//     int lbound1;
//     Oid values[1]; /* VARIABLE LENGTH ARRAY */
// };

struct Form_pg_proc
{
	Oid oid;
    std::string proname; /* procedure name */
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
    //oidvector proargtypes; /* parameter types (excludes OUT params) */
	std::vector<Oid> proargtypes;
    //added by kindred
    std::vector<Oid> proallargtypes;
    std::vector<char> proargmodes;
    std::vector<std::string> proargnames;
    duckdb_libpgquery::PGNode * proargdefaults;

    // Form_pg_proc(
    //     Oid oid_,
    //     std::string proname_,
    //     Oid pronamespace_,
    //     Oid proowner_,
    //     Oid prolang_,
    //     float4 procost_,
    //     float4 prorows_,
    //     Oid provariadic_,
    //     Oid protransform_,
    //     bool proisagg_,
    //     bool proiswindow_,
    //     bool prosecdef_,
    //     bool proleakproof_,
    //     bool proisstrict_,
    //     bool proretset_,
    //     char provolatile_,
    //     int16 pronargs_,
    //     int16 pronargdefaults_,
    //     Oid prorettype_,
    //     std::vector<Oid> proargtypes_)
    // {
    //     oid = oid_;
    //     proname = proname_;
    //     pronamespace = pronamespace_;
    //     proowner = proowner_;
    //     prolang = prolang_;
    //     procost = procost_;
    //     prorows = prorows_;
    //     provariadic = provariadic_;
    //     protransform = protransform_;
    //     proisagg = proisagg_;
    //     proiswindow = proiswindow_;
    //     prosecdef = prosecdef_;
    //     proleakproof = proleakproof_;
    //     proisstrict = proisstrict_;
    //     proretset = proretset_;
    //     provolatile = provolatile_;
    //     pronargs = pronargs_;
    //     pronargdefaults = pronargdefaults_;
    //     prorettype = prorettype_;
    //     proargtypes = proargtypes_;
    // }
};

using PGProcPtr = std::shared_ptr<Form_pg_proc>;

struct Form_pg_cast
{
    Oid oid;
    Oid castsource; /* source datatype for cast */
    Oid casttarget; /* destination datatype for cast */
    Oid castfunc; /* cast function; 0 = binary coercible */
    char castcontext; /* contexts in which cast can be used */
    char castmethod; /* cast method */
};

using PGCastPtr = std::shared_ptr<Form_pg_cast>;

struct Form_pg_agg
{
    Oid aggfnoid;
    char aggkind;
    int16 aggnumdirectargs;
    Oid aggtransfn;
    Oid aggfinalfn;
    Oid aggcombinefn;
    Oid aggserialfn;
    Oid aggdeserialfn;
    Oid aggmtransfn;
    Oid aggminvtransfn;
    Oid aggmfinalfn;
    bool aggfinalextra;
    bool aggmfinalextra;
    Oid aggsortop;
    Oid aggtranstype;
    int32 aggtransspace;
    Oid aggmtranstype;
    int32 aggmtransspace;
};

using PGAggPtr = std::shared_ptr<Form_pg_agg>;

struct Sort_group_operator
{
    /* typeId is the hash lookup key and MUST BE FIRST */
    Oid type_id; /* OID of the data type */

    /* some subsidiary information copied from the pg_type row */
    int16 typlen;
    bool typbyval;
    char typalign;
    char typstorage;
    char typtype;
    Oid typrelid;

    /*
	 * Information obtained from opfamily entries
	 *
	 * These will be InvalidOid if no match could be found, or if the
	 * information hasn't yet been requested.  Also note that for array and
	 * composite types, typcache.c checks that the contained types are
	 * comparable or hashable before allowing eq_opr etc to become set.
	 */
    Oid btree_opf; /* the default btree opclass' family */
    Oid btree_opintype; /* the default btree opclass' opcintype */
    Oid hash_opf; /* the default hash opclass' family */
    Oid hash_opintype; /* the default hash opclass' opcintype */
    Oid eq_opr; /* the equality operator */
    Oid lt_opr; /* the less-than operator */
    Oid gt_opr; /* the greater-than operator */
    Oid cmp_proc; /* the btree comparison function */
    Oid hash_proc; /* the hash calculation function */
};

using PGSortGroupOperPtr = std::shared_ptr<Sort_group_operator>;

PGTupleDescPtr PGCreateTemplateTupleDesc(int natts, bool hasoid)
{
	//TODO kindred
    // PGTupleDesc * desc;
    // char * stg;
    // int attroffset;

    // /*
	//  * sanity checks
	//  */
    // Assert(natts >= 0)

    /*
	 * Allocate enough memory for the tuple descriptor, including the
	 * attribute rows, and set up the attribute row pointers.
	 *
	 * Note: we assume that sizeof(struct tupleDesc) is a multiple of the
	 * struct pointer alignment requirement, and hence we don't need to insert
	 * alignment padding between the struct and the array of attribute row
	 * pointers.
	 *
	 * Note: Only the fixed part of pg_attribute rows is included in tuple
	 * descriptors, so we only need ATTRIBUTE_FIXED_PART_SIZE space per attr.
	 * That might need alignment padding, however.
	 */
    // attroffset = sizeof(struct tupleDesc) + natts * sizeof(Form_pg_attribute);
    // attroffset = MAXALIGN(attroffset);
    // stg = (char *)palloc(attroffset + natts * MAXALIGN(ATTRIBUTE_FIXED_PART_SIZE));
    // desc = (PGTupleDesc *)stg;

    // if (natts > 0)
    // {
    //     Form_pg_attribute * attrs;
    //     int i;

    //     attrs = (Form_pg_attribute *)(stg + sizeof(struct tupleDesc));
    //     desc->attrs = attrs;
    //     stg += attroffset;
    //     for (i = 0; i < natts; i++)
    //     {
    //         attrs[i] = (Form_pg_attribute)stg;
    //         stg += MAXALIGN(ATTRIBUTE_FIXED_PART_SIZE);
    //     }
    // }
    // else
    //     desc->attrs = NULL;

    /*
	 * Initialize other fields of the tupdesc.
	 */
    // desc->natts = natts;
    // desc->constr = NULL;
    // desc->tdtypeid = RECORDOID;
    // desc->tdtypmod = -1;
    // desc->tdhasoid = hasoid;
    // desc->tdrefcount = -1; /* assume not reference-counted */

    // return desc;

	auto result = std::make_shared<PGTupleDesc>();
	result->natts = natts;
    result->attrs.resize(natts);
	result->tdtypeid = RECORDOID;
    result->tdtypmod = -1;
    result->tdhasoid = hasoid;
    result->tdrefcount = -1;

	return result;
};

/*
 * CreateTupleDescCopy
 *		This function creates a new TupleDesc by copying from an existing
 *		TupleDesc.
 *
 * !!! Constraints and defaults are not copied !!!
 */
PGTupleDescPtr PGCreateTupleDescCopy(PGTupleDescPtr tupdesc)
{
    PGTupleDescPtr desc = PGCreateTemplateTupleDesc(tupdesc->natts, tupdesc->tdhasoid);

    for (int i = 0; i < desc->natts; i++)
    {
        desc->attrs.push_back(std::make_shared<Form_pg_attribute>(*tupdesc->attrs[i].get()));
        desc->attrs[i]->attnotnull = false;
        desc->attrs[i]->atthasdef = false;
    }

    desc->tdtypeid = tupdesc->tdtypeid;
    desc->tdtypmod = tupdesc->tdtypmod;

    return desc;
};

/*
 * CreateTupleDescCopyConstr
 *		This function creates a new TupleDesc by copying from an existing
 *		TupleDesc (including its constraints and defaults).
 */
PGTupleDescPtr PGCreateTupleDescCopyConstr(PGTupleDescPtr tupdesc)
{
    PGTupleDescPtr desc = PGCreateTemplateTupleDesc(tupdesc->natts, tupdesc->tdhasoid);

    for (int i = 0; i < desc->natts; i++)
    {
        desc->attrs.push_back(std::make_shared<Form_pg_attribute>(*tupdesc->attrs[i].get()));
    }

    if (tupdesc->constr != nullptr)
    {
        PGTupleConstrPtr cpy = std::make_shared<PGTupleConstr>();

        cpy->has_not_null = tupdesc->constr->has_not_null;

        if ((cpy->num_defval = tupdesc->constr->num_defval) > 0)
        {
            //cpy->defval = (AttrDefault *)palloc(cpy->num_defval * sizeof(AttrDefault));
            //memcpy(cpy->defval, tupdesc->constr->defval, cpy->num_defval * sizeof(AttrDefault));
            for (auto def_val : tupdesc->constr->defval)
            {
                cpy->defval.push_back(std::make_shared<PGAttrDefault>(*def_val.get()));
            }
            for (int i = cpy->num_defval - 1; i >= 0; i--)
            {
                if (tupdesc->constr->defval[i]->adbin != "")
                    cpy->defval[i]->adbin = tupdesc->constr->defval[i]->adbin;
            }
        }

        if ((cpy->num_check = tupdesc->constr->num_check) > 0)
        {
            //cpy->check = (ConstrCheck *)palloc(cpy->num_check * sizeof(ConstrCheck));
            //memcpy(cpy->check, tupdesc->constr->check, cpy->num_check * sizeof(ConstrCheck));
            for (auto chck : tupdesc->constr->check)
            {
                cpy->check.push_back(std::make_shared<PGConstrCheck>(*chck.get()));
            }

            for (int i = cpy->num_check - 1; i >= 0; i--)
            {
                if (tupdesc->constr->check[i]->ccname != "")
                    cpy->check[i]->ccname = tupdesc->constr->check[i]->ccname;
                if (tupdesc->constr->check[i]->ccbin != "")
                    cpy->check[i]->ccbin = tupdesc->constr->check[i]->ccbin;
                cpy->check[i]->ccvalid = tupdesc->constr->check[i]->ccvalid;
                cpy->check[i]->ccnoinherit = tupdesc->constr->check[i]->ccnoinherit;
            }
        }

        desc->constr = cpy;
    }

    desc->tdtypeid = tupdesc->tdtypeid;
    desc->tdtypmod = tupdesc->tdtypmod;

    return desc;
};

// int
// namestrcpy(Name name, const char *str)
// {
// 	if (!name || !str)
// 		return -1;
// 	StrNCpy(NameStr(*name), str, NAMEDATALEN);
// 	return 0;
// };

void PGTupleDescInitEntryCollation(PGTupleDescPtr desc, PGAttrNumber attributeNumber, Oid collationid)
{
    /*
	 * sanity checks
	 */
    Assert(PointerIsValid(desc.get()))
    Assert(attributeNumber >= 1)
    Assert(attributeNumber <= desc->natts)

    desc->attrs[attributeNumber - 1]->attcollation = collationid;
};

void PGTupleDescCopyEntry(PGTupleDescPtr dst, PGAttrNumber dstAttno, PGTupleDescPtr src, PGAttrNumber srcAttno)
{
    /*
	 * sanity checks
	 */
    Assert(src != nullptr)
    Assert(dst != nullptr)
    Assert(srcAttno >= 1)
    Assert(srcAttno <= src->natts)
    Assert(dstAttno >= 1)
    Assert(dstAttno <= dst->natts)

	dst->attrs[dstAttno - 1] = src->attrs[srcAttno - 1];
    //memcpy(dst->attrs[dstAttno - 1], src->attrs[srcAttno - 1], ATTRIBUTE_FIXED_PART_SIZE);

    /*
	 * Aside from updating the attno, we'd better reset attcacheoff.
	 *
	 * XXX Actually, to be entirely safe we'd need to reset the attcacheoff of
	 * all following columns in dst as well.  Current usage scenarios don't
	 * require that though, because all following columns will get initialized
	 * by other uses of this function or TupleDescInitEntry.  So we cheat a
	 * bit to avoid a useless O(N^2) penalty.
	 */
    dst->attrs[dstAttno - 1]->attnum = dstAttno;
    dst->attrs[dstAttno - 1]->attcacheoff = -1;

    /* since we're not copying constraints or defaults, clear these */
    dst->attrs[dstAttno - 1]->attnotnull = false;
    dst->attrs[dstAttno - 1]->atthasdef = false;
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

struct FuncCandidateList;
using FuncCandidateListPtr = std::shared_ptr<FuncCandidateList>;

struct FuncCandidateList
{
    FuncCandidateListPtr next;
    int pathpos; /* for internal use of namespace lookup */
    Oid oid; /* the function or operator's OID */
    int nargs; /* number of arg types returned */
    int nvargs; /* number of args to become variadic array */
    int ndargs; /* number of defaulted args */
    int * argnumbers; /* args' positional indexes, if named call */
    Oid * args; /* arg types --- VARIABLE LENGTH ARRAY */

    ~FuncCandidateList()
    {
        delete [] args;
        delete [] argnumbers;
    }
}; /* VARIABLE LENGTH STRUCT */

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

// struct FuzzyAttrMatchState
// {
// 	int			distance;		/* Weighted distance (lowest so far) */
// 	duckdb_libpgquery::PGRangeTblEntry *rfirst;		/* RTE of first */
// 	PGAttrNumber	first;			/* Closest attribute so far */
// 	duckdb_libpgquery::PGRangeTblEntry *rsecond;		/* RTE of second */
// 	PGAttrNumber	second;			/* Second closest attribute so far */
// };

struct OprCacheKey
{
	char		oprname[NAMEDATALEN];
	Oid			left_arg;		/* Left input OID, or 0 if prefix op */
	Oid			right_arg;		/* Right input OID, or 0 if postfix op */
	Oid			search_path[MAX_CACHED_PATH_LEN];
};

/**
 * Planning configuration information
 */
struct PGPlannerConfig
{
    bool enable_sort;
    bool enable_hashagg;
    bool enable_groupagg;
    bool enable_nestloop;
    bool enable_mergejoin;
    bool enable_hashjoin;
    bool gp_enable_hashjoin_size_heuristic;
    bool gp_enable_predicate_propagation;
    int constraint_exclusion;

    bool gp_enable_minmax_optimization;
    bool gp_enable_multiphase_agg;
    bool gp_enable_preunique;
    bool gp_eager_preunique;
    bool gp_hashagg_streambottom;
    bool gp_enable_agg_distinct;
    bool gp_enable_dqa_pruning;
    bool gp_eager_dqa_pruning;
    bool gp_eager_one_phase_agg;
    bool gp_eager_two_phase_agg;
    bool gp_enable_groupext_distinct_pruning;
    bool gp_enable_groupext_distinct_gather;
    bool gp_enable_sort_distinct;

    bool gp_enable_direct_dispatch;
    bool gp_dynamic_partition_pruning;

    bool gp_cte_sharing; /* Indicate whether sharing is to be disabled on any CTEs */

    bool honor_order_by;

    bool is_under_subplan; /* True for plan rooted at a subquery which is planned as a subplan */

    /* These ones are tricky */
    //GpRoleValue	Gp_role; // TODO: this one is tricky
    bool force_singleQE; /* True means force gather base rel to singleQE  */
};

/* ----------------
 *		Plan node
 *
 * All plan nodes "derive" from the Plan structure by having the
 * Plan structure as the first field.  This ensures that everything works
 * when nodes are cast to Plan's.  (node pointers are frequently cast to Plan*
 * when passed around generically in the executor)
 *
 * We never actually instantiate any Plan nodes; this is just the common
 * abstract superclass for all Plan-type nodes.
 * ----------------
 */
struct PGPlan
{
    duckdb_libpgquery::PGNodeTag type;

    /* Plan node id */
    int plan_node_id; /* unique across entire final plan tree */

    /*
	 * estimated execution costs for plan (see costsize.c for more info)
	 */
	//TODO kindred
    //Cost startup_cost; /* cost expended before fetching any tuples */
    //Cost total_cost; /* total cost (assuming all tuples fetched) */

    /*
	 * planner's estimate of result size of this plan step
	 */
    double plan_rows; /* number of rows plan is expected to emit */
    int plan_width; /* average row width in bytes */

    /*
	 * Common structural data for all Plan types.
	 */
    duckdb_libpgquery::PGList * targetlist; /* target list to be computed at this node */
    duckdb_libpgquery::PGList * qual; /* implicitly-ANDed qual conditions */
    struct PGPlan * lefttree; /* input plan tree(s) */
    struct PGPlan * righttree;
    duckdb_libpgquery::PGList * initPlan; /* Init Plan nodes (un-correlated expr
								 * subselects) */

    /*
	 * Information for management of parameter-change-driven rescanning
	 *
	 * extParam includes the paramIDs of all external PARAM_EXEC params
	 * affecting this plan node or its children.  setParam params from the
	 * node's initPlans are not included, but their extParams are.
	 *
	 * allParam includes all the extParam paramIDs, plus the IDs of local
	 * params that affect the node (i.e., the setParams of its initplans).
	 * These are _all_ the PARAM_EXEC params that affect this node.
	 */
    duckdb_libpgquery::PGBitmapset * extParam;
    duckdb_libpgquery::PGBitmapset * allParam;

    /*
	 * MPP needs to keep track of the characteristics of flow of output
	 * tuple of Plan nodes.
	 */
	//TODO kindred
    //Flow * flow;
	/* Flow description.  Initially NULL.
	 * Set during parallelization.
	 */

    /*
	 * CDB:  How should this plan tree be dispatched?  Initially this is set
	 * to DISPATCH_UNDETERMINED and, in non-root nodes, may remain so.
	 * However, in Plan nodes at the root of any separately dispatchable plan
	 * fragment, it must be set to a specific dispatch type.
	 */
	//TODO kindred
    //DispatchMethod dispatch;

    /*
	 * CDB: if we're going to direct dispatch, point it at a particular id.
	 *
	 * For motion nodes, this direct dispatch data is for the slice rooted at the
	 *   motion node (the sending side!)
	 * For other nodes, it is for the slice rooted at this plan so it must be a root
	 *   plan for a query
	 * Note that for nodes that are internal to a slice then this data is not
	 *   set.
	 */
	//TODO kindred
    //DirectDispatchInfo directDispatch;

    /*
	 * CDB: Now many motion nodes are there in the Plan.  How many init plans?
	 * Additional plan tree global significant only in the root node.
	 */
    int nMotionNodes;
    int nInitPlans;

    /*
	 * CDB: This allows the slice table to accompany the plan as it
	 * moves around the executor. This is anoter plan tree global that
	 * should be non-NULL only in the top node of a dispatchable tree.
	 * It could (and should) move to a TopPlan node if we ever do that.
	 *
	 * Currently, the slice table should not be installed on the QD.
	 * Rather is it shipped to QEs as a separate parameter to MPPEXEC.
	 * The implementation of MPPEXEC, which runs on the QEs, installs
	 * the slice table in the plan as required there.
	 */
    duckdb_libpgquery::PGNode * sliceTable;

    /**
	 * How much memory (in KB) should be used to execute this plan node?
	 */
    uint64 operatorMemKB;

    /*
	 * The parent motion node of a plan node.
	 */
    struct PGPlan * motionNode;
};

/*----------
 * PlannerGlobal
 *		Global information for planning/optimization
 *
 * PlannerGlobal holds state for an entire planner invocation; this state
 * is shared across all levels of sub-Queries that exist in the command being
 * planned.
 *----------
 */
struct PGPlannerGlobal
{
    duckdb_libpgquery::PGNodeTag type;

	//TODO kindred
    //ParamListInfo boundParams; /* Param values provided to planner() */

    duckdb_libpgquery::PGList * subplans; /* Plans for SubPlan nodes */

    duckdb_libpgquery::PGList * subroots; /* PlannerInfos for SubPlan nodes */

    duckdb_libpgquery::PGBitmapset * rewindPlanIDs; /* indices of subplans that require REWIND */

    duckdb_libpgquery::PGList * finalrtable; /* "flat" rangetable for executor */

    duckdb_libpgquery::PGList * finalrowmarks; /* "flat" list of PlanRowMarks */

    duckdb_libpgquery::PGList * resultRelations; /* "flat" list of integer RT indexes */

    duckdb_libpgquery::PGList * relationOids; /* OIDs of relations the plan depends on */

    duckdb_libpgquery::PGList * invalItems; /* other dependencies, as PlanInvalItems */

    int nParamExec; /* number of PARAM_EXEC Params used */

    Index lastPHId; /* highest PlaceHolderVar ID assigned */

    Index lastRowMarkId; /* highest PlanRowMark ID assigned */

    bool transientPlan; /* redo plan when TransactionXmin changes? */
    bool oneoffPlan; /* redo plan on every execution? */
    bool simplyUpdatable; /* can be used with CURRENT OF? */

    bool is_parallel_cursor; /* is the query a parallel retrieve cursor? */

	//TODO kindred
    //ApplyShareInputContext share; /* workspace for GPDB plan sharing */

};

/*----------
 * PlannerInfo
 *		Per-query information for planning/optimization
 *
 * This struct is conventionally called "root" in all the planner routines.
 * It holds links to all of the planner's working state, in addition to the
 * original Query.  Note that at present the planner extensively modifies
 * the passed-in Query data structure; someday that should stop.
 *----------
 */
struct PGPlannerInfo
{
    duckdb_libpgquery::PGNodeTag type;

    duckdb_libpgquery::PGQuery * parse; /* the Query being planned */

    PGPlannerGlobal * glob; /* global info for current planner run */

    Index query_level; /* 1 at the outermost Query */

    struct PGPlannerInfo * parent_root; /* NULL at outermost Query */

    duckdb_libpgquery::PGList * plan_params; /* list of PlannerParamItems, see below */

    /*
	 * simple_rel_array holds pointers to "base rels" and "other rels" (see
	 * comments for RelOptInfo for more info).  It is indexed by rangetable
	 * index (so entry 0 is always wasted).  Entries can be NULL when an RTE
	 * does not correspond to a base relation, such as a join RTE or an
	 * unreferenced view RTE; or if the RelOptInfo hasn't been made yet.
	 */
	//TODO kindred
    //struct RelOptInfo ** simple_rel_array; /* All 1-rel RelOptInfos */
    int simple_rel_array_size; /* allocated size of array */

    /*
	 * simple_rte_array is the same length as simple_rel_array and holds
	 * pointers to the associated rangetable entries.  This lets us avoid
	 * rt_fetch(), which can be a bit slow once large inheritance sets have
	 * been expanded.
	 */
    duckdb_libpgquery::PGRangeTblEntry ** simple_rte_array; /* rangetable as an array */

    /*
	 * all_baserels is a Relids set of all base relids (but not "other"
	 * relids) in the query; that is, the Relids identifier of the final join
	 * we need to form.  This is computed in make_one_rel, just before we
	 * start making Paths.
	 */
    PGRelids all_baserels;

    /*
	 * nullable_baserels is a Relids set of base relids that are nullable by
	 * some outer join in the jointree; these are rels that are potentially
	 * nullable below the WHERE clause, SELECT targetlist, etc.  This is
	 * computed in deconstruct_jointree.
	 */
    PGRelids nullable_baserels;

    /*
	 * join_rel_list is a list of all join-relation RelOptInfos we have
	 * considered in this planning run.  For small problems we just scan the
	 * list to do lookups, but when there are many join relations we build a
	 * hash table for faster lookups.  The hash table is present and valid
	 * when join_rel_hash is not NULL.  Note that we still maintain the list
	 * even when using the hash table for lookups; this simplifies life for
	 * GEQO.
	 */
    duckdb_libpgquery::PGList * join_rel_list; /* list of join-relation RelOptInfos */
	//TODO kindred
    //struct HTAB * join_rel_hash; /* optional hashtable for join relations */

    /*
	 * When doing a dynamic-programming-style join search, join_rel_level[k]
	 * is a list of all join-relation RelOptInfos of level k, and
	 * join_cur_level is the current level.  New join-relation RelOptInfos are
	 * automatically added to the join_rel_level[join_cur_level] list.
	 * join_rel_level is NULL if not in use.
	 */
    duckdb_libpgquery::PGList ** join_rel_level; /* lists of join-relation RelOptInfos */
    int join_cur_level; /* index of list being extended */

    duckdb_libpgquery::PGList * init_plans; /* init SubPlans for query */

    duckdb_libpgquery::PGList * cte_plan_ids; /* per-CTE-item list of subplan IDs */

    duckdb_libpgquery::PGList * eq_classes; /* list of active EquivalenceClasses */

    duckdb_libpgquery::PGList * non_eq_clauses; /* list of non-equivalence clauses */

    duckdb_libpgquery::PGList * canon_pathkeys; /* list of "canonical" PathKeys */

    //TODO kindred
    //PartitionNode * result_partitions;
    duckdb_libpgquery::PGList * result_aosegnos;

    duckdb_libpgquery::PGList * list_cteplaninfo; /* list of CtePlannerInfo, one for each CTE */

    /*
	 * Outer join info
	 */
    duckdb_libpgquery::PGList * left_join_clauses; /* list of RestrictInfos for
										 * mergejoinable outer join clauses
										 * w/nonnullable var on left */

    duckdb_libpgquery::PGList * right_join_clauses; /* list of RestrictInfos for
										 * mergejoinable outer join clauses
										 * w/nonnullable var on right */

    duckdb_libpgquery::PGList * full_join_clauses; /* list of RestrictInfos for
										 * mergejoinable full join clauses */

    duckdb_libpgquery::PGList * join_info_list; /* list of SpecialJoinInfos */

    duckdb_libpgquery::PGList * lateral_info_list; /* list of LateralJoinInfos */

    duckdb_libpgquery::PGList * append_rel_list; /* list of AppendRelInfos */

    duckdb_libpgquery::PGList * rowMarks; /* list of PlanRowMarks */

    duckdb_libpgquery::PGList * placeholder_list; /* list of PlaceHolderInfos */

    duckdb_libpgquery::PGList * query_pathkeys; /* desired pathkeys for query_planner(), and
								 * actual pathkeys after planning */

    duckdb_libpgquery::PGList * group_pathkeys; /* groupClause pathkeys, if any */
    duckdb_libpgquery::PGList * window_pathkeys; /* pathkeys of bottom window, if any */
    duckdb_libpgquery::PGList * distinct_pathkeys; /* distinctClause pathkeys, if any */
    duckdb_libpgquery::PGList * sort_pathkeys; /* sortClause pathkeys, if any */

    duckdb_libpgquery::PGList * minmax_aggs; /* List of MinMaxAggInfos */

    duckdb_libpgquery::PGList * initial_rels; /* RelOptInfos we are now trying to join */

    //TODO kindred
    //MemoryContext planner_cxt; /* context holding PlannerInfo */

    double total_table_pages; /* # of pages in all tables of query */

    double tuple_fraction; /* tuple_fraction passed to query_planner */
    double limit_tuples; /* limit_tuples passed to query_planner */

    bool hasInheritedTarget; /* true if parse->resultRelation is an
										 * inheritance child rel */
    bool hasJoinRTEs; /* true if any RTEs are RTE_JOIN kind */
    bool hasLateralRTEs; /* true if any RTEs are marked LATERAL */
    bool hasHavingQual; /* true if havingQual was non-null */
    bool hasPseudoConstantQuals; /* true if any RestrictInfo has
										 * pseudoconstant = true */
    bool hasRecursion; /* true if planning a recursive WITH item */

    /* These fields are used only when hasRecursion is true: */
    int wt_param_id; /* PARAM_EXEC ID for the work table */
    struct PGPlan * non_recursive_plan; /* plan for non-recursive term */

    /* These fields are workspace for createplan.c */
    PGRelids curOuterRels; /* outer rels above current node */
    duckdb_libpgquery::PGList * curOuterParams; /* not-yet-assigned NestLoopParams */

    PGPlannerConfig * config; /* Planner configuration */

    duckdb_libpgquery::PGList * dynamicScans; /* DynamicScanInfos */

    /* optional private data for join_search_hook, e.g., GEQO */
    void * join_search_private;

    int upd_del_replicated_table;
    bool is_split_update; /* true if UPDATE that modifies
									 * distribution key columns */
    bool is_correlated_subplan; /* true for correlated subqueries nested within subplans */
    bool disallow_unique_rowid_path; /* true if we decide not to generate unique rowid path */
};

static inline Datum BoolGetDatum(bool b) { return (b ? 1 : 0); } 


static inline int32 DatumGetInt32(Datum d) { return (int32) d; };
static inline Datum Int16GetDatum(int16 i16) { return (Datum) i16; } 
static inline Datum Int32GetDatum(int32 i32) { return (Datum) i32; };
static inline Datum Int64GetDatum(int64 i64) { return (Datum) i64; };

#define DatumGetPointer(X) ((Pointer) (X))
#define PointerGetDatum(X) ((Datum) (X))

static inline Datum CStringGetDatum(const char *p) { return PointerGetDatum(p); };
static inline Datum ObjectIdGetDatum(Oid oid) { return (Datum) oid; } ;
static inline char *DatumGetCString(Datum d) { return (char* ) DatumGetPointer(d); };

static const int oldprecedence_l[] = {
	0, 10, 10, 3, 2, 8, 4, 5, 6, 4, 5, 6, 7, 8, 9
};
static const int oldprecedence_r[] = {
	0, 10, 10, 3, 2, 8, 4, 5, 6, 1, 1, 1, 7, 8, 9
};

#define rt_fetch(rangetable_index, rangetable) \
	((duckdb_libpgquery::PGRangeTblEntry *) list_nth(rangetable, (rangetable_index)-1))

#define foreach_with_count(cell, list, counter) \
	for ((cell) = list_head(list), (counter)=0; \
	     (cell) != NULL; \
	     (cell) = lnext(cell), ++(counter))

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

	//TODO kindred
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

std::string PGNameListToString(duckdb_libpgquery::PGList * names)
{
	using duckdb_libpgquery::elog;
	using duckdb_libpgquery::PGListCell;
	using duckdb_libpgquery::PGNode;
	using duckdb_libpgquery::T_PGString;
	using duckdb_libpgquery::PGAStar;
	using duckdb_libpgquery::PGValue;
	using duckdb_libpgquery::T_PGAStar;

    std::string result = "";
    PGListCell * l;

    foreach (l, names)
    {
        PGNode * name = (PGNode *)lfirst(l);

        if (l != list_head(names))
		{
            //appendStringInfoChar(&string, '.');
			result += ".";
		}

        if (IsA(name, PGString))
		{
            //appendStringInfoString(&string, strVal(name));
			result += std::string(strVal(name));
		}
        else if (IsA(name, PGAStar))
		{
            //appendStringInfoString(&string, "*");
			result += "*";
		}
        else
            elog(ERROR, "unexpected node type in name list: %d", (int)nodeTag(name));
    }

    return result;
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
                (errcode(ERRCODE_SYNTAX_ERROR), errmsg("improper qualified name (too many dotted names): %s", PGNameListToString(names).c_str())));
            break;
    }

    *nspname_p = schemaname;
    *objname_p = objname;
};

struct grouped_window_ctx
{
    duckdb_libpgquery::PGList * subtlist; /* target list for subquery */
    duckdb_libpgquery::PGList * subgroupClause; /* group clause for subquery */
    duckdb_libpgquery::PGList * windowClause; /* window clause for outer query*/

    /* Scratch area for init_grouped_window context and map_sgr_mutator.
	 */
    Index * sgr_map;

    /* Scratch area for grouped_window_mutator and var_for_gw_expr.
	 */
    duckdb_libpgquery::PGList * subrtable;
    int call_depth;
    duckdb_libpgquery::PGTargetEntry * tle;
};

struct RelFileNode
{
	Oid			spcNode;		/* tablespace */
	Oid			dbNode;			/* database */
	Oid			relNode;		/* relation */
};

typedef int BackendId;

typedef uint32 SubTransactionId;

typedef struct LockRelId
{
	Oid			relId;			/* a relation identifier */
	Oid			dbId;			/* a database identifier */
} LockRelId;

typedef struct LockInfoData
{
	LockRelId	lockRelId;
} LockInfoData;

/*
 * GpPolicyType represents a type of policy under which a relation's
 * tuples may be assigned to a component database.
 */
typedef enum
{
    POLICYTYPE_PARTITIONED, /* Tuples partitioned onto segment database. */
    POLICYTYPE_ENTRY, /* Tuples stored on entry database. */
    POLICYTYPE_REPLICATED /* Tuples stored a copy on all segment database. */
} PGPolicyType;

typedef enum
{
    /* the assigned enum values appear in pg_proc, don't change 'em! */
    PG_FUNC_PARAM_IN = 'i', /* input only */
    PG_FUNC_PARAM_OUT = 'o', /* output only */
    PG_FUNC_PARAM_INOUT = 'b', /* both */
    PG_FUNC_PARAM_VARIADIC = 'v', /* variadic (always input) */
    PG_FUNC_PARAM_TABLE = 't' /* table function output column */
} PGFunctionParameterMode;

/*
 * GpPolicy represents a Greenplum DB data distribution policy. The ptype field
 * is always significant.  Other fields may be specific to a particular
 * type.
 *
 * A GpPolicy is typically palloc'd with space for nattrs integer
 * attribute numbers (attrs) in addition to sizeof(GpPolicy).
 */
struct PGPolicy
{
    duckdb_libpgquery::PGNodeTag type;
    PGPolicyType ptype;
    int numsegments;

    /* These fields apply to POLICYTYPE_PARTITIONED. */
    int nattrs;
    PGAttrNumber * attrs; /* array of attribute numbers  */
    Oid * opclasses; /* and their opclasses */
};

using PGPolicyPtr = std::shared_ptr<PGPolicy>;

namespace DB
{
    class IStorage;
    using StoragePtr = std::shared_ptr<IStorage>;
};


struct PGRelation
{
    Oid oid;
    DB::StoragePtr storage_ptr;
    RelFileNode rd_node; /* relation physical identifier */
    /* use "struct" here to avoid needing to include smgr.h: */
	//TODO kindred
    //struct SMgrRelationData * rd_smgr; /* cached file handle, or NULL */
    int rd_refcnt; /* reference count */
    BackendId rd_backend; /* owning backend id, if temporary relation */
    bool rd_islocaltemp; /* rel is a temp rel of this session */
    bool rd_isnailed; /* rel is nailed in cache */
    bool rd_isvalid; /* relcache entry is valid */
    char rd_indexvalid; /* state of rd_indexlist: 0 = not valid, 1 =
								 * valid, 2 = temporarily forced */

    /*
	 * rd_createSubid is the ID of the highest subtransaction the rel has
	 * survived into; or zero if the rel was not created in the current top
	 * transaction.  This can be now be relied on, whereas previously it could
	 * be "forgotten" in earlier releases. Likewise, rd_newRelfilenodeSubid is
	 * the ID of the highest subtransaction the relfilenode change has
	 * survived into, or zero if not changed in the current transaction (or we
	 * have forgotten changing it). rd_newRelfilenodeSubid can be forgotten
	 * when a relation has multiple new relfilenodes within a single
	 * transaction, with one of them occuring in a subsequently aborted
	 * subtransaction, e.g. BEGIN; TRUNCATE t; SAVEPOINT save; TRUNCATE t;
	 * ROLLBACK TO save; -- rd_newRelfilenode is now forgotten
	 */
    SubTransactionId rd_createSubid; /* rel was created in current xact */
    SubTransactionId rd_newRelfilenodeSubid; /* new relfilenode assigned in
												 * current xact */

    PGClassPtr rd_rel; /* RELATION tuple */
    PGTupleDescPtr rd_att; /* tuple descriptor */
    Oid rd_id; /* relation's object id */
    LockInfoData rd_lockInfo; /* lock mgr's info for locking relation */
    //RuleLock * rd_rules; /* rewrite rules */
    //MemoryContext rd_rulescxt; /* private memory cxt for rd_rules, if any */
    //TriggerDesc * trigdesc; /* Trigger info, or NULL if rel has none */
    PGPolicyPtr rd_cdbpolicy; /* Partitioning info if distributed rel */
    bool rd_cdbDefaultStatsWarningIssued;

    /* data managed by RelationGetIndexList: */
    duckdb_libpgquery::PGList * rd_indexlist; /* list of OIDs of indexes on relation */
    Oid rd_oidindex; /* OID of unique index on OID, if any */
    Oid rd_replidindex; /* OID of replica identity index, if any */

    /* data managed by RelationGetIndexAttrBitmap: */
    duckdb_libpgquery::PGBitmapset * rd_indexattr; /* identifies columns used in indexes */
    duckdb_libpgquery::PGBitmapset * rd_keyattr; /* cols that can be ref'd by foreign keys */
    duckdb_libpgquery::PGBitmapset * rd_idattr; /* included in replica identity index */

    /*
	 * rd_options is set whenever rd_rel is loaded into the relcache entry.
	 * Note that you can NOT look into rd_rel for this data.  NULL means "use
	 * defaults".
	 */
    bytea * rd_options; /* parsed pg_class.reloptions */

    /* These are non-NULL only for an index relation: */
    //Form_pg_index rd_index; /* pg_index tuple describing this index */
    /* use "struct" here to avoid needing to include htup.h: */
    //struct HeapTupleData * rd_indextuple; /* all of pg_index tuple */
    //Form_pg_am rd_am; /* pg_am tuple for index's AM */

    /*
	 * index access support info (used only for an index relation)
	 *
	 * Note: only default support procs for each opclass are cached, namely
	 * those with lefttype and righttype equal to the opclass's opcintype. The
	 * arrays are indexed by support function number, which is a sufficient
	 * identifier given that restriction.
	 *
	 * Note: rd_amcache is available for index AMs to cache private data about
	 * an index.  This must be just a cache since it may get reset at any time
	 * (in particular, it will get reset by a relcache inval message for the
	 * index).  If used, it must point to a single memory chunk palloc'd in
	 * rd_indexcxt.  A relcache reset will include freeing that chunk and
	 * setting rd_amcache = NULL.
	 */
    //MemoryContext rd_indexcxt; /* private memory cxt for this stuff */
    //RelationAmInfo * rd_aminfo; /* lookup info for funcs found in pg_am */
    Oid * rd_opfamily; /* OIDs of op families for each index col */
    Oid * rd_opcintype; /* OIDs of opclass declared input data types */
    //RegProcedure * rd_support; /* OIDs of support procedures */
    //FmgrInfo * rd_supportinfo; /* lookup info for support procedures */
    int16 * rd_indoption; /* per-column AM-specific flags */
    duckdb_libpgquery::PGList * rd_indexprs; /* index expression trees, if any */
    duckdb_libpgquery::PGList * rd_indpred; /* index predicate tree, if any */
    Oid * rd_exclops; /* OIDs of exclusion operators, if any */
    Oid * rd_exclprocs; /* OIDs of exclusion ops' procs, if any */
    uint16 * rd_exclstrats; /* exclusion ops' strategy numbers, if any */
    void * rd_amcache; /* available for use by index AM */
    Oid * rd_indcollation; /* OIDs of index collations */

    /*
	 * foreign-table support
	 *
	 * rd_fdwroutine must point to a single memory chunk palloc'd in
	 * CacheMemoryContext.  It will be freed and reset to NULL on a relcache
	 * reset.
	 */

    /* use "struct" here to avoid needing to include fdwapi.h: */
    //struct FdwRoutine * rd_fdwroutine; /* cached function pointers, or NULL */

    /*
	 * Hack for CLUSTER, rewriting ALTER TABLE, etc: when writing a new
	 * version of a table, we need to make any toast pointers inserted into it
	 * have the existing toast table's OID, not the OID of the transient toast
	 * table.  If rd_toastoid isn't InvalidOid, it is the OID to place in
	 * toast pointers inserted into this rel.  (Note it's set on the new
	 * version of the main heap, not the toast table itself.)  This also
	 * causes toast_save_datum() to try to preserve toast value OIDs.
	 */
    Oid rd_toastoid; /* Real TOAST table's OID, or InvalidOid */

    /*
	 * AO table support info (used only for AO and AOCS relations)
	 */
    //Form_pg_appendonly rd_appendonly;
    //struct HeapTupleData * rd_aotuple; /* all of pg_appendonly tuple */

    /* use "struct" here to avoid needing to include pgstat.h: */
    //struct PgStat_TableStatus * pgstat_info; /* statistics collection area */
};

using PGRelationPtr = std::shared_ptr<PGRelation>;


struct PGDatabase
{
    Oid oid;
    String name;
};

using PGDatabasePtr = std::shared_ptr<PGDatabase>;

#define RelationGetRelationName(relation) \
	((relation)->rd_rel->relname.c_str())

duckdb_libpgquery::PGList * stringToQualifiedNameList(const char * string)
{
	using duckdb_libpgquery::ereport;
	using duckdb_libpgquery::errcode;
	using duckdb_libpgquery::errmsg;
	using duckdb_libpgquery::pstrdup;
	using duckdb_libpgquery::lappend;
	using duckdb_libpgquery::PGList;
	using duckdb_libpgquery::PGListCell;

    PGList * result = NULL;

	std::string rawname = std::string(string);

	std::vector<std::string> vecSegTag;
	boost::split(vecSegTag, rawname, boost::is_any_of(","));

	if (vecSegTag.size() == 0)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_NAME), errmsg("invalid name syntax")));
	}

    for (auto l : vecSegTag)
    {
        result = lappend(result, makeString(pstrdup(l.c_str())));
    }

    return result;
};

duckdb_libpgquery::PGRangeVar * makeRangeVarFromNameList(duckdb_libpgquery::PGList * names)
{
	using duckdb_libpgquery::ereport;
	using duckdb_libpgquery::errcode;
	using duckdb_libpgquery::errmsg;
	using duckdb_libpgquery::PGValue;
	using duckdb_libpgquery::makeRangeVar;

    duckdb_libpgquery::PGRangeVar * rel = makeRangeVar(NULL, NULL, -1);

    switch (list_length(names))
    {
        case 1:
            rel->relname = strVal(linitial(names));
            break;
        case 2:
            rel->schemaname = strVal(linitial(names));
            rel->relname = strVal(lsecond(names));
            break;
        case 3:
            rel->catalogname = strVal(linitial(names));
            rel->schemaname = strVal(lsecond(names));
            rel->relname = strVal(lthird(names));
            break;
        default:
            ereport(
                ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR), errmsg("improper relation name (too many dotted names): %s", NameListToString(names))));
            break;
    }

    return rel;
};

int pg_strcasecmp(const char * s1, const char * s2)
{
    for (;;)
    {
        unsigned char ch1 = (unsigned char)*s1++;
        unsigned char ch2 = (unsigned char)*s2++;

        if (ch1 != ch2)
        {
            if (ch1 >= 'A' && ch1 <= 'Z')
                ch1 += 'a' - 'A';
            else if (IS_HIGHBIT_SET(ch1) && isupper(ch1))
                ch1 = tolower(ch1);

            if (ch2 >= 'A' && ch2 <= 'Z')
                ch2 += 'a' - 'A';
            else if (IS_HIGHBIT_SET(ch2) && isupper(ch2))
                ch2 = tolower(ch2);

            if (ch1 != ch2)
                return (int)ch1 - (int)ch2;
        }
        if (ch1 == 0)
            break;
    }
    return 0;
};
