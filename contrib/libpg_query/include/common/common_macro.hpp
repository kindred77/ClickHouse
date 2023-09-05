#pragma once

#define PGFLOAT8PASSBYVAL true

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
#define INT2OID			21
#define BPCHAROID		1042
#define FLOAT4OID 700
#define FLOAT8OID 701
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
#define TIMEOID			1083
#define TIMETZOID		1266
#define TIMESTAMPOID	1114
#define TIMESTAMPTZOID	1184
#define VARBITOID	  1562
#define VARCHAROID		1043

/* Is a type OID a polymorphic pseudotype?	(Beware of multiple evaluation) */
#define IsPolymorphicType(typid)  \
	((typid) == ANYELEMENTOID || \
	 (typid) == ANYARRAYOID || \
	 (typid) == ANYNONARRAYOID || \
	 (typid) == ANYENUMOID || \
	 (typid) == ANYRANGEOID)

#define OidIsValid(objectId)  ((bool) ((objectId) != InvalidOid))

/*
 * Symbolic values for prokind column
 */
#define PG_PROKIND_FUNCTION 'f'
#define PG_PROKIND_AGGREGATE 'a'
#define PG_PROKIND_WINDOW 'w'
#define PG_PROKIND_PROCEDURE 'p'

#define NAMEDATALEN 64

#define DEFAULT_COLLATION_OID	100

#define NAMEDATALEN 64
#define MAX_CACHED_PATH_LEN		16

#define PointerIsValid(pointer) ((const void*)(pointer) != NULL)

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

#define INT64CONST(x)  (x##L)

#define StrNCpy(dst,src,len) \
	do \
	{ \
		char * _dst = (dst); \
		size_t _len = (len); \
\
		if (_len > 0) \
		{ \
			strncpy(_dst, (src), _len); \
			_dst[_len-1] = '\0'; \
		} \
	} while (0)

/* NoLock is not a lock mode, but a flag value meaning "don't get a lock" */
#define NoLock					0

#define AccessShareLock			1		/* SELECT */
#define RowShareLock			2		/* SELECT FOR UPDATE/FOR SHARE */
#define RowExclusiveLock		3		/* INSERT, UPDATE, DELETE */
#define ShareUpdateExclusiveLock 4		/* VACUUM (non-FULL),ANALYZE, CREATE
										 * INDEX CONCURRENTLY */
#define ShareLock				5		/* CREATE INDEX (WITHOUT CONCURRENTLY) */
#define ShareRowExclusiveLock	6		/* like EXCLUSIVE MODE, but allows ROW
										 * SHARE */
#define ExclusiveLock			7		/* blocks ROW SHARE/SELECT...FOR
										 * UPDATE */
#define AccessExclusiveLock		8		/* ALTER TABLE, DROP TABLE, VACUUM
										 * FULL, and unqualified LOCK TABLE */

#define PG_RELKIND_RELATION 'r' /* ordinary table */
#define PG_RELKIND_INDEX 'i' /* secondary index */
#define PG_RELKIND_SEQUENCE 'S' /* sequence object */
#define PG_RELKIND_TOASTVALUE 't' /* for out-of-line values */
#define PG_RELKIND_VIEW 'v' /* view */
#define PG_RELKIND_COMPOSITE_TYPE 'c' /* composite type */
#define PG_RELKIND_FOREIGN_TABLE 'f' /* foreign table */
#define PG_RELKIND_UNCATALOGED 'u' /* not yet cataloged */
#define PG_RELKIND_MATVIEW 'm' /* materialized view */
#define PG_RELKIND_AOSEGMENTS 'o' /* AO segment files and eof's */
#define PG_RELKIND_AOBLOCKDIR 'b' /* AO block directory */
#define PG_RELKIND_AOVISIMAP 'M' /* AO visibility map */

#define PG_RELPERSISTENCE_PERMANENT_CHR 'p' /* regular table */
#define PG_RELPERSISTENCE_UNLOGGED_CHR 'u' /* unlogged permanent table */
#define PG_RELPERSISTENCE_TEMP_CHR 't' /* temporary table */

/*
 * relstorage describes how a relkind is physically stored in the database.
 *
 * RELSTORAGE_HEAP    - stored on disk using heap storage.
 * RELSTORAGE_AOROWS  - stored on disk using append only storage.
 * RELSTORAGE_AOCOLS  - stored on dist using append only column storage.
 * RELSTORAGE_VIRTUAL - has virtual storage, meaning, relation has no
 *						data directly stored forit  (right now this
 *						relates to views and comp types).
 * RELSTORAGE_EXTERNAL-	stored externally using external tables.
 * RELSTORAGE_FOREIGN - stored in another server.  
 */
#define		  PG_RELSTORAGE_HEAP	'h'
#define		  PG_RELSTORAGE_AOROWS	'a'
#define 	  PG_RELSTORAGE_AOCOLS	'c'
#define		  PG_RELSTORAGE_VIRTUAL	'v'
#define		  PG_RELSTORAGE_EXTERNAL 'x'
#define		  PG_RELSTORAGE_FOREIGN 'f'

#define GP_DIST_RANDOM_NAME "GP_DIST_RANDOM"

#define QTW_EXAMINE_SORTGROUP 0x80 /* include SortGroupNode lists */

/*
 * Symbolic values for proargmodes column.  Note that these must agree with
 * the FunctionParameterMode enum in parsenodes.h; we declare them here to
 * be accessible from either header.
 */
#define PG_PROARGMODE_IN 'i'
#define PG_PROARGMODE_OUT 'o'
#define PG_PROARGMODE_INOUT 'b'
#define PG_PROARGMODE_VARIADIC 'v'
#define PG_PROARGMODE_TABLE 't'

#define PG_PARENTSTMTTYPE_NONE	0
#define PG_PARENTSTMTTYPE_CTAS	1
#define PG_PARENTSTMTTYPE_COPY	2
#define PG_PARENTSTMTTYPE_REFRESH_MATVIEW	3

#define PGVARHDRSZ ((int32)sizeof(int32))

/*
 * Attribute numbers for the system-defined attributes
 */
#define SelfItemPointerAttributeNumber (-1)
#define ObjectIdAttributeNumber (-2)
#define MinTransactionIdAttributeNumber (-3)
#define MinCommandIdAttributeNumber (-4)
#define MaxTransactionIdAttributeNumber (-5)
#define MaxCommandIdAttributeNumber (-6)
#define TableOidAttributeNumber (-7)
#define GpSegmentIdAttributeNumber (-8) /*CDB*/
#define FirstLowInvalidHeapAttributeNumber (-9)

#define PG_PROEXECLOCATION_ANY 'a'
#define PG_PROEXECLOCATION_MASTER 'm'
#define PG_PROEXECLOCATION_INITPLAN 'i'
#define PG_PROEXECLOCATION_ALL_SEGMENTS 's'

#define PG_STATISTIC_KIND_MCV 1
#define PG_STATISTIC_KIND_HISTOGRAM 2

/* Flag bits for get_attstatsslot */
#define PG_ATTSTATSSLOT_VALUES		0x01
#define PG_ATTSTATSSLOT_NUMBERS	0x02

#define PG_BTREE_AM_OID 403
#define PG_BITMAP_AM_OID 7013
#define PG_GIST_AM_OID 783
#define PG_GIN_AM_OID 2742

#define PG_COUNT_ANY_OID 2147

/*
 * Interpretation of high bits.
 */
#define PG_NUMERIC_SIGN_MASK	0xC000
#define PG_NUMERIC_POS			0x0000
#define PG_NUMERIC_NEG			0x4000
#define PG_NUMERIC_SHORT		0x8000
#define PG_NUMERIC_NAN			0xC000

#define PG_NUMERIC_FLAGBITS(n) ((n)->choice.n_header & PG_NUMERIC_SIGN_MASK)
#define PG_NUMERIC_IS_NAN(n)		(PG_NUMERIC_FLAGBITS(n) == PG_NUMERIC_NAN)
#define PG_NUMERIC_IS_SHORT(n)		(PG_NUMERIC_FLAGBITS(n) == PG_NUMERIC_SHORT)
