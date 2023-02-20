#pragma once

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