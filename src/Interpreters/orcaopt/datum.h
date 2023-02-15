#pragma once

#include <Interpreters/orcaopt/parser_common.h>
#include<netinet/in.h>

/*
 * struct varatt_external is a traditional "TOAST pointer", that is, the
 * information needed to fetch a Datum stored out-of-line in a TOAST table.
 * The data is compressed if and only if va_extsize < va_rawsize - VARHDRSZ.
 * This struct must not contain any padding, because we sometimes compare
 * these pointers using memcmp.
 *
 * Note that this information is stored unaligned within actual tuples, so
 * you need to memcpy from the tuple into a local struct variable before
 * you can look at these fields!  (The reason we use memcmp is to avoid
 * having to do that just to detect equality of two TOAST pointers...)
 */
typedef struct varatt_external
{
	int32		va_rawsize;		/* Original data size (includes header) */
	int32		va_extsize;		/* External saved size (doesn't) */
	Oid			va_valueid;		/* Unique ID of value within TOAST table */
	Oid			va_toastrelid;	/* RelID of TOAST table containing it */
}	varatt_external;

/*
 * struct varatt_indirect is a "TOAST pointer" representing an out-of-line
 * Datum that's stored in memory, not in an external toast relation.
 * The creator of such a Datum is entirely responsible that the referenced
 * storage survives for as long as referencing pointer Datums can exist.
 *
 * Note that just as for struct varatt_external, this struct is stored
 * unaligned within any containing tuple.
 */
typedef struct varatt_indirect
{
	struct varlena *pointer;	/* Pointer to in-memory varlena */
}	varatt_indirect;

/*
 * Type tag for the various sorts of "TOAST pointer" datums.  The peculiar
 * value for VARTAG_ONDISK comes from a requirement for on-disk compatibility
 * with a previous notion that the tag field was the pointer datum's length.
 *
 * GPDB: In PostgreSQL VARTAG_ONDISK is set to 18 in order to match the
 * historic (VARHDRSZ_EXTERNAL + sizeof(struct varatt_external)) value of the
 * pointer datum's length. In Greenplum VARHDRSZ_EXTERNAL is two bytes longer
 * than PostgreSQL due to extra padding in varattrib_1b_e, so VARTAG_ONDISK has
 * to be set to 20.
 */
typedef enum vartag_external
{
	VARTAG_INDIRECT = 1,
	VARTAG_ONDISK = 20
} vartag_external;

#define TrapMacro(condition, errorType) (true)

#define VARTAG_SIZE(tag) \
	((tag) == VARTAG_INDIRECT ? sizeof(varatt_indirect) : \
	 (tag) == VARTAG_ONDISK ? sizeof(varatt_external) : \
	 TrapMacro(true, "unrecognized TOAST vartag"))

typedef struct
{
    uint8 va_header;
    char va_data[1]; /* Data begins here */
} varattrib_1b;

typedef struct
{
    uint8 va_header; /* Always 0x80  */
    uint8 va_tag; /* Type of datum */
    uint8 va_padding[2]; /*** GPDB only:  Alignment padding ***/
    char va_data[1]; /* Data (of the type indicated by va_tag) */
} varattrib_1b_e;

typedef union
{
    struct /* Normal varlena (4-byte length) */
    {
        uint32 va_header;
        char va_data[1];
    } va_4byte;
    struct /* Compressed-in-line format */
    {
        uint32 va_header;
        uint32 va_rawsize; /* Original data size (excludes header) */
        char va_data[1]; /* Compressed data */
    } va_compressed;
} varattrib_4b;

#define VARHDRSZ_EXTERNAL		offsetof(varattrib_1b_e, va_data)

#define VARATT_IS_1B_E(PTR) \
	((((varattrib_1b *) (PTR))->va_header) == 0x80)
#define VARATT_IS_1B(PTR) \
	((((varattrib_1b *) (PTR))->va_header & 0x80) == 0x80)
#define VARSIZE_1B(PTR) \
	(((varattrib_1b *) (PTR))->va_header & 0x7F)
#define VARSIZE_4B(PTR) \
	(ntohl(((varattrib_4b *) (PTR))->va_4byte.va_header) & 0x3FFFFFFF)
#define VARTAG_1B_E(PTR) \
	(((varattrib_1b_e *) (PTR))->va_tag)

#define VARTAG_EXTERNAL(PTR)				VARTAG_1B_E(PTR)
#define VARSIZE_EXTERNAL(PTR) (VARHDRSZ_EXTERNAL + VARTAG_SIZE(VARTAG_EXTERNAL(PTR)))

#define VARSIZE_ANY(PTR) \
	(VARATT_IS_1B_E(PTR) ? VARSIZE_EXTERNAL(PTR) : \
	 (VARATT_IS_1B(PTR) ? VARSIZE_1B(PTR) : \
	  VARSIZE_4B(PTR)))

#define SET_VARSIZE_4B(PTR,len) \
	(((varattrib_4b *) (PTR))->va_4byte.va_header = htonl( (len) & 0x3FFFFFFF ))

#define SET_VARSIZE(PTR, len)				SET_VARSIZE_4B(PTR, len)

typedef size_t Size;

struct varlena
{
    char vl_len_[4]; /* Do not touch this field directly! */
    char vl_dat[1];
};

Size datumGetSize(Datum value, bool typByVal, int typLen)
{
    using duckdb_libpgquery::ereport;
    using duckdb_libpgquery::errcode;
    using duckdb_libpgquery::errmsg;
    using duckdb_libpgquery::elog;

    Size size;

    if (typByVal)
    {
        /* Pass-by-value types are always fixed-length */
        Assert(typLen > 0 && typLen <= sizeof(Datum))
        size = (Size)typLen;
    }
    else
    {
        if (typLen > 0)
        {
            /* Fixed-length pass-by-ref type */
            size = (Size)typLen;
        }
        else if (typLen == -1)
        {
            /* It is a varlena datatype */
            struct varlena * s = (struct varlena *)DatumGetPointer(value);

            if (!PointerIsValid(s))
                ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("invalid Datum pointer")));

            size = (Size)VARSIZE_ANY(s);
        }
        else if (typLen == -2)
        {
            /* It is a cstring datatype */
            char * s = (char *)DatumGetPointer(value);

            if (!PointerIsValid(s))
                ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("invalid Datum pointer")));

            size = (Size)(strlen(s) + 1);
        }
        else
        {
            elog(ERROR, "invalid typLen: %d", typLen);
            size = 0; /* keep compiler quiet */
        }
    }

    return size;
};

bool datumIsEqual(Datum value1, Datum value2, bool typByVal, int typLen)
{
    bool res;

    if (typByVal)
    {
        /*
		 * just compare the two datums. NOTE: just comparing "len" bytes will
		 * not do the work, because we do not know how these bytes are aligned
		 * inside the "Datum".  We assume instead that any given datatype is
		 * consistent about how it fills extraneous bits in the Datum.
		 */
        res = (value1 == value2);
    }
    else
    {
        Size size1, size2;
        char *s1, *s2;

        /*
		 * Compare the bytes pointed by the pointers stored in the datums.
		 */
        size1 = datumGetSize(value1, typByVal, typLen);
        size2 = datumGetSize(value2, typByVal, typLen);
        if (size1 != size2)
            return false;
        s1 = (char *)DatumGetPointer(value1);
        s2 = (char *)DatumGetPointer(value2);
        res = (memcmp(s1, s2, size1) == 0);
    }
    return res;
};
