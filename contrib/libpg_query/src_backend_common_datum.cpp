/*--------------------------------------------------------------------
 * Symbols referenced in this file:
 * - datumGetSize
 * - datumIsEqual
 *--------------------------------------------------------------------
 */
#include "common/common_datum.hpp"
#include "common/common_def.hpp"
#include "common/common_macro.hpp"
#include "pg_functions.hpp"

namespace duckdb_libpgquery {

Size datumGetSize(Datum value, bool typByVal, int typLen)
{
    //using duckdb_libpgquery::ereport;
    //using duckdb_libpgquery::errcode;
    //using duckdb_libpgquery::errmsg;
    //using duckdb_libpgquery::elog;

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
                ereport(ERROR, (errcode(PG_ERRCODE_DATA_EXCEPTION), errmsg("invalid Datum pointer")));

            size = (Size)VARSIZE_ANY(s);
        }
        else if (typLen == -2)
        {
            /* It is a cstring datatype */
            char * s = (char *)DatumGetPointer(value);

            if (!PointerIsValid(s))
                ereport(ERROR, (errcode(PG_ERRCODE_DATA_EXCEPTION), errmsg("invalid Datum pointer")));

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

}