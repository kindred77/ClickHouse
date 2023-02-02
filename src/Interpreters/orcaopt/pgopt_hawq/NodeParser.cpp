#include <Interpreters/orcaopt/pgopt_hawq/NodeParser.h>

namespace DB
{
using namespace duckdb_libpgquery;

PGConst * NodeParser::make_const(PGParseState * pstate,
		PGValue * value, int location)
{
    Datum val;
    int64 val64;
    Oid typeid;
    int typelen;
    bool typebyval;
    PGConst * con;

    switch (nodeTag(value))
    {
        case T_PGInteger:
            val = Int32GetDatum(intVal(value));

            typeid = INT4OID;
            typelen = sizeof(int32);
            typebyval = true;
            break;

        case T_PGFloat:
            /* could be an oversize integer as well as a float ... */
            if (scanint8(strVal(value), true, &val64))
            {
                /*
				 * It might actually fit in int32. Probably only INT_MIN can
				 * occur, but we'll code the test generally just to be sure.
				 */
                int32 val32 = (int32)val64;

                if (val64 == (int64)val32)
                {
                    val = Int32GetDatum(val32);

                    typeid = INT4OID;
                    typelen = sizeof(int32);
                    typebyval = true;
                }
                else
                {
                    val = Int64GetDatum(val64);

                    typeid = INT8OID;
                    typelen = sizeof(int64);
                    typebyval = true; /* XXX might change someday */
                }
            }
            else
            {
                val = DirectFunctionCall3(numeric_in, CStringGetDatum(strVal(value)), ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1));

                typeid = NUMERICOID;
                typelen = -1; /* variable len */
                typebyval = false;
            }
            break;

        case T_PGString:

            /*
			 * We assume here that UNKNOWN's internal representation is the
			 * same as CSTRING
			 */
            val = CStringGetDatum(strVal(value));

            typeid = UNKNOWNOID; /* will be coerced later */
            typelen = -2; /* cstring-style varwidth type */
            typebyval = false;
            break;

        case T_PGBitString:
            val = DirectFunctionCall3(bit_in, CStringGetDatum(strVal(value)), ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1));
            typeid = BITOID;
            typelen = -1;
            typebyval = false;
            break;

        case T_PGNull:
            /* return a null const */
            con = makeConst(UNKNOWNOID, -1, -2, (Datum)0, true, false);
            return con;

        default:
            elog(ERROR, "unrecognized node type: %d", (int)nodeTag(value));
            return NULL; /* keep compiler quiet */
    }

    con = makeConst(typeid, -1, typelen, val, false, typebyval);

    return con;
};

PGParseState * NodeParser::make_parsestate(PGParseState * parentParseState)
{
    PGParseState * pstate;

    pstate = palloc0(sizeof(PGParseState));

    pstate->parentParseState = parentParseState;

    /* Fill in fields that don't start at null/false/zero */
    pstate->p_next_resno = 1;

    if (parentParseState)
    {
        pstate->p_sourcetext = parentParseState->p_sourcetext;
        pstate->p_variableparams = parentParseState->p_variableparams;
        pstate->p_setopTypes = parentParseState->p_setopTypes;
        pstate->p_setopTypmods = parentParseState->p_setopTypmods;
    }

    return pstate;
};

void NodeParser::free_parsestate(PGParseState ** pstate)
{
    if (pstate == NULL || *pstate == NULL)
        return; /* already freed? */

    if ((*pstate)->p_namecache)
        hash_destroy((*pstate)->p_namecache);

    pfree(*pstate);
    *pstate = NULL;
    return;
};

}