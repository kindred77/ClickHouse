#include <Interpreters/orcaopt/pgopt/NodeParser.h>

namespace DB
{
using namespace duckdb_libpgquery;

PGConst *
NodeParser::make_const(PGParseState *pstate, PGValue *value, int location)
{
	PGConst	   *con;
	PGDatum		val;
	int64		val64;
	Oid			type_id;
	int			typelen;
	bool		typebyval;
	ParseCallbackState pcbstate;

	switch (nodeTag(value))
	{
		case T_PGInteger:
			val = Int32GetDatum(intVal(value));

			type_id = INT4OID;
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
				int32		val32 = (int32) val64;

				if (val64 == (int64) val32)
				{
					val = Int32GetDatum(val32);

					type_id = INT4OID;
					typelen = sizeof(int32);
					typebyval = true;
				}
				else
				{
					val = Int64GetDatum(val64);

					type_id = INT8OID;
					typelen = sizeof(int64);
					typebyval = FLOAT8PASSBYVAL;		/* int8 and float8 alike */
				}
			}
			else
			{
				/* arrange to report location if numeric_in() fails */
				setup_parser_errposition_callback(&pcbstate, pstate, location);
				val = DirectFunctionCall3(numeric_in,
										  CStringGetDatum(strVal(value)),
										  ObjectIdGetDatum(InvalidOid),
										  Int32GetDatum(-1));
				cancel_parser_errposition_callback(&pcbstate);

				type_id = NUMERICOID;
				typelen = -1;	/* variable len */
				typebyval = false;
			}
			break;

		case T_PGString:

			/*
			 * We assume here that UNKNOWN's internal representation is the
			 * same as CSTRING
			 */
			val = CStringGetDatum(strVal(value));

			type_id = UNKNOWNOID;	/* will be coerced later */
			typelen = -2;		/* cstring-style varwidth type */
			typebyval = false;
			break;

		case T_PGBitString:
			/* arrange to report location if bit_in() fails */
			setup_parser_errposition_callback(&pcbstate, pstate, location);
			val = DirectFunctionCall3(bit_in,
									  CStringGetDatum(strVal(value)),
									  ObjectIdGetDatum(InvalidOid),
									  Int32GetDatum(-1));
			cancel_parser_errposition_callback(&pcbstate);
			type_id = BITOID;
			typelen = -1;
			typebyval = false;
			break;

		case T_PGNull:
			/* return a null const */
			con = makeConst(UNKNOWNOID,
							-1,
							InvalidOid,
							-2,
							(PGDatum) 0,
							true,
							false);
			con->location = location;
			return con;

		default:
			elog(ERROR, "unrecognized node type: %d", (int) nodeTag(value));
			return NULL;		/* keep compiler quiet */
	}

	con = makeConst(type_id,
					-1,			/* typmod -1 is OK for all cases */
					InvalidOid, /* all cases are uncollatable types */
					typelen,
					val,
					false,
					typebyval);
	con->location = location;

	return con;
};

int
NodeParser::parser_errposition(PGParseState *pstate, int location)
{
	int			pos;

	/* No-op if location was not provided */
	if (location < 0)
		return 0;
	/* Can't do anything if source text is not available */
	if (pstate == NULL || pstate->p_sourcetext == NULL)
		return 0;
	/* Convert offset to character number */
	pos = pg_mbstrlen_with_len(pstate->p_sourcetext, location) + 1;
	/* And pass it to the ereport mechanism */
	return errposition(pos);
};

Oid NodeParser::transformArrayType(Oid *arrayType, int32 *arrayTypmod)
{
	Oid			origArrayType = *arrayType;
	Oid			elementType;
	HeapTuple	type_tuple_array;
	Form_pg_type type_struct_array;

	/*
	 * If the input is a domain, smash to base type, and extract the actual
	 * typmod to be applied to the base type.  Subscripting a domain is an
	 * operation that necessarily works on the base array type, not the domain
	 * itself.  (Note that we provide no method whereby the creator of a
	 * domain over an array type could hide its ability to be subscripted.)
	 */
	*arrayType = getBaseTypeAndTypmod(*arrayType, arrayTypmod);

	/*
	 * We treat int2vector and oidvector as though they were domains over
	 * int2[] and oid[].  This is needed because array slicing could create an
	 * array that doesn't satisfy the dimensionality constraints of the
	 * xxxvector type; so we want the result of a slice operation to be
	 * considered to be of the more general type.
	 */
	if (*arrayType == INT2VECTOROID)
		*arrayType = INT2ARRAYOID;
	else if (*arrayType == OIDVECTOROID)
		*arrayType = OIDARRAYOID;

	/* Get the type tuple for the array */
	type_tuple_array = SearchSysCache1(TYPEOID, ObjectIdGetDatum(*arrayType));
	if (!HeapTupleIsValid(type_tuple_array))
		elog(ERROR, "cache lookup failed for type %u", *arrayType);
	type_struct_array = (Form_pg_type) GETSTRUCT(type_tuple_array);

	/* needn't check typisdefined since this will fail anyway */

	elementType = type_struct_array->typelem;
	if (elementType == InvalidOid)
		ereport(ERROR,
				(errcode(PG_ERRCODE_SYNTAX_ERROR),
				 errmsg("cannot subscript type %s because it is not an array",
						format_type_be(origArrayType))));

	ReleaseSysCache(type_tuple_array);

	return elementType;
};

}