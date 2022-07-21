#include <CoerceParser.h>

namespace DB
{

bool
CoerceParser::can_coerce_type(int nargs, Oid *input_typeids, Oid *target_typeids,
				duckdb_libpgquery::PGCoercionContext ccontext)
{
    bool		have_generics = false;
	int			i;

	/* run through argument list... */
	for (i = 0; i < nargs; i++)
	{
		Oid			inputTypeId = input_typeids[i];
		Oid			targetTypeId = target_typeids[i];
		CoercionPathType pathtype;
		Oid			funcId;

		/* no problem if same type */
		if (inputTypeId == targetTypeId)
			continue;

		/* 
		 * ANYTABLE is a special case that can occur when a function is 
		 * called with a TableValue expression.  A table value expression
		 * can only match a parameter to a function defined as a "anytable".
		 *
		 * Only allow ANYTABLE to match another ANYTABLE, anything else would
		 * be a mismatch of Table domain and Value domain expressions.  
		 *
		 * Validation of ANYTABLE coercion is processed at a higher level
		 * that has more context related to the tupleDesc for the tables
		 * involved.
		 */
		if (targetTypeId == ANYTABLEOID || inputTypeId == ANYTABLEOID)
			return false;

		/* accept if target is ANY */
		if (targetTypeId == ANYOID)
			continue;

		/* accept if target is polymorphic, for now */
		if (IsPolymorphicType(targetTypeId))
		{
			have_generics = true;		/* do more checking later */
			continue;
		}

		/*
		 * If input is an untyped string constant, assume we can convert it to
		 * anything.
		 */
		if (inputTypeId == UNKNOWNOID)
			continue;

		/*
		 * If pg_cast shows that we can coerce, accept.  This test now covers
		 * both binary-compatible and coercion-function cases.
		 */
		pathtype = find_coercion_pathway(targetTypeId, inputTypeId, ccontext,
										 &funcId);
		if (pathtype != COERCION_PATH_NONE)
			continue;

		/*
		 * If input is RECORD and target is a composite type, assume we can
		 * coerce (may need tighter checking here)
		 */
		if (inputTypeId == RECORDOID &&
			ISCOMPLEX(targetTypeId))
			continue;

		/*
		 * If input is a composite type and target is RECORD, accept
		 */
		if (targetTypeId == RECORDOID &&
			ISCOMPLEX(inputTypeId))
			continue;

#ifdef NOT_USED					/* not implemented yet */

		/*
		 * If input is record[] and target is a composite array type, assume
		 * we can coerce (may need tighter checking here)
		 */
		if (inputTypeId == RECORDARRAYOID &&
			is_complex_array(targetTypeId))
			continue;
#endif

		/*
		 * If input is a composite array type and target is record[], accept
		 */
		if (targetTypeId == RECORDARRAYOID &&
			is_complex_array(inputTypeId))
			continue;

		/*
		 * If input is a class type that inherits from target, accept
		 */
		if (typeInheritsFrom(inputTypeId, targetTypeId)
			|| typeIsOfTypedTable(inputTypeId, targetTypeId))
			continue;

		/*
		 * Else, cannot coerce at this argument position
		 */
		return false;
	}

	/* If we found any generic argument types, cross-check them */
	if (have_generics)
	{
		if (!check_generic_type_consistency(input_typeids, target_typeids,
											nargs))
			return false;
	}

	return true;
};

CoercionPathType
CoerceParser::find_coercion_pathway(Oid targetTypeId, Oid sourceTypeId,
					  duckdb_libpgquery::PGCoercionContext ccontext,
					  Oid *funcid)
{
    CoercionPathType result = COERCION_PATH_NONE;
	HeapTuple	tuple;

	*funcid = InvalidOid;

	/* Perhaps the types are domains; if so, look at their base types */
	if (OidIsValid(sourceTypeId))
		sourceTypeId = getBaseType(sourceTypeId);
	if (OidIsValid(targetTypeId))
		targetTypeId = getBaseType(targetTypeId);

	/* Domains are always coercible to and from their base type */
	if (sourceTypeId == targetTypeId)
		return COERCION_PATH_RELABELTYPE;

	/* SELECT castcontext from pg_cast */

	/* Look in pg_cast */
	tuple = SearchSysCache2(CASTSOURCETARGET,
							ObjectIdGetDatum(sourceTypeId),
							ObjectIdGetDatum(targetTypeId));

	if (HeapTupleIsValid(tuple))
	{
		Form_pg_cast castForm = (Form_pg_cast) GETSTRUCT(tuple);
		CoercionContext castcontext;

		/* convert char value for castcontext to CoercionContext enum */
		switch (castForm->castcontext)
		{
			case COERCION_CODE_IMPLICIT:
				castcontext = COERCION_IMPLICIT;
				break;
			case COERCION_CODE_ASSIGNMENT:
				castcontext = COERCION_ASSIGNMENT;
				break;
			case COERCION_CODE_EXPLICIT:
				castcontext = COERCION_EXPLICIT;
				break;
			default:
				elog(ERROR, "unrecognized castcontext: %d",
					 (int) castForm->castcontext);
				castcontext = 0;	/* keep compiler quiet */
				break;
		}

		/* Rely on ordering of enum for correct behavior here */
		if (ccontext >= castcontext)
		{
			switch (castForm->castmethod)
			{
				case COERCION_METHOD_FUNCTION:
					result = COERCION_PATH_FUNC;
					*funcid = castForm->castfunc;
					break;
				case COERCION_METHOD_INOUT:
					result = COERCION_PATH_COERCEVIAIO;
					break;
				case COERCION_METHOD_BINARY:
					result = COERCION_PATH_RELABELTYPE;
					break;
				default:
					elog(ERROR, "unrecognized castmethod: %d",
						 (int) castForm->castmethod);
					break;
			}
		}

		ReleaseSysCache(tuple);
	}
	else
	{
		/*
		 * If there's no pg_cast entry, perhaps we are dealing with a pair of
		 * array types.  If so, and if the element types have a suitable cast,
		 * report that we can coerce with an ArrayCoerceExpr.
		 *
		 * Note that the source type can be a domain over array, but not the
		 * target, because ArrayCoerceExpr won't check domain constraints.
		 *
		 * Hack: disallow coercions to oidvector and int2vector, which
		 * otherwise tend to capture coercions that should go to "real" array
		 * types.  We want those types to be considered "real" arrays for many
		 * purposes, but not this one.  (Also, ArrayCoerceExpr isn't
		 * guaranteed to produce an output that meets the restrictions of
		 * these datatypes, such as being 1-dimensional.)
		 */
		if (targetTypeId != OIDVECTOROID && targetTypeId != INT2VECTOROID)
		{
			Oid			targetElem;
			Oid			sourceElem;

			if ((targetElem = get_element_type(targetTypeId)) != InvalidOid &&
			(sourceElem = get_base_element_type(sourceTypeId)) != InvalidOid)
			{
				CoercionPathType elempathtype;
				Oid			elemfuncid;

				elempathtype = find_coercion_pathway(targetElem,
													 sourceElem,
													 ccontext,
													 &elemfuncid);
				if (elempathtype != COERCION_PATH_NONE &&
					elempathtype != COERCION_PATH_ARRAYCOERCE)
				{
					*funcid = elemfuncid;
					if (elempathtype == COERCION_PATH_COERCEVIAIO)
						result = COERCION_PATH_COERCEVIAIO;
					else
						result = COERCION_PATH_ARRAYCOERCE;
				}
			}
		}

		/*
		 * If we still haven't found a possibility, consider automatic casting
		 * using I/O functions.  We allow assignment casts to string types and
		 * explicit casts from string types to be handled this way. (The
		 * CoerceViaIO mechanism is a lot more general than that, but this is
		 * all we want to allow in the absence of a pg_cast entry.) It would
		 * probably be better to insist on explicit casts in both directions,
		 * but this is a compromise to preserve something of the pre-8.3
		 * behavior that many types had implicit (yipes!) casts to text.
		 */
		if (result == COERCION_PATH_NONE)
		{
			if (ccontext >= COERCION_ASSIGNMENT &&
				TypeCategory(targetTypeId) == TYPCATEGORY_STRING)
				result = COERCION_PATH_COERCEVIAIO;
			else if (ccontext >= COERCION_EXPLICIT &&
					 TypeCategory(sourceTypeId) == TYPCATEGORY_STRING)
				result = COERCION_PATH_COERCEVIAIO;
		}
	}

	return result;
};

void
CoerceParser::fixup_unknown_vars_in_targetlist(PGParseState *pstate, duckdb_libpgquery::PGList *targetlist)
{
    duckdb_libpgquery::ListCell   *cell;

    foreach(cell, targetlist)
    {
        duckdb_libpgquery::PGTargetEntry    *tle = (duckdb_libpgquery::PGTargetEntry *)lfirst(cell);

        Assert(IsA(tle, duckdb_libpgquery::PGTargetEntry));

        if (IsA(tle->expr, duckdb_libpgquery::PGVar) &&
            ((duckdb_libpgquery::PGVar *)tle->expr)->vartype == UNKNOWNOID)
        {
            tle->expr = (duckdb_libpgquery::PGExpr *)coerce_unknown_var(pstate, (duckdb_libpgquery::PGVar *)tle->expr,
                                                   UNKNOWNOID, -1,
                                                   COERCION_IMPLICIT, COERCE_EXPLICIT_CALL,
                                                   0);
        }
    }
}                               /* fixup_unknown_vars_in_targetlist */
};

}