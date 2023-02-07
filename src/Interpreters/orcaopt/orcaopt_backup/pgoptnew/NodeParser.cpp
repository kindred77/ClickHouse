#include <Interpreters/orcaopt/pgoptnew/NodeParser.h>

namespace DB
{
using namespace duckdb_libpgquery;

Oid
NodeParser::transformContainerType(Oid *containerType, int32 *containerTypmod)
{
	Oid			origContainerType = *containerType;
	Oid			elementType;
	HeapTuple	type_tuple_container;
	Form_pg_type type_struct_container;

	/*
	 * If the input is a domain, smash to base type, and extract the actual
	 * typmod to be applied to the base type. Subscripting a domain is an
	 * operation that necessarily works on the base container type, not the
	 * domain itself. (Note that we provide no method whereby the creator of a
	 * domain over a container type could hide its ability to be subscripted.)
	 */
	*containerType = getBaseTypeAndTypmod(*containerType, containerTypmod);

	/*
	 * Here is an array specific code. We treat int2vector and oidvector as
	 * though they were domains over int2[] and oid[].  This is needed because
	 * array slicing could create an array that doesn't satisfy the
	 * dimensionality constraints of the xxxvector type; so we want the result
	 * of a slice operation to be considered to be of the more general type.
	 */
	if (*containerType == INT2VECTOROID)
		*containerType = INT2ARRAYOID;
	else if (*containerType == OIDVECTOROID)
		*containerType = OIDARRAYOID;

	/* Get the type tuple for the container */
	type_tuple_container = SearchSysCache1(TYPEOID, ObjectIdGetDatum(*containerType));
	if (!HeapTupleIsValid(type_tuple_container))
		elog(ERROR, "cache lookup failed for type %u", *containerType);
	type_struct_container = (Form_pg_type) GETSTRUCT(type_tuple_container);

	/* needn't check typisdefined since this will fail anyway */

	elementType = type_struct_container->typelem;
	if (elementType == InvalidOid)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("cannot subscript type %s because it is not an array",
						format_type_be(origContainerType))));

	ReleaseSysCache(type_tuple_container);

	return elementType;
};

SubscriptingRef *
NodeParser::transformContainerSubscripts(PGParseState *pstate,
							 PGNode *containerBase,
							 Oid containerType,
							 Oid elementType,
							 int32 containerTypMod,
							 PGList *indirection,
							 PGNode *assignFrom)
{
    bool		isSlice = false;
	PGList	   *upperIndexpr = NIL;
	PGList	   *lowerIndexpr = NIL;
	ListCell   *idx;
	SubscriptingRef *sbsref;

	/*
	 * Caller may or may not have bothered to determine elementType.  Note
	 * that if the caller did do so, containerType/containerTypMod must be as
	 * modified by transformContainerType, ie, smash domain to base type.
	 */
	if (!OidIsValid(elementType))
		elementType = transformContainerType(&containerType, &containerTypMod);

	/*
	 * A list containing only simple subscripts refers to a single container
	 * element.  If any of the items are slice specifiers (lower:upper), then
	 * the subscript expression means a container slice operation.  In this
	 * case, we convert any non-slice items to slices by treating the single
	 * subscript as the upper bound and supplying an assumed lower bound of 1.
	 * We have to prescan the list to see if there are any slice items.
	 */
	foreach(idx, indirection)
	{
		PGAIndices  *ai = (PGAIndices *) lfirst(idx);

		if (ai->is_slice)
		{
			isSlice = true;
			break;
		}
	}

	/*
	 * Transform the subscript expressions.
	 */
	foreach(idx, indirection)
	{
		PGAIndices  *ai = lfirst_node(PGAIndices, idx);
		PGNode	   *subexpr;

		if (isSlice)
		{
			if (ai->lidx)
			{
				subexpr = expr_parser.transformExpr(pstate, ai->lidx, pstate->p_expr_kind);
				/* If it's not int4 already, try to coerce */
				subexpr = coerce_parser.coerce_to_target_type(pstate,
												subexpr, exprType(subexpr),
												INT4OID, -1,
												PG_COERCION_ASSIGNMENT,
												PG_COERCE_IMPLICIT_CAST,
												-1);
				if (subexpr == NULL)
				{
					parser_errposition(pstate, exprLocation(ai->lidx));
					ereport(ERROR,
							(errcode(ERRCODE_DATATYPE_MISMATCH),
							 errmsg("array subscript must have type integer")));
				}
			}
			else if (!ai->is_slice)
			{
				/* Make a constant 1 */
				subexpr = (PGNode *) makeConst(INT4OID,
											 -1,
											 InvalidOid,
											 sizeof(int32),
											 Int32GetDatum(1),
											 false,
											 true); /* pass by value */
			}
			else
			{
				/* Slice with omitted lower bound, put NULL into the list */
				subexpr = NULL;
			}
			lowerIndexpr = lappend(lowerIndexpr, subexpr);
		}
		else
			Assert(ai->lidx == NULL && !ai->is_slice);

		if (ai->uidx)
		{
			subexpr = expr_parser.transformExpr(pstate, ai->uidx, pstate->p_expr_kind);
			/* If it's not int4 already, try to coerce */
			subexpr = coerce_parser.coerce_to_target_type(pstate,
											subexpr, exprType(subexpr),
											INT4OID, -1,
											PG_COERCION_ASSIGNMENT,
											PG_COERCE_IMPLICIT_CAST,
											-1);
			if (subexpr == NULL)
			{
				parser_errposition(pstate, exprLocation(ai->uidx));
				ereport(ERROR,
						(errcode(ERRCODE_DATATYPE_MISMATCH),
						 errmsg("array subscript must have type integer")));
			}
		}
		else
		{
			/* Slice with omitted upper bound, put NULL into the list */
			Assert(isSlice && ai->is_slice);
			subexpr = NULL;
		}
		upperIndexpr = lappend(upperIndexpr, subexpr);
	}

	/*
	 * If doing an array store, coerce the source value to the right type.
	 * (This should agree with the coercion done by transformAssignedExpr.)
	 */
	if (assignFrom != NULL)
	{
		Oid			typesource = exprType(assignFrom);
		Oid			typeneeded = isSlice ? containerType : elementType;
		PGNode	   *newFrom;

		newFrom = coerce_parser.coerce_to_target_type(pstate,
										assignFrom, typesource,
										typeneeded, containerTypMod,
										PG_COERCION_ASSIGNMENT,
										PG_COERCE_IMPLICIT_CAST,
										-1);
		if (newFrom == NULL)
		{
			parser_errposition(pstate, exprLocation(assignFrom));
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("array assignment requires type %s"
							" but expression is of type %s",
							format_type_be(typeneeded),
							format_type_be(typesource)),
					 errhint("You will need to rewrite or cast the expression.")));
		}
		assignFrom = newFrom;
	}

	/*
	 * Ready to build the SubscriptingRef node.
	 */
	sbsref = (SubscriptingRef *) makeNode(SubscriptingRef);
	if (assignFrom != NULL)
		sbsref->refassgnexpr = (PGExpr *) assignFrom;

	sbsref->refcontainertype = containerType;
	sbsref->refelemtype = elementType;
	sbsref->reftypmod = containerTypMod;
	/* refcollid will be set by parse_collate.c */
	sbsref->refupperindexpr = upperIndexpr;
	sbsref->reflowerindexpr = lowerIndexpr;
	sbsref->refexpr = (PGExpr *) containerBase;
	sbsref->refassgnexpr = (PGExpr *) assignFrom;

	return sbsref;
};

// void
// NodeParser::setup_parser_errposition_callback(PGParseCallbackState *pcbstate,
// 								  PGParseState *pstate, int location)
// {
// 	/* Setup error traceback support for ereport() */
// 	pcbstate->pstate = pstate;
// 	pcbstate->location = location;
// 	pcbstate->errcallback.callback = pcb_error_callback;
// 	pcbstate->errcallback.arg = (void *) pcbstate;
// 	pcbstate->errcallback.previous = error_context_stack;
// 	error_context_stack = &pcbstate->errcallback;
// };

// void
// NodeParser::cancel_parser_errposition_callback(PGParseCallbackState *pcbstate)
// {
// 	/* Pop the error context stack */
// 	error_context_stack = pcbstate->errcallback.previous;
// };

PGVar *
NodeParser::make_var(PGParseState *pstate, PGRangeTblEntry *rte, int attrno, int location)
{
	PGVar		   *result;
	int			vnum,
				sublevels_up;
	Oid			vartypeid;
	int32		type_mod;
	Oid			varcollid;

	vnum = relation_parser.RTERangeTablePosn(pstate, rte, &sublevels_up);
	relation_parser.get_rte_attribute_type(rte, attrno, &vartypeid, &type_mod, &varcollid);
	result = makeVar(vnum, attrno, vartypeid, type_mod, varcollid, sublevels_up);
	result->location = location;
	return result;
};

PGConst *
NodeParser::make_const(PGParseState *pstate, PGValue *value, int location)
{
	PGConst	   *con;
	Datum		val;
	int64		val64;
	Oid			typeoid;
	int			typelen;
	bool		typebyval;
	PGParseCallbackState pcbstate;

	switch (nodeTag(value))
	{
		case T_PGInteger:
			val = Int32GetDatum(intVal(value));

			typeoid = INT4OID;
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

					typeoid = INT4OID;
					typelen = sizeof(int32);
					typebyval = true;
				}
				else
				{
					val = Int64GetDatum(val64);

					typeoid = INT8OID;
					typelen = sizeof(int64);
					typebyval = FLOAT8PASSBYVAL;	/* int8 and float8 alike */
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

				typeoid = NUMERICOID;
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

			typeoid = UNKNOWNOID;	/* will be coerced later */
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
			typeoid = BITOID;
			typelen = -1;
			typebyval = false;
			break;

		case T_PGNull:
			/* return a null const */
			con = makeConst(UNKNOWNOID,
							-1,
							InvalidOid,
							-2,
							(Datum) 0,
							true,
							false);
			con->location = location;
			return con;

		default:
			elog(ERROR, "unrecognized node type: %d", (int) nodeTag(value));
			return NULL;		/* keep compiler quiet */
	}

	con = makeConst(typeoid,
					-1,			/* typmod -1 is OK for all cases */
					InvalidOid, /* all cases are uncollatable types */
					typelen,
					val,
					false,
					typebyval);
	con->location = location;

	return con;
};

// void
// NodeParser::parser_errposition(PGParseState *pstate, int location)
// {
// 	int			pos;

// 	/* No-op if location was not provided */
// 	if (location < 0)
// 		return;
// 	/* Can't do anything if source text is not available */
// 	if (pstate == NULL || pstate->p_sourcetext == NULL)
// 		return;
// 	/* Convert offset to character number */
// 	pos = pg_mbstrlen_with_len(pstate->p_sourcetext, location) + 1;
// 	/* And pass it to the ereport mechanism */
// 	errposition(pos);
// };

}