#include <Interpreters/orcaopt/NodeParser.h>

#include <Interpreters/orcaopt/CoerceParser.h>
#include <Interpreters/orcaopt/ExprParser.h>
#include <Interpreters/orcaopt/RelationParser.h>

#include <Interpreters/orcaopt/provider/TypeProvider.h>

#include <Interpreters/Context.h>

using namespace duckdb_libpgquery;

namespace DB
{

// NodeParser::NodeParser(const ContextPtr& context_) : context(context_)
// {
// 	coerce_parser = std::make_shared<CoerceParser>(context);
// 	expr_parser = std::make_shared<ExprParser>(context);
// 	relation_parser = std::make_shared<RelationParser>(context);
// };

PGOid NodeParser::transformArrayType(PGOid *arrayType, int32 *arrayTypmod)
{
    PGOid origArrayType = *arrayType;
    PGOid elementType;
    
    /*
	 * If the input is a domain, smash to base type, and extract the actual
	 * typmod to be applied to the base type.  Subscripting a domain is an
	 * operation that necessarily works on the base array type, not the domain
	 * itself.  (Note that we provide no method whereby the creator of a
	 * domain over an array type could hide its ability to be subscripted.)
	 */
    *arrayType = TypeProvider::getBaseTypeAndTypmod(*arrayType, arrayTypmod);

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
    PGTypePtr type_tuple_array = TypeProvider::getTypeByOid(*arrayType);
    if (type_tuple_array == NULL)
        elog(ERROR, "cache lookup failed for type %u", *arrayType);

    /* needn't check typisdefined since this will fail anyway */

    elementType = type_tuple_array->typelem;
    if (elementType == InvalidOid)
        ereport(
            ERROR,
            (errcode(PG_ERRCODE_DATATYPE_MISMATCH),
             errmsg("cannot subscript type %s because it is not an array", TypeProvider::format_type_be(origArrayType).c_str())));


    return elementType;
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

PGArrayRef * NodeParser::transformArraySubscripts(
        PGParseState * pstate, PGNode * arrayBase, PGOid arrayType,
		PGOid elementType, int32 arrayTypMod, PGList * indirection,
		PGNode * assignFrom)
{
    bool isSlice = false;
    PGList * upperIndexpr = NIL;
    PGList * lowerIndexpr = NIL;
    PGListCell * idx;
    PGArrayRef * aref;

    /*
	 * Caller may or may not have bothered to determine elementType.  Note
	 * that if the caller did do so, arrayType/arrayTypMod must be as modified
	 * by transformArrayType, ie, smash domain to base type.
	 */
    if (!OidIsValid(elementType))
        elementType = transformArrayType(&arrayType, &arrayTypMod);

    /*
	 * A list containing only single subscripts refers to a single array
	 * element.  If any of the items are double subscripts (lower:upper), then
	 * the subscript expression means an array slice operation. In this case,
	 * we supply a default lower bound of 1 for any items that contain only a
	 * single subscript.  We have to prescan the indirection list to see if
	 * there are any double subscripts.
	 */
    foreach (idx, indirection)
    {
        PGAIndices * ai = (PGAIndices *)lfirst(idx);

        if (ai->lidx != NULL)
        {
            isSlice = true;
            break;
        }
    }

    /*
	 * Transform the subscript expressions.
	 */
    foreach (idx, indirection)
    {
        PGAIndices * ai = (PGAIndices *)lfirst(idx);
        PGNode * subexpr;

        Assert(IsA(ai, PGAIndices))
        if (isSlice)
        {
            if (ai->lidx)
            {
                subexpr = ExprParser::transformExpr(pstate, ai->lidx, pstate->p_expr_kind);
                /* If it's not int4 already, try to coerce */
                subexpr
                    = CoerceParser::coerce_to_target_type(pstate, subexpr, exprType(subexpr), INT4OID, -1, PG_COERCION_ASSIGNMENT, PG_COERCE_IMPLICIT_CAST, -1);
                if (subexpr == NULL)
                    ereport(
                        ERROR,
                        (errcode(PG_ERRCODE_DATATYPE_MISMATCH),
                         errmsg("array subscript must have type integer"),
                         parser_errposition(pstate, exprLocation(ai->lidx))));
            }
            else
            {
                /* Make a constant 1 */
                subexpr = (PGNode *)makeConst(INT4OID, -1, InvalidOid, sizeof(int32), Int32GetDatum(1), false, true); /* pass by value */
            }
            lowerIndexpr = lappend(lowerIndexpr, subexpr);
        }
        subexpr = ExprParser::transformExpr(pstate, ai->uidx, pstate->p_expr_kind);
        /* If it's not int4 already, try to coerce */
        subexpr = CoerceParser::coerce_to_target_type(pstate, subexpr, exprType(subexpr), INT4OID, -1, PG_COERCION_ASSIGNMENT, PG_COERCE_IMPLICIT_CAST, -1);
        if (subexpr == NULL)
            ereport(
                ERROR,
                (errcode(PG_ERRCODE_DATATYPE_MISMATCH),
                 errmsg("array subscript must have type integer"),
                 parser_errposition(pstate, exprLocation(ai->uidx))));
        upperIndexpr = lappend(upperIndexpr, subexpr);
    }

    /*
	 * If doing an array store, coerce the source value to the right type.
	 * (This should agree with the coercion done by transformAssignedExpr.)
	 */
    if (assignFrom != NULL)
    {
        PGOid typesource = exprType(assignFrom);
        PGOid typeneeded = isSlice ? arrayType : elementType;
        PGNode * newFrom;

        newFrom = CoerceParser::coerce_to_target_type(pstate, assignFrom, typesource, typeneeded, arrayTypMod,
			PG_COERCION_ASSIGNMENT, PG_COERCE_IMPLICIT_CAST, -1);
        if (newFrom == NULL)
            ereport(
                ERROR,
                (errcode(PG_ERRCODE_DATATYPE_MISMATCH),
                 errmsg(
                     "array assignment requires type %s"
                     " but expression is of type %s",
                     TypeProvider::format_type_be(typeneeded).c_str(),
                     TypeProvider::format_type_be(typesource).c_str()),
                 errhint("You will need to rewrite or cast the expression."),
                 parser_errposition(pstate, exprLocation(assignFrom))));
        assignFrom = newFrom;
    }

    /*
	 * Ready to build the ArrayRef node.
	 */
    aref = makeNode(PGArrayRef);
    aref->refarraytype = arrayType;
    aref->refelemtype = elementType;
    aref->reftypmod = arrayTypMod;
    /* refcollid will be set by parse_collate.c */
    aref->refupperindexpr = upperIndexpr;
    aref->reflowerindexpr = lowerIndexpr;
    aref->refexpr = (PGExpr *)arrayBase;
    aref->refassgnexpr = (PGExpr *)assignFrom;

    return aref;
};

PGVar *
NodeParser::make_var(PGParseState *pstate, PGRangeTblEntry *rte, int attrno, int location)
{
	PGVar		   *result;
	int			vnum,
				sublevels_up;
	PGOid			vartypeid;
	int32		type_mod;
	PGOid			varcollid;

	vnum = RelationParser::RTERangeTablePosn(pstate, rte, &sublevels_up);
	RelationParser::get_rte_attribute_type(rte, attrno, &vartypeid, &type_mod, &varcollid);
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
	PGOid			typeoid;
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
					typebyval = PGFLOAT8PASSBYVAL;	/* int8 and float8 alike */
				}
			}
			else
			{
				/* arrange to report location if numeric_in() fails */
				//TODO kindred
				ereport(
                ERROR,
                (errcode(PG_ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("Do not supported!")));


				// setup_parser_errposition_callback(&pcbstate, pstate, location);
				// val = DirectFunctionCall3(numeric_in,
				// 						  CStringGetDatum(strVal(value)),
				// 						  ObjectIdGetDatum(InvalidOid),
				// 						  Int32GetDatum(-1));
				// cancel_parser_errposition_callback(&pcbstate);

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

            //TODO kindred
            ereport(ERROR, (errcode(PG_ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("Do not supported!")));

            /* arrange to report location if bit_in() fails */
			// setup_parser_errposition_callback(&pcbstate, pstate, location);
			// val = DirectFunctionCall3(bit_in,
			// 						  CStringGetDatum(strVal(value)),
			// 						  ObjectIdGetDatum(InvalidOid),
			// 						  Int32GetDatum(-1));
			// cancel_parser_errposition_callback(&pcbstate);
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
