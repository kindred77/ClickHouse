#include <Interpreters/orcaopt/pgoptnew/NodeParser.h>

namespace DB
{
using namespace duckdb_libpgquery;

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
					ereport(ERROR,
							(errcode(ERRCODE_DATATYPE_MISMATCH),
							 errmsg("array subscript must have type integer"),
							 parser_errposition(pstate, exprLocation(ai->lidx))));
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
				ereport(ERROR,
						(errcode(ERRCODE_DATATYPE_MISMATCH),
						 errmsg("array subscript must have type integer"),
						 parser_errposition(pstate, exprLocation(ai->uidx))));
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
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("array assignment requires type %s"
							" but expression is of type %s",
							format_type_be(typeneeded),
							format_type_be(typesource)),
					 errhint("You will need to rewrite or cast the expression."),
					 parser_errposition(pstate, exprLocation(assignFrom))));
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

}