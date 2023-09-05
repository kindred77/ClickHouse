/*--------------------------------------------------------------------
 * Symbols referenced in this file:
 * - strip_implicit_coercions
 * - exprType
 *--------------------------------------------------------------------
 */

/*-------------------------------------------------------------------------
 *
 * list.c
 *	  implementation for PostgreSQL generic linked list package
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development PGGroup
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/nodes/list.c
 *
 *-------------------------------------------------------------------------
 */
#include "nodes/nodeFuncs.hpp"
#include "common/common_macro.hpp"
#include "common/common_datum.hpp"
#include "common/common_walkers.hpp"
#include "pg_functions.hpp"


namespace duckdb_libpgquery {

/*
 *	exprInputCollation -
 *	  returns the Oid of the collation a function should use, if available.
 *
 * Result is InvalidOid if the node type doesn't store this information.
 */
PGOid
exprInputCollation(const PGNode *expr)
{
	PGOid			coll;

	if (!expr)
		return InvalidOid;

	switch (nodeTag(expr))
	{
		case T_PGAggref:
			coll = ((const PGAggref *) expr)->inputcollid;
			break;
		case T_PGWindowFunc:
			coll = ((const PGWindowFunc *) expr)->inputcollid;
			break;
		case T_PGFuncExpr:
			coll = ((const PGFuncExpr *) expr)->inputcollid;
			break;
		case T_PGOpExpr:
			coll = ((const PGOpExpr *) expr)->inputcollid;
			break;
		case T_PGDistinctExpr:
			coll = ((const PGDistinctExpr *) expr)->inputcollid;
			break;
		case T_PGNullIfExpr:
			coll = ((const PGNullIfExpr *) expr)->inputcollid;
			break;
		case T_PGScalarArrayOpExpr:
			coll = ((const PGScalarArrayOpExpr *) expr)->inputcollid;
			break;
		case T_PGMinMaxExpr:
			coll = ((const PGMinMaxExpr *) expr)->inputcollid;
			break;
		default:
			coll = InvalidOid;
			break;
	}
	return coll;
}

static bool
expression_returns_set_walker(PGNode *node, void *context)
{
	if (node == NULL)
		return false;
	if (IsA(node, PGFuncExpr))
	{
		PGFuncExpr   *expr = (PGFuncExpr *) node;

		if (expr->funcretset)
			return true;
		/* else fall through to check args */
	}
	if (IsA(node, PGOpExpr))
	{
		PGOpExpr	   *expr = (PGOpExpr *) node;

		if (expr->opretset)
			return true;
		/* else fall through to check args */
	}

	/* Avoid recursion for some cases that can't return a set */
	if (IsA(node, PGAggref))
		return false;
	if (IsA(node, PGWindowFunc))
		return false;
	if (IsA(node, PGDistinctExpr))
		return false;
	if (IsA(node, PGNullIfExpr))
		return false;
	if (IsA(node, PGScalarArrayOpExpr))
		return false;
	if (IsA(node, PGBoolExpr))
		return false;
	if (IsA(node, PGSubLink))
		return false;
	if (IsA(node, PGSubPlan))
		return false;
	if (IsA(node, PGAlternativeSubPlan))
		return false;
	if (IsA(node, PGArrayExpr))
		return false;
	if (IsA(node, PGRowExpr))
		return false;
	if (IsA(node, PGRowCompareExpr))
		return false;
	if (IsA(node, PGCoalesceExpr))
		return false;
	if (IsA(node, PGMinMaxExpr))
		return false;
	if (IsA(node, PGXmlExpr))
		return false;

	return pg_expression_tree_walker(node, expression_returns_set_walker,
								  context);
}

/*
 * expression_returns_set
 *	  Test whether an expression returns a set result.
 *
 * Because we use expression_tree_walker(), this can also be applied to
 * whole targetlists; it'll produce TRUE if any one of the tlist items
 * returns a set.
 */
bool expression_returns_set(PGNode *clause)
{
	return expression_returns_set_walker(clause, NULL);
}

/*
 * exprIsLengthCoercion
 *		Detect whether an expression tree is an application of a datatype's
 *		typmod-coercion function.  Optionally extract the result's typmod.
 *
 * If coercedTypmod is not NULL, the typmod is stored there if the expression
 * is a length-coercion function, else -1 is stored there.
 *
 * Note that a combined type-and-length coercion will be treated as a
 * length coercion by this routine.
 */
bool exprIsLengthCoercion(const PGNode *expr, int32_t *coercedTypmod)
{
	if (coercedTypmod != NULL)
		*coercedTypmod = -1;	/* default result on failure */

	/*
	 * Scalar-type length coercions are FuncExprs, array-type length coercions
	 * are ArrayCoerceExprs
	 */
	if (expr && IsA(expr, PGFuncExpr))
	{
		const PGFuncExpr *func = (const PGFuncExpr *) expr;
		int			nargs;
		PGConst	   *second_arg;

		/*
		 * If it didn't come from a coercion context, reject.
		 */
		if (func->funcformat != PG_COERCE_EXPLICIT_CAST &&
			func->funcformat != PG_COERCE_IMPLICIT_CAST)
			return false;

		/*
		 * If it's not a two-argument or three-argument function with the
		 * second argument being an int4 constant, it can't have been created
		 * from a length coercion (it must be a type coercion, instead).
		 */
		nargs = list_length(func->args);
		if (nargs < 2 || nargs > 3)
			return false;

		second_arg = (PGConst *) lsecond(func->args);
		if (!IsA(second_arg, PGConst) ||
			second_arg->consttype != INT4OID ||
			second_arg->constisnull)
			return false;

		/*
		 * OK, it is indeed a length-coercion function.
		 */
		if (coercedTypmod != NULL)
			*coercedTypmod = DatumGetInt32(second_arg->constvalue);

		return true;
	}

	if (expr && IsA(expr, PGArrayCoerceExpr))
	{
		const PGArrayCoerceExpr *acoerce = (const PGArrayCoerceExpr *) expr;

		/* It's not a length coercion unless there's a nondefault typmod */
		if (acoerce->resulttypmod < 0)
			return false;

		/*
		 * OK, it is indeed a length-coercion expression.
		 */
		if (coercedTypmod != NULL)
			*coercedTypmod = acoerce->resulttypmod;

		return true;
	}

	return false;
}

/*
 * strip_implicit_coercions: remove implicit coercions at top level of tree
 *
 * This doesn't modify or copy the input expression tree, just return a
 * pointer to a suitable place within it.
 *
 * Note: there isn't any useful thing we can do with a RowExpr here, so
 * just return it unchanged, even if it's marked as an implicit coercion.
 */
PGNode *
strip_implicit_coercions(PGNode *node)
{
	if (node == NULL)
		return NULL;
	if (IsA(node, PGFuncExpr))
	{
		PGFuncExpr   *f = (PGFuncExpr *) node;

		if (f->funcformat == PG_COERCE_IMPLICIT_CAST)
			return strip_implicit_coercions((PGNode *)linitial(f->args));
	}
	else if (IsA(node, PGRelabelType))
	{
		PGRelabelType *r = (PGRelabelType *) node;

		if (r->relabelformat == PG_COERCE_IMPLICIT_CAST)
			return strip_implicit_coercions((PGNode *) r->arg);
	}
	else if (IsA(node, PGCoerceViaIO))
	{
		PGCoerceViaIO *c = (PGCoerceViaIO *) node;

		if (c->coerceformat == PG_COERCE_IMPLICIT_CAST)
			return strip_implicit_coercions((PGNode *) c->arg);
	}
	else if (IsA(node, PGArrayCoerceExpr))
	{
		PGArrayCoerceExpr *c = (PGArrayCoerceExpr *) node;

		if (c->coerceformat == PG_COERCE_IMPLICIT_CAST)
			return strip_implicit_coercions((PGNode *) c->arg);
	}
	else if (IsA(node, PGConvertRowtypeExpr))
	{
		PGConvertRowtypeExpr *c = (PGConvertRowtypeExpr *) node;

		if (c->convertformat == PG_COERCE_IMPLICIT_CAST)
			return strip_implicit_coercions((PGNode *) c->arg);
	}
	else if (IsA(node, PGCoerceToDomain))
	{
		PGCoerceToDomain *c = (PGCoerceToDomain *) node;

		if (c->coercionformat == PG_COERCE_IMPLICIT_CAST)
			return strip_implicit_coercions((PGNode *) c->arg);
	}
	return node;
};

/*
 *	exprType -
 *	  returns the Oid of the type of the expression's result.
 */
PGOid exprType(const PGNode *expr)
{
	PGOid type;

	if (!expr)
		return InvalidOid;

	switch (nodeTag(expr))
	{
		case T_PGVar:
			type = ((const PGVar *) expr)->vartype;
			break;
		case T_PGConst:
			type = ((const PGConst *) expr)->consttype;
			break;
		case T_PGParam:
			type = ((const PGParam *) expr)->paramtype;
			break;
		case T_PGAggref:
			type = ((const PGAggref *) expr)->aggtype;
			break;
		case T_PGWindowFunc:
			type = ((const PGWindowFunc *) expr)->wintype;
			break;
		case T_PGArrayRef:
			{
				const PGArrayRef *arrayref = (const PGArrayRef *) expr;

				/* slice and/or store operations yield the array type */
				if (arrayref->reflowerindexpr || arrayref->refassgnexpr)
					type = arrayref->refarraytype;
				else
					type = arrayref->refelemtype;
			}
			break;
		case T_PGFuncExpr:
			type = ((const PGFuncExpr *) expr)->funcresulttype;
			break;
		case T_PGNamedArgExpr:
			type = exprType((PGNode *) ((const PGNamedArgExpr *) expr)->arg);
			break;
		case T_PGOpExpr:
			type = ((const PGOpExpr *) expr)->opresulttype;
			break;
		case T_PGDistinctExpr:
			type = ((const PGDistinctExpr *) expr)->opresulttype;
			break;
		case T_PGNullIfExpr:
			type = ((const PGNullIfExpr *) expr)->opresulttype;
			break;
		case T_PGScalarArrayOpExpr:
			type = BOOLOID;
			break;
		case T_PGBoolExpr:
			type = BOOLOID;
			break;
		// case T_PGSubLink:
		// 	{
		// 		const PGSubLink *sublink = (const PGSubLink *) expr;

		// 		if (sublink->subLinkType == PG_EXPR_SUBLINK ||
		// 			sublink->subLinkType == PG_ARRAY_SUBLINK)
		// 		{
		// 			/* get the type of the subselect's first target column */
		// 			PGQuery	   *qtree = (PGQuery *) sublink->subselect;
		// 			PGTargetEntry *tent;

		// 			if (!qtree || !IsA(qtree, PGQuery))
		// 				elog(ERROR, "cannot get type for untransformed sublink");
		// 			tent = (PGTargetEntry *) linitial(qtree->targetList);
		// 			Assert(IsA(tent, PGTargetEntry));
		// 			Assert(!tent->resjunk);
		// 			type = exprType((PGNode *) tent->expr);
		// 			if (sublink->subLinkType == PG_ARRAY_SUBLINK)
		// 			{
		// 				type = get_array_type(type);
		// 				if (!OidIsValid(type))
		// 					ereport(ERROR,
		// 							(errcode(ERRCODE_UNDEFINED_OBJECT),
		// 							 errmsg("could not find array type for data type %s",
		// 					format_type_be(exprType((PGNode *) tent->expr)))));
		// 			}
		// 		}
		// 		else
		// 		{
		// 			/* for all other sublink types, result is boolean */
		// 			type = BOOLOID;
		// 		}
		// 	}
		// 	break;
		// case T_PGSubPlan:
		// 	{
		// 		const PGSubPlan *subplan = (const PGSubPlan *) expr;

		// 		if (subplan->subLinkType == PG_EXPR_SUBLINK ||
		// 			subplan->subLinkType == PG_ARRAY_SUBLINK)
		// 		{
		// 			/* get the type of the subselect's first target column */
		// 			type = subplan->firstColType;
		// 			if (subplan->subLinkType == PG_ARRAY_SUBLINK)
		// 			{
		// 				type = get_array_type(type);
		// 				if (!OidIsValid(type))
		// 					ereport(ERROR,
		// 							(errcode(ERRCODE_UNDEFINED_OBJECT),
		// 							 errmsg("could not find array type for data type %s",
		// 							format_type_be(subplan->firstColType))));
		// 			}
		// 		}
		// 		else
		// 		{
		// 			/* for all other subplan types, result is boolean */
		// 			type = BOOLOID;
		// 		}
		// 	}
		// 	break;
		case T_PGAlternativeSubPlan:
			{
				const PGAlternativeSubPlan *asplan = (const PGAlternativeSubPlan *) expr;

				/* subplans should all return the same thing */
				type = exprType((PGNode *) linitial(asplan->subplans));
			}
			break;
		case T_PGFieldSelect:
			type = ((const PGFieldSelect *) expr)->resulttype;
			break;
		case T_PGFieldStore:
			type = ((const PGFieldStore *) expr)->resulttype;
			break;
		case T_PGRelabelType:
			type = ((const PGRelabelType *) expr)->resulttype;
			break;
		case T_PGCoerceViaIO:
			type = ((const PGCoerceViaIO *) expr)->resulttype;
			break;
		case T_PGArrayCoerceExpr:
			type = ((const PGArrayCoerceExpr *) expr)->resulttype;
			break;
		case T_PGConvertRowtypeExpr:
			type = ((const PGConvertRowtypeExpr *) expr)->resulttype;
			break;
		case T_PGCollateExpr:
			type = exprType((PGNode *) ((const PGCollateExpr *) expr)->arg);
			break;
		case T_PGCaseExpr:
			type = ((const PGCaseExpr *) expr)->casetype;
			break;
		case T_PGCaseTestExpr:
			type = ((const PGCaseTestExpr *) expr)->typeId;
			break;
		case T_PGArrayExpr:
			type = ((const PGArrayExpr *) expr)->array_typeid;
			break;
		case T_PGRowExpr:
			type = ((const PGRowExpr *) expr)->row_typeid;
			break;
		// case T_TableValueExpr:
		// 	type = ANYTABLEOID;  /* MULTISET values are a special pseudotype */
		// 	break;
		case T_PGRowCompareExpr:
			type = BOOLOID;
			break;
		case T_PGCoalesceExpr:
			type = ((const PGCoalesceExpr *) expr)->coalescetype;
			break;
		case T_PGMinMaxExpr:
			type = ((const PGMinMaxExpr *) expr)->minmaxtype;
			break;
		// case T_PGXmlExpr:
		// 	if (((const XmlExpr *) expr)->op == IS_DOCUMENT)
		// 		type = BOOLOID;
		// 	else if (((const XmlExpr *) expr)->op == IS_XMLSERIALIZE)
		// 		type = TEXTOID;
		// 	else
		// 		type = XMLOID;
		// 	break;
		case T_PGNullTest:
			type = BOOLOID;
			break;
		case T_PGBooleanTest:
			type = BOOLOID;
			break;
		case T_PGCoerceToDomain:
			type = ((const PGCoerceToDomain *) expr)->resulttype;
			break;
		case T_PGCoerceToDomainValue:
			type = ((const PGCoerceToDomainValue *) expr)->typeId;
			break;
		case T_PGSetToDefault:
			type = ((const PGSetToDefault *) expr)->typeId;
			break;
		case T_PGCurrentOfExpr:
			type = BOOLOID;
			break;
		// case T_PGPlaceHolderVar:
		// 	type = exprType((PGNode *) ((const PGPlaceHolderVar *) expr)->phexpr);
		// 	break;

		case T_PGGroupingFunc:
			type = INT8OID;
			break;
		// case T_Grouping:
		// 	type = INT8OID;
		// 	break;
		// case T_GroupId:
		// 	type = INT4OID;
		// 	break;
		// case T_DMLActionExpr:
		// 	type = INT4OID;
		// 	break;
		// case T_PartDefaultExpr:
		// 	type = BOOLOID;
		// 	break;
		// case T_PartBoundExpr:
		// 	type = ((PartBoundExpr *) expr)->boundType;
		// 	break;
		// case T_PartBoundInclusionExpr:
		// 	type = BOOLOID;
		// 	break;
		// case T_PartBoundOpenExpr:
		// 	type = BOOLOID;
		// 	break;
		// case T_PartListRuleExpr:
		// 	type = ((PartListRuleExpr *) expr)->resulttype;
		// 	break;
		// case T_PartListNullTestExpr:
		// 	type = BOOLOID;
		// 	break;

		default:
			elog(ERROR, "unrecognized node type: %d", (int) nodeTag(expr));
			type = InvalidOid;	/* keep compiler quiet */
			break;
	}
	return type;
};

int32_t exprTypmod(const PGNode *expr)
{
	if (!expr)
		return -1;

	switch (nodeTag(expr))
	{
		case T_PGVar:
			return ((const PGVar *) expr)->vartypmod;
		case T_PGConst:
			return ((const PGConst *) expr)->consttypmod;
		case T_PGParam:
			return ((const PGParam *) expr)->paramtypmod;
		case T_PGArrayRef:
			/* typmod is the same for array or element */
			return ((const PGArrayRef *) expr)->reftypmod;
		case T_PGFuncExpr:
			{
				int32_t coercedTypmod;

				/* Be smart about length-coercion functions... */
				if (exprIsLengthCoercion(expr, &coercedTypmod))
					return coercedTypmod;
			}
			break;
		case T_PGNamedArgExpr:
			return exprTypmod((PGNode *) ((const PGNamedArgExpr *) expr)->arg);
		case T_PGNullIfExpr:
			{
				/*
				 * Result is either first argument or NULL, so we can report
				 * first argument's typmod if known.
				 */
				const PGNullIfExpr *nexpr = (const PGNullIfExpr *) expr;

				return exprTypmod((PGNode *) linitial(nexpr->args));
			}
			break;
		case T_PGSubLink:
			{
				const PGSubLink *sublink = (const PGSubLink *) expr;

				if (sublink->subLinkType == PG_EXPR_SUBLINK ||
					sublink->subLinkType == PG_ARRAY_SUBLINK)
				{
					/* get the typmod of the subselect's first target column */
					PGQuery	   *qtree = (PGQuery *) sublink->subselect;
					PGTargetEntry *tent;

					if (!qtree || !IsA(qtree, PGQuery))
						elog(ERROR, "cannot get type for untransformed sublink");
					tent = (PGTargetEntry *) linitial(qtree->targetList);
					Assert(IsA(tent, PGTargetEntry));
					Assert(!tent->resjunk);
					return exprTypmod((PGNode *) tent->expr);
					/* note we don't need to care if it's an array */
				}
			}
			break;
		case T_PGSubPlan:
			{
				const PGSubPlan *subplan = (const PGSubPlan *) expr;

				if (subplan->subLinkType == PG_EXPR_SUBLINK ||
					subplan->subLinkType == PG_ARRAY_SUBLINK)
				{
					/* get the typmod of the subselect's first target column */
					/* note we don't need to care if it's an array */
					return subplan->firstColTypmod;
				}
				else
				{
					/* for all other subplan types, result is boolean */
					return -1;
				}
			}
			break;
		case T_PGAlternativeSubPlan:
			{
				const PGAlternativeSubPlan *asplan = (const PGAlternativeSubPlan *) expr;

				/* subplans should all return the same thing */
				return exprTypmod((PGNode *) linitial(asplan->subplans));
			}
			break;
		case T_PGFieldSelect:
			return ((const PGFieldSelect *) expr)->resulttypmod;
		case T_PGRelabelType:
			return ((const PGRelabelType *) expr)->resulttypmod;
		case T_PGArrayCoerceExpr:
			return ((const PGArrayCoerceExpr *) expr)->resulttypmod;
		case T_PGCollateExpr:
			return exprTypmod((PGNode *) ((const PGCollateExpr *) expr)->arg);
		case T_PGCaseExpr:
			{
				/*
				 * If all the alternatives agree on type/typmod, return that
				 * typmod, else use -1
				 */
				const PGCaseExpr *cexpr = (const PGCaseExpr *) expr;
				PGOid casetype = cexpr->casetype;
				int32_t typmod;
				PGListCell   *arg;

				if (!cexpr->defresult)
					return -1;
				if (exprType((PGNode *) cexpr->defresult) != casetype)
					return -1;
				typmod = exprTypmod((PGNode *) cexpr->defresult);
				if (typmod < 0)
					return -1;	/* no point in trying harder */
				foreach(arg, cexpr->args)
				{
					PGCaseWhen   *w = (PGCaseWhen *) lfirst(arg);

					Assert(IsA(w, PGCaseWhen));
					if (exprType((PGNode *) w->result) != casetype)
						return -1;
					if (exprTypmod((PGNode *) w->result) != typmod)
						return -1;
				}
				return typmod;
			}
			break;
		case T_PGCaseTestExpr:
			return ((const PGCaseTestExpr *) expr)->typeMod;
		case T_PGArrayExpr:
			{
				/*
				 * If all the elements agree on type/typmod, return that
				 * typmod, else use -1
				 */
				const PGArrayExpr *arrayexpr = (const PGArrayExpr *) expr;
				PGOid commontype;
				int32_t typmod;
				PGListCell   *elem;

				if (arrayexpr->elements == NIL)
					return -1;
				typmod = exprTypmod((PGNode *) linitial(arrayexpr->elements));
				if (typmod < 0)
					return -1;	/* no point in trying harder */
				if (arrayexpr->multidims)
					commontype = arrayexpr->array_typeid;
				else
					commontype = arrayexpr->element_typeid;
				foreach(elem, arrayexpr->elements)
				{
					PGNode	   *e = (PGNode *) lfirst(elem);

					if (exprType(e) != commontype)
						return -1;
					if (exprTypmod(e) != typmod)
						return -1;
				}
				return typmod;
			}
			break;
		case T_PGCoalesceExpr:
			{
				/*
				 * If all the alternatives agree on type/typmod, return that
				 * typmod, else use -1
				 */
				const PGCoalesceExpr *cexpr = (const PGCoalesceExpr *) expr;
				PGOid coalescetype = cexpr->coalescetype;
				int32_t typmod;
				PGListCell *arg;

				if (exprType((PGNode *) linitial(cexpr->args)) != coalescetype)
					return -1;
				typmod = exprTypmod((PGNode *) linitial(cexpr->args));
				if (typmod < 0)
					return -1;	/* no point in trying harder */
				for_each_cell(arg, lnext(list_head(cexpr->args)))
				{
					PGNode	   *e = (PGNode *) lfirst(arg);

					if (exprType(e) != coalescetype)
						return -1;
					if (exprTypmod(e) != typmod)
						return -1;
				}
				return typmod;
			}
			break;
		case T_PGMinMaxExpr:
			{
				/*
				 * If all the alternatives agree on type/typmod, return that
				 * typmod, else use -1
				 */
				const PGMinMaxExpr *mexpr = (const PGMinMaxExpr *) expr;
				PGOid minmaxtype = mexpr->minmaxtype;
				int32_t typmod;
				PGListCell *arg;

				if (exprType((PGNode *) linitial(mexpr->args)) != minmaxtype)
					return -1;
				typmod = exprTypmod((PGNode *) linitial(mexpr->args));
				if (typmod < 0)
					return -1;	/* no point in trying harder */
				for_each_cell(arg, lnext(list_head(mexpr->args)))
				{
					PGNode *e = (PGNode *) lfirst(arg);

					if (exprType(e) != minmaxtype)
						return -1;
					if (exprTypmod(e) != typmod)
						return -1;
				}
				return typmod;
			}
			break;
		case T_PGCoerceToDomain:
			return ((const PGCoerceToDomain *) expr)->resulttypmod;
		case T_PGCoerceToDomainValue:
			return ((const PGCoerceToDomainValue *) expr)->typeMod;
		case T_PGSetToDefault:
			return ((const PGSetToDefault *) expr)->typeMod;
		// case T_PGPlaceHolderVar:
		// 	return exprTypmod((PGNode *) ((const PGPlaceHolderVar *) expr)->phexpr);
		default:
			break;
	}
	return -1;
};

/*
 *	exprCollation -
 *	  returns the Oid of the collation of the expression's result.
 *
 * Note: expression nodes that can invoke functions generally have an
 * "inputcollid" field, which is what the function should use as collation.
 * That is the resolved common collation of the node's inputs.  It is often
 * but not always the same as the result collation; in particular, if the
 * function produces a non-collatable result type from collatable inputs
 * or vice versa, the two are different.
 */
PGOid exprCollation(const PGNode *expr)
{
	PGOid			coll;

	if (!expr)
		return InvalidOid;

	switch (nodeTag(expr))
	{
		case T_PGVar:
			coll = ((const PGVar *) expr)->varcollid;
			break;
		case T_PGConst:
			coll = ((const PGConst *) expr)->constcollid;
			break;
		case T_PGParam:
			coll = ((const PGParam *) expr)->paramcollid;
			break;
		case T_PGAggref:
			coll = ((const PGAggref *) expr)->aggcollid;
			break;
		case T_PGWindowFunc:
			coll = ((const PGWindowFunc *) expr)->wincollid;
			break;
		case T_PGArrayRef:
			coll = ((const PGArrayRef *) expr)->refcollid;
			break;
		case T_PGFuncExpr:
			coll = ((const PGFuncExpr *) expr)->funccollid;
			break;
		case T_PGNamedArgExpr:
			coll = exprCollation((PGNode *) ((const PGNamedArgExpr *) expr)->arg);
			break;
		case T_PGOpExpr:
			coll = ((const PGOpExpr *) expr)->opcollid;
			break;
		case T_PGDistinctExpr:
			coll = ((const PGDistinctExpr *) expr)->opcollid;
			break;
		case T_PGNullIfExpr:
			coll = ((const PGNullIfExpr *) expr)->opcollid;
			break;
		case T_PGScalarArrayOpExpr:
			coll = InvalidOid;	/* result is always boolean */
			break;
		case T_PGBoolExpr:
			coll = InvalidOid;	/* result is always boolean */
			break;
		case T_PGSubLink:
			{
				const PGSubLink *sublink = (const PGSubLink *) expr;

				if (sublink->subLinkType == PG_EXPR_SUBLINK ||
					sublink->subLinkType == PG_ARRAY_SUBLINK)
				{
					/* get the collation of subselect's first target column */
					PGQuery *qtree = (PGQuery *) sublink->subselect;
					PGTargetEntry *tent;

					if (!qtree || !IsA(qtree, PGQuery))
						elog(ERROR, "cannot get collation for untransformed sublink");
					tent = (PGTargetEntry *) linitial(qtree->targetList);
					Assert(IsA(tent, PGTargetEntry));
					Assert(!tent->resjunk);
					coll = exprCollation((PGNode *) tent->expr);
					/* collation doesn't change if it's converted to array */
				}
				else
				{
					/* for all other sublink types, result is boolean */
					coll = InvalidOid;
				}
			}
			break;
		case T_PGSubPlan:
			{
				const PGSubPlan *subplan = (const PGSubPlan *) expr;

				if (subplan->subLinkType == PG_EXPR_SUBLINK ||
					subplan->subLinkType == PG_ARRAY_SUBLINK)
				{
					/* get the collation of subselect's first target column */
					coll = subplan->firstColCollation;
					/* collation doesn't change if it's converted to array */
				}
				else
				{
					/* for all other subplan types, result is boolean */
					coll = InvalidOid;
				}
			}
			break;
		case T_PGAlternativeSubPlan:
			{
				const PGAlternativeSubPlan *asplan = (const PGAlternativeSubPlan *) expr;

				/* subplans should all return the same thing */
				coll = exprCollation((PGNode *) linitial(asplan->subplans));
			}
			break;
		case T_PGFieldSelect:
			coll = ((const PGFieldSelect *) expr)->resultcollid;
			break;
		case T_PGFieldStore:
			coll = InvalidOid;	/* result is always composite */
			break;
		case T_PGRelabelType:
			coll = ((const PGRelabelType *) expr)->resultcollid;
			break;
		case T_PGCoerceViaIO:
			coll = ((const PGCoerceViaIO *) expr)->resultcollid;
			break;
		case T_PGArrayCoerceExpr:
			coll = ((const PGArrayCoerceExpr *) expr)->resultcollid;
			break;
		case T_PGConvertRowtypeExpr:
			coll = InvalidOid;	/* result is always composite */
			break;
		case T_PGCollateExpr:
			coll = ((const PGCollateExpr *) expr)->collOid;
			break;
		case T_PGCaseExpr:
			coll = ((const PGCaseExpr *) expr)->casecollid;
			break;
		case T_PGCaseTestExpr:
			coll = ((const PGCaseTestExpr *) expr)->collation;
			break;
		case T_PGArrayExpr:
			coll = ((const PGArrayExpr *) expr)->array_collid;
			break;
		case T_PGRowExpr:
			coll = InvalidOid;	/* result is always composite */
			break;
		// case T_PGTableValueExpr:
		// 	coll = InvalidOid;  /* result is always anytable */
		// 	break;
		case T_PGRowCompareExpr:
			coll = InvalidOid;	/* result is always boolean */
			break;
		case T_PGCoalesceExpr:
			coll = ((const PGCoalesceExpr *) expr)->coalescecollid;
			break;
		case T_PGMinMaxExpr:
			coll = ((const PGMinMaxExpr *) expr)->minmaxcollid;
			break;
		// case T_PGXmlExpr:

		// 	/*
		// 	 * XMLSERIALIZE returns text from non-collatable inputs, so its
		// 	 * collation is always default.  The other cases return boolean or
		// 	 * XML, which are non-collatable.
		// 	 */
		// 	if (((const XmlExpr *) expr)->op == IS_XMLSERIALIZE)
		// 		coll = PGDEFAULT_COLLATION_OID;
		// 	else
		// 		coll = InvalidOid;
		// 	break;
		case T_PGNullTest:
			coll = InvalidOid;	/* result is always boolean */
			break;
		case T_PGBooleanTest:
			coll = InvalidOid;	/* result is always boolean */
			break;
		case T_PGCoerceToDomain:
			coll = ((const PGCoerceToDomain *) expr)->resultcollid;
			break;
		case T_PGCoerceToDomainValue:
			coll = ((const PGCoerceToDomainValue *) expr)->collation;
			break;
		case T_PGSetToDefault:
			coll = ((const PGSetToDefault *) expr)->collation;
			break;
		case T_PGCurrentOfExpr:
			coll = InvalidOid;	/* result is always boolean */
			break;
		// case T_PGPlaceHolderVar:
		// 	coll = exprCollation((PGNode *) ((const PGPlaceHolderVar *) expr)->phexpr);
		// 	break;

		case T_PGGroupingFunc:
			coll = InvalidOid;	/* result is always int8 */
			break;
		// case T_Grouping:
		// 	coll = InvalidOid;	/* result is always int8 */
		// 	break;
		// case T_GroupId:
		// 	coll = InvalidOid;	/* result is always int4 */
		// 	break;

		// case T_DMLActionExpr:
		// case T_PartSelectedExpr:
		// case T_PartDefaultExpr:
		// case T_PartBoundExpr:
		// case T_PartBoundInclusionExpr:
		// case T_PartBoundOpenExpr:
		// case T_PartListRuleExpr:
		// case T_PartListNullTestExpr:
		// 	/*
		// 	 * ORCA currently does not support collation,
		// 	 * so return invalid oid for ORCA only expressions
		// 	 */
		// 	coll = InvalidOid;
		// 	break;
		default:
			elog(ERROR, "unrecognized node type: %d", (int) nodeTag(expr));
			coll = InvalidOid;	/* keep compiler quiet */
			break;
	}
	return coll;
};

}
