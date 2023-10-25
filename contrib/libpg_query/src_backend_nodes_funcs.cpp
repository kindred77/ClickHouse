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
 * leftmostLoc - support for exprLocation
 *
 * Take the minimum of two parse location values, but ignore unknowns
 */
static int
leftmostLoc(int loc1, int loc2)
{
	if (loc1 < 0)
		return loc2;
	else if (loc2 < 0)
		return loc1;
	else
		return Min(loc1, loc2);
}

/*
 *	exprLocation -
 *	  returns the parse location of an expression tree, for error reports
 *
 * -1 is returned if the location can't be determined.
 *
 * For expressions larger than a single token, the intent here is to
 * return the location of the expression's leftmost token, not necessarily
 * the topmost Node's location field.  For example, an OpExpr's location
 * field will point at the operator name, but if it is not a prefix operator
 * then we should return the location of the left-hand operand instead.
 * The reason is that we want to reference the entire expression not just
 * that operator, and pointing to its start seems to be the most natural way.
 *
 * The location is not perfect --- for example, since the grammar doesn't
 * explicitly represent parentheses in the parsetree, given something that
 * had been written "(a + b) * c" we are going to point at "a" not "(".
 * But it should be plenty good enough for error reporting purposes.
 *
 * You might think that this code is overly general, for instance why check
 * the operands of a FuncExpr node, when the function name can be expected
 * to be to the left of them?  There are a couple of reasons.  The grammar
 * sometimes builds expressions that aren't quite what the user wrote;
 * for instance x IS NOT BETWEEN ... becomes a NOT-expression whose keyword
 * pointer is to the right of its leftmost argument.  Also, nodes that were
 * inserted implicitly by parse analysis (such as FuncExprs for implicit
 * coercions) will have location -1, and so we can have odd combinations of
 * known and unknown locations in a tree.
 */
int
exprLocation(const PGNode *expr)
{
	int			loc;

	if (expr == NULL)
		return -1;
	switch (nodeTag(expr))
	{
		case T_PGRangeVar:
			loc = ((const PGRangeVar *) expr)->location;
			break;
		case T_PGVar:
			loc = ((const PGVar *) expr)->location;
			break;
		case T_PGConst:
			loc = ((const PGConst *) expr)->location;
			break;
		case T_PGParam:
			loc = ((const PGParam *) expr)->location;
			break;
		case T_PGAggref:
			/* function name should always be the first thing */
			loc = ((const PGAggref *) expr)->location;
			break;
		case T_PGWindowFunc:
			/* function name should always be the first thing */
			loc = ((const PGWindowFunc *) expr)->location;
			break;
		case T_PGArrayRef:
			/* just use array argument's location */
			loc = exprLocation((PGNode *) ((const PGArrayRef *) expr)->refexpr);
			break;
		case T_PGFuncExpr:
			{
				const PGFuncExpr *fexpr = (const PGFuncExpr *) expr;

				/* consider both function name and leftmost arg */
				loc = leftmostLoc(fexpr->location,
								  exprLocation((PGNode *) fexpr->args));
			}
			break;
		case T_PGNamedArgExpr:
			{
				const PGNamedArgExpr *na = (const PGNamedArgExpr *) expr;

				/* consider both argument name and value */
				loc = leftmostLoc(na->location,
								  exprLocation((PGNode *) na->arg));
			}
			break;
		case T_PGOpExpr:
		case T_PGDistinctExpr:	/* struct-equivalent to OpExpr */
		case T_PGNullIfExpr:		/* struct-equivalent to OpExpr */
			{
				const PGOpExpr *opexpr = (const PGOpExpr *) expr;

				/* consider both operator name and leftmost arg */
				loc = leftmostLoc(opexpr->location,
								  exprLocation((PGNode *) opexpr->args));
			}
			break;
		case T_PGScalarArrayOpExpr:
			{
				const PGScalarArrayOpExpr *saopexpr = (const PGScalarArrayOpExpr *) expr;

				/* consider both operator name and leftmost arg */
				loc = leftmostLoc(saopexpr->location,
								  exprLocation((PGNode *) saopexpr->args));
			}
			break;
		case T_PGBoolExpr:
			{
				const PGBoolExpr *bexpr = (const PGBoolExpr *) expr;

				/*
				 * Same as above, to handle either NOT or AND/OR.  We can't
				 * special-case NOT because of the way that it's used for
				 * things like IS NOT BETWEEN.
				 */
				loc = leftmostLoc(bexpr->location,
								  exprLocation((PGNode *) bexpr->args));
			}
			break;
		case T_PGSubLink:
			{
				const PGSubLink *sublink = (const PGSubLink *) expr;

				/* check the testexpr, if any, and the operator/keyword */
				loc = leftmostLoc(exprLocation(sublink->testexpr),
								  sublink->location);
			}
			break;
		case T_PGFieldSelect:
			/* just use argument's location */
			loc = exprLocation((PGNode *) ((const PGFieldSelect *) expr)->arg);
			break;
		case T_PGFieldStore:
			/* just use argument's location */
			loc = exprLocation((PGNode *) ((const PGFieldStore *) expr)->arg);
			break;
		case T_PGRelabelType:
			{
				const PGRelabelType *rexpr = (const PGRelabelType *) expr;

				/* Much as above */
				loc = leftmostLoc(rexpr->location,
								  exprLocation((PGNode *) rexpr->arg));
			}
			break;
		case T_PGCoerceViaIO:
			{
				const PGCoerceViaIO *cexpr = (const PGCoerceViaIO *) expr;

				/* Much as above */
				loc = leftmostLoc(cexpr->location,
								  exprLocation((PGNode *) cexpr->arg));
			}
			break;
		case T_PGArrayCoerceExpr:
			{
				const PGArrayCoerceExpr *cexpr = (const PGArrayCoerceExpr *) expr;

				/* Much as above */
				loc = leftmostLoc(cexpr->location,
								  exprLocation((PGNode *) cexpr->arg));
			}
			break;
		case T_PGConvertRowtypeExpr:
			{
				const PGConvertRowtypeExpr *cexpr = (const PGConvertRowtypeExpr *) expr;

				/* Much as above */
				loc = leftmostLoc(cexpr->location,
								  exprLocation((PGNode *) cexpr->arg));
			}
			break;
		case T_PGCollateExpr:
			/* just use argument's location */
			loc = exprLocation((PGNode *) ((const PGCollateExpr *) expr)->arg);
			break;
		case T_PGCaseExpr:
			/* CASE keyword should always be the first thing */
			loc = ((const PGCaseExpr *) expr)->location;
			break;
		case T_PGCaseWhen:
			/* WHEN keyword should always be the first thing */
			loc = ((const PGCaseWhen *) expr)->location;
			break;
		case T_PGArrayExpr:
			/* the location points at ARRAY or [, which must be leftmost */
			loc = ((const PGArrayExpr *) expr)->location;
			break;
		case T_PGRowExpr:
			/* the location points at ROW or (, which must be leftmost */
			loc = ((const PGRowExpr *) expr)->location;
			break;
		// case T_TableValueExpr:
		// 	/* the location points at TABLE, which must be leftmost */
		// 	loc = ((TableValueExpr *) expr)->location;
		// 	break;
		case T_PGRowCompareExpr:
			/* just use leftmost argument's location */
			loc = exprLocation((PGNode *) ((const PGRowCompareExpr *) expr)->largs);
			break;
		case T_PGCoalesceExpr:
			/* COALESCE keyword should always be the first thing */
			loc = ((const PGCoalesceExpr *) expr)->location;
			break;
		case T_PGMinMaxExpr:
			/* GREATEST/LEAST keyword should always be the first thing */
			loc = ((const PGMinMaxExpr *) expr)->location;
			break;
		// case T_XmlExpr:
		// 	{
		// 		const XmlExpr *xexpr = (const XmlExpr *) expr;

		// 		/* consider both function name and leftmost arg */
		// 		loc = leftmostLoc(xexpr->location,
		// 						  exprLocation((Node *) xexpr->args));
		// 	}
		// 	break;
		case T_PGNullTest:
			/* just use argument's location */
			loc = exprLocation((PGNode *) ((const PGNullTest *) expr)->arg);
			break;
		case T_PGBooleanTest:
			/* just use argument's location */
			loc = exprLocation((PGNode *) ((const PGBooleanTest *) expr)->arg);
			break;
		case T_PGCoerceToDomain:
			{
				const PGCoerceToDomain *cexpr = (const PGCoerceToDomain *) expr;

				/* Much as above */
				loc = leftmostLoc(cexpr->location,
								  exprLocation((PGNode *) cexpr->arg));
			}
			break;
		case T_PGCoerceToDomainValue:
			loc = ((const PGCoerceToDomainValue *) expr)->location;
			break;
		case T_PGSetToDefault:
			loc = ((const PGSetToDefault *) expr)->location;
			break;
		case T_PGTargetEntry:
			/* just use argument's location */
			loc = exprLocation((PGNode *) ((const PGTargetEntry *) expr)->expr);
			break;
		case T_PGIntoClause:
			/* use the contained RangeVar's location --- close enough */
			loc = exprLocation((PGNode *) ((const PGIntoClause *) expr)->rel);
			break;
		case T_PGList:
			{
				/* report location of first list member that has a location */
				PGListCell   *lc;

				loc = -1;		/* just to suppress compiler warning */
				foreach(lc, (const PGList *) expr)
				{
					loc = exprLocation((PGNode *) lfirst(lc));
					if (loc >= 0)
						break;
				}
			}
			break;
		case T_PGAExpr:
			{
				const PGAExpr *aexpr = (const PGAExpr *) expr;

				/* use leftmost of operator or left operand (if any) */
				/* we assume right operand can't be to left of operator */
				loc = leftmostLoc(aexpr->location,
								  exprLocation(aexpr->lexpr));
			}
			break;
		case T_PGColumnRef:
			loc = ((const PGColumnRef *) expr)->location;
			break;
		case T_PGParamRef:
			loc = ((const PGParamRef *) expr)->location;
			break;
		case T_PGAConst:
			loc = ((const PGAConst *) expr)->location;
			break;
		case T_PGFuncCall:
			{
				const PGFuncCall *fc = (const PGFuncCall *) expr;

				/* consider both function name and leftmost arg */
				/* (we assume any ORDER BY nodes must be to right of name) */
				loc = leftmostLoc(fc->location,
								  exprLocation((PGNode *) fc->args));
			}
			break;
		case T_PGAArrayExpr:
			/* the location points at ARRAY or [, which must be leftmost */
			loc = ((const PGAArrayExpr *) expr)->location;
			break;
		case T_PGResTarget:
			/* we need not examine the contained expression (if any) */
			loc = ((const PGResTarget *) expr)->location;
			break;
		case T_PGTypeCast:
			{
				const PGTypeCast *tc = (const PGTypeCast *) expr;

				/*
				 * This could represent CAST(), ::, or TypeName 'literal', so
				 * any of the components might be leftmost.
				 */
				loc = exprLocation(tc->arg);
				loc = leftmostLoc(loc, tc->typeName->location);
				loc = leftmostLoc(loc, tc->location);
			}
			break;
		case T_PGCollateClause:
			/* just use argument's location */
			loc = exprLocation(((const PGCollateClause *) expr)->arg);
			break;
		case T_PGSortBy:
			/* just use argument's location (ignore operator, if any) */
			loc = exprLocation(((const PGSortBy *) expr)->node);
			break;
		case T_PGWindowDef:
			loc = ((const PGWindowDef *) expr)->location;
			break;
		case T_PGTypeName:
			loc = ((const PGTypeName *) expr)->location;
			break;
		case T_PGColumnDef:
			loc = ((const PGColumnDef *) expr)->location;
			break;
		case T_PGConstraint:
			loc = ((const PGConstraint *) expr)->location;
			break;
		// case T_FunctionParameter:
		// 	/* just use typename's location */
		// 	loc = exprLocation((PGNode *) ((const FunctionParameter *) expr)->argType);
		// 	break;
		// case T_XmlSerialize:
		// 	/* XMLSERIALIZE keyword should always be the first thing */
		// 	loc = ((const XmlSerialize *) expr)->location;
		// 	break;
		case T_PGWithClause:
			loc = ((const PGWithClause *) expr)->location;
			break;
		case T_PGCommonTableExpr:
			loc = ((const PGCommonTableExpr *) expr)->location;
			break;
		// case T_PlaceHolderVar:
		// 	/* just use argument's location */
		// 	loc = exprLocation((Node *) ((const PlaceHolderVar *) expr)->phexpr);
		// 	break;
		default:
			/* for any other node type it's just unknown... */
			loc = -1;
			break;
	}
	return loc;
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
