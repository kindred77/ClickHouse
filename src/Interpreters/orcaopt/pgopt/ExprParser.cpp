#include <ExprParser.h>

using namespace duckdb_libpgquery;

namespace DB
{

PGNode *
ExprParser::transformExpr(PGParseState *pstate, PGNode *expr, PGParseExprKind exprKind)
{
    PGNode	   *result;
	PGParseExprKind sv_expr_kind;

	/* Save and restore identity of expression type we're parsing */
	Assert(exprKind != EXPR_KIND_NONE);
	sv_expr_kind = pstate->p_expr_kind;
	pstate->p_expr_kind = exprKind;

	result = transformExprRecurse(pstate, expr);

	pstate->p_expr_kind = sv_expr_kind;

	return result;
};

PGNode *
ExprParser::transformExprRecurse(PGParseState *pstate, PGNode *expr)
{
    PGNode *result;

	if (expr == NULL)
		return NULL;

	/* Guard against stack overflow due to overly complex expressions */
	check_stack_depth();

	switch (nodeTag(expr))
	{
		case T_PGColumnRef:
			result = transformColumnRef(pstate, (PGColumnRef *) expr);
			break;

		case T_PGParamRef:
			result = transformParamRef(pstate, (PGParamRef *) expr);
			break;

		case T_PGAConst:
			{
				PGAConst    *con = (PGAConst *) expr;
				PGValue	   *val = &con->val;

				result = (PGNode *) make_const(pstate, val, con->location);
				break;
			}

		case T_PGAIndirection:
			{
				PGAIndirection *ind = (PGAIndirection *) expr;

				result = transformExprRecurse(pstate, ind->arg);
				result = transformIndirection(pstate, result,
											  ind->indirection);
				break;
			}

		case T_PGAArrayExpr:
			result = transformArrayExpr(pstate, (PGAArrayExpr *) expr,
										InvalidOid, InvalidOid, -1);
			break;

		case T_PGTypeCast:
			{
				PGTypeCast   *tc = (PGTypeCast *) expr;

				/*
				 * If the subject of the typecast is an ARRAY[] construct and
				 * the target type is an array type, we invoke
				 * transformArrayExpr() directly so that we can pass down the
				 * type information.  This avoids some cases where
				 * transformArrayExpr() might not infer the correct type.
				 */
				if (IsA(tc->arg, PGAArrayExpr))
				{
					Oid			targetType;
					Oid			elementType;
					int32		targetTypmod;

					typenameTypeIdAndMod(pstate, tc->typeName,
										 &targetType, &targetTypmod);

					/*
					 * If target is a domain over array, work with the base
					 * array type here.  transformTypeCast below will cast the
					 * array type to the domain.  In the usual case that the
					 * target is not a domain, transformTypeCast is a no-op.
					 */
					targetType = getBaseTypeAndTypmod(targetType,
													  &targetTypmod);
					elementType = get_element_type(targetType);
					if (OidIsValid(elementType))
					{
						tc = copyObject(tc);
						tc->arg = transformArrayExpr(pstate,
													 (PGAArrayExpr *) tc->arg,
													 targetType,
													 elementType,
													 targetTypmod);
					}
				}

				result = transformTypeCast(pstate, tc);
				break;
			}

		case T_PGCollateClause:
			result = transformCollateClause(pstate, (PGCollateClause *) expr);
			break;

		case T_PGAExpr:
			{
				PGAExpr	   *a = (PGAExpr *) expr;

				switch (a->kind)
				{
					case PG_AEXPR_OP:
						result = transformAExprOp(pstate, a);
						break;
					case PG_AEXPR_AND:
						result = transformAExprAnd(pstate, a);
						break;
					case PG_AEXPR_OR:
						result = transformAExprOr(pstate, a);
						break;
					case PG_AEXPR_NOT:
						result = transformAExprNot(pstate, a);
						break;
					case PG_AEXPR_OP_ANY:
						result = transformAExprOpAny(pstate, a);
						break;
					case PG_AEXPR_OP_ALL:
						result = transformAExprOpAll(pstate, a);
						break;
					case PG_AEXPR_DISTINCT:
						result = transformAExprDistinct(pstate, a);
						break;
					case PG_AEXPR_NULLIF:
						result = transformAExprNullIf(pstate, a);
						break;
					case PG_AEXPR_OF:
						result = transformAExprOf(pstate, a);
						break;
					case PG_AEXPR_IN:
						result = transformAExprIn(pstate, a);
						break;
					default:
						elog(ERROR, "unrecognized A_Expr kind: %d", a->kind);
						result = NULL;	/* keep compiler quiet */
						break;
				}
				break;
			}

		case T_PGFuncCall:
			result = transformFuncCall(pstate, (PGFuncCall *) expr);
			break;

		case T_PGNamedArgExpr:
			{
				PGNamedArgExpr *na = (PGNamedArgExpr *) expr;

				na->arg = (PGExpr *) transformExprRecurse(pstate, (PGNode *) na->arg);
				result = expr;
				break;
			}

		case T_PGSubLink:
			result = transformSubLink(pstate, (PGSubLink *) expr);
			break;

		case T_PGCaseExpr:
			result = transformCaseExpr(pstate, (PGCaseExpr *) expr);
			break;

		case T_PGRowExpr:
			result = transformRowExpr(pstate, (PGRowExpr *) expr);
			break;

		case T_PGTableValueExpr:
			result = transformTableValueExpr(pstate, (PGTableValueExpr *) expr);
			break;

		case T_PGCoalesceExpr:
			result = transformCoalesceExpr(pstate, (PGCoalesceExpr *) expr);
			break;

		case T_PGMinMaxExpr:
			result = transformMinMaxExpr(pstate, (PGMinMaxExpr *) expr);
			break;

		case T_PGXmlExpr:
			result = transformXmlExpr(pstate, (PGXmlExpr *) expr);
			break;

		case T_PGXmlSerialize:
			result = transformXmlSerialize(pstate, (PGXmlSerialize *) expr);
			break;

		case T_PGNullTest:
			{
				PGNullTest   *n = (PGNullTest *) expr;

				n->arg = (PGExpr *) transformExprRecurse(pstate, (PGNode *) n->arg);
				/* the argument can be any type, so don't coerce it */
				n->argisrow = type_is_rowtype(exprType((PGNode *) n->arg));
				result = expr;
				break;
			}

		case T_PGBooleanTest:
			result = transformBooleanTest(pstate, (PGBooleanTest *) expr);
			break;

		case T_PGCurrentOfExpr:
			result = transformCurrentOfExpr(pstate, (PGCurrentOfExpr *) expr);
			break;

		case T_PGGroupingFunc:
			{
				PGGroupingFunc *gf = (PGGroupingFunc *)expr;
				result = transformGroupingFunc(pstate, gf);
				break;
			}

		case T_PGPartitionBoundSpec:
			{
				PGPartitionBoundSpec *in = (PGPartitionBoundSpec *)expr;
				PGPartitionRangeItem *ri;
				PGList *out = NIL;
				PGListCell *lc;

				if (in->partStart)
				{
					ri = (PGPartitionRangeItem *)in->partStart;

					/* ALTER TABLE ... ADD PARTITION might feed
					 * "pre-cooked" expressions into the boundspec for
					 * range items (which are Lists) 
					 */
					{
						Assert(IsA(in->partStart, PartitionRangeItem));

						foreach(lc, ri->partRangeVal)
						{
							PGNode *n = lfirst(lc);
							out = lappend(out, transformExpr(pstate, n,
															 EXPR_KIND_PARTITION_EXPRESSION));
						}
						ri->partRangeVal = out;
						out = NIL;
					}
				}
				if (in->partEnd)
				{
					ri = (PGPartitionRangeItem *)in->partEnd;

					/* ALTER TABLE ... ADD PARTITION might feed
					 * "pre-cooked" expressions into the boundspec for
					 * range items (which are Lists) 
					 */
					{
						Assert(IsA(in->partEnd, PGPartitionRangeItem));
						foreach(lc, ri->partRangeVal)
						{
							PGNode *n = lfirst(lc);
							out = lappend(out, transformExpr(pstate, n,
															 EXPR_KIND_PARTITION_EXPRESSION));
						}
						ri->partRangeVal = out;
						out = NIL;
					}
				}
				if (in->partEvery)
				{
					ri = (PGPartitionRangeItem *)in->partEvery;
					Assert(IsA(in->partEvery, PGPartitionRangeItem));
					foreach(lc, ri->partRangeVal)
					{
						PGNode *n = lfirst(lc);
						out = lappend(out, transformExpr(pstate, n,
														 EXPR_KIND_PARTITION_EXPRESSION));
					}
					ri->partRangeVal = out;
				}

				result = (PGNode *)in;
			}
			break;

			/*********************************************
			 * Quietly accept node types that may be presented when we are
			 * called on an already-transformed tree.
			 *
			 * Do any other node types need to be accepted?  For now we are
			 * taking a conservative approach, and only accepting node
			 * types that are demonstrably necessary to accept.
			 *********************************************/
		case T_PGVar:
		case T_PGConst:
		case T_PGParam:
		case T_PGAggref:
		case T_PGArrayRef:
		case T_PGFuncExpr:
		case T_PGOpExpr:
		case T_PGDistinctExpr:
		case T_PGNullIfExpr:
		case T_PGScalarArrayOpExpr:
		case T_PGBoolExpr:
		case T_PGFieldSelect:
		case T_PGFieldStore:
		case T_PGRelabelType:
		case T_PGCoerceViaIO:
		case T_PGArrayCoerceExpr:
		case T_PGConvertRowtypeExpr:
		case T_PGCollateExpr:
		case T_PGCaseTestExpr:
		case T_PGArrayExpr:
		case T_PGCoerceToDomain:
		case T_PGCoerceToDomainValue:
		case T_PGSetToDefault:
		case T_PGGroupId:
		case T_PGInteger:
			{
				result = (PGNode *) expr;
				break;
			}

		default:
			/* should not reach here */
			elog(ERROR, "unrecognized node type: %d", (int) nodeTag(expr));
			result = NULL;		/* keep compiler quiet */
			break;
	}

	return result;
};

PGNode *
ExprParser::transformColumnRef(PGParseState *pstate, PGColumnRef *cref)
{
    PGNode	   *node = NULL;
	char	   *nspname = NULL;
	char	   *relname = NULL;
	char	   *colname = NULL;
	PGRangeTblEntry *rte;
	int			levels_up;
	enum
	{
		CRERR_NO_COLUMN,
		CRERR_NO_RTE,
		CRERR_WRONG_DB,
		CRERR_TOO_MANY
	}			crerr = CRERR_NO_COLUMN;

	/*
	 * Give the PreParseColumnRefHook, if any, first shot.  If it returns
	 * non-null then that's all, folks.
	 */
	/*if (pstate->p_pre_columnref_hook != NULL)
	{
		node = (*pstate->p_pre_columnref_hook) (pstate, cref);
		if (node != NULL)
			return node;
	}*/

	/*----------
	 * The allowed syntaxes are:
	 *
	 * A		First try to resolve as unqualified column name;
	 *			if no luck, try to resolve as unqualified table name (A.*).
	 * A.B		A is an unqualified table name; B is either a
	 *			column or function name (trying column name first).
	 * A.B.C	schema A, table B, col or func name C.
	 * A.B.C.D	catalog A, schema B, table C, col or func D.
	 * A.*		A is an unqualified table name; means whole-row value.
	 * A.B.*	whole-row value of table B in schema A.
	 * A.B.C.*	whole-row value of table C in schema B in catalog A.
	 *
	 * We do not need to cope with bare "*"; that will only be accepted by
	 * the grammar at the top level of a SELECT list, and transformTargetList
	 * will take care of it before it ever gets here.  Also, "A.*" etc will
	 * be expanded by transformTargetList if they appear at SELECT top level,
	 * so here we are only going to see them as function or operator inputs.
	 *
	 * Currently, if a catalog name is given then it must equal the current
	 * database name; we check it here and then discard it.
	 *----------
	 */
	switch (list_length(cref->fields))
	{
		case 1:
			{
				PGNode *field1 = (PGNode *) linitial(cref->fields);

				Assert(IsA(field1, String));
				colname = strVal(field1);

				/* Try to identify as an unqualified column */
				node = colNameToVar(pstate, colname, false, cref->location);

				if (node == NULL)
				{
					/*
					 * Not known as a column of any range-table entry.
					 *
					 * Consider the possibility that it's VALUE in a domain
					 * check expression.  (We handle VALUE as a name, not a
					 * keyword, to avoid breaking a lot of applications that
					 * have used VALUE as a column name in the past.)
					 */
					if (pstate->p_value_substitute != NULL &&
						strcmp(colname, "value") == 0)
					{
						node = (Node *) copyObject(pstate->p_value_substitute);

						/*
						 * Try to propagate location knowledge.  This should
						 * be extended if p_value_substitute can ever take on
						 * other node types.
						 */
						if (IsA(node, CoerceToDomainValue))
							((CoerceToDomainValue *) node)->location = cref->location;
						break;
					}

					/*
					 * Try to find the name as a relation.  Note that only
					 * relations already entered into the rangetable will be
					 * recognized.
					 *
					 * This is a hack for backwards compatibility with
					 * PostQUEL-inspired syntax.  The preferred form now is
					 * "rel.*".
					 */
					rte = refnameRangeTblEntry(pstate, NULL, colname,
											   cref->location,
											   &levels_up);
					if (rte)
						node = transformWholeRowRef(pstate, rte,
													cref->location);
				}
				break;
			}
		case 2:
			{
				Node	   *field1 = (Node *) linitial(cref->fields);
				Node	   *field2 = (Node *) lsecond(cref->fields);

				Assert(IsA(field1, String));
				relname = strVal(field1);

				/* Locate the referenced RTE */
				rte = refnameRangeTblEntry(pstate, nspname, relname,
										   cref->location,
										   &levels_up);
				if (rte == NULL)
				{
					crerr = CRERR_NO_RTE;
					break;
				}

				/* Whole-row reference? */
				if (IsA(field2, A_Star))
				{
					node = transformWholeRowRef(pstate, rte, cref->location);
					break;
				}

				Assert(IsA(field2, String));
				colname = strVal(field2);

				/* Try to identify as a column of the RTE */
				node = scanRTEForColumn(pstate, rte, colname, cref->location);
				if (node == NULL)
				{
					/* Try it as a function call on the whole row */
					node = transformWholeRowRef(pstate, rte, cref->location);
					node = ParseFuncOrColumn(pstate,
											 list_make1(makeString(colname)),
											 list_make1(node),
											 NULL,
											 cref->location);
				}
				break;
			}
		case 3:
			{
				Node	   *field1 = (Node *) linitial(cref->fields);
				Node	   *field2 = (Node *) lsecond(cref->fields);
				Node	   *field3 = (Node *) lthird(cref->fields);

				Assert(IsA(field1, String));
				nspname = strVal(field1);
				Assert(IsA(field2, String));
				relname = strVal(field2);

				/* Locate the referenced RTE */
				rte = refnameRangeTblEntry(pstate, nspname, relname,
										   cref->location,
										   &levels_up);
				if (rte == NULL)
				{
					crerr = CRERR_NO_RTE;
					break;
				}

				/* Whole-row reference? */
				if (IsA(field3, A_Star))
				{
					node = transformWholeRowRef(pstate, rte, cref->location);
					break;
				}

				Assert(IsA(field3, String));
				colname = strVal(field3);

				/* Try to identify as a column of the RTE */
				node = scanRTEForColumn(pstate, rte, colname, cref->location);
				if (node == NULL)
				{
					/* Try it as a function call on the whole row */
					node = transformWholeRowRef(pstate, rte, cref->location);
					node = ParseFuncOrColumn(pstate,
											 list_make1(makeString(colname)),
											 list_make1(node),
											 NULL,
											 cref->location);
				}
				break;
			}
		case 4:
			{
				Node	   *field1 = (Node *) linitial(cref->fields);
				Node	   *field2 = (Node *) lsecond(cref->fields);
				Node	   *field3 = (Node *) lthird(cref->fields);
				Node	   *field4 = (Node *) lfourth(cref->fields);
				char	   *catname;

				Assert(IsA(field1, String));
				catname = strVal(field1);
				Assert(IsA(field2, String));
				nspname = strVal(field2);
				Assert(IsA(field3, String));
				relname = strVal(field3);

				/*
				 * We check the catalog name and then ignore it.
				 */
				if (strcmp(catname, get_database_name(MyDatabaseId)) != 0)
				{
					crerr = CRERR_WRONG_DB;
					break;
				}

				/* Locate the referenced RTE */
				rte = refnameRangeTblEntry(pstate, nspname, relname,
										   cref->location,
										   &levels_up);
				if (rte == NULL)
				{
					crerr = CRERR_NO_RTE;
					break;
				}

				/* Whole-row reference? */
				if (IsA(field4, A_Star))
				{
					node = transformWholeRowRef(pstate, rte, cref->location);
					break;
				}

				Assert(IsA(field4, String));
				colname = strVal(field4);

				/* Try to identify as a column of the RTE */
				node = scanRTEForColumn(pstate, rte, colname, cref->location);
				if (node == NULL)
				{
					/* Try it as a function call on the whole row */
					node = transformWholeRowRef(pstate, rte, cref->location);
					node = ParseFuncOrColumn(pstate,
											 list_make1(makeString(colname)),
											 list_make1(node),
											 NULL,
											 cref->location);
				}
				break;
			}
		default:
			crerr = CRERR_TOO_MANY;		/* too many dotted names */
			break;
	}

	/*
	 * Now give the PostParseColumnRefHook, if any, a chance.  We pass the
	 * translation-so-far so that it can throw an error if it wishes in the
	 * case that it has a conflicting interpretation of the ColumnRef. (If it
	 * just translates anyway, we'll throw an error, because we can't undo
	 * whatever effects the preceding steps may have had on the pstate.) If it
	 * returns NULL, use the standard translation, or throw a suitable error
	 * if there is none.
	 */
	if (pstate->p_post_columnref_hook != NULL)
	{
		Node	   *hookresult;

		hookresult = (*pstate->p_post_columnref_hook) (pstate, cref, node);
		if (node == NULL)
			node = hookresult;
		else if (hookresult != NULL)
			ereport(ERROR,
					(errcode(ERRCODE_AMBIGUOUS_COLUMN),
					 errmsg("column reference \"%s\" is ambiguous",
							NameListToString(cref->fields)),
					 parser_errposition(pstate, cref->location)));
	}

	/*
	 * Throw error if no translation found.
	 */
	if (node == NULL)
	{
		switch (crerr)
		{
			case CRERR_NO_COLUMN:
				errorMissingColumn(pstate, relname, colname, cref->location);
				break;
			case CRERR_NO_RTE:
				errorMissingRTE(pstate, makeRangeVar(nspname, relname,
													 cref->location));
				break;
			case CRERR_WRONG_DB:
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				  errmsg("cross-database references are not implemented: %s",
						 NameListToString(cref->fields)),
						 parser_errposition(pstate, cref->location)));
				break;
			case CRERR_TOO_MANY:
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
				errmsg("improper qualified name (too many dotted names): %s",
					   NameListToString(cref->fields)),
						 parser_errposition(pstate, cref->location)));
				break;
		}
	}

	return node;
};

}