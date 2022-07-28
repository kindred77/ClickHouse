#include <Interpreters/orcaopt/pgopt/ExprParser.h>

using namespace duckdb_libpgquery;

namespace DB
{

PGNode *
ExprParser::transformExpr(PGParseState *pstate, PGNode *expr, PGParseExprKind exprKind)
{
    PGNode	   *result;
	PGParseExprKind sv_expr_kind;

	/* Save and restore identity of expression type we're parsing */
	Assert(exprKind != PGParseExprKind::EXPR_KIND_NONE);
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

					type_parser.typenameTypeIdAndMod(pstate, tc->typeName,
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
					if (elementType != InvalidOid)
					{
						tc = reinterpret_cast<PGTypeCast*>(copyObject(tc));
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
					// case PG_AEXPR_AND:
					// 	result = transformAExprAnd(pstate, a);
					// 	break;
					// case PG_AEXPR_OR:
					// 	result = transformAExprOr(pstate, a);
					// 	break;
					// case PG_AEXPR_NOT:
					// 	result = transformAExprNot(pstate, a);
					// 	break;
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

		case T_PGBoolExpr:
			result = transformBoolExpr(pstate, (PGBoolExpr *) expr);
			break;

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

		// case T_PGTableValueExpr:
		// 	result = transformTableValueExpr(pstate, (PGTableValueExpr *) expr);
		// 	break;

		case T_PGCoalesceExpr:
			result = transformCoalesceExpr(pstate, (PGCoalesceExpr *) expr);
			break;

		case T_PGMinMaxExpr:
			result = transformMinMaxExpr(pstate, (PGMinMaxExpr *) expr);
			break;

		// case T_PGXmlExpr:
		// 	result = transformXmlExpr(pstate, (PGXmlExpr *) expr);
		// 	break;

		// case T_PGXmlSerialize:
		// 	result = transformXmlSerialize(pstate, (PGXmlSerialize *) expr);
		// 	break;

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

		// case T_PGPartitionBoundSpec:
		// 	{
		// 		PGPartitionBoundSpec *in = (PGPartitionBoundSpec *)expr;
		// 		PGPartitionRangeItem *ri;
		// 		PGList *out = NIL;
		// 		PGListCell *lc;

		// 		if (in->partStart)
		// 		{
		// 			ri = (PGPartitionRangeItem *)in->partStart;

		// 			/* ALTER TABLE ... ADD PARTITION might feed
		// 			 * "pre-cooked" expressions into the boundspec for
		// 			 * range items (which are Lists) 
		// 			 */
		// 			{
		// 				Assert(IsA(in->partStart, PartitionRangeItem));

		// 				foreach(lc, ri->partRangeVal)
		// 				{
		// 					PGNode *n = lfirst(lc);
		// 					out = lappend(out, transformExpr(pstate, n,
		// 													 PGParseExprKind::EXPR_KIND_PARTITION_EXPRESSION));
		// 				}
		// 				ri->partRangeVal = out;
		// 				out = NIL;
		// 			}
		// 		}
		// 		if (in->partEnd)
		// 		{
		// 			ri = (PGPartitionRangeItem *)in->partEnd;

		// 			/* ALTER TABLE ... ADD PARTITION might feed
		// 			 * "pre-cooked" expressions into the boundspec for
		// 			 * range items (which are Lists) 
		// 			 */
		// 			{
		// 				Assert(IsA(in->partEnd, PGPartitionRangeItem));
		// 				foreach(lc, ri->partRangeVal)
		// 				{
		// 					PGNode *n = lfirst(lc);
		// 					out = lappend(out, transformExpr(pstate, n,
		// 													 PGParseExprKind::EXPR_KIND_PARTITION_EXPRESSION));
		// 				}
		// 				ri->partRangeVal = out;
		// 				out = NIL;
		// 			}
		// 		}
		// 		if (in->partEvery)
		// 		{
		// 			ri = (PGPartitionRangeItem *)in->partEvery;
		// 			Assert(IsA(in->partEvery, PGPartitionRangeItem));
		// 			foreach(lc, ri->partRangeVal)
		// 			{
		// 				PGNode *n = lfirst(lc);
		// 				out = lappend(out, transformExpr(pstate, n,
		// 												 PGParseExprKind::EXPR_KIND_PARTITION_EXPRESSION));
		// 			}
		// 			ri->partRangeVal = out;
		// 		}

		// 		result = (PGNode *)in;
		// 	}
		// 	break;

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
		//case T_PGGroupId:
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

				Assert(IsA(field1, PGString));
				colname = strVal(field1);

				/* Try to identify as an unqualified column */
				node = relation_parser.colNameToVar(pstate, colname, false, cref->location);

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
						node = (PGNode *) copyObject(pstate->p_value_substitute);

						/*
						 * Try to propagate location knowledge.  This should
						 * be extended if p_value_substitute can ever take on
						 * other node types.
						 */
						if (IsA(node, PGCoerceToDomainValue))
							((PGCoerceToDomainValue *) node)->location = cref->location;
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
					rte = relation_parser.refnameRangeTblEntry(pstate, NULL, colname,
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
				PGNode	   *field1 = (PGNode *) linitial(cref->fields);
				PGNode	   *field2 = (PGNode *) lsecond(cref->fields);

				Assert(IsA(field1, PGString));
				relname = strVal(field1);

				/* Locate the referenced RTE */
				rte = relation_parser.refnameRangeTblEntry(pstate, nspname, relname,
										   cref->location,
										   &levels_up);
				if (rte == NULL)
				{
					crerr = CRERR_NO_RTE;
					break;
				}

				/* Whole-row reference? */
				if (IsA(field2, PGAStar))
				{
					node = transformWholeRowRef(pstate, rte, cref->location);
					break;
				}

				Assert(IsA(field2, PGString));
				colname = strVal(field2);

				/* Try to identify as a column of the RTE */
				node = relation_parser.scanRTEForColumn(pstate, rte, colname, cref->location);
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
				PGNode	   *field1 = (PGNode *) linitial(cref->fields);
				PGNode	   *field2 = (PGNode *) lsecond(cref->fields);
				PGNode	   *field3 = (PGNode *) lthird(cref->fields);

				Assert(IsA(field1, PGString));
				nspname = strVal(field1);
				Assert(IsA(field2, PGString));
				relname = strVal(field2);

				/* Locate the referenced RTE */
				rte = relation_parser.refnameRangeTblEntry(pstate, nspname, relname,
										   cref->location,
										   &levels_up);
				if (rte == NULL)
				{
					crerr = CRERR_NO_RTE;
					break;
				}

				/* Whole-row reference? */
				if (IsA(field3, PGAStar))
				{
					node = transformWholeRowRef(pstate, rte, cref->location);
					break;
				}

				Assert(IsA(field3, PGString));
				colname = strVal(field3);

				/* Try to identify as a column of the RTE */
				node = relation_parser.scanRTEForColumn(pstate, rte, colname, cref->location);
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
				PGNode	   *field1 = (PGNode *) linitial(cref->fields);
				PGNode	   *field2 = (PGNode *) lsecond(cref->fields);
				PGNode	   *field3 = (PGNode *) lthird(cref->fields);
				PGNode	   *field4 = (PGNode *) lfourth(cref->fields);
				char	   *catname;

				Assert(IsA(field1, PGString));
				catname = strVal(field1);
				Assert(IsA(field2, PGString));
				nspname = strVal(field2);
				Assert(IsA(field3, PGString));
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
				rte = relation_parser.refnameRangeTblEntry(pstate, nspname, relname,
										   cref->location,
										   &levels_up);
				if (rte == NULL)
				{
					crerr = CRERR_NO_RTE;
					break;
				}

				/* Whole-row reference? */
				if (IsA(field4, PGAStar))
				{
					node = transformWholeRowRef(pstate, rte, cref->location);
					break;
				}

				Assert(IsA(field4, PGString));
				colname = strVal(field4);

				/* Try to identify as a column of the RTE */
				node = relation_parser.scanRTEForColumn(pstate, rte, colname, cref->location);
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
		PGNode	   *hookresult;

		hookresult = (*pstate->p_post_columnref_hook) (pstate, cref, node);
		if (node == NULL)
			node = hookresult;
		else if (hookresult != NULL)
			ereport(ERROR,
					(errcode(PG_ERRCODE_SYNTAX_ERROR),
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
						(errcode(PG_ERRCODE_FEATURE_NOT_SUPPORTED),
				  errmsg("cross-database references are not implemented: %s",
						 NameListToString(cref->fields)),
						 parser_errposition(pstate, cref->location)));
				break;
			case CRERR_TOO_MANY:
				ereport(ERROR,
						(errcode(PG_ERRCODE_SYNTAX_ERROR),
				errmsg("improper qualified name (too many dotted names): %s",
					   NameListToString(cref->fields)),
						 parser_errposition(pstate, cref->location)));
				break;
		}
	}

	return node;
};

PGNode *
ExprParser::transformAExprOp(PGParseState *pstate, PGAExpr *a)
{
	PGNode	   *lexpr = a->lexpr;
	PGNode	   *rexpr = a->rexpr;
	PGNode	   *result;

	/*
	 * Special-case "foo = NULL" and "NULL = foo" for compatibility with
	 * standards-broken products (like Microsoft's).  Turn these into IS NULL
	 * exprs. (If either side is a CaseTestExpr, then the expression was
	 * generated internally from a CASE-WHEN expression, and
	 * transform_null_equals does not apply.)
	 */
	if (Transform_null_equals &&
		list_length(a->name) == 1 &&
		strcmp(strVal(linitial(a->name)), "=") == 0 &&
		(exprIsNullConstant(lexpr) || exprIsNullConstant(rexpr)) &&
		(!IsA(lexpr, PGCaseTestExpr) &&!IsA(rexpr, PGCaseTestExpr)))
	{
		PGNullTest   *n = makeNode(PGNullTest);

		n->nulltesttype = PG_IS_NULL;

		if (exprIsNullConstant(lexpr))
			n->arg = (PGExpr *) rexpr;
		else
			n->arg = (PGExpr *) lexpr;

		result = transformExprRecurse(pstate, (PGNode *) n);
	}
	else if (lexpr && IsA(lexpr, PGRowExpr) &&
			 rexpr && IsA(rexpr, PGSubLink) &&
			 ((PGSubLink *) rexpr)->subLinkType == PG_EXPR_SUBLINK)
	{
		/*
		 * Convert "row op subselect" into a ROWCOMPARE sublink. Formerly the
		 * grammar did this, but now that a row construct is allowed anywhere
		 * in expressions, it's easier to do it here.
		 */
		PGSubLink    *s = (PGSubLink *) rexpr;

		s->subLinkType = PG_ROWCOMPARE_SUBLINK;
		s->testexpr = lexpr;
		s->operName = a->name;
		s->location = a->location;
		result = transformExprRecurse(pstate, (PGNode *) s);
	}
	else if (lexpr && IsA(lexpr, PGRowExpr) &&
			 rexpr && IsA(rexpr, PGRowExpr))
	{
		/* ROW() op ROW() is handled specially */
		lexpr = transformExprRecurse(pstate, lexpr);
		rexpr = transformExprRecurse(pstate, rexpr);
		Assert(IsA(lexpr, PGRowExpr));
		Assert(IsA(rexpr, PGRowExpr));

		result = make_row_comparison_op(pstate,
										a->name,
										((PGRowExpr *) lexpr)->args,
										((PGRowExpr *) rexpr)->args,
										a->location);
	}
	else
	{
		/* Ordinary scalar operator */
		lexpr = transformExprRecurse(pstate, lexpr);
		rexpr = transformExprRecurse(pstate, rexpr);

		result = (PGNode *) make_op(pstate,
								  a->name,
								  lexpr,
								  rexpr,
								  a->location);
	}

	return result;
};

PGNode *
ExprParser::transformParamRef(PGParseState *pstate, PGParamRef *pref)
{
	PGNode	   *result;

	/*
	 * The core parser knows nothing about Params.  If a hook is supplied,
	 * call it.  If not, or if the hook returns NULL, throw a generic error.
	 */
	if (pstate->p_paramref_hook != NULL)
		result = (*pstate->p_paramref_hook) (pstate, pref);
	else
		result = NULL;

	if (result == NULL)
		ereport(ERROR,
				(errcode(PG_ERRCODE_SYNTAX_ERROR),
				 errmsg("there is no parameter $%d", pref->number),
				 parser_errposition(pstate, pref->location)));

	return result;
};

PGNode *
ExprParser::transformIndirection(PGParseState *pstate, PGNode *basenode, PGList *indirection)
{
	PGNode	   *result = basenode;
	PGList	   *subscripts = NIL;
	int			location = exprLocation(basenode);
	ListCell   *i;

	/*
	 * We have to split any field-selection operations apart from
	 * subscripting.  Adjacent A_Indices nodes have to be treated as a single
	 * multidimensional subscript operation.
	 */
	foreach(i, indirection)
	{
		PGNode	   *n = reinterpret_cast<PGNode*>(lfirst(i));

		if (IsA(n, PGAIndices))
			subscripts = lappend(subscripts, n);
		else if (IsA(n, PGAStar))
		{
			ereport(ERROR,
					(errcode(PG_ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("row expansion via \"*\" is not supported here"),
					 parser_errposition(pstate, location)));
		}
		else
		{
			PGNode	   *newresult;

			Assert(IsA(n, PGString));

			/* process subscripts before this field selection */
			if (subscripts)
				result = (PGNode *) transformArraySubscripts(pstate,
														   result,
														   exprType(result),
														   InvalidOid,
														   exprTypmod(result),
														   subscripts,
														   NULL);
			subscripts = NIL;

			newresult = ParseFuncOrColumn(pstate,
										  list_make1(n),
										  list_make1(result),
										  NULL,
										  location);
			if (newresult == NULL)
				unknown_attribute(pstate, result, strVal(n), location);
			result = newresult;
		}
	}
	/* process trailing subscripts, if any */
	if (subscripts)
		result = (PGNode *) transformArraySubscripts(pstate,
												   result,
												   exprType(result),
												   InvalidOid,
												   exprTypmod(result),
												   subscripts,
												   NULL);

	return result;
};

PGNode *
ExprParser::transformArrayExpr(PGParseState *pstate, PGAArrayExpr *a,
				   Oid array_type, Oid element_type, int32 typmod)
{
	PGArrayExpr  *newa = makeNode(PGArrayExpr);
	PGList	   *newelems = NIL;
	PGList	   *newcoercedelems = NIL;
	PGListCell   *element;
	Oid			coerce_type;
	bool		coerce_hard;

	/*
	 * Transform the element expressions
	 *
	 * Assume that the array is one-dimensional unless we find an array-type
	 * element expression.
	 */
	newa->multidims = false;
	foreach(element, a->elements)
	{
		PGNode	   *e = (PGNode *) lfirst(element);
		PGNode	   *newe;

		/*
		 * If an element is itself an A_ArrayExpr, recurse directly so that we
		 * can pass down any target type we were given.
		 */
		if (IsA(e, PGAArrayExpr))
		{
			newe = transformArrayExpr(pstate,
									  (PGAArrayExpr *) e,
									  array_type,
									  element_type,
									  typmod);
			/* we certainly have an array here */
			Assert(array_type == InvalidOid || array_type == exprType(newe));
			newa->multidims = true;
		}
		else
		{
			newe = transformExprRecurse(pstate, e);

			/*
			 * Check for sub-array expressions, if we haven't already found
			 * one.
			 */
			if (!newa->multidims && type_is_array(exprType(newe)))
				newa->multidims = true;
		}

		newelems = lappend(newelems, newe);
	}

	/*
	 * Select a target type for the elements.
	 *
	 * If we haven't been given a target array type, we must try to deduce a
	 * common type based on the types of the individual elements present.
	 */
	if (array_type != InvalidOid)
	{
		/* Caller must ensure array_type matches element_type */
		Assert(element_type != InvalidOid);
		coerce_type = (newa->multidims ? array_type : element_type);
		coerce_hard = true;
	}
	else
	{
		/* Can't handle an empty array without a target type */
		if (newelems == NIL)
			ereport(ERROR,
					(errcode(PG_ERRCODE_SYNTAX_ERROR),
					 errmsg("cannot determine type of empty array"),
					 errhint("Explicitly cast to the desired type, "
							 "for example ARRAY[]::integer[]."),
					 parser_errposition(pstate, a->location)));

		/* Select a common type for the elements */
		coerce_type = select_common_type(pstate, newelems, "ARRAY", NULL);

		if (newa->multidims)
		{
			array_type = coerce_type;
			element_type = get_element_type(array_type);
			if (element_type == InvalidOid)
				ereport(ERROR,
						(errcode(PG_ERRCODE_SYNTAX_ERROR),
					   errmsg("could not find element type for data type %s",
							  format_type_be(array_type)),
						 parser_errposition(pstate, a->location)));
		}
		else
		{
			element_type = coerce_type;
			array_type = get_array_type(element_type);
			if (array_type == InvalidOid)
				ereport(ERROR,
						(errcode(PG_ERRCODE_SYNTAX_ERROR),
						 errmsg("could not find array type for data type %s",
								format_type_be(element_type)),
						 parser_errposition(pstate, a->location)));
		}
		coerce_hard = false;
	}

	/*
	 * Coerce elements to target type
	 *
	 * If the array has been explicitly cast, then the elements are in turn
	 * explicitly coerced.
	 *
	 * If the array's type was merely derived from the common type of its
	 * elements, then the elements are implicitly coerced to the common type.
	 * This is consistent with other uses of select_common_type().
	 */
	foreach(element, newelems)
	{
		PGNode	   *e = (PGNode *) lfirst(element);
		PGNode	   *newe;

		if (coerce_hard)
		{
			newe = coerce_parser.coerce_to_target_type(pstate, e,
										 exprType(e),
										 coerce_type,
										 typmod,
										 PG_COERCION_EXPLICIT,
										 PG_COERCE_EXPLICIT_CAST,
										 -1);
			if (newe == NULL)
				ereport(ERROR,
						(errcode(PG_ERRCODE_SYNTAX_ERROR),
						 errmsg("cannot cast type %s to %s",
								format_type_be(exprType(e)),
								format_type_be(coerce_type)),
						 parser_errposition(pstate, exprLocation(e))));
		}
		else
			newe = coerce_parser.coerce_to_common_type(pstate, e,
										 coerce_type,
										 "ARRAY");
		newcoercedelems = lappend(newcoercedelems, newe);
	}

	newa->array_typeid = array_type;
	/* array_collid will be set by parse_collate.c */
	newa->element_typeid = element_type;
	newa->elements = newcoercedelems;
	newa->location = a->location;

	return (PGNode *) newa;
};

duckdb_libpgquery::PGNode *
ExprParser::transformTypeCast(PGParseState *pstate, PGTypeCast *tc)
{
	PGNode	   *result;
	PGNode	   *expr = transformExprRecurse(pstate, tc->arg);
	Oid			inputType = exprType(expr);
	Oid			targetType;
	int32		targetTypmod;
	int			location;

	typenameTypeIdAndMod(pstate, tc->typeName, &targetType, &targetTypmod);

	if (inputType == InvalidOid)
		return expr;			/* do nothing if NULL input */

	/*
	 * Location of the coercion is preferentially the location of the :: or
	 * CAST symbol, but if there is none then use the location of the type
	 * name (this can happen in TypeName 'string' syntax, for instance).
	 */
	location = tc->location;
	if (location < 0)
		location = tc->typeName->location;

	result = coerce_parser.coerce_to_target_type(pstate, expr, inputType,
								   targetType, targetTypmod,
								   PG_COERCION_EXPLICIT,
								   PG_COERCE_EXPLICIT_CAST,
								   location);
	if (result == NULL)
		ereport(ERROR,
				(errcode(PG_ERRCODE_SYNTAX_ERROR),
				 errmsg("cannot cast type %s to %s",
						format_type_be(inputType),
						format_type_be(targetType)),
				 parser_coercion_errposition(pstate, location, expr)));

	return result;
};

duckdb_libpgquery::PGNode *
ExprParser::transformCollateClause(PGParseState *pstate, PGCollateClause *c)
{
	PGCollateExpr *newc;
	Oid			argtype;

	newc = makeNode(PGCollateExpr);
	newc->arg = (PGExpr *) transformExprRecurse(pstate, c->arg);

	argtype = exprType((PGNode *) newc->arg);

	/*
	 * The unknown type is not collatable, but coerce_type() takes care of it
	 * separately, so we'll let it go here.
	 */
	if (!type_is_collatable(argtype) && argtype != UNKNOWNOID)
		ereport(ERROR,
				(errcode(PG_ERRCODE_SYNTAX_ERROR),
				 errmsg("collations are not supported by type %s",
						format_type_be(argtype)),
				 parser_errposition(pstate, c->location)));

	newc->collOid = LookupCollation(pstate, c->collname, c->location);
	newc->location = c->location;

	return (PGNode *) newc;
};

PGNode *
ExprParser::transformAExprOpAny(PGParseState *pstate, PGAExpr *a)
{
	PGNode	   *lexpr = transformExprRecurse(pstate, a->lexpr);
	PGNode	   *rexpr = transformExprRecurse(pstate, a->rexpr);

	return (PGNode *) make_scalar_array_op(pstate,
										 a->name,
										 true,
										 lexpr,
										 rexpr,
										 a->location);
};

PGNode *
ExprParser::transformAExprOpAll(PGParseState *pstate, PGAExpr *a)
{
	PGNode	   *lexpr = transformExprRecurse(pstate, a->lexpr);
	PGNode	   *rexpr = transformExprRecurse(pstate, a->rexpr);

	return (PGNode *) make_scalar_array_op(pstate,
										 a->name,
										 false,
										 lexpr,
										 rexpr,
										 a->location);
};

PGNode *
ExprParser::transformAExprDistinct(PGParseState *pstate, PGAExpr *a)
{
	PGNode	   *lexpr = transformExprRecurse(pstate, a->lexpr);
	PGNode	   *rexpr = transformExprRecurse(pstate, a->rexpr);

	if (lexpr && IsA(lexpr, PGRowExpr) &&
		rexpr && IsA(rexpr, PGRowExpr))
	{
		/* ROW() op ROW() is handled specially */
		return make_row_distinct_op(pstate, a->name,
									(PGRowExpr *) lexpr,
									(PGRowExpr *) rexpr,
									a->location);
	}
	else
	{
		/* Ordinary scalar operator */
		return (PGNode *) make_distinct_op(pstate,
										 a->name,
										 lexpr,
										 rexpr,
										 a->location);
	}
};

PGNode *
ExprParser::transformAExprNullIf(PGParseState *pstate, PGAExpr *a)
{
	PGNode	   *lexpr = transformExprRecurse(pstate, a->lexpr);
	PGNode	   *rexpr = transformExprRecurse(pstate, a->rexpr);
	PGOpExpr	   *result;

	result = (PGOpExpr *) make_op(pstate,
								a->name,
								lexpr,
								rexpr,
								a->location);

	/*
	 * The comparison operator itself should yield boolean ...
	 */
	if (result->opresulttype != BOOLOID)
		ereport(ERROR,
				(errcode(PG_ERRCODE_SYNTAX_ERROR),
				 errmsg("NULLIF requires = operator to yield boolean"),
				 parser_errposition(pstate, a->location)));

	/*
	 * ... but the NullIfExpr will yield the first operand's type.
	 */
	result->opresulttype = exprType((PGNode *) linitial(result->args));

	/*
	 * We rely on NullIfExpr and OpExpr being the same struct
	 */
	NodeSetTag(result, T_PGNullIfExpr);

	return (PGNode *) result;
};

PGNode *
ExprParser::transformAExprOf(PGParseState *pstate, PGAExpr *a)
{
	/*
	 * Checking an expression for match to a list of type names. Will result
	 * in a boolean constant node.
	 */
	PGNode	   *lexpr = transformExprRecurse(pstate, a->lexpr);
	PGConst	   *result;
	ListCell   *telem;
	Oid			ltype,
				rtype;
	bool		matched = false;

	ltype = exprType(lexpr);
	foreach(telem, (PGList *) a->rexpr)
	{
		rtype = typenameTypeId(pstate, lfirst(telem));
		matched = (rtype == ltype);
		if (matched)
			break;
	}

	/*
	 * We have two forms: equals or not equals. Flip the sense of the result
	 * for not equals.
	 */
	if (strcmp(strVal(linitial(a->name)), "<>") == 0)
		matched = (!matched);

	result = (PGConst *) makeBoolConst(matched, false);

	/* Make the result have the original input's parse location */
	result->location = exprLocation((PGNode *) a);

	return (PGNode *) result;
};

PGNode *
ExprParser::transformAExprIn(PGParseState *pstate, PGAExpr *a)
{
	PGNode	   *result = NULL;
	PGNode	   *lexpr;
	PGList	   *rexprs;
	PGList	   *rvars;
	PGList	   *rnonvars;
	bool		useOr;
	ListCell   *l;

	/*
	 * If the operator is <>, combine with AND not OR.
	 */
	if (strcmp(strVal(linitial(a->name)), "<>") == 0)
		useOr = false;
	else
		useOr = true;

	/*
	 * We try to generate a ScalarArrayOpExpr from IN/NOT IN, but this is only
	 * possible if there is a suitable array type available.  If not, we fall
	 * back to a boolean condition tree with multiple copies of the lefthand
	 * expression.  Also, any IN-list items that contain Vars are handled as
	 * separate boolean conditions, because that gives the planner more scope
	 * for optimization on such clauses.
	 *
	 * First step: transform all the inputs, and detect whether any contain
	 * Vars.
	 */
	lexpr = transformExprRecurse(pstate, a->lexpr);
	rexprs = rvars = rnonvars = NIL;
	foreach(l, (PGList *) a->rexpr)
	{
		PGNode	   *rexpr = transformExprRecurse(pstate, (PGNode *)lfirst(l));

		rexprs = lappend(rexprs, rexpr);
		if (contain_vars_of_level(rexpr, 0))
			rvars = lappend(rvars, rexpr);
		else
			rnonvars = lappend(rnonvars, rexpr);
	}

	/*
	 * ScalarArrayOpExpr is only going to be useful if there's more than one
	 * non-Var righthand item.
	 */
	if (list_length(rnonvars) > 1)
	{
		PGList	   *allexprs;
		Oid			scalar_type;
		Oid			array_type;

		/*
		 * Try to select a common type for the array elements.  Note that
		 * since the LHS' type is first in the list, it will be preferred when
		 * there is doubt (eg, when all the RHS items are unknown literals).
		 *
		 * Note: use list_concat here not lcons, to avoid damaging rnonvars.
		 */
		allexprs = list_concat(list_make1(lexpr), rnonvars);
		scalar_type = select_common_type(pstate, allexprs, NULL, NULL);

		/*
		 * Do we have an array type to use?  Aside from the case where there
		 * isn't one, we don't risk using ScalarArrayOpExpr when the common
		 * type is RECORD, because the RowExpr comparison logic below can cope
		 * with some cases of non-identical row types.
		 */
		if (scalar_type != InvalidOid && scalar_type != RECORDOID)
			array_type = get_array_type(scalar_type);
		else
			array_type = InvalidOid;
		if (array_type != InvalidOid)
		{
			/*
			 * OK: coerce all the right-hand non-Var inputs to the common type
			 * and build an ArrayExpr for them.
			 */
			PGList	   *aexprs;
			PGArrayExpr  *newa;

			aexprs = NIL;
			foreach(l, rnonvars)
			{
				PGNode	   *rexpr = (PGNode *) lfirst(l);

				rexpr = coerce_parser.coerce_to_common_type(pstate, rexpr,
											  scalar_type,
											  "IN");
				aexprs = lappend(aexprs, rexpr);
			}
			newa = makeNode(PGArrayExpr);
			newa->array_typeid = array_type;
			/* array_collid will be set by parse_collate.c */
			newa->element_typeid = scalar_type;
			newa->elements = aexprs;
			newa->multidims = false;
			newa->location = -1;

			result = (PGNode *) make_scalar_array_op(pstate,
												   a->name,
												   useOr,
												   lexpr,
												   (PGNode *) newa,
												   a->location);

			/* Consider only the Vars (if any) in the loop below */
			rexprs = rvars;
		}
	}

	/*
	 * Must do it the hard way, ie, with a boolean expression tree.
	 */
	foreach(l, rexprs)
	{
		PGNode	   *rexpr = (PGNode *) lfirst(l);
		PGNode	   *cmp;

		if (IsA(lexpr, PGRowExpr) &&
			IsA(rexpr, PGRowExpr))
		{
			/* ROW() op ROW() is handled specially */
			cmp = make_row_comparison_op(pstate,
										 a->name,
							  (PGList *) copyObject(((PGRowExpr *) lexpr)->args),
										 ((PGRowExpr *) rexpr)->args,
										 a->location);
		}
		else
		{
			/* Ordinary scalar operator */
			cmp = (PGNode *) make_op(pstate,
								   a->name,
								   copyObject(lexpr),
								   rexpr,
								   a->location);
		}

		cmp = coerce_parser.coerce_to_boolean(pstate, cmp, "IN");
		if (result == NULL)
			result = cmp;
		else
			result = (PGNode *) makeBoolExpr(useOr ? PG_OR_EXPR : PG_AND_EXPR,
										   list_make2(result, cmp),
										   a->location);
	}

	return result;
};

duckdb_libpgquery::PGNode *
ExprParser::transformBoolExpr(PGParseState *pstate, PGBoolExpr *a)
{
	PGList	   *args = NIL;
	const char *opname;
	PGListCell   *lc;

	switch (a->boolop)
	{
		case PG_AND_EXPR:
			opname = "AND";
			break;
		case PG_OR_EXPR:
			opname = "OR";
			break;
		case PG_NOT_EXPR:
			opname = "NOT";
			break;
		default:
			elog(ERROR, "unrecognized boolop: %d", (int) a->boolop);
			opname = NULL;		/* keep compiler quiet */
			break;
	}

	foreach(lc, a->args)
	{
		PGNode	   *arg = (PGNode *) lfirst(lc);

		arg = transformExprRecurse(pstate, arg);
		arg = coerce_parser.coerce_to_boolean(pstate, arg, opname);
		args = lappend(args, arg);
	}

	return (PGNode *) makeBoolExpr(a->boolop, args, a->location);
};

duckdb_libpgquery::PGNode *
ExprParser::transformFuncCall(PGParseState *pstate, PGFuncCall *fn)
{
	PGList	   *targs;
	ListCell   *args;

	/* Transform the list of arguments ... */
	targs = NIL;
	foreach(args, fn->args)
	{
		targs = lappend(targs, transformExprRecurse(pstate,
													(PGNode *) lfirst(args)));
	}

	/*
	 * When WITHIN GROUP is used, we treat its ORDER BY expressions as
	 * additional arguments to the function, for purposes of function lookup
	 * and argument type coercion.  So, transform each such expression and add
	 * them to the targs list.  We don't explicitly mark where each argument
	 * came from, but ParseFuncOrColumn can tell what's what by reference to
	 * list_length(fn->agg_order).
	 */
	if (fn->agg_within_group)
	{
		Assert(fn->agg_order != NIL);
		foreach(args, fn->agg_order)
		{
			PGSortBy	   *arg = (PGSortBy *) lfirst(args);

			targs = lappend(targs, transformExpr(pstate, arg->node,
												 PGParseExprKind::EXPR_KIND_ORDER_BY));
		}
	}

	/* ... and hand off to ParseFuncOrColumn */
	return ParseFuncOrColumn(pstate,
							 fn->funcname,
							 targs,
							 fn,
							 fn->location);
};

duckdb_libpgquery::PGNode *
ExprParser::transformSubLink(PGParseState *pstate, PGSubLink *sublink)
{
	PGNode	   *result = (PGNode *) sublink;
	PGQuery	   *qtree;
	const char *err;

	/* If we already transformed this node, do nothing */
	if (IsA(sublink->subselect, PGQuery))
		return result;

	/*
	 * Check to see if the sublink is in an invalid place within the query. We
	 * allow sublinks everywhere in SELECT/INSERT/UPDATE/DELETE, but generally
	 * not in utility statements.
	 */
	err = NULL;
	switch (pstate->p_expr_kind)
	{
		case PGParseExprKind::EXPR_KIND_NONE:
			Assert(false);		/* can't happen */
			break;
		case PGParseExprKind::EXPR_KIND_OTHER:
			/* Accept sublink here; caller must throw error if wanted */
			break;
		case PGParseExprKind::EXPR_KIND_JOIN_ON:
		case PGParseExprKind::EXPR_KIND_JOIN_USING:
		case PGParseExprKind::EXPR_KIND_FROM_SUBSELECT:
		case PGParseExprKind::EXPR_KIND_FROM_FUNCTION:
		case PGParseExprKind::EXPR_KIND_WHERE:
		case PGParseExprKind::EXPR_KIND_HAVING:
		case PGParseExprKind::EXPR_KIND_FILTER:
		case PGParseExprKind::EXPR_KIND_WINDOW_PARTITION:
		case PGParseExprKind::EXPR_KIND_WINDOW_ORDER:
		case PGParseExprKind::EXPR_KIND_WINDOW_FRAME_RANGE:
		case PGParseExprKind::EXPR_KIND_WINDOW_FRAME_ROWS:
		case PGParseExprKind::EXPR_KIND_SELECT_TARGET:
		case PGParseExprKind::EXPR_KIND_INSERT_TARGET:
		case PGParseExprKind::EXPR_KIND_UPDATE_SOURCE:
		case PGParseExprKind::EXPR_KIND_UPDATE_TARGET:
		case PGParseExprKind::EXPR_KIND_GROUP_BY:
		case PGParseExprKind::EXPR_KIND_ORDER_BY:
		case PGParseExprKind::EXPR_KIND_DISTINCT_ON:
		case PGParseExprKind::EXPR_KIND_LIMIT:
		case PGParseExprKind::EXPR_KIND_OFFSET:
		case PGParseExprKind::EXPR_KIND_RETURNING:
		case PGParseExprKind::EXPR_KIND_VALUES:
			/* okay */
			break;
		case PGParseExprKind::EXPR_KIND_CHECK_CONSTRAINT:
		case PGParseExprKind::EXPR_KIND_DOMAIN_CHECK:
			err = _("cannot use subquery in check constraint");
			break;
		case PGParseExprKind::EXPR_KIND_COLUMN_DEFAULT:
		case PGParseExprKind::EXPR_KIND_FUNCTION_DEFAULT:
			err = _("cannot use subquery in DEFAULT expression");
			break;
		case PGParseExprKind::EXPR_KIND_INDEX_EXPRESSION:
			err = _("cannot use subquery in index expression");
			break;
		case PGParseExprKind::EXPR_KIND_INDEX_PREDICATE:
			err = _("cannot use subquery in index predicate");
			break;
		case PGParseExprKind::EXPR_KIND_ALTER_COL_TRANSFORM:
			err = _("cannot use subquery in transform expression");
			break;
		case PGParseExprKind::EXPR_KIND_EXECUTE_PARAMETER:
			err = _("cannot use subquery in EXECUTE parameter");
			break;
		case PGParseExprKind::EXPR_KIND_TRIGGER_WHEN:
			err = _("cannot use subquery in trigger WHEN condition");
			break;
		case PGParseExprKind::EXPR_KIND_PARTITION_EXPRESSION:
			err = _("cannot use subquery in partition key expression");
			break;

		case PGParseExprKind::EXPR_KIND_SCATTER_BY:
			/* okay */
			break;

			/*
			 * There is intentionally no default: case here, so that the
			 * compiler will warn if we add a new ParseExprKind without
			 * extending this switch.  If we do see an unrecognized value at
			 * runtime, the behavior will be the same as for EXPR_KIND_OTHER,
			 * which is sane anyway.
			 */
	}
	if (err)
		ereport(ERROR,
				(errcode(PG_ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg_internal("%s", err),
				 parser_errposition(pstate, sublink->location)));

	pstate->p_hasSubLinks = true;

	/*
	 * OK, let's transform the sub-SELECT.
	 */
	qtree = select_parser.parse_sub_analyze(sublink->subselect, pstate, NULL, NULL);

	/*
	 * Check that we got something reasonable.  Many of these conditions are
	 * impossible given restrictions of the grammar, but check 'em anyway.
	 */
	if (!IsA(qtree, PGQuery) ||
		qtree->commandType != PG_CMD_SELECT ||
		qtree->utilityStmt != NULL)
		elog(ERROR, "unexpected non-SELECT command in SubLink");

	sublink->subselect = (PGNode *) qtree;

	if (sublink->subLinkType == PG_EXISTS_SUBLINK)
	{
		/*
		 * EXISTS needs no test expression or combining operator. These fields
		 * should be null already, but make sure.
		 */
		sublink->testexpr = NULL;
		sublink->operName = NIL;
	}
	else if (sublink->subLinkType == PG_EXPR_SUBLINK ||
			 sublink->subLinkType == PG_ARRAY_SUBLINK)
	{
		ListCell   *tlist_item = list_head(qtree->targetList);

		/*
		 * Make sure the subselect delivers a single column (ignoring resjunk
		 * targets).
		 */
		if (tlist_item == NULL ||
			((PGTargetEntry *) lfirst(tlist_item))->resjunk)
			ereport(ERROR,
					(errcode(PG_ERRCODE_SYNTAX_ERROR),
					 errmsg("subquery must return a column"),
					 parser_errposition(pstate, sublink->location)));
		while ((tlist_item = lnext(tlist_item)) != NULL)
		{
			if (!((PGTargetEntry *) lfirst(tlist_item))->resjunk)
				ereport(ERROR,
						(errcode(PG_ERRCODE_SYNTAX_ERROR),
						 errmsg("subquery must return only one column"),
						 parser_errposition(pstate, sublink->location)));
		}

		/*
		 * EXPR and ARRAY need no test expression or combining operator. These
		 * fields should be null already, but make sure.
		 */
		sublink->testexpr = NULL;
		sublink->operName = NIL;
	}
	else
	{
		/* ALL, ANY, or ROWCOMPARE: generate row-comparing expression */
		PGNode	   *lefthand;
		PGList	   *left_list;
		PGList	   *right_list;
		ListCell   *l;

		/*
		 * Transform lefthand expression, and convert to a list
		 */
		lefthand = transformExprRecurse(pstate, sublink->testexpr);
		if (lefthand && IsA(lefthand, PGRowExpr))
			left_list = ((PGRowExpr *) lefthand)->args;
		else
			left_list = list_make1(lefthand);

		/*
		 * Build a list of PARAM_SUBLINK nodes representing the output columns
		 * of the subquery.
		 */
		right_list = NIL;
		foreach(l, qtree->targetList)
		{
			PGTargetEntry *tent = (PGTargetEntry *) lfirst(l);
			PGParam	   *param;

			if (tent->resjunk)
				continue;

			param = makeNode(PGParam);
			param->paramkind = PG_PARAM_SUBLINK;
			param->paramid = tent->resno;
			param->paramtype = exprType((PGNode *) tent->expr);
			param->paramtypmod = exprTypmod((PGNode *) tent->expr);
			param->paramcollid = exprCollation((PGNode *) tent->expr);
			param->location = -1;

			right_list = lappend(right_list, param);
		}

		/*
		 * We could rely on make_row_comparison_op to complain if the list
		 * lengths differ, but we prefer to generate a more specific error
		 * message.
		 */
		if (list_length(left_list) < list_length(right_list))
			ereport(ERROR,
					(errcode(PG_ERRCODE_SYNTAX_ERROR),
					 errmsg("subquery has too many columns"),
					 parser_errposition(pstate, sublink->location)));
		if (list_length(left_list) > list_length(right_list))
			ereport(ERROR,
					(errcode(PG_ERRCODE_SYNTAX_ERROR),
					 errmsg("subquery has too few columns"),
					 parser_errposition(pstate, sublink->location)));

		/*
		 * Identify the combining operator(s) and generate a suitable
		 * row-comparison expression.
		 */
		sublink->testexpr = make_row_comparison_op(pstate,
												   sublink->operName,
												   left_list,
												   right_list,
												   sublink->location);
	}

	return result;
};

duckdb_libpgquery::PGNode *
ExprParser::transformCaseExpr(PGParseState *pstate, PGCaseExpr *c)
{
	PGCaseExpr   *newc;
	PGNode	   *arg;
	PGCaseTestExpr *placeholder;
	PGList	   *newargs;
	PGList	   *resultexprs;
	PGListCell   *l;
	PGNode	   *defresult;
	Oid			ptype;

	/* If we already transformed this node, do nothing */
	if (c->casetype != InvalidOid)
		return (PGNode *) c;

	newc = makeNode(PGCaseExpr);

	/* transform the test expression, if any */
	arg = transformExprRecurse(pstate, (PGNode *) c->arg);

	/* generate placeholder for test expression */
	if (arg)
	{
		/*
		 * If test expression is an untyped literal, force it to text. We have
		 * to do something now because we won't be able to do this coercion on
		 * the placeholder.  This is not as flexible as what was done in 7.4
		 * and before, but it's good enough to handle the sort of silly coding
		 * commonly seen.
		 */
		if (exprType(arg) == UNKNOWNOID)
			arg = coerce_parser.coerce_to_common_type(pstate, arg, TEXTOID, "CASE");

		/*
		 * Run collation assignment on the test expression so that we know
		 * what collation to mark the placeholder with.  In principle we could
		 * leave it to parse_collate.c to do that later, but propagating the
		 * result to the CaseTestExpr would be unnecessarily complicated.
		 */
		assign_expr_collations(pstate, arg);

		placeholder = makeNode(PGCaseTestExpr);
		placeholder->typeId = exprType(arg);
		placeholder->typeMod = exprTypmod(arg);
		placeholder->collation = exprCollation(arg);
	}
	else
		placeholder = NULL;

	newc->arg = (PGExpr *) arg;

	/* transform the list of arguments */
	newargs = NIL;
	resultexprs = NIL;
	foreach(l, c->args)
	{
		PGCaseWhen   *w = (PGCaseWhen *) lfirst(l);
		PGCaseWhen   *neww = makeNode(PGCaseWhen);
		PGNode	   *warg;

		Assert(IsA(w, PGCaseWhen));

		warg = (PGNode *) w->expr;
		if (placeholder)
		{
			/* 
			 * CASE placeholder WHEN IS NOT DISTINCT FROM warg:
			 * 		set: warg->rhs->lhs = placeholder
			 */
			if (isWhenIsNotDistinctFromExpr(warg))
			{
				/*
				 * Make a copy before we change warg.
				 * In transformation we don't want to change source (CaseExpr* Node).
				 * Always create new node and do the transformation
				 */
				warg = (PGNode *)copyObject(warg);
				PGAExpr *top  = (PGAExpr *) warg;
				PGAExpr *expr = (PGAExpr *) top->rexpr;
				expr->lexpr = (PGNode *) placeholder;
			}
			else
				warg = (PGNode *) makeSimpleAExpr(PG_AEXPR_OP, "=",
													(PGNode *) placeholder,
													 warg,
													 w->location);
		}
		else
		{
			if (isWhenIsNotDistinctFromExpr(warg))
				ereport(ERROR,
						(errcode(PG_ERRCODE_SYNTAX_ERROR),
						 errmsg("syntax error at or near \"NOT\""),
						 errhint("Missing <operand> for \"CASE <operand> WHEN IS NOT DISTINCT FROM ...\""),
						 parser_errposition(pstate, exprLocation((PGNode *) warg))));
		}
		neww->expr = (PGExpr *) transformExprRecurse(pstate, warg);

		neww->expr = (PGExpr *) coerce_parser.coerce_to_boolean(pstate,
												(PGNode *) neww->expr,
												"CASE/WHEN");

		warg = (PGNode *) w->result;
		neww->result = (PGExpr *) transformExprRecurse(pstate, warg);
		neww->location = w->location;

		newargs = lappend(newargs, neww);
		resultexprs = lappend(resultexprs, neww->result);
	}

	newc->args = newargs;

	/* transform the default clause */
	defresult = (PGNode *) c->defresult;
	if (defresult == NULL)
	{
		PGAConst    *n = makeNode(PGAConst);

		n->val.type = T_PGNull;
		n->location = -1;
		defresult = (PGNode *) n;
	}
	newc->defresult = (PGExpr *) transformExprRecurse(pstate, defresult);

	/*
	 * Note: default result is considered the most significant type in
	 * determining preferred type. This is how the code worked before, but it
	 * seems a little bogus to me --- tgl
	 */
	resultexprs = lcons(newc->defresult, resultexprs);

	ptype = coerce_parser.select_common_type(pstate, resultexprs, "CASE", NULL);
	Assert(ptype != InvalidOid);
	newc->casetype = ptype;
	/* casecollid will be set by parse_collate.c */

	/* Convert default result clause, if necessary */
	newc->defresult = (PGExpr *)
		coerce_parser.coerce_to_common_type(pstate,
							  (PGNode *) newc->defresult,
							  ptype,
							  "CASE/ELSE");

	/* Convert when-clause results, if necessary */
	foreach(l, newc->args)
	{
		PGCaseWhen   *w = (PGCaseWhen *) lfirst(l);

		w->result = (PGExpr *)
			coerce_parser.coerce_to_common_type(pstate,
								  (PGNode *) w->result,
								  ptype,
								  "CASE/WHEN");
	}

	newc->location = c->location;

	return (PGNode *) newc;
};

PGNode *
ExprParser::transformRowExpr(PGParseState *pstate, PGRowExpr *r)
{
	PGRowExpr    *newr;
	char		fname[16];
	int			fnum;
	ListCell   *lc;

	/* If we already transformed this node, do nothing */
	if (r->row_typeid != InvalidOid)
		return (PGNode *) r;

	newr = makeNode(PGRowExpr);

	/* Transform the field expressions */
	newr->args = target_parser.transformExpressionList(pstate, r->args, pstate->p_expr_kind);

	/* Barring later casting, we consider the type RECORD */
	newr->row_typeid = RECORDOID;
	newr->row_format = PG_COERCE_IMPLICIT_CAST;

	/* ROW() has anonymous columns, so invent some field names */
	newr->colnames = NIL;
	fnum = 1;
	foreach(lc, newr->args)
	{
		snprintf(fname, sizeof(fname), "f%d", fnum++);
		newr->colnames = lappend(newr->colnames, makeString(pstrdup(fname)));
	}

	newr->location = r->location;

	return (PGNode *) newr;
};

PGNode *
ExprParser::transformWholeRowRef(PGParseState *pstate, PGRangeTblEntry *rte, int location)
{
	PGVar		   *result;
	int			vnum;
	int			sublevels_up;

	/* Find the RTE's rangetable location */
	vnum = relation_parser.RTERangeTablePosn(pstate, rte, &sublevels_up);

	/*
	 * Build the appropriate referencing node.  Note that if the RTE is a
	 * function returning scalar, we create just a plain reference to the
	 * function value, not a composite containing a single column.  This is
	 * pretty inconsistent at first sight, but it's what we've done
	 * historically.  One argument for it is that "rel" and "rel.*" mean the
	 * same thing for composite relations, so why not for scalar functions...
	 */
	result = makeWholeRowVar(rte, vnum, sublevels_up, true);

	/* location is not filled in by makeWholeRowVar */
	result->location = location;

	/* mark relation as requiring whole-row SELECT access */
	markVarForSelectPriv(pstate, result, rte);

	return (PGNode *) result;
};

PGArrayRef *
ExprParser::transformArraySubscripts(PGParseState *pstate,
						 PGNode *arrayBase,
						 Oid arrayType,
						 Oid elementType,
						 int32 arrayTypMod,
						 PGList *indirection,
						 PGNode *assignFrom)
{
	bool		isSlice = false;
	PGList	   *upperIndexpr = NIL;
	PGList	   *lowerIndexpr = NIL;
	PGListCell   *idx;
	PGArrayRef   *aref;

	/*
	 * Caller may or may not have bothered to determine elementType.  Note
	 * that if the caller did do so, arrayType/arrayTypMod must be as modified
	 * by transformArrayType, ie, smash domain to base type.
	 */
	if (elementType == InvalidOid)
		elementType = transformArrayType(&arrayType, &arrayTypMod);

	/*
	 * A list containing only single subscripts refers to a single array
	 * element.  If any of the items are double subscripts (lower:upper), then
	 * the subscript expression means an array slice operation. In this case,
	 * we supply a default lower bound of 1 for any items that contain only a
	 * single subscript.  We have to prescan the indirection list to see if
	 * there are any double subscripts.
	 */
	foreach(idx, indirection)
	{
		PGAIndices  *ai = (PGAIndices *) lfirst(idx);

		if (ai->lidx != NULL)
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
		PGAIndices  *ai = (PGAIndices *) lfirst(idx);
		PGNode	   *subexpr;

		Assert(IsA(ai, PGAIndices));
		if (isSlice)
		{
			if (ai->lidx)
			{
				subexpr = transformExpr(pstate, ai->lidx, pstate->p_expr_kind);
				/* If it's not int4 already, try to coerce */
				subexpr = coerce_parser.coerce_to_target_type(pstate,
												subexpr, exprType(subexpr),
												INT4OID, -1,
												PG_COERCION_ASSIGNMENT,
												PG_COERCE_IMPLICIT_CAST,
												-1);
				if (subexpr == NULL)
					ereport(ERROR,
							(errcode(PG_ERRCODE_SYNTAX_ERROR),
							 errmsg("array subscript must have type integer"),
						parser_errposition(pstate, exprLocation(ai->lidx))));
			}
			else
			{
				/* Make a constant 1 */
				subexpr = (PGNode *) makeConst(INT4OID,
											 -1,
											 InvalidOid,
											 sizeof(int32),
											 Int32GetDatum(1),
											 false,
											 true);		/* pass by value */
			}
			lowerIndexpr = lappend(lowerIndexpr, subexpr);
		}
		subexpr = transformExpr(pstate, ai->uidx, pstate->p_expr_kind);
		/* If it's not int4 already, try to coerce */
		subexpr = coerce_parser.coerce_to_target_type(pstate,
										subexpr, exprType(subexpr),
										INT4OID, -1,
										PG_COERCION_ASSIGNMENT,
										PG_COERCE_IMPLICIT_CAST,
										-1);
		if (subexpr == NULL)
			ereport(ERROR,
					(errcode(PG_ERRCODE_SYNTAX_ERROR),
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
		Oid			typesource = exprType(assignFrom);
		Oid			typeneeded = isSlice ? arrayType : elementType;
		PGNode	   *newFrom;

		newFrom = coerce_parser.coerce_to_target_type(pstate,
										assignFrom, typesource,
										typeneeded, arrayTypMod,
										PG_COERCION_ASSIGNMENT,
										PG_COERCE_IMPLICIT_CAST,
										-1);
		if (newFrom == NULL)
			ereport(ERROR,
					(errcode(PG_ERRCODE_SYNTAX_ERROR),
					 errmsg("array assignment requires type %s"
							" but expression is of type %s",
							format_type_be(typeneeded),
							format_type_be(typesource)),
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
	aref->refexpr = (PGExpr *) arrayBase;
	aref->refassgnexpr = (PGExpr *) assignFrom;

	return aref;
};

Oid ExprParser::transformArrayType(Oid *arrayType, int32 *arrayTypmod)
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

PGConst *
ExprParser::make_const(PGParseState *pstate, PGValue *value, int location)
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

PGNode *
ExprParser::transformCoalesceExpr(PGParseState *pstate, PGCoalesceExpr *c)
{
	PGCoalesceExpr *newc = makeNode(PGCoalesceExpr);
	PGList	   *newargs = NIL;
	PGList	   *newcoercedargs = NIL;
	PGListCell   *args;

	foreach(args, c->args)
	{
		PGNode	   *e = (PGNode *) lfirst(args);
		PGNode	   *newe;

		newe = transformExprRecurse(pstate, e);
		newargs = lappend(newargs, newe);
	}

	newc->coalescetype = coerce_parser.select_common_type(pstate, newargs, "COALESCE", NULL);
	/* coalescecollid will be set by parse_collate.c */

	/* Convert arguments if necessary */
	foreach(args, newargs)
	{
		PGNode	   *e = (PGNode *) lfirst(args);
		PGNode	   *newe;

		newe = coerce_parser.coerce_to_common_type(pstate, e,
									 newc->coalescetype,
									 "COALESCE");
		newcoercedargs = lappend(newcoercedargs, newe);
	}

	newc->args = newcoercedargs;
	newc->location = c->location;
	return (PGNode *) newc;
};

PGNode *
ExprParser::transformMinMaxExpr(PGParseState *pstate, PGMinMaxExpr *m)
{
	PGMinMaxExpr *newm = makeNode(PGMinMaxExpr);
	PGList	   *newargs = NIL;
	PGList	   *newcoercedargs = NIL;
	const char *funcname = (m->op == PG_IS_GREATEST) ? "GREATEST" : "LEAST";
	ListCell   *args;

	newm->op = m->op;
	foreach(args, m->args)
	{
		PGNode	   *e = (PGNode *) lfirst(args);
		PGNode	   *newe;

		newe = transformExprRecurse(pstate, e);
		newargs = lappend(newargs, newe);
	}

	newm->minmaxtype = coerce_parser.select_common_type(pstate, newargs, funcname, NULL);
	/* minmaxcollid and inputcollid will be set by parse_collate.c */

	/* Convert arguments if necessary */
	foreach(args, newargs)
	{
		PGNode	   *e = (PGNode *) lfirst(args);
		PGNode	   *newe;

		newe = coerce_parser.coerce_to_common_type(pstate, e,
									 newm->minmaxtype,
									 funcname);
		newcoercedargs = lappend(newcoercedargs, newe);
	}

	newm->args = newcoercedargs;
	newm->location = m->location;
	return (PGNode *) newm;
};

PGNode *
ExprParser::transformBooleanTest(PGParseState *pstate, PGBooleanTest *b)
{
	const char *clausename;

	switch (b->booltesttype)
	{
		case PG_IS_TRUE:
			clausename = "IS TRUE";
			break;
		case IS_NOT_TRUE:
			clausename = "IS NOT TRUE";
			break;
		case IS_FALSE:
			clausename = "IS FALSE";
			break;
		case IS_NOT_FALSE:
			clausename = "IS NOT FALSE";
			break;
		case IS_UNKNOWN:
			clausename = "IS UNKNOWN";
			break;
		case IS_NOT_UNKNOWN:
			clausename = "IS NOT UNKNOWN";
			break;
		default:
			elog(ERROR, "unrecognized booltesttype: %d",
				 (int) b->booltesttype);
			clausename = NULL;	/* keep compiler quiet */
	}

	b->arg = (PGExpr *) transformExprRecurse(pstate, (PGNode *) b->arg);

	b->arg = (PGExpr *) coerce_parser.coerce_to_boolean(pstate,
										(PGNode *) b->arg,
										clausename);

	return (PGNode *) b;
};

PGNode *
ExprParser::transformCurrentOfExpr(PGParseState *pstate, PGCurrentOfExpr *cexpr)
{
	int			sublevels_up;

	/*
	 * The target RTE must be simply updatable. If not, we error out
	 * early here to avoid having to deal with error cases later:
	 * rewriting/planning against views, for example.
	 */
	Assert(pstate->p_target_rangetblentry != NULL);
	(void) isSimplyUpdatableRelation(pstate->p_target_rangetblentry->relid, false);

	/* CURRENT OF can only appear at top level of UPDATE/DELETE */
	Assert(pstate->p_target_rangetblentry != NULL);
	cexpr->cvarno = relation_parser.RTERangeTablePosn(pstate,
									  pstate->p_target_rangetblentry,
									  &sublevels_up);
	Assert(sublevels_up == 0);

	cexpr->target_relid = pstate->p_target_rangetblentry->relid;

	/*
	 * Check to see if the cursor name matches a parameter of type REFCURSOR.
	 * If so, replace the raw name reference with a parameter reference. (This
	 * is a hack for the convenience of plpgsql.)
	 */
	if (cexpr->cursor_name != NULL)		/* in case already transformed */
	{
		PGColumnRef  *cref = makeNode(PGColumnRef);
		PGNode	   *node = NULL;

		/* Build an unqualified ColumnRef with the given name */
		cref->fields = list_make1(makeString(cexpr->cursor_name));
		cref->location = -1;

		/* See if there is a translation available from a parser hook */
		// if (pstate->p_pre_columnref_hook != NULL)
		// 	node = (*pstate->p_pre_columnref_hook) (pstate, cref);
		// if (node == NULL && pstate->p_post_columnref_hook != NULL)
		// 	node = (*pstate->p_post_columnref_hook) (pstate, cref, NULL);

		/*
		 * XXX Should we throw an error if we get a translation that isn't a
		 * refcursor Param?  For now it seems best to silently ignore false
		 * matches.
		 */
		if (node != NULL && IsA(node, PGParam))
		{
			PGParam	   *p = (PGParam *) node;

			if (p->paramkind == PG_PARAM_EXTERN &&
				p->paramtype == REFCURSOROID)
			{
				/* Matches, so convert CURRENT OF to a param reference */
				cexpr->cursor_name = NULL;
				cexpr->cursor_param = p->paramid;
			}
		}
	}

	return (PGNode *) cexpr;
};

PGNode *
ExprParser::transformGroupingFunc(PGParseState *pstate, PGGroupingFunc *gf)
{
	PGList *targs = NIL;
	PGListCell *lc;
	PGGroupingFunc *new_gf;

	new_gf = makeNode(PGGroupingFunc);

	/*
	 * Transform the list of arguments.
	 */
	foreach (lc, gf->args)
		targs = lappend(targs, transformExpr(pstate, (PGNode *)lfirst(lc),
											 PGParseExprKind::EXPR_KIND_GROUP_BY));

	new_gf->args = targs;

	new_gf->ngrpcols = gf->ngrpcols;

	return (PGNode *)new_gf;
};

}