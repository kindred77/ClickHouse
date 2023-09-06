#include <Interpreters/orcaopt/TargetParser.h>

#include <Interpreters/orcaopt/RelationParser.h>
#include <Interpreters/orcaopt/ExprParser.h>
#include <Interpreters/orcaopt/NodeParser.h>
#include <Interpreters/orcaopt/CoerceParser.h>
#include <Interpreters/orcaopt/provider/TypeProvider.h>
#include <Interpreters/orcaopt/provider/RelationProvider.h>

#include <Interpreters/Context.h>

using namespace duckdb_libpgquery;

namespace DB
{

TargetParser::TargetParser(const ContextPtr& context_) : context(context_)
{
	relation_parser = std::make_shared<RelationParser>(context);
    expr_parser = std::make_shared<ExprParser>(context);
    node_parser = std::make_shared<NodeParser>(context);
    coerce_parser = std::make_shared<CoerceParser>(context);
    // type_provider = std::make_shared<TypeProvider>(context);
    // relation_provider = std::make_shared<RelationProvider>(context);
};

int
TargetParser::FigureColnameInternal(PGNode *node, std::string & name)
{
    int strength = 0;

    if (node == NULL)
        return strength;

    switch (nodeTag(node))
    {
        case T_PGColumnRef: {
            char * fname = NULL;
            ListCell * l;

            /* find last field name, if any, ignoring "*" */
            foreach (l, ((PGColumnRef *)node)->fields)
            {
                PGNode * i = (PGNode *)lfirst(l);

                if (IsA(i, PGString))
                    fname = strVal(i);
            }
            if (fname)
            {
                name = std::string(fname);
                return 2;
            }
        }
        break;
        case T_PGAIndirection: {
            PGAIndirection * ind = (PGAIndirection *)node;
            char * fname = NULL;
            ListCell * l;

            /* find last field name, if any, ignoring "*" and subscripts */
            foreach (l, ind->indirection)
            {
                PGNode * i = (PGNode *)lfirst(l);

                if (IsA(i, PGString))
                    fname = strVal(i);
            }
            if (fname)
            {
                name = std::string(fname);
                return 2;
            }
            return FigureColnameInternal(ind->arg, name);
        }
        //break;
        case T_PGFuncCall:
            name = std::string(strVal(llast(((PGFuncCall *)node)->funcname)));
            return 2;
        case T_PGAExpr:
            /* make nullif() act like a regular function */
            if (((PGAExpr *)node)->kind == PG_AEXPR_NULLIF)
            {
                name = "nullif";
                return 2;
            }
            break;
        case T_PGTypeCast:
            strength = FigureColnameInternal(((PGTypeCast *)node)->arg, name);
            if (strength <= 1)
            {
                if (((PGTypeCast *)node)->typeName != NULL)
                {
                    name = std::string(strVal(llast(((PGTypeCast *)node)->typeName->names)));
                    return 1;
                }
            }
            break;
        case T_PGCollateClause:
            return FigureColnameInternal(((PGCollateClause *)node)->arg, name);
        case T_PGSubLink:
            switch (((PGSubLink *)node)->subLinkType)
            {
                case PG_EXISTS_SUBLINK:
                    name = "exists";
                    return 2;
                case PG_ARRAY_SUBLINK:
                    name = "array";
                    return 2;
                case PG_EXPR_SUBLINK: {
                    /* Get column name of the subquery's single target */
                    PGSubLink * sublink = (PGSubLink *)node;
                    PGQuery * query = (PGQuery *)sublink->subselect;

                    /*
						 * The subquery has probably already been transformed,
						 * but let's be careful and check that.  (The reason
						 * we can see a transformed subquery here is that
						 * transformSubLink is lazy and modifies the SubLink
						 * node in-place.)
						 */
                    if (IsA(query, PGQuery))
                    {
                            PGTargetEntry * te = (PGTargetEntry *)linitial(query->targetList);

                            if (te->resname)
                            {
                                name = std::string(te->resname);
                                return 2;
                            }
                    }
                }
                break;

                    /* As with other operator-like nodes, these have no names */
                case PG_ALL_SUBLINK:
                case PG_ANY_SUBLINK:
                case PG_ROWCOMPARE_SUBLINK:
                case PG_CTE_SUBLINK:
				//TODO kindred
                // case INITPLAN_FUNC_SUBLINK:
                // case NOT_EXISTS_SUBLINK:
                    break;
            }
            break;
        case T_PGCaseExpr:
            strength = FigureColnameInternal((PGNode *)((PGCaseExpr *)node)->defresult, name);
            if (strength <= 1)
            {
                name = "case";
                return 1;
            }
            break;
        case T_PGAArrayExpr:
            /* make ARRAY[] act like a function */
            name = "array";
            return 2;
        case T_PGRowExpr:
            /* make ROW() act like a function */
            name = "row";
            return 2;
        case T_PGCoalesceExpr:
            /* make coalesce() act like a regular function */
            name = "coalesce";
            return 2;
        case T_PGMinMaxExpr:
            /* make greatest/least act like a regular function */
            switch (((PGMinMaxExpr *)node)->op)
            {
                case PG_IS_GREATEST:
                    name = "greatest";
                    return 2;
                case IS_LEAST:
                    name = "least";
                    return 2;
            }
            break;
		//TODO kindred
        // case T_PGXmlExpr:
        //     /* make SQL/XML functions act like a regular function */
        //     switch (((XmlExpr *)node)->op)
        //     {
        //         case IS_XMLCONCAT:
        //             *name = "xmlconcat";
        //             return 2;
        //         case IS_XMLELEMENT:
        //             *name = "xmlelement";
        //             return 2;
        //         case IS_XMLFOREST:
        //             *name = "xmlforest";
        //             return 2;
        //         case IS_XMLPARSE:
        //             *name = "xmlparse";
        //             return 2;
        //         case IS_XMLPI:
        //             *name = "xmlpi";
        //             return 2;
        //         case IS_XMLROOT:
        //             *name = "xmlroot";
        //             return 2;
        //         case IS_XMLSERIALIZE:
        //             *name = "xmlserialize";
        //             return 2;
        //         case IS_DOCUMENT:
        //             /* nothing */
        //             break;
        //     }
        //     break;
        case T_PGXmlSerialize:
            name = "xmlserialize";
            return 2;
        case T_PGGroupingFunc:
            name = "grouping";
            return 2;
        default:
            break;
    }

    return strength;
};

std::string
TargetParser::FigureColname(PGNode *node)
{
	std::string name = "";

	(void) FigureColnameInternal(node, name);
	if (name != "")
		return name;
	/* default result if we can't guess anything */
	return "?column?";
};

PGTargetEntry *
TargetParser::transformTargetEntry(PGParseState *pstate,
					 PGNode *node,
					 PGNode *expr,
					 PGParseExprKind exprKind,
					 char *colname,
					 bool resjunk)
{
	/* Transform the node if caller didn't do it already */
	if (expr == NULL)
	{
		/*
		 * If it's a SetToDefault node and we should allow that, pass it
		 * through unmodified.  (transformExpr will throw the appropriate
		 * error if we're disallowing it.)
		 */
		if (exprKind == EXPR_KIND_UPDATE_SOURCE && IsA(node, PGSetToDefault))
			expr = node;
		else
			expr = expr_parser->transformExpr(pstate, node, exprKind);
	}

	if (colname == NULL && !resjunk)
	{
		/*
		 * Generate a suitable column name for a column without any explicit
		 * 'AS ColumnName' clause.
		 */
		//TODO kindred
		colname = pstrdup(FigureColname(node).c_str());
	}

	return makeTargetEntry((PGExpr *) expr,
						   (PGAttrNumber) pstate->p_next_resno++,
						   colname,
						   resjunk);
};

PGList *
TargetParser::ExpandAllTables(PGParseState *pstate, int location)
{
	PGList	   *target = NIL;
	bool		found_table = false;
	PGListCell   *l;

	foreach(l, pstate->p_namespace)
	{
		PGParseNamespaceItem *nsitem = (PGParseNamespaceItem *) lfirst(l);
		PGRangeTblEntry *rte = nsitem->p_rte;

		/* Ignore table-only items */
		if (!nsitem->p_cols_visible)
			continue;
		/* Should not have any lateral-only items when parsing targetlist */
		Assert(!nsitem->p_lateral_only)
		/* Remember we found a p_cols_visible item */
		found_table = true;

		target = list_concat(target,
							 relation_parser->expandRelAttrs(pstate,
											rte,
											relation_parser->RTERangeTablePosn(pstate, rte,
															  NULL),
											0,
											location));
	}

	/*
	 * Check for "SELECT *;".  We do it this way, rather than checking for
	 * target == NIL, because we want to allow SELECT * FROM a zero_column
	 * table.
	 */
	if (!found_table)
	{
		parser_errposition(pstate, location);
		ereport(ERROR,
				(errcode(PG_ERRCODE_SYNTAX_ERROR),
				 errmsg("SELECT * with no tables specified is not valid")));
	}

	return target;
};

PGTupleDescPtr
TargetParser::expandRecordVariable(PGParseState *pstate, PGVar *var, int levelsup)
{
    int netlevelsup;
    PGRangeTblEntry * rte;
    PGAttrNumber attnum;
    PGNode * expr;
	PGTupleDescPtr tupleDesc = nullptr;

    /* Check my caller didn't mess up */
    Assert(IsA(var, PGVar))
    Assert(var->vartype == RECORDOID)

    netlevelsup = var->varlevelsup + levelsup;
    rte = relation_parser->GetRTEByRangeTablePosn(pstate, var->varno, netlevelsup);
    attnum = var->varattno;

    if (attnum == InvalidAttrNumber)
    {
        /* Whole-row reference to an RTE, so expand the known fields */
        PGList *names, *vars;
        PGListCell *lname, *lvar;
        int i;

        relation_parser->expandRTE(rte, var->varno, 0, var->location, false, &names, &vars);

        tupleDesc = PGCreateTemplateTupleDesc(list_length(vars), false);
        i = 1;
        forboth(lname, names, lvar, vars)
        {
            char * label = strVal(lfirst(lname));
            PGNode * varnode = (PGNode *)lfirst(lvar);

            TypeProvider::PGTupleDescInitEntry(tupleDesc, i, label, exprType(varnode), exprTypmod(varnode), 0);
            PGTupleDescInitEntryCollation(tupleDesc, i, exprCollation(varnode));
            i++;
        }
        Assert(lname == NULL && lvar == NULL) /* lists same length? */

        return tupleDesc;
    }

    expr = (PGNode *)var; /* default if we can't drill down */

    switch (rte->rtekind)
    {
        case PG_RTE_RELATION:
        case PG_RTE_VALUES:

            /*
			 * This case should not occur: a column of a table or values list
			 * shouldn't have type RECORD.  Fall through and fail (most
			 * likely) at the bottom.
			 */
            break;
        case PG_RTE_SUBQUERY: {
            /* Subselect-in-FROM: examine sub-select's output expr */
            PGTargetEntry * ste = relation_parser->get_tle_by_resno(rte->subquery->targetList, attnum);

            if (ste == NULL || ste->resjunk)
                elog(ERROR, "subquery %s does not have attribute %d", rte->eref->aliasname, attnum);
            expr = (PGNode *)ste->expr;
            if (IsA(expr, PGVar))
            {
                /*
					 * Recurse into the sub-select to see what its Var refers
					 * to.  We have to build an additional level of ParseState
					 * to keep in step with varlevelsup in the subselect.
					 */
                PGParseState mypstate;

                MemSet(&mypstate, 0, sizeof(mypstate));
                mypstate.parentParseState = pstate;
                mypstate.p_rtable = rte->subquery->rtable;
                /* don't bother filling the rest of the fake pstate */

                return expandRecordVariable(&mypstate, (PGVar *)expr, 0);
            }
            /* else fall through to inspect the expression */
        }
        break;
        case PG_RTE_JOIN:
            /* Join RTE --- recursively inspect the alias variable */
            Assert(attnum > 0 && attnum <= list_length(rte->joinaliasvars))
            expr = (PGNode *)list_nth(rte->joinaliasvars, attnum - 1);
            Assert(expr != NULL)
            /* We intentionally don't strip implicit coercions here */
            if (IsA(expr, PGVar))
                return expandRecordVariable(pstate, (PGVar *)expr, netlevelsup);
            /* else fall through to inspect the expression */
            break;
		//TODO kindred
        // case RTE_TABLEFUNCTION:
        case PG_RTE_FUNCTION:

            /*
			 * We couldn't get here unless a function is declared with one of
			 * its result columns as RECORD, which is not allowed.
			 */
            break;
        case PG_RTE_CTE:
            /* CTE reference: examine subquery's output expr */
            if (!rte->self_reference)
            {
                PGCommonTableExpr * cte = relation_parser->GetCTEForRTE(pstate, rte, netlevelsup);
                PGTargetEntry * ste;

                ste = relation_parser->get_tle_by_resno(GetCTETargetList(cte), attnum);
                if (ste == NULL || ste->resjunk)
                    elog(ERROR, "subquery %s does not have attribute %d", rte->eref->aliasname, attnum);
                expr = (PGNode *)ste->expr;
                if (IsA(expr, PGVar))
                {
                    /*
					 * Recurse into the CTE to see what its Var refers to. We
					 * have to build an additional level of ParseState to keep
					 * in step with varlevelsup in the CTE; furthermore it
					 * could be an outer CTE.
					 */
                    PGParseState mypstate;
                    PGIndex levelsup_cte;

                    MemSet(&mypstate, 0, sizeof(mypstate));
                    /* this loop must work, since GetCTEForRTE did */
                    for (levelsup_cte = 0; levelsup_cte < rte->ctelevelsup + netlevelsup; levelsup_cte++)
                        pstate = pstate->parentParseState;
                    mypstate.parentParseState = pstate;
                    mypstate.p_rtable = ((PGQuery *)cte->ctequery)->rtable;
                    /* don't bother filling the rest of the fake pstate */

                    return expandRecordVariable(&mypstate, (PGVar *)expr, 0);
                }
                /* else fall through to inspect the expression */
            }
            break;
		//TODO kindred
        // case RTE_VOID:
        //     Insist(0);
        //     break;
    }

    /*
	 * We now have an expression we can't expand any more, so see if
	 * get_expr_result_type() can do anything with it.  If not, pass to
	 * lookup_rowtype_tupdesc() which will probably fail, but will give an
	 * appropriate error message while failing.
	 */
    if (TypeProvider::get_expr_result_type(expr, NULL, tupleDesc) != TYPEFUNC_COMPOSITE)
	{
        tupleDesc = TypeProvider::lookup_rowtype_tupdesc_copy(exprType(expr), exprTypmod(expr));
	}

	return tupleDesc;
};

PGList *
TargetParser::ExpandRowReference(PGParseState *pstate, PGNode *expr,
				   bool make_target_entry)
{
    PGList * result = NIL;
    PGTupleDescPtr tupleDesc = nullptr;
    int numAttrs;
    int i;

    /*
	 * If the rowtype expression is a whole-row Var, we can expand the fields
	 * as simple Vars.  Note: if the RTE is a relation, this case leaves us
	 * with the RTE's selectedCols bitmap showing the whole row as needing
	 * select permission, as well as the individual columns.  However, we can
	 * only get here for weird notations like (table.*).*, so it's not worth
	 * trying to clean up --- arguably, the permissions marking is correct
	 * anyway for such cases.
	 */
    if (IsA(expr, PGVar) && ((PGVar *)expr)->varattno == InvalidAttrNumber)
    {
        PGVar * var = (PGVar *)expr;
        PGRangeTblEntry * rte;

        rte = relation_parser->GetRTEByRangeTablePosn(pstate, var->varno, var->varlevelsup);
        return ExpandSingleTable(pstate, rte, var->location, make_target_entry);
    }

    /*
	 * Otherwise we have to do it the hard way.  Our current implementation is
	 * to generate multiple copies of the expression and do FieldSelects.
	 * (This can be pretty inefficient if the expression involves nontrivial
	 * computation :-(.)
	 *
	 * Verify it's a composite type, and get the tupdesc.  We use
	 * get_expr_result_type() because that can handle references to functions
	 * returning anonymous record types.  If that fails, use
	 * lookup_rowtype_tupdesc(), which will almost certainly fail as well, but
	 * it will give an appropriate error message.
	 *
	 * If it's a Var of type RECORD, we have to work even harder: we have to
	 * find what the Var refers to, and pass that to get_expr_result_type.
	 * That task is handled by expandRecordVariable().
	 */
    if (IsA(expr, PGVar) && ((PGVar *)expr)->vartype == RECORDOID)
        tupleDesc = expandRecordVariable(pstate, (PGVar *)expr, 0);
    else if (TypeProvider::get_expr_result_type(expr, NULL, tupleDesc) != TYPEFUNC_COMPOSITE)
        tupleDesc = TypeProvider::lookup_rowtype_tupdesc_copy(exprType(expr), exprTypmod(expr));
    Assert(tupleDesc)

    /* Generate a list of references to the individual fields */
    numAttrs = tupleDesc->natts;
    for (i = 0; i < numAttrs; i++)
    {
        PGAttrPtr att = tupleDesc->attrs[i];
        PGFieldSelect * fselect;

        if (att->attisdropped)
            continue;

        fselect = makeNode(PGFieldSelect);
        fselect->arg = (PGExpr *)copyObject(expr);
        fselect->fieldnum = i + 1;
        fselect->resulttype = att->atttypid;
        fselect->resulttypmod = att->atttypmod;
        /* save attribute's collation for parse_collate.c */
        fselect->resultcollid = att->attcollation;

        if (make_target_entry)
        {
            /* add TargetEntry decoration */
            PGTargetEntry * te;

            te = makeTargetEntry((PGExpr *)fselect, (PGAttrNumber)pstate->p_next_resno++, pstrdup(att->attname.c_str()), false);
            result = lappend(result, te);
        }
        else
            result = lappend(result, fselect);
    }

    return result;
};

PGList *
TargetParser::ExpandSingleTable(PGParseState *pstate, PGRangeTblEntry *rte,
				  int location, bool make_target_entry)
{
	int			sublevels_up;
	int			rtindex;

	rtindex = relation_parser->RTERangeTablePosn(pstate, rte, &sublevels_up);

	if (make_target_entry)
	{
		/* expandRelAttrs handles permissions marking */
		return relation_parser->expandRelAttrs(pstate, rte, rtindex, sublevels_up,
							  location);
	}
	else
	{
		PGList	   *vars;
		ListCell   *l;

		relation_parser->expandRTE(rte, rtindex, sublevels_up, location, false,
				  NULL, &vars);

		/*
		 * Require read access to the table.  This is normally redundant with
		 * the markVarForSelectPriv calls below, but not if the table has zero
		 * columns.
		 */
		// rte->requiredPerms |= ACL_SELECT;

		/* Require read access to each column */
		foreach(l, vars)
		{
			PGVar		   *var = (PGVar *) lfirst(l);

			relation_parser->markVarForSelectPriv(pstate, var, rte);
		}

		return vars;
	}
};

PGList *
TargetParser::ExpandColumnRefStar(PGParseState *pstate, PGColumnRef *cref,
					bool make_target_entry)
{
	PGList	   *fields = cref->fields;
	int			numnames = list_length(fields);

	if (numnames == 1)
	{
		/*
		 * Target item is a bare '*', expand all tables
		 *
		 * (e.g., SELECT * FROM emp, dept)
		 *
		 * Since the grammar only accepts bare '*' at top level of SELECT, we
		 * need not handle the make_target_entry==false case here.
		 */
		if (!make_target_entry)
			elog(ERROR, "invalid use of *");

		return ExpandAllTables(pstate, cref->location);
	}
	else
	{
		/*
		 * Target item is relation.*, expand that table
		 *
		 * (e.g., SELECT emp.*, dname FROM emp, dept)
		 *
		 * Note: this code is a lot like transformColumnRef; it's tempting to
		 * call that instead and then replace the resulting whole-row Var with
		 * a list of Vars.  However, that would leave us with the RTE's
		 * selectedCols bitmap showing the whole row as needing select
		 * permission, as well as the individual columns.  That would be
		 * incorrect (since columns added later shouldn't need select
		 * permissions).  We could try to remove the whole-row permission bit
		 * after the fact, but duplicating code is less messy.
		 */
		char	   *nspname = NULL;
		char	   *relname = NULL;
		PGRangeTblEntry *rte = NULL;
		int			levels_up;
		enum
		{
			CRSERR_NO_RTE,
			CRSERR_WRONG_DB,
			CRSERR_TOO_MANY
		}			crserr = CRSERR_NO_RTE;

		/*
		 * Give the PreParseColumnRefHook, if any, first shot.  If it returns
		 * non-null then we should use that expression.
		 */
		// if (pstate->p_pre_columnref_hook != NULL)
		// {
		// 	PGNode	   *node;

		// 	node = pstate->p_pre_columnref_hook(pstate, cref);
		// 	if (node != NULL)
		// 		return ExpandRowReference(pstate, node, make_target_entry);
		// }

		switch (numnames)
		{
			case 2:
				relname = strVal(linitial(fields));
				rte = relation_parser->refnameRangeTblEntry(pstate, nspname, relname,
										   cref->location,
										   &levels_up);
				break;
			case 3:
				nspname = strVal(linitial(fields));
				relname = strVal(lsecond(fields));
				rte = relation_parser->refnameRangeTblEntry(pstate, nspname, relname,
										   cref->location,
										   &levels_up);
				break;
			case 4:
				{
					char	   *catname = strVal(linitial(fields));

					/*
					 * We check the catalog name and then ignore it.
					 */
					if (strcmp(catname, RelationProvider::get_database_name(MyDatabaseId).c_str()) != 0)
					{
						crserr = CRSERR_WRONG_DB;
						break;
					}
					nspname = strVal(lsecond(fields));
					relname = strVal(lthird(fields));
					rte = relation_parser->refnameRangeTblEntry(pstate, nspname, relname,
											   cref->location,
											   &levels_up);
					break;
				}
			default:
				crserr = CRSERR_TOO_MANY;
				break;
		}

		/*
		 * Now give the PostParseColumnRefHook, if any, a chance. We cheat a
		 * bit by passing the RangeTblEntry, not a Var, as the planned
		 * translation.  (A single Var wouldn't be strictly correct anyway.
		 * This convention allows hooks that really care to know what is
		 * happening.)
		 */
		// if (pstate->p_post_columnref_hook != NULL)
		// {
		// 	PGNode	   *node;

		// 	node = pstate->p_post_columnref_hook(pstate, cref,
		// 										 (PGNode *) rte);
		// 	if (node != NULL)
		// 	{
		// 		if (rte != NULL)
		// 			ereport(ERROR,
		// 					(errcode(ERRCODE_AMBIGUOUS_COLUMN),
		// 					 errmsg("column reference \"%s\" is ambiguous",
		// 							NameListToString(cref->fields)),
		// 					 parser_errposition(pstate, cref->location)));
		// 		return ExpandRowReference(pstate, node, make_target_entry);
		// 	}
		// }

		/*
		 * Throw error if no translation found.
		 */
		if (rte == NULL)
		{
			switch (crserr)
			{
				case CRSERR_NO_RTE:
					relation_parser->errorMissingRTE(pstate, makeRangeVar(nspname, relname,
														 cref->location));
					break;
				case CRSERR_WRONG_DB:
					parser_errposition(pstate, cref->location);
					ereport(ERROR,
							(errcode(PG_ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("cross-database references are not implemented: %s",
									PGNameListToString(cref->fields).c_str())));
					break;
				case CRSERR_TOO_MANY:
					parser_errposition(pstate, cref->location);
					ereport(ERROR,
							(errcode(PG_ERRCODE_SYNTAX_ERROR),
							 errmsg("improper qualified name (too many dotted names): %s",
									PGNameListToString(cref->fields).c_str())));
					break;
			}
		}

		/*
		 * OK, expand the RTE into fields.
		 */
		return ExpandSingleTable(pstate, rte, cref->location, make_target_entry);
	}
};

PGList *
TargetParser::ExpandIndirectionStar(PGParseState *pstate, PGAIndirection *ind,
					  bool make_target_entry, PGParseExprKind exprKind)
{
	PGNode	   *expr;

	/* Strip off the '*' to create a reference to the rowtype object */
	ind = (PGAIndirection *)copyObject(ind);
	ind->indirection = list_truncate(ind->indirection,
									 list_length(ind->indirection) - 1);

	/* And transform that */
	expr = expr_parser->transformExpr(pstate, (PGNode *) ind, exprKind);

	/* Expand the rowtype expression into individual fields */
	return ExpandRowReference(pstate, expr, make_target_entry);
};

PGList *
TargetParser::transformTargetList(PGParseState *pstate, PGList *targetlist,
					PGParseExprKind exprKind)
{
    PGList	   *p_target = NIL;
	bool		expand_star;
	PGListCell   *o_target;

	/* Shouldn't have any leftover multiassign items at start */
	Assert(pstate->p_multiassign_exprs == NIL)

	/* Expand "something.*" in SELECT and RETURNING, but not UPDATE */
	expand_star = (exprKind != PGParseExprKind::EXPR_KIND_UPDATE_SOURCE);

	foreach(o_target, targetlist)
	{
		PGResTarget  *res = (PGResTarget *) lfirst(o_target);

		/*
		 * Check for "something.*".  Depending on the complexity of the
		 * "something", the star could appear as the last field in ColumnRef,
		 * or as the last indirection item in A_Indirection.
		 */
		if (expand_star)
		{
			if (IsA(res->val, PGColumnRef))
			{
				PGColumnRef  *cref = (PGColumnRef *) res->val;

				if (IsA(llast(cref->fields), PGAStar))
				{
					/* It is something.*, expand into multiple items */
					p_target = list_concat(p_target,
										   ExpandColumnRefStar(pstate,
															   cref,
															   true));
					continue;
				}
			}
			else if (IsA(res->val, PGAIndirection))
			{
				PGAIndirection *ind = (PGAIndirection *) res->val;

				if (IsA(llast(ind->indirection), PGAStar))
				{
					/* It is something.*, expand into multiple items */
					p_target = list_concat(p_target,
										   ExpandIndirectionStar(pstate,
																 ind,
																 true,
																 exprKind));
					continue;
				}
			}
		}

		/*
		 * Not "something.*", or we want to treat that as a plain whole-row
		 * variable, so transform as a single expression
		 */
		p_target = lappend(p_target,
						   transformTargetEntry(pstate,
												res->val,
												NULL,
												exprKind,
												res->name,
												false));
	}

	/*
	 * If any multiassign resjunk items were created, attach them to the end
	 * of the targetlist.  This should only happen in an UPDATE tlist.  We
	 * don't need to worry about numbering of these items; transformUpdateStmt
	 * will set their resnos.
	 */
	// if (pstate->p_multiassign_exprs)
	// {
	// 	Assert(exprKind == PGParseExprKind::EXPR_KIND_UPDATE_SOURCE);
	// 	p_target = list_concat(p_target, pstate->p_multiassign_exprs);
	// 	pstate->p_multiassign_exprs = NIL;
	// }

	return p_target;
};

PGList *
TargetParser::transformExpressionList(PGParseState *pstate, PGList *exprlist,
						PGParseExprKind exprKind)
{
    PGList * result = NIL;
    PGListCell * lc;

    foreach (lc, exprlist)
    {
        PGNode * e = (PGNode *)lfirst(lc);

        /*
		 * Check for "something.*".  Depending on the complexity of the
		 * "something", the star could appear as the last field in ColumnRef,
		 * or as the last indirection item in A_Indirection.
		 */
        if (IsA(e, PGColumnRef))
        {
            PGColumnRef * cref = (PGColumnRef *)e;

            if (IsA(llast(cref->fields), PGAStar))
            {
                /* It is something.*, expand into multiple items */
                result = list_concat(result, ExpandColumnRefStar(pstate, cref, false));
                continue;
            }
        }
        else if (IsA(e, PGAIndirection))
        {
            PGAIndirection * ind = (PGAIndirection *)e;

            if (IsA(llast(ind->indirection), PGAStar))
            {
                /* It is something.*, expand into multiple items */
                result = list_concat(result, ExpandIndirectionStar(pstate, ind, false, exprKind));
                continue;
            }
        }

        /*
		 * Not "something.*", so transform as a single expression
		 */
        result = lappend(result, expr_parser->transformExpr(pstate, e, exprKind));
    }

    return result;
};

void
TargetParser::resolveTargetListUnknowns(PGParseState *pstate, PGList *targetlist)
{
	PGListCell   *l;

	foreach(l, targetlist)
	{
		PGTargetEntry *tle = (PGTargetEntry *) lfirst(l);
		PGOid			restype = exprType((PGNode *) tle->expr);

		if (restype == UNKNOWNOID)
		{
			tle->expr = (PGExpr *) coerce_parser->coerce_type(pstate, (PGNode *) tle->expr,
											 restype, TEXTOID, -1,
											 PG_COERCION_IMPLICIT,
											 PG_COERCE_IMPLICIT_CAST,
											 -1);
		}
	}
};

}
