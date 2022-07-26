#include <Interpreters/orcaopt/pgopt/TargetParser.h>

using namespace duckdb_libpgquery;

namespace DB
{

PGList *
    ExpandSingleTable(PGParseState *pstate, PGRangeTblEntry *rte,
				  int location, bool make_target_entry)
{
	int			sublevels_up;
	int			rtindex;

	rtindex = RTERangeTablePosn(pstate, rte, &sublevels_up);

	if (make_target_entry)
	{
		/* expandRelAttrs handles permissions marking */
		return expandRelAttrs(pstate, rte, rtindex, sublevels_up,
							  location);
	}
	else
	{
		PGList	   *vars;
		PGListCell   *l;

		expandRTE(rte, rtindex, sublevels_up, location, false,
				  NULL, &vars);

		/*
		 * Require read access to the table.  This is normally redundant with
		 * the markVarForSelectPriv calls below, but not if the table has zero
		 * columns.
		 */
		rte->requiredPerms |= ACL_SELECT;

		/* Require read access to each column */
		foreach(l, vars)
		{
			PGVar		   *var = (Var *) lfirst(l);

			markVarForSelectPriv(pstate, var, rte);
		}

		return vars;
	}
};

PGList *
    ExpandColumnRefStar(PGParseState *pstate, PGColumnRef *cref,
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
		if (pstate->p_pre_columnref_hook != NULL)
		{
			PGNode	   *node;

			node = (*pstate->p_pre_columnref_hook) (pstate, cref);
			if (node != NULL)
				return ExpandRowReference(pstate, node, make_target_entry);
		}

		switch (numnames)
		{
			case 2:
				relname = strVal(linitial(fields));
				rte = refnameRangeTblEntry(pstate, nspname, relname,
										   cref->location,
										   &levels_up);
				break;
			case 3:
				nspname = strVal(linitial(fields));
				relname = strVal(lsecond(fields));
				rte = refnameRangeTblEntry(pstate, nspname, relname,
										   cref->location,
										   &levels_up);
				break;
			case 4:
				{
					char	   *catname = strVal(linitial(fields));

					/*
					 * We check the catalog name and then ignore it.
					 */
					if (strcmp(catname, get_database_name(MyDatabaseId)) != 0)
					{
						crserr = CRSERR_WRONG_DB;
						break;
					}
					nspname = strVal(lsecond(fields));
					relname = strVal(lthird(fields));
					rte = refnameRangeTblEntry(pstate, nspname, relname,
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
		if (pstate->p_post_columnref_hook != NULL)
		{
			PGNode	   *node;

			node = (*pstate->p_post_columnref_hook) (pstate, cref,
													 (PGNode *) rte);
			if (node != NULL)
			{
				if (rte != NULL)
					ereport(ERROR,
							(errcode(ERRCODE_AMBIGUOUS_COLUMN),
							 errmsg("column reference \"%s\" is ambiguous",
									NameListToString(cref->fields)),
							 parser_errposition(pstate, cref->location)));
				return ExpandRowReference(pstate, node, make_target_entry);
			}
		}

		/*
		 * Throw error if no translation found.
		 */
		if (rte == NULL)
		{
			switch (crserr)
			{
				case CRSERR_NO_RTE:
					errorMissingRTE(pstate, makeRangeVar(nspname, relname,
														 cref->location));
					break;
				case CRSERR_WRONG_DB:
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("cross-database references are not implemented: %s",
									NameListToString(cref->fields)),
							 parser_errposition(pstate, cref->location)));
					break;
				case CRSERR_TOO_MANY:
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("improper qualified name (too many dotted names): %s",
									NameListToString(cref->fields)),
							 parser_errposition(pstate, cref->location)));
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
TargetParser::transformTargetList(PGParseState *pstate, PGList *targetlist,
					PGParseExprKind exprKind)
{
	PGList	   *p_target = NIL;
	bool		expand_star;
	PGListCell   *o_target;

	/* Expand "something.*" in SELECT and RETURNING, but not UPDATE */
	expand_star = (exprKind != EXPR_KIND_UPDATE_SOURCE);

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

	return p_target;
};

void
TargetParser::markTargetListOrigins(PGParseState *pstate, PGList *targetlist)
{
	PGListCell   *l;

	foreach(l, targetlist)
	{
		PGTargetEntry *tle = (PGTargetEntry *) lfirst(l);

		markTargetListOrigin(pstate, tle, (PGVar *) tle->expr, 0);
	}
};

void
TargetParser::markTargetListOrigin(PGParseState *pstate, PGTargetEntry *tle,
					 PGVar *var, int levelsup)
{
	int			netlevelsup;
	PGRangeTblEntry *rte;
	PGAttrNumber	attnum;

	if (var == NULL || !IsA(var, PGVar))
		return;
	netlevelsup = var->varlevelsup + levelsup;
	rte = relation_parser.GetRTEByRangeTablePosn(pstate, var->varno, netlevelsup);
	attnum = var->varattno;

	switch (rte->rtekind)
	{
		case PG_RTE_RELATION:
			/* It's a table or view, report it */
			tle->resorigtbl = rte->relid;
			tle->resorigcol = attnum;
			break;
		case PG_RTE_SUBQUERY:
			/* Subselect-in-FROM: copy up from the subselect */
			if (attnum != InvalidAttrNumber)
			{
				PGTargetEntry *ste = get_tle_by_resno(rte->subquery->targetList,
													attnum);

				if (ste == NULL || ste->resjunk)
					elog(ERROR, "subquery %s does not have attribute %d",
						 rte->eref->aliasname, attnum);
				tle->resorigtbl = ste->resorigtbl;
				tle->resorigcol = ste->resorigcol;
			}
			break;
		case PG_RTE_JOIN:
			/* Join RTE --- recursively inspect the alias variable */
			if (attnum != InvalidAttrNumber)
			{
				PGVar		   *aliasvar;

				Assert(attnum > 0 && attnum <= list_length(rte->joinaliasvars));
				aliasvar = (PGVar *) list_nth(rte->joinaliasvars, attnum - 1);
				/* We intentionally don't strip implicit coercions here */
				markTargetListOrigin(pstate, tle, aliasvar, netlevelsup);
			}
			break;
		//case PG_RTE_TABLEFUNCTION:
		case PG_RTE_FUNCTION:
		case PG_RTE_VALUES:
		//case PG_RTE_VOID:
			/* not a simple relation, leave it unmarked */
			break;
		case PG_RTE_CTE:

			/*
			 * CTE reference: copy up from the subquery, if possible. If the
			 * RTE is a recursive self-reference then we can't do anything
			 * because we haven't finished analyzing it yet. However, it's no
			 * big loss because we must be down inside the recursive term of a
			 * recursive CTE, and so any markings on the current targetlist
			 * are not going to affect the results anyway.
			 */
			if (attnum != InvalidAttrNumber && !rte->self_reference)
			{
				PGCommonTableExpr *cte = GetCTEForRTE(pstate, rte, netlevelsup);
				PGTargetEntry *ste;

				ste = get_tle_by_resno(GetCTETargetList(cte), attnum);
				if (ste == NULL || ste->resjunk)
					elog(ERROR, "subquery %s does not have attribute %d",
						 rte->eref->aliasname, attnum);
				tle->resorigtbl = ste->resorigtbl;
				tle->resorigcol = ste->resorigcol;
			}
			break;
	}
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
		expr = expr_parser.transformExpr(pstate, node, exprKind);

	if (colname == NULL && !resjunk)
	{
		/*
		 * Generate a suitable column name for a column without any explicit
		 * 'AS ColumnName' clause.
		 */
		colname = FigureColname(node);
	}

	return makeTargetEntry((Expr *) expr,
						   (AttrNumber) pstate->p_next_resno++,
						   colname,
						   resjunk);
};

char *
TargetParser::FigureColname(PGNode *node)
{
	char	   *name = NULL;

	(void) FigureColnameInternal(node, &name);
	if (name != NULL)
		return name;
	/* default result if we can't guess anything */
	return "?column?";
};

int FigureColnameInternal(PGNode *node, char **name)
{
	int			strength = 0;

	if (node == NULL)
		return strength;

	switch (nodeTag(node))
	{
		case T_PGColumnRef:
			{
				char	   *fname = NULL;
				PGListCell   *l;

				/* find last field name, if any, ignoring "*" */
				foreach(l, ((PGColumnRef *) node)->fields)
				{
					PGNode	   *i = lfirst(l);

					if (IsA(i, String))
						fname = strVal(i);
				}
				if (fname)
				{
					*name = fname;
					return 2;
				}
			}
			break;
		case T_PGAIndirection:
			{
				PGAIndirection *ind = (PGAIndirection *) node;
				char	   *fname = NULL;
				PGListCell   *l;

				/* find last field name, if any, ignoring "*" and subscripts */
				foreach(l, ind->indirection)
				{
					PGNode	   *i = lfirst(l);

					if (IsA(i, String))
						fname = strVal(i);
				}
				if (fname)
				{
					*name = fname;
					return 2;
				}
				return FigureColnameInternal(ind->arg, name);
			}
			break;
		case T_PGFuncCall:
			*name = strVal(llast(((PGFuncCall *) node)->funcname));
			return 2;
		case T_PGAExpr:
			/* make nullif() act like a regular function */
			if (((PGAExpr *) node)->kind == AEXPR_NULLIF)
			{
				*name = "nullif";
				return 2;
			}
			break;
		case T_PGTypeCast:
			strength = FigureColnameInternal(((PGTypeCast *) node)->arg,
											 name);
			if (strength <= 1)
			{
				if (((PGTypeCast *) node)->typeName != NULL)
				{
					*name = strVal(llast(((PGTypeCast *) node)->typeName->names));
					return 1;
				}
			}
			break;
		case T_PGCollateClause:
			return FigureColnameInternal(((PGCollateClause *) node)->arg, name);
		case T_PGSubLink:
			switch (((PGSubLink *) node)->subLinkType)
			{
				case PG_EXISTS_SUBLINK:
					*name = "exists";
					return 2;
				case PG_ARRAY_SUBLINK:
					*name = "array";
					return 2;
				case PG_EXPR_SUBLINK:
					{
						/* Get column name of the subquery's single target */
						PGSubLink    *sublink = (PGSubLink *) node;
						PGQuery	   *query = (PGQuery *) sublink->subselect;

						/*
						 * The subquery has probably already been transformed,
						 * but let's be careful and check that.  (The reason
						 * we can see a transformed subquery here is that
						 * transformSubLink is lazy and modifies the SubLink
						 * node in-place.)
						 */
						if (IsA(query, PGQuery))
						{
							PGTargetEntry *te = (PGTargetEntry *) linitial(query->targetList);

							if (te->resname)
							{
								*name = te->resname;
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
				case PG_INITPLAN_FUNC_SUBLINK:
				case PG_NOT_EXISTS_SUBLINK:
					break;
			}
			break;
		case T_PGCaseExpr:
			strength = FigureColnameInternal((PGNode *) ((PGCaseExpr *) node)->defresult,
											 name);
			if (strength <= 1)
			{
				*name = "case";
				return 1;
			}
			break;
		case T_PGAArrayExpr:
			/* make ARRAY[] act like a function */
			*name = "array";
			return 2;
		case T_PGRowExpr:
			/* make ROW() act like a function */
			*name = "row";
			return 2;
		case T_PGCoalesceExpr:
			/* make coalesce() act like a regular function */
			*name = "coalesce";
			return 2;
		case T_PGMinMaxExpr:
			/* make greatest/least act like a regular function */
			switch (((PGMinMaxExpr *) node)->op)
			{
				case PG_IS_GREATEST:
					*name = "greatest";
					return 2;
				case PG_IS_LEAST:
					*name = "least";
					return 2;
			}
			break;
		case T_PGXmlExpr:
			/* make SQL/XML functions act like a regular function */
			switch (((XmlExpr *) node)->op)
			{
				case IS_XMLCONCAT:
					*name = "xmlconcat";
					return 2;
				case IS_XMLELEMENT:
					*name = "xmlelement";
					return 2;
				case IS_XMLFOREST:
					*name = "xmlforest";
					return 2;
				case IS_XMLPARSE:
					*name = "xmlparse";
					return 2;
				case IS_XMLPI:
					*name = "xmlpi";
					return 2;
				case IS_XMLROOT:
					*name = "xmlroot";
					return 2;
				case IS_XMLSERIALIZE:
					*name = "xmlserialize";
					return 2;
				case IS_DOCUMENT:
					/* nothing */
					break;
			}
			break;
		case T_PGXmlSerialize:
			*name = "xmlserialize";
			return 2;
		case T_PGGroupingFunc:
			*name = "grouping";
			return 2;
		default:
			break;
	}

	return strength;
};

}