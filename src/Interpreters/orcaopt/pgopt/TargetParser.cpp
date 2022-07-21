#include <TargetParser.h>

namespace DB
{

duckdb_libpgquery::PGList *
    ExpandSingleTable(PGParseState *pstate, duckdb_libpgquery::PGRangeTblEntry *rte,
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
		duckdb_libpgquery::PGList	   *vars;
		duckdb_libpgquery::PGListCell   *l;

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
			duckdb_libpgquery::PGVar		   *var = (Var *) lfirst(l);

			markVarForSelectPriv(pstate, var, rte);
		}

		return vars;
	}
};

duckdb_libpgquery::PGList *
    ExpandColumnRefStar(PGParseState *pstate, duckdb_libpgquery::PGColumnRef *cref,
					bool make_target_entry)
{
	duckdb_libpgquery::PGList	   *fields = cref->fields;
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
		duckdb_libpgquery::PGRangeTblEntry *rte = NULL;
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
			duckdb_libpgquery::PGNode	   *node;

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
			duckdb_libpgquery::PGNode	   *node;

			node = (*pstate->p_post_columnref_hook) (pstate, cref,
													 (duckdb_libpgquery::PGNode *) rte);
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
					errorMissingRTE(pstate, duckdb_libpgquery::makeRangeVar(nspname, relname,
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

duckdb_libpgquery::PGList *
TargetParser::transformTargetList(PGParseState *pstate, duckdb_libpgquery::PGList *targetlist,
					PGParseExprKind exprKind)
{
	duckdb_libpgquery::PGList	   *p_target = NIL;
	bool		expand_star;
	duckdb_libpgquery::PGListCell   *o_target;

	/* Expand "something.*" in SELECT and RETURNING, but not UPDATE */
	expand_star = (exprKind != EXPR_KIND_UPDATE_SOURCE);

	foreach(o_target, targetlist)
	{
		duckdb_libpgquery::PGResTarget  *res = (duckdb_libpgquery::PGResTarget *) lfirst(o_target);

		/*
		 * Check for "something.*".  Depending on the complexity of the
		 * "something", the star could appear as the last field in ColumnRef,
		 * or as the last indirection item in A_Indirection.
		 */
		if (expand_star)
		{
			if (IsA(res->val, duckdb_libpgquery::PGColumnRef))
			{
				duckdb_libpgquery::PGColumnRef  *cref = (duckdb_libpgquery::PGColumnRef *) res->val;

				if (IsA(llast(cref->fields), duckdb_libpgquery::PGAStar))
				{
					/* It is something.*, expand into multiple items */
					p_target = list_concat(p_target,
										   ExpandColumnRefStar(pstate,
															   cref,
															   true));
					continue;
				}
			}
			else if (IsA(res->val, duckdb_libpgquery::PGAIndirection))
			{
				duckdb_libpgquery::PGAIndirection *ind = (duckdb_libpgquery::PGAIndirection *) res->val;

				if (IsA(llast(ind->indirection), duckdb_libpgquery::PGAStar))
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
TargetParser::markTargetListOrigins(PGParseState *pstate, duckdb_libpgquery::PGList *targetlist)
{
	duckdb_libpgquery::PGListCell   *l;

	foreach(l, targetlist)
	{
		duckdb_libpgquery::PGTargetEntry *tle = (duckdb_libpgquery::PGTargetEntry *) lfirst(l);

		markTargetListOrigin(pstate, tle, (duckdb_libpgquery::PGVar *) tle->expr, 0);
	}
};

void
TargetParser::markTargetListOrigin(PGParseState *pstate, duckdb_libpgquery::PGTargetEntry *tle,
					 duckdb_libpgquery::PGVar *var, int levelsup)
{
	int			netlevelsup;
	duckdb_libpgquery::PGRangeTblEntry *rte;
	PGAttrNumber	attnum;

	if (var == NULL || !IsA(var, duckdb_libpgquery::PGVar))
		return;
	netlevelsup = var->varlevelsup + levelsup;
	rte = relation_parser.GetRTEByRangeTablePosn(pstate, var->varno, netlevelsup);
	attnum = var->varattno;

	switch (rte->rtekind)
	{
		case duckdb_libpgquery::PG_RTE_RELATION:
			/* It's a table or view, report it */
			tle->resorigtbl = rte->relid;
			tle->resorigcol = attnum;
			break;
		case duckdb_libpgquery::PG_RTE_SUBQUERY:
			/* Subselect-in-FROM: copy up from the subselect */
			if (attnum != InvalidAttrNumber)
			{
				duckdb_libpgquery::PGTargetEntry *ste = get_tle_by_resno(rte->subquery->targetList,
													attnum);

				if (ste == NULL || ste->resjunk)
					elog(ERROR, "subquery %s does not have attribute %d",
						 rte->eref->aliasname, attnum);
				tle->resorigtbl = ste->resorigtbl;
				tle->resorigcol = ste->resorigcol;
			}
			break;
		case duckdb_libpgquery::PG_RTE_JOIN:
			/* Join RTE --- recursively inspect the alias variable */
			if (attnum != InvalidAttrNumber)
			{
				duckdb_libpgquery::PGVar		   *aliasvar;

				Assert(attnum > 0 && attnum <= list_length(rte->joinaliasvars));
				aliasvar = (duckdb_libpgquery::PGVar *) list_nth(rte->joinaliasvars, attnum - 1);
				/* We intentionally don't strip implicit coercions here */
				markTargetListOrigin(pstate, tle, aliasvar, netlevelsup);
			}
			break;
		//case duckdb_libpgquery::PG_RTE_TABLEFUNCTION:
		case duckdb_libpgquery::PG_RTE_FUNCTION:
		case duckdb_libpgquery::PG_RTE_VALUES:
		//case duckdb_libpgquery::PG_RTE_VOID:
			/* not a simple relation, leave it unmarked */
			break;
		case duckdb_libpgquery::PG_RTE_CTE:

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
				duckdb_libpgquery::PGCommonTableExpr *cte = GetCTEForRTE(pstate, rte, netlevelsup);
				duckdb_libpgquery::PGTargetEntry *ste;

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

duckdb_libpgquery::PGTargetEntry *
TargetParser::transformTargetEntry(PGParseState *pstate,
					 duckdb_libpgquery::PGNode *node,
					 duckdb_libpgquery::PGNode *expr,
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
TargetParser::FigureColname(duckdb_libpgquery::PGNode *node)
{
	char	   *name = NULL;

	(void) FigureColnameInternal(node, &name);
	if (name != NULL)
		return name;
	/* default result if we can't guess anything */
	return "?column?";
};

int FigureColnameInternal(duckdb_libpgquery::PGNode *node, char **name)
{
	int			strength = 0;

	if (node == NULL)
		return strength;

	switch (nodeTag(node))
	{
		case duckdb_libpgquery::T_PGColumnRef:
			{
				char	   *fname = NULL;
				duckdb_libpgquery::PGListCell   *l;

				/* find last field name, if any, ignoring "*" */
				foreach(l, ((duckdb_libpgquery::PGColumnRef *) node)->fields)
				{
					duckdb_libpgquery::PGNode	   *i = lfirst(l);

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
		case duckdb_libpgquery::T_PGAIndirection:
			{
				duckdb_libpgquery::PGAIndirection *ind = (duckdb_libpgquery::PGAIndirection *) node;
				char	   *fname = NULL;
				duckdb_libpgquery::PGListCell   *l;

				/* find last field name, if any, ignoring "*" and subscripts */
				foreach(l, ind->indirection)
				{
					duckdb_libpgquery::PGNode	   *i = lfirst(l);

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
		case duckdb_libpgquery::T_PGFuncCall:
			*name = strVal(llast(((duckdb_libpgquery::PGFuncCall *) node)->funcname));
			return 2;
		case duckdb_libpgquery::T_PGAExpr:
			/* make nullif() act like a regular function */
			if (((duckdb_libpgquery::PGAExpr *) node)->kind == AEXPR_NULLIF)
			{
				*name = "nullif";
				return 2;
			}
			break;
		case duckdb_libpgquery::T_PGTypeCast:
			strength = FigureColnameInternal(((duckdb_libpgquery::PGTypeCast *) node)->arg,
											 name);
			if (strength <= 1)
			{
				if (((duckdb_libpgquery::PGTypeCast *) node)->typeName != NULL)
				{
					*name = strVal(llast(((duckdb_libpgquery::PGTypeCast *) node)->typeName->names));
					return 1;
				}
			}
			break;
		case duckdb_libpgquery::T_PGCollateClause:
			return FigureColnameInternal(((duckdb_libpgquery::PGCollateClause *) node)->arg, name);
		case duckdb_libpgquery::T_PGSubLink:
			switch (((duckdb_libpgquery::PGSubLink *) node)->subLinkType)
			{
				case duckdb_libpgquery::PG_EXISTS_SUBLINK:
					*name = "exists";
					return 2;
				case duckdb_libpgquery::PG_ARRAY_SUBLINK:
					*name = "array";
					return 2;
				case duckdb_libpgquery::PG_EXPR_SUBLINK:
					{
						/* Get column name of the subquery's single target */
						duckdb_libpgquery::PGSubLink    *sublink = (duckdb_libpgquery::PGSubLink *) node;
						duckdb_libpgquery::PGQuery	   *query = (duckdb_libpgquery::PGQuery *) sublink->subselect;

						/*
						 * The subquery has probably already been transformed,
						 * but let's be careful and check that.  (The reason
						 * we can see a transformed subquery here is that
						 * transformSubLink is lazy and modifies the SubLink
						 * node in-place.)
						 */
						if (IsA(query, duckdb_libpgquery::PGQuery))
						{
							duckdb_libpgquery::PGTargetEntry *te = (duckdb_libpgquery::PGTargetEntry *) linitial(query->targetList);

							if (te->resname)
							{
								*name = te->resname;
								return 2;
							}
						}
					}
					break;

					/* As with other operator-like nodes, these have no names */
				case duckdb_libpgquery::PG_ALL_SUBLINK:
				case duckdb_libpgquery::PG_ANY_SUBLINK:
				case duckdb_libpgquery::PG_ROWCOMPARE_SUBLINK:
				case duckdb_libpgquery::PG_CTE_SUBLINK:
				case duckdb_libpgquery::PG_INITPLAN_FUNC_SUBLINK:
				case duckdb_libpgquery::PG_NOT_EXISTS_SUBLINK:
					break;
			}
			break;
		case duckdb_libpgquery::T_PGCaseExpr:
			strength = FigureColnameInternal((duckdb_libpgquery::PGNode *) ((duckdb_libpgquery::PGCaseExpr *) node)->defresult,
											 name);
			if (strength <= 1)
			{
				*name = "case";
				return 1;
			}
			break;
		case duckdb_libpgquery::T_PGAArrayExpr:
			/* make ARRAY[] act like a function */
			*name = "array";
			return 2;
		case duckdb_libpgquery::T_PGRowExpr:
			/* make ROW() act like a function */
			*name = "row";
			return 2;
		case duckdb_libpgquery::T_PGCoalesceExpr:
			/* make coalesce() act like a regular function */
			*name = "coalesce";
			return 2;
		case duckdb_libpgquery::T_PGMinMaxExpr:
			/* make greatest/least act like a regular function */
			switch (((duckdb_libpgquery::PGMinMaxExpr *) node)->op)
			{
				case duckdb_libpgquery::PG_IS_GREATEST:
					*name = "greatest";
					return 2;
				case duckdb_libpgquery::PG_IS_LEAST:
					*name = "least";
					return 2;
			}
			break;
		case duckdb_libpgquery::T_PGXmlExpr:
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
		case duckdb_libpgquery::T_PGXmlSerialize:
			*name = "xmlserialize";
			return 2;
		case duckdb_libpgquery::T_PGGroupingFunc:
			*name = "grouping";
			return 2;
		default:
			break;
	}

	return strength;
};

}