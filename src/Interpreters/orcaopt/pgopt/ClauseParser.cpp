#include <ClauseParser.h>

namespace DB
{

duckdb_libpgquery::PGRangeTblEntry *
ClauseParser::transformCTEReference(PGParseState *pstate, duckdb_libpgquery::PGRangeVar *r,
					  duckdb_libpgquery::PGCommonTableExpr *cte, Index levelsup)
{
	duckdb_libpgquery::PGRangeTblEntry *rte;

	rte = relation_parser.addRangeTableEntryForCTE(pstate, cte, levelsup, r, true);

	return rte;
};

void
ClauseParser::setNamespaceLateralState(duckdb_libpgquery::PGList *namespace, bool lateral_only, bool lateral_ok)
{
	duckdb_libpgquery::PGListCell   *lc;

	foreach(lc, namespace)
	{
		PGParseNamespaceItem *nsitem = (PGParseNamespaceItem *) lfirst(lc);

		nsitem->p_lateral_only = lateral_only;
		nsitem->p_lateral_ok = lateral_ok;
	}
};

duckdb_libpgquery::PGRangeTblEntry *
ClauseParser::transformTableEntry(PGParseState *pstate, duckdb_libpgquery::PGRangeVar *r)
{
	duckdb_libpgquery::PGRangeTblEntry *rte;

	/* We need only build a range table entry */
	rte = relation_parser.addRangeTableEntry(pstate, r, r->alias,
							 interpretInhOption(r->inhOpt), true);

	return rte;
};

bool ClauseParser::interpretInhOption(InhOption inhOpt)
{
	switch (inhOpt)
	{
		case INH_NO:
			return false;
		case INH_YES:
			return true;
		case INH_DEFAULT:
			return SQL_inheritance;
	}
	elog(ERROR, "bogus InhOption value: %d", inhOpt);
	return false;				/* keep compiler quiet */
}

void
ClauseParser::transformFromClause(PGParseState *pstate, duckdb_libpgquery::PGList *frmList)
{
	duckdb_libpgquery::PGListCell   *fl;

	/*
	 * The grammar will have produced a list of RangeVars, RangeSubselects,
	 * RangeFunctions, and/or JoinExprs. Transform each one (possibly adding
	 * entries to the rtable), check for duplicate refnames, and then add it
	 * to the joinlist and namespace.
	 *
	 * Note we must process the items left-to-right for proper handling of
	 * LATERAL references.
	 */
	foreach(fl, frmList)
	{
		duckdb_libpgquery::PGNode	   *n = lfirst(fl);
		duckdb_libpgquery::PGRangeTblEntry *rte = NULL;
		int			rtindex = 0;
		duckdb_libpgquery::PGList * namespace = NULL;

		n = transformFromClauseItem(pstate, n,
									&rte,
									&rtindex,
									&namespace);

		relation_parser.checkNameSpaceConflicts(pstate, pstate->p_namespace, namespace);

		/* Mark the new namespace items as visible only to LATERAL */
		setNamespaceLateralState(namespace, true, true);

		pstate->p_joinlist = lappend(pstate->p_joinlist, n);
		pstate->p_namespace = list_concat(pstate->p_namespace, namespace);
	}

	/*
	 * We're done parsing the FROM list, so make all namespace items
	 * unconditionally visible.  Note that this will also reset lateral_only
	 * for any namespace items that were already present when we were called;
	 * but those should have been that way already.
	 */
	setNamespaceLateralState(pstate->p_namespace, false, true);
};

PGParseNamespaceItem *
ClauseParser::makeNamespaceItem(duckdb_libpgquery::PGRangeTblEntry *rte, bool rel_visible, bool cols_visible,
				  bool lateral_only, bool lateral_ok)
{
	PGParseNamespaceItem *nsitem;

	//nsitem = (PGParseNamespaceItem *) palloc(sizeof(PGParseNamespaceItem));
	nsitem = new PGParseNamespaceItem();
	nsitem->p_rte = rte;
	nsitem->p_rel_visible = rel_visible;
	nsitem->p_cols_visible = cols_visible;
	nsitem->p_lateral_only = lateral_only;
	nsitem->p_lateral_ok = lateral_ok;
	return nsitem;
}

duckdb_libpgquery::PGRangeTblEntry *
ClauseParser::transformRangeSubselect(PGParseState *pstate, duckdb_libpgquery::PGRangeSubselect *r)
{
	duckdb_libpgquery::PGQuery	   *query;
	duckdb_libpgquery::PGRangeTblEntry *rte;

	/*
	 * We require user to supply an alias for a subselect, per SQL92. To relax
	 * this, we'd have to be prepared to gin up a unique alias for an
	 * unlabeled subselect.  (This is just elog, not ereport, because the
	 * grammar should have enforced it already.  It'd probably be better to
	 * report the error here, but we don't have a good error location here.)
	 */
	if (r->alias == NULL)
		elog(ERROR, "subquery in FROM must have an alias");

	/*
	 * Set p_expr_kind to show this parse level is recursing to a subselect.
	 * We can't be nested within any expression, so don't need save-restore
	 * logic here.
	 */
	Assert(pstate->p_expr_kind == EXPR_KIND_NONE);
	pstate->p_expr_kind = EXPR_KIND_FROM_SUBSELECT;

	/*
	 * If the subselect is LATERAL, make lateral_only names of this level
	 * visible to it.  (LATERAL can't nest within a single pstate level, so we
	 * don't need save/restore logic here.)
	 */
	Assert(!pstate->p_lateral_active);
	pstate->p_lateral_active = r->lateral;

	/*
	 * Analyze and transform the subquery.
	 */
	query = select_parser.parse_sub_analyze(r->subquery, pstate, NULL,
							  getLockedRefname(pstate, r->alias->aliasname));

	/* Restore state */
	pstate->p_lateral_active = false;
	pstate->p_expr_kind = EXPR_KIND_NONE;

	/*
	 * Check that we got something reasonable.  Many of these conditions are
	 * impossible given restrictions of the grammar, but check 'em anyway.
	 */
	if (!IsA(query, duckdb_libpgquery::PGQuery) ||
		query->commandType != duckdb_libpgquery::PG_CMD_SELECT ||
		query->utilityStmt != NULL)
		elog(ERROR, "unexpected non-SELECT command in subquery in FROM");

	/*
	 * OK, build an RTE for the subquery.
	 */
	rte = relation_parser.addRangeTableEntryForSubquery(pstate,
										query,
										r->alias,
										r->lateral,
										true);

	return rte;
}

duckdb_libpgquery::PGNode *
ClauseParser::transformFromClauseItem(PGParseState *pstate, duckdb_libpgquery::PGNode *n,
        duckdb_libpgquery::PGRangeTblEntry **top_rte, int *top_rti,
        duckdb_libpgquery::PGList **namespace)
{
    duckdb_libpgquery::PGNode *result;

	if (IsA(n, duckdb_libpgquery::PGRangeVar))
	{
		/* Plain relation reference, or perhaps a CTE reference */
		duckdb_libpgquery::PGRangeVar   *rv = (duckdb_libpgquery::PGRangeVar *) n;
		duckdb_libpgquery::PGRangeTblRef *rtr;
		duckdb_libpgquery::PGRangeTblEntry *rte = NULL;
		int			rtindex;

		/* if it is an unqualified name, it might be a CTE reference */
		if (!rv->schemaname)
		{
			duckdb_libpgquery::PGCommonTableExpr *cte;
			unsigned int levelsup;

			cte = relation_parser.scanNameSpaceForCTE(pstate, rv->relname, &levelsup);
			if (cte)
				rte = transformCTEReference(pstate, rv, cte, levelsup);
		}

		/* if not found as a CTE, must be a table reference */
		if (!rte)
			rte = transformTableEntry(pstate, rv);

		/* assume new rte is at end */
		rtindex = list_length(pstate->p_rtable);
		Assert(rte == rt_fetch(rtindex, pstate->p_rtable));
		*top_rte = rte;
		*top_rti = rtindex;
		*namespace = list_make1(makeNamespaceItem(rte));
		rtr = makeNode(duckdb_libpgquery::PGRangeTblRef);
		rtr->rtindex = rtindex;
		result = (duckdb_libpgquery::PGNode *) rtr;
	}
	else if (IsA(n, duckdb_libpgquery::PGRangeSubselect))
	{
		/* sub-SELECT is like a plain relation */
		duckdb_libpgquery::PGRangeTblRef *rtr;
		duckdb_libpgquery::PGRangeTblEntry *rte;
		int			rtindex;

		rte = transformRangeSubselect(pstate, (duckdb_libpgquery::PGRangeSubselect *) n);
		/* assume new rte is at end */
		rtindex = list_length(pstate->p_rtable);
		Assert(rte == rt_fetch(rtindex, pstate->p_rtable));
		*top_rte = rte;
		*top_rti = rtindex;
		*namespace = list_make1(makeNamespaceItem(rte));
		rtr = makeNode(duckdb_libpgquery::PGRangeTblRef);
		rtr->rtindex = rtindex;
		result = (duckdb_libpgquery::PGNode *) rtr;
	}
	// else if (IsA(n, duckdb_libpgquery::PGRangeFunction))
	// {
	// 	/* function is like a plain relation */
	// 	duckdb_libpgquery::PGRangeTblRef *rtr;
	// 	duckdb_libpgquery::PGRangeTblEntry *rte;
	// 	int			rtindex;

	// 	rte = transformRangeFunction(pstate, (duckdb_libpgquery::PGRangeFunction *) n);
	// 	/* assume new rte is at end */
	// 	rtindex = list_length(pstate->p_rtable);
	// 	Assert(rte == rt_fetch(rtindex, pstate->p_rtable));
	// 	*top_rte = rte;
	// 	*top_rti = rtindex;
	// 	*namespace = list_make1(makeNamespaceItem(rte));
	// 	rtr = makeNode(duckdb_libpgquery::PGRangeTblRef);
	// 	rtr->rtindex = rtindex;
	// 	result = (duckdb_libpgquery::PGNode *) rtr;
	// }
	else if (IsA(n, duckdb_libpgquery::PGJoinExpr))
	{
		/* A newfangled join expression */
		duckdb_libpgquery::PGJoinExpr   *j = (duckdb_libpgquery::PGJoinExpr *) n;
		duckdb_libpgquery::PGRangeTblEntry *l_rte;
		duckdb_libpgquery::PGRangeTblEntry *r_rte;
		int			l_rtindex;
		int			r_rtindex;
		duckdb_libpgquery::PGList	   *l_namespace,
				   *r_namespace,
				   *my_namespace,
				   *l_colnames,
				   *r_colnames,
				   *res_colnames,
				   *l_colvars,
				   *r_colvars,
				   *res_colvars;
		bool		lateral_ok;
		int			sv_namespace_length;
		duckdb_libpgquery::PGRangeTblEntry *rte;
		int			k;

		/*
		 * Recursively process the left subtree, then the right.  We must do
		 * it in this order for correct visibility of LATERAL references.
		 */
		j->larg = transformFromClauseItem(pstate, j->larg,
										  &l_rte,
										  &l_rtindex,
										  &l_namespace);

		/*
		 * Make the left-side RTEs available for LATERAL access within the
		 * right side, by temporarily adding them to the pstate's namespace
		 * list.  Per SQL:2008, if the join type is not INNER or LEFT then the
		 * left-side names must still be exposed, but it's an error to
		 * reference them.  (Stupid design, but that's what it says.)  Hence,
		 * we always push them into the namespace, but mark them as not
		 * lateral_ok if the jointype is wrong.
		 *
		 * Notice that we don't require the merged namespace list to be
		 * conflict-free.  See the comments for scanNameSpaceForRefname().
		 *
		 * NB: this coding relies on the fact that list_concat is not
		 * destructive to its second argument.
		 */
		lateral_ok = (j->jointype == duckdb_libpgquery::PGJoinType::PG_JOIN_INNER || j->jointype == duckdb_libpgquery::PGJoinType::PG_JOIN_LEFT);
		setNamespaceLateralState(l_namespace, true, lateral_ok);

		sv_namespace_length = list_length(pstate->p_namespace);
		pstate->p_namespace = list_concat(pstate->p_namespace, l_namespace);

		/* And now we can process the RHS */
		j->rarg = transformFromClauseItem(pstate, j->rarg,
										  &r_rte,
										  &r_rtindex,
										  &r_namespace);

		/* Remove the left-side RTEs from the namespace list again */
		pstate->p_namespace = list_truncate(pstate->p_namespace,
											sv_namespace_length);

		/*
		 * Check for conflicting refnames in left and right subtrees. Must do
		 * this because higher levels will assume I hand back a self-
		 * consistent namespace list.
		 */
		relation_parser.checkNameSpaceConflicts(pstate, l_namespace, r_namespace);

		/*
		 * Generate combined namespace info for possible use below.
		 */
		my_namespace = list_concat(l_namespace, r_namespace);

		/*
		 * Extract column name and var lists from both subtrees
		 *
		 * Note: expandRTE returns new lists, safe for me to modify
		 */
		relation_parser.expandRTE(l_rte, l_rtindex, 0, -1, false,
				  &l_colnames, &l_colvars);
		relation_parser.expandRTE(r_rte, r_rtindex, 0, -1, false,
				  &r_colnames, &r_colvars);

		/*
		 * Natural join does not explicitly specify columns; must generate
		 * columns to join. Need to run through the list of columns from each
		 * table or join result and match up the column names. Use the first
		 * table, and check every column in the second table for a match.
		 * (We'll check that the matches were unique later on.) The result of
		 * this step is a list of column names just like an explicitly-written
		 * USING list.
		 */
		if (j->isNatural)
		{
			duckdb_libpgquery::PGList	   *rlist = NIL;
			duckdb_libpgquery::PGListCell   *lx,
					   *rx;

			Assert(j->usingClause == NIL);		/* shouldn't have USING() too */

			foreach(lx, l_colnames)
			{
				char	   *l_colname = strVal(lfirst(lx));
				duckdb_libpgquery::PGValue	   *m_name = NULL;

				foreach(rx, r_colnames)
				{
					char	   *r_colname = strVal(lfirst(rx));

					if (strcmp(l_colname, r_colname) == 0)
					{
						m_name = duckdb_libpgquery::makeString(l_colname);
						break;
					}
				}

				/* matched a right column? then keep as join column... */
				if (m_name != NULL)
					rlist = lappend(rlist, m_name);
			}

			j->usingClause = rlist;
		}

		/*
		 * Now transform the join qualifications, if any.
		 */
		res_colnames = NIL;
		res_colvars = NIL;

		if (j->usingClause)
		{
			/*
			 * JOIN/USING (or NATURAL JOIN, as transformed above). Transform
			 * the list into an explicit ON-condition, and generate a list of
			 * merged result columns.
			 */
			duckdb_libpgquery::PGList	   *ucols = j->usingClause;
			duckdb_libpgquery::PGList	   *l_usingvars = NIL;
			duckdb_libpgquery::PGList	   *r_usingvars = NIL;
			duckdb_libpgquery::PGListCell   *ucol;

			Assert(j->quals == NULL);	/* shouldn't have ON() too */

			foreach(ucol, ucols)
			{
				char	   *u_colname = strVal(lfirst(ucol));
				duckdb_libpgquery::PGListCell   *col;
				int			ndx;
				int			l_index = -1;
				int			r_index = -1;
				duckdb_libpgquery::PGVar		   *l_colvar,
						   *r_colvar;

				/* Check for USING(foo,foo) */
				foreach(col, res_colnames)
				{
					char	   *res_colname = strVal(lfirst(col));

					if (strcmp(res_colname, u_colname) == 0)
						ereport(ERROR,
								(errcode(ERRCODE_DUPLICATE_COLUMN),
								 errmsg("column name \"%s\" appears more than once in USING clause",
										u_colname)));
				}

				/* Find it in left input */
				ndx = 0;
				foreach(col, l_colnames)
				{
					char	   *l_colname = strVal(lfirst(col));

					if (strcmp(l_colname, u_colname) == 0)
					{
						if (l_index >= 0)
							ereport(ERROR,
									(errcode(ERRCODE_AMBIGUOUS_COLUMN),
									 errmsg("common column name \"%s\" appears more than once in left table",
											u_colname)));
						l_index = ndx;
					}
					ndx++;
				}
				if (l_index < 0)
					ereport(ERROR,
							(errcode(ERRCODE_UNDEFINED_COLUMN),
							 errmsg("column \"%s\" specified in USING clause does not exist in left table",
									u_colname)));

				/* Find it in right input */
				ndx = 0;
				foreach(col, r_colnames)
				{
					char	   *r_colname = strVal(lfirst(col));

					if (strcmp(r_colname, u_colname) == 0)
					{
						if (r_index >= 0)
							ereport(ERROR,
									(errcode(ERRCODE_AMBIGUOUS_COLUMN),
									 errmsg("common column name \"%s\" appears more than once in right table",
											u_colname)));
						r_index = ndx;
					}
					ndx++;
				}
				if (r_index < 0)
					ereport(ERROR,
							(errcode(ERRCODE_UNDEFINED_COLUMN),
							 errmsg("column \"%s\" specified in USING clause does not exist in right table",
									u_colname)));

				l_colvar = list_nth(l_colvars, l_index);
				l_usingvars = lappend(l_usingvars, l_colvar);
				r_colvar = list_nth(r_colvars, r_index);
				r_usingvars = lappend(r_usingvars, r_colvar);

				res_colnames = lappend(res_colnames, lfirst(ucol));
				res_colvars = lappend(res_colvars,
									  buildMergedJoinVar(pstate,
														 j->jointype,
														 l_colvar,
														 r_colvar));
			}

			j->quals = transformJoinUsingClause(pstate,
												l_rte,
												r_rte,
												l_usingvars,
												r_usingvars);
		}
		else if (j->quals)
		{
			/* User-written ON-condition; transform it */
			j->quals = transformJoinOnClause(pstate, j, my_namespace);
		}
		else
		{
			/* CROSS JOIN: no quals */
		}

		/* Add remaining columns from each side to the output columns */
		extractRemainingColumns(res_colnames,
								l_colnames, l_colvars,
								&l_colnames, &l_colvars);
		extractRemainingColumns(res_colnames,
								r_colnames, r_colvars,
								&r_colnames, &r_colvars);
		res_colnames = list_concat(res_colnames, l_colnames);
		res_colvars = list_concat(res_colvars, l_colvars);
		res_colnames = list_concat(res_colnames, r_colnames);
		res_colvars = list_concat(res_colvars, r_colvars);

		/*
		 * Check alias (AS clause), if any.
		 */
		if (j->alias)
		{
			if (j->alias->colnames != NIL)
			{
				if (list_length(j->alias->colnames) > list_length(res_colnames))
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("column alias list for \"%s\" has too many entries",
									j->alias->aliasname)));
			}
		}

		/*
		 * Now build an RTE for the result of the join
		 */
		rte = addRangeTableEntryForJoin(pstate,
										res_colnames,
										j->jointype,
										res_colvars,
										j->alias,
										true);

		/* assume new rte is at end */
		j->rtindex = list_length(pstate->p_rtable);
		Assert(rte == rt_fetch(j->rtindex, pstate->p_rtable));

		*top_rte = rte;
		*top_rti = j->rtindex;

		/* make a matching link to the JoinExpr for later use */
		for (k = list_length(pstate->p_joinexprs) + 1; k < j->rtindex; k++)
			pstate->p_joinexprs = lappend(pstate->p_joinexprs, NULL);
		pstate->p_joinexprs = lappend(pstate->p_joinexprs, j);
		Assert(list_length(pstate->p_joinexprs) == j->rtindex);

		/*
		 * Prepare returned namespace list.  If the JOIN has an alias then it
		 * hides the contained RTEs completely; otherwise, the contained RTEs
		 * are still visible as table names, but are not visible for
		 * unqualified column-name access.
		 *
		 * Note: if there are nested alias-less JOINs, the lower-level ones
		 * will remain in the list although they have neither p_rel_visible
		 * nor p_cols_visible set.  We could delete such list items, but it's
		 * unclear that it's worth expending cycles to do so.
		 */
		if (j->alias != NULL)
			my_namespace = NIL;
		else
			setNamespaceColumnVisibility(my_namespace, false);

		/*
		 * The join RTE itself is always made visible for unqualified column
		 * names.  It's visible as a relation name only if it has an alias.
		 */
		*namespace = lappend(my_namespace,
							 makeNamespaceItem(rte,
											   (j->alias != NULL),
											   true,
											   false,
											   true));

		result = (duckdb_libpgquery::PGNode *) j;
	}
	else
    {
        result = NULL;
		elog(ERROR, "unrecognized node type: %d", (int) nodeTag(n));
    }

	return result;
};

duckdb_libpgquery::PGNode *
ClauseParser::transformJoinUsingClause(PGParseState *pstate,
						 duckdb_libpgquery::PGRangeTblEntry *leftRTE, duckdb_libpgquery::PGRangeTblEntry *rightRTE,
						 duckdb_libpgquery::PGList *leftVars, duckdb_libpgquery::PGList *rightVars)
{
	duckdb_libpgquery::PGNode	   *result = NULL;
	duckdb_libpgquery::PGListCell   *lvars,
			   *rvars;

	/*
	 * We cheat a little bit here by building an untransformed operator tree
	 * whose leaves are the already-transformed Vars.  This is OK because
	 * transformExpr() won't complain about already-transformed subnodes.
	 * However, this does mean that we have to mark the columns as requiring
	 * SELECT privilege for ourselves; transformExpr() won't do it.
	 */
	forboth(lvars, leftVars, rvars, rightVars)
	{
		duckdb_libpgquery::PGVar		   *lvar = (duckdb_libpgquery::PGVar *) lfirst(lvars);
		duckdb_libpgquery::PGVar		   *rvar = (duckdb_libpgquery::PGVar *) lfirst(rvars);
		duckdb_libpgquery::PGAExpr	   *e;

		/* Require read access to the join variables */
		relation_parser.markVarForSelectPriv(pstate, lvar, leftRTE);
		relation_parser.markVarForSelectPriv(pstate, rvar, rightRTE);

		/* Now create the lvar = rvar join condition */
		e = duckdb_libpgquery::makeSimpleAExpr(duckdb_libpgquery::PG_AEXPR_OP, "=",
							 reinterpret_cast<duckdb_libpgquery::PGNode *>(duckdb_libpgquery::copyObject((void*)lvar)),
							 reinterpret_cast<duckdb_libpgquery::PGNode *>(duckdb_libpgquery::copyObject((void*)rvar)),
							 -1);

		/* And combine into an AND clause, if multiple join columns */
		if (result == NULL)
			result = (duckdb_libpgquery::PGNode *) e;
		else
		{
			duckdb_libpgquery::PGAExpr	   *a;

			a = duckdb_libpgquery::makeAExpr(duckdb_libpgquery::PG_AEXPR_AND, NIL, result, (duckdb_libpgquery::PGNode *) e, -1);
			result = (duckdb_libpgquery::PGNode *) a;
		}
	}

	/*
	 * Since the references are already Vars, and are certainly from the input
	 * relations, we don't have to go through the same pushups that
	 * transformJoinOnClause() does.  Just invoke transformExpr() to fix up
	 * the operators, and we're done.
	 */
	result = expr_parser.transformExpr(pstate, result, EXPR_KIND_JOIN_USING);

	result = coerce_parser.coerce_to_boolean(pstate, result, "JOIN/USING");

	return result;
};

duckdb_libpgquery::PGNode *
ClauseParser::transformJoinOnClause(PGParseState *pstate, duckdb_libpgquery::PGJoinExpr *j, duckdb_libpgquery::PGList *namespace)
{
	duckdb_libpgquery::PGNode	   *result;
	duckdb_libpgquery::PGList	   *save_namespace;

	/*
	 * The namespace that the join expression should see is just the two
	 * subtrees of the JOIN plus any outer references from upper pstate
	 * levels.  Temporarily set this pstate's namespace accordingly.  (We need
	 * not check for refname conflicts, because transformFromClauseItem()
	 * already did.)  All namespace items are marked visible regardless of
	 * LATERAL state.
	 */
	setNamespaceLateralState(namespace, false, true);

	save_namespace = pstate->p_namespace;
	pstate->p_namespace = namespace;

	result = transformWhereClause(pstate, j->quals,
								  EXPR_KIND_JOIN_ON, "JOIN/ON");

	pstate->p_namespace = save_namespace;

	return result;
};

duckdb_libpgquery::PGNode *
ClauseParser::transformWhereClause(PGParseState *pstate, duckdb_libpgquery::PGNode *clause,
					 PGParseExprKind exprKind, const char *constructName)
{
	duckdb_libpgquery::PGNode	   *qual;

	if (clause == NULL)
		return NULL;

	qual = expr_parser.transformExpr(pstate, clause, exprKind);

	qual = coerce_parser.coerce_to_boolean(pstate, qual, constructName);

	return qual;
};

duckdb_libpgquery::PGNode *
ClauseParser::buildMergedJoinVar(PGParseState *pstate, duckdb_libpgquery::PGJoinType jointype,
				   duckdb_libpgquery::PGVar *l_colvar, duckdb_libpgquery::PGVar *r_colvar)
{
	Oid			outcoltype;
	int32		outcoltypmod;
	duckdb_libpgquery::PGNode	   *l_node,
			   *r_node,
			   *res_node;

	/*
	 * Choose output type if input types are dissimilar.
	 */
	outcoltype = l_colvar->vartype;
	outcoltypmod = l_colvar->vartypmod;
	if (outcoltype != r_colvar->vartype)
	{
		outcoltype = select_common_type(pstate,
										list_make2(l_colvar, r_colvar),
										"JOIN/USING",
										NULL);
		outcoltypmod = -1;		/* ie, unknown */
	}
	else if (outcoltypmod != r_colvar->vartypmod)
	{
		/* same type, but not same typmod */
		outcoltypmod = -1;		/* ie, unknown */
	}

	/*
	 * Insert coercion functions if needed.  Note that a difference in typmod
	 * can only happen if input has typmod but outcoltypmod is -1. In that
	 * case we insert a RelabelType to clearly mark that result's typmod is
	 * not same as input.  We never need coerce_type_typmod.
	 */
	if (l_colvar->vartype != outcoltype)
		l_node = coerce_type(pstate, (duckdb_libpgquery::PGNode *) l_colvar, l_colvar->vartype,
							 outcoltype, outcoltypmod,
							 duckdb_libpgquery::PG_COERCION_IMPLICIT, duckdb_libpgquery::PG_COERCE_IMPLICIT_CAST, -1);
	else if (l_colvar->vartypmod != outcoltypmod)
		l_node = (duckdb_libpgquery::PGNode *) makeRelabelType((duckdb_libpgquery::PGExpr *) l_colvar,
										  outcoltype, outcoltypmod,
										  InvalidOid,	/* fixed below */
										  duckdb_libpgquery::PG_COERCE_IMPLICIT_CAST);
	else
		l_node = (duckdb_libpgquery::PGNode *) l_colvar;

	if (r_colvar->vartype != outcoltype)
		r_node = coerce_type(pstate, (duckdb_libpgquery::PGNode *) r_colvar, r_colvar->vartype,
							 outcoltype, outcoltypmod,
							 duckdb_libpgquery::PG_COERCION_IMPLICIT, duckdb_libpgquery::PG_COERCE_IMPLICIT_CAST, -1);
	else if (r_colvar->vartypmod != outcoltypmod)
		r_node = (duckdb_libpgquery::PGNode *) makeRelabelType((duckdb_libpgquery::PGExpr *) r_colvar,
										  outcoltype, outcoltypmod,
										  InvalidOid,	/* fixed below */
										  duckdb_libpgquery::PG_COERCE_IMPLICIT_CAST);
	else
		r_node = (duckdb_libpgquery::PGNode *) r_colvar;

	/*
	 * Choose what to emit
	 */
	switch (jointype)
	{
		case duckdb_libpgquery::PG_JOIN_INNER:

			/*
			 * We can use either var; prefer non-coerced one if available.
			 */
			if (IsA(l_node, duckdb_libpgquery::PGVar))
				res_node = l_node;
			else if (IsA(r_node, duckdb_libpgquery::PGVar))
				res_node = r_node;
			else
				res_node = l_node;
			break;
		case duckdb_libpgquery::PG_JOIN_LEFT:
			/* Always use left var */
			res_node = l_node;
			break;
		case duckdb_libpgquery::PG_JOIN_RIGHT:
			/* Always use right var */
			res_node = r_node;
			break;
		case duckdb_libpgquery::PG_JOIN_FULL:
			{
				/*
				 * Here we must build a COALESCE expression to ensure that the
				 * join output is non-null if either input is.
				 */
				duckdb_libpgquery::PGCoalesceExpr *c = makeNode(duckdb_libpgquery::PGCoalesceExpr);

				c->coalescetype = outcoltype;
				/* coalescecollid will get set below */
				c->args = list_make2(l_node, r_node);
				c->location = -1;
				res_node = (duckdb_libpgquery::PGNode *) c;
				break;
			}
		default:
			elog(ERROR, "unrecognized join type: %d", (int) jointype);
			res_node = NULL;	/* keep compiler quiet */
			break;
	}

	/*
	 * Apply assign_expr_collations to fix up the collation info in the
	 * coercion and CoalesceExpr nodes, if we made any.  This must be done now
	 * so that the join node's alias vars show correct collation info.
	 */
	assign_expr_collations(pstate, res_node);

	return res_node;
};

duckdb_libpgquery::PGList *
ClauseParser::transformSortClause(PGParseState *pstate,
					duckdb_libpgquery::PGList *orderlist,
					duckdb_libpgquery::PGList **targetlist,
					PGParseExprKind exprKind,
					bool resolveUnknown,
					bool useSQL99)
{
	duckdb_libpgquery::PGList	   *sortlist = NIL;
	duckdb_libpgquery::PGListCell   *olitem;

	foreach(olitem, orderlist)
	{
		duckdb_libpgquery::PGSortBy	   *sortby = (duckdb_libpgquery::PGSortBy *) lfirst(olitem);
		duckdb_libpgquery::PGTargetEntry *tle;

		if (useSQL99)
			tle = findTargetlistEntrySQL99(pstate, sortby->node,
										   targetlist, exprKind);
		else
			tle = findTargetlistEntrySQL92(pstate, sortby->node,
										   targetlist, exprKind);

		sortlist = addTargetToSortList(pstate, tle,
									   sortlist, *targetlist, sortby,
									   resolveUnknown);
	}

	return sortlist;
};

duckdb_libpgquery::PGTargetEntry *
ClauseParser::findTargetlistEntrySQL99(PGParseState *pstate, duckdb_libpgquery::PGNode *node, duckdb_libpgquery::PGList **tlist,
						 PGParseExprKind exprKind)
{
	duckdb_libpgquery::PGTargetEntry *target_result;
	duckdb_libpgquery::PGListCell   *tl;
	duckdb_libpgquery::PGNode	   *expr;

	/*
	 * Convert the untransformed node to a transformed expression, and search
	 * for a match in the tlist.  NOTE: it doesn't really matter whether there
	 * is more than one match.  Also, we are willing to match an existing
	 * resjunk target here, though the SQL92 cases above must ignore resjunk
	 * targets.
	 */
	expr = expr_parser.transformExpr(pstate, node, exprKind);

	foreach(tl, *tlist)
	{
		duckdb_libpgquery::PGTargetEntry *tle = (duckdb_libpgquery::PGTargetEntry *) lfirst(tl);
		duckdb_libpgquery::PGNode	   *texpr;

		/*
		 * Ignore any implicit cast on the existing tlist expression.
		 *
		 * This essentially allows the ORDER/GROUP/etc item to adopt the same
		 * datatype previously selected for a textually-equivalent tlist item.
		 * There can't be any implicit cast at top level in an ordinary SELECT
		 * tlist at this stage, but the case does arise with ORDER BY in an
		 * aggregate function.
		 */
		texpr = strip_implicit_coercions((duckdb_libpgquery::PGNode *) tle->expr);

		if (equal(expr, texpr))
			return tle;
	}

	/*
	 * If no matches, construct a new target entry which is appended to the
	 * end of the target list.  This target is given resjunk = TRUE so that it
	 * will not be projected into the final tuple.
	 */
	target_result = target_parser.transformTargetEntry(pstate, node, expr, exprKind,
										 NULL, true);

	*tlist = lappend(*tlist, target_result);

	return target_result;
};

duckdb_libpgquery::PGTargetEntry *
ClauseParser::findTargetlistEntrySQL92(PGParseState *pstate, duckdb_libpgquery::PGNode *node, duckdb_libpgquery::PGList **tlist,
						 PGParseExprKind exprKind)
{
	duckdb_libpgquery::PGListCell   *tl;

	/*----------
	 * Handle two special cases as mandated by the SQL92 spec:
	 *
	 * 1. Bare ColumnName (no qualifier or subscripts)
	 *	  For a bare identifier, we search for a matching column name
	 *	  in the existing target list.  Multiple matches are an error
	 *	  unless they refer to identical values; for example,
	 *	  we allow	SELECT a, a FROM table ORDER BY a
	 *	  but not	SELECT a AS b, b FROM table ORDER BY b
	 *	  If no match is found, we fall through and treat the identifier
	 *	  as an expression.
	 *	  For GROUP BY, it is incorrect to match the grouping item against
	 *	  targetlist entries: according to SQL92, an identifier in GROUP BY
	 *	  is a reference to a column name exposed by FROM, not to a target
	 *	  list column.  However, many implementations (including pre-7.0
	 *	  PostgreSQL) accept this anyway.  So for GROUP BY, we look first
	 *	  to see if the identifier matches any FROM column name, and only
	 *	  try for a targetlist name if it doesn't.  This ensures that we
	 *	  adhere to the spec in the case where the name could be both.
	 *	  DISTINCT ON isn't in the standard, so we can do what we like there;
	 *	  we choose to make it work like ORDER BY, on the rather flimsy
	 *	  grounds that ordinary DISTINCT works on targetlist entries.
	 *
	 * 2. IntegerConstant
	 *	  This means to use the n'th item in the existing target list.
	 *	  Note that it would make no sense to order/group/distinct by an
	 *	  actual constant, so this does not create a conflict with SQL99.
	 *	  GROUP BY column-number is not allowed by SQL92, but since
	 *	  the standard has no other behavior defined for this syntax,
	 *	  we may as well accept this common extension.
	 *
	 * Note that pre-existing resjunk targets must not be used in either case,
	 * since the user didn't write them in his SELECT list.
	 *
	 * If neither special case applies, fall through to treat the item as
	 * an expression per SQL99.
	 *----------
	 */
	if (IsA(node, duckdb_libpgquery::PGColumnRef) &&
		list_length(((duckdb_libpgquery::PGColumnRef *) node)->fields) == 1 &&
		IsA(linitial(((duckdb_libpgquery::PGColumnRef *) node)->fields), String))
	{
		char	   *name = strVal(linitial(((duckdb_libpgquery::PGColumnRef *) node)->fields));
		int			location = ((duckdb_libpgquery::PGColumnRef *) node)->location;

		if (exprKind == EXPR_KIND_GROUP_BY)
		{
			/*
			 * In GROUP BY, we must prefer a match against a FROM-clause
			 * column to one against the targetlist.  Look to see if there is
			 * a matching column.  If so, fall through to use SQL99 rules.
			 * NOTE: if name could refer ambiguously to more than one column
			 * name exposed by FROM, colNameToVar will ereport(ERROR). That's
			 * just what we want here.
			 *
			 * Small tweak for 7.4.3: ignore matches in upper query levels.
			 * This effectively changes the search order for bare names to (1)
			 * local FROM variables, (2) local targetlist aliases, (3) outer
			 * FROM variables, whereas before it was (1) (3) (2). SQL92 and
			 * SQL99 do not allow GROUPing BY an outer reference, so this
			 * breaks no cases that are legal per spec, and it seems a more
			 * self-consistent behavior.
			 */
			if (relation_parser.colNameToVar(pstate, name, true, location) != NULL)
				name = NULL;
		}

		if (name != NULL)
		{
			duckdb_libpgquery::PGTargetEntry *target_result = NULL;

			foreach(tl, *tlist)
			{
				duckdb_libpgquery::PGTargetEntry *tle = (duckdb_libpgquery::PGTargetEntry *) lfirst(tl);

				if (!tle->resjunk &&
					strcmp(tle->resname, name) == 0)
				{
					if (target_result != NULL)
					{
						if (!equal(target_result->expr, tle->expr))
							ereport(ERROR,
									(errcode(ERRCODE_AMBIGUOUS_COLUMN),

							/*------
							  translator: first %s is name of a SQL construct, eg ORDER BY */
									 errmsg("%s \"%s\" is ambiguous",
											ParseExprKindName(exprKind),
											name),
									 parser_errposition(pstate, location)));
					}
					else
						target_result = tle;
					/* Stay in loop to check for ambiguity */
				}
			}
			if (target_result != NULL)
			{
				/* return the first match, after suitable validation */
				checkTargetlistEntrySQL92(pstate, target_result, exprKind);
				return target_result;
			}
		}
	}
	if (IsA(node, duckdb_libpgquery::PGAConst))
	{
		duckdb_libpgquery::PGValue	   *val = &((duckdb_libpgquery::PGAConst *) node)->val;
		int			location = ((duckdb_libpgquery::PGAConst *) node)->location;
		int			targetlist_pos = 0;
		int			target_pos;

		if (!IsA(val, Integer))
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
			/* translator: %s is name of a SQL construct, eg ORDER BY */
					 errmsg("non-integer constant in %s",
							ParseExprKindName(exprKind)),
					 parser_errposition(pstate, location)));

		target_pos = intVal(val);
		foreach(tl, *tlist)
		{
			duckdb_libpgquery::PGTargetEntry *tle = (duckdb_libpgquery::PGTargetEntry *) lfirst(tl);

			if (!tle->resjunk)
			{
				if (++targetlist_pos == target_pos)
				{
					/* return the unique match, after suitable validation */
					checkTargetlistEntrySQL92(pstate, tle, exprKind);
					return tle;
				}
			}
		}
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
		/* translator: %s is name of a SQL construct, eg ORDER BY */
				 errmsg("%s position %d is not in select list",
						ParseExprKindName(exprKind), target_pos),
				 parser_errposition(pstate, location)));
	}

	/*
	 * Otherwise, we have an expression, so process it per SQL99 rules.
	 */
	return findTargetlistEntrySQL99(pstate, node, tlist, exprKind);
};

void
ClauseParser::checkTargetlistEntrySQL92(PGParseState *pstate, duckdb_libpgquery::PGTargetEntry *tle,
						  PGParseExprKind exprKind)
{
	switch (exprKind)
	{
		case EXPR_KIND_GROUP_BY:
			/* reject aggregates and window functions */
			if (pstate->p_hasAggs &&
				contain_aggs_of_level((duckdb_libpgquery::PGNode *) tle->expr, 0))
				ereport(ERROR,
						(errcode(ERRCODE_GROUPING_ERROR),
				/* translator: %s is name of a SQL construct, eg GROUP BY */
						 errmsg("aggregate functions are not allowed in %s",
								ParseExprKindName(exprKind)),
						 parser_errposition(pstate,
							   locate_agg_of_level((duckdb_libpgquery::PGNode *) tle->expr, 0))));
			if (pstate->p_hasWindowFuncs &&
				contain_windowfuncs((duckdb_libpgquery::PGNode *) tle->expr))
				ereport(ERROR,
						(errcode(ERRCODE_WINDOWING_ERROR),
				/* translator: %s is name of a SQL construct, eg GROUP BY */
						 errmsg("window functions are not allowed in %s",
								ParseExprKindName(exprKind)),
						 parser_errposition(pstate,
									locate_windowfunc((duckdb_libpgquery::PGNode *) tle->expr))));
			break;
		case EXPR_KIND_ORDER_BY:
			/* no extra checks needed */
			break;
		case EXPR_KIND_DISTINCT_ON:
			/* no extra checks needed */
			break;
		default:
			elog(ERROR, "unexpected exprKind in checkTargetlistEntrySQL92");
			break;
	}
};

duckdb_libpgquery::PGList *
ClauseParser::addTargetToSortList(PGParseState *pstate, duckdb_libpgquery::PGTargetEntry *tle,
					duckdb_libpgquery::PGList *sortlist, duckdb_libpgquery::PGList *targetlist, duckdb_libpgquery::PGSortBy *sortby,
					bool resolveUnknown)
{
	Oid			restype = duckdb_libpgquery::exprType((duckdb_libpgquery::PGNode *) tle->expr);
	Oid			sortop;
	Oid			eqop;
	bool		hashable;
	bool		reverse;
	int			location;
	//ParseCallbackState pcbstate;

	/* if tlist item is an UNKNOWN literal, change it to TEXT */
	if (restype == UNKNOWNOID && resolveUnknown)
	{
		tle->expr = (duckdb_libpgquery::PGExpr *) coerce_type(pstate, (duckdb_libpgquery::PGNode *) tle->expr,
										 restype, TEXTOID, -1,
										 duckdb_libpgquery::PG_COERCION_IMPLICIT,
										 duckdb_libpgquery::PG_COERCE_IMPLICIT_CAST,
										 exprLocation((duckdb_libpgquery::PGNode *) tle->expr));
		restype = TEXTOID;
	}

	/*
	 * Rather than clutter the API of get_sort_group_operators and the other
	 * functions we're about to use, make use of error context callback to
	 * mark any error reports with a parse position.  We point to the operator
	 * location if present, else to the expression being sorted.  (NB: use the
	 * original untransformed expression here; the TLE entry might well point
	 * at a duplicate expression in the regular SELECT list.)
	 */
	location = sortby->location;
	if (location < 0)
		location = exprLocation(sortby->node);
	//setup_parser_errposition_callback(&pcbstate, pstate, location);

	/* determine the sortop, eqop, and directionality */
	switch (sortby->sortby_dir)
	{
		case duckdb_libpgquery::PG_SORTBY_DEFAULT:
		case duckdb_libpgquery::PG_SORTBY_ASC:
			get_sort_group_operators(restype,
									 true, true, false,
									 &sortop, &eqop, NULL,
									 &hashable);
			reverse = false;
			break;
		case duckdb_libpgquery::PG_SORTBY_DESC:
			get_sort_group_operators(restype,
									 false, true, true,
									 NULL, &eqop, &sortop,
									 &hashable);
			reverse = true;
			break;
		case duckdb_libpgquery::PG_SORTBY_USING:
			Assert(sortby->useOp != NIL);
			sortop = compatible_oper_opid(sortby->useOp,
										  restype,
										  restype,
										  false);

			/*
			 * Verify it's a valid ordering operator, fetch the corresponding
			 * equality operator, and determine whether to consider it like
			 * ASC or DESC.
			 */
			eqop = get_equality_op_for_ordering_op(sortop, &reverse);
			if (!OidIsValid(eqop))
				ereport(ERROR,
						(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					   errmsg("operator %s is not a valid ordering operator",
							  strVal(llast(sortby->useOp))),
						 errhint("Ordering operators must be \"<\" or \">\" members of btree operator families.")));

			/*
			 * Also see if the equality operator is hashable.
			 */
			hashable = op_hashjoinable(eqop, restype);
			break;
		default:
			elog(ERROR, "unrecognized sortby_dir: %d", sortby->sortby_dir);
			sortop = InvalidOid;	/* keep compiler quiet */
			eqop = InvalidOid;
			hashable = false;
			reverse = false;
			break;
	}

	//cancel_parser_errposition_callback(&pcbstate);

	/* avoid making duplicate sortlist entries */
	if (!targetIsInSortList(tle, sortop, sortlist))
	{
		duckdb_libpgquery::PGSortGroupClause *sortcl = makeNode(duckdb_libpgquery::PGSortGroupClause);

		sortcl->tleSortGroupRef = assignSortGroupRef(tle, targetlist);

		sortcl->eqop = eqop;
		sortcl->sortop = sortop;
		sortcl->hashable = hashable;

		switch (sortby->sortby_nulls)
		{
			case duckdb_libpgquery::PG_SORTBY_NULLS_DEFAULT:
				/* NULLS FIRST is default for DESC; other way for ASC */
				sortcl->nulls_first = reverse;
				break;
			case duckdb_libpgquery::PG_SORTBY_NULLS_FIRST:
				sortcl->nulls_first = true;
				break;
			case duckdb_libpgquery::PG_SORTBY_NULLS_LAST:
				sortcl->nulls_first = false;
				break;
			default:
				elog(ERROR, "unrecognized sortby_nulls: %d",
					 sortby->sortby_nulls);
				break;
		}

		sortlist = lappend(sortlist, sortcl);
	}

	return sortlist;
};

bool
ClauseParser::targetIsInSortList(duckdb_libpgquery::PGTargetEntry *tle, Oid sortop, duckdb_libpgquery::PGList *sortgroupList)
{
	Index		ref = tle->ressortgroupref;
	duckdb_libpgquery::PGListCell   *l;

	/* no need to scan list if tle has no marker */
	if (ref == 0)
		return false;

	foreach(l, sortgroupList)
	{
		duckdb_libpgquery::PGNode *node = (duckdb_libpgquery::PGNode *) lfirst(l);

		/* Skip the empty grouping set */
		if (node == NULL)
			continue;

		if (IsA(node, duckdb_libpgquery::PGSortGroupClause))
		{
			duckdb_libpgquery::PGSortGroupClause *scl = (duckdb_libpgquery::PGSortGroupClause *) node;

			if (scl->tleSortGroupRef == ref &&
				(sortop == InvalidOid ||
				 sortop == scl->sortop ||
				 sortop == get_commutator(scl->sortop)))
				return true;
		}
	}
	return false;
};

Index
ClauseParser::assignSortGroupRef(duckdb_libpgquery::PGTargetEntry *tle, duckdb_libpgquery::PGList *tlist)
{
	Index		maxRef;
	duckdb_libpgquery::PGListCell   *l;

	if (tle->ressortgroupref)	/* already has one? */
		return tle->ressortgroupref;

	/* easiest way to pick an unused refnumber: max used + 1 */
	maxRef = 0;
	foreach(l, tlist)
	{
		Index		ref = ((duckdb_libpgquery::PGTargetEntry *) lfirst(l))->ressortgroupref;

		if (ref > maxRef)
			maxRef = ref;
	}
	tle->ressortgroupref = maxRef + 1;
	return tle->ressortgroupref;
};

duckdb_libpgquery::PGList *
ClauseParser::transformGroupClause(PGParseState *pstate, duckdb_libpgquery::PGList *grouplist,
					 duckdb_libpgquery::PGList **targetlist, duckdb_libpgquery::PGList *sortClause,
					 PGParseExprKind exprKind, bool useSQL99)
{
	duckdb_libpgquery::PGList	   *result = NIL;
	duckdb_libpgquery::PGList	   *tle_list = NIL;
	duckdb_libpgquery::PGListCell   *l;
	duckdb_libpgquery::PGList       *reorder_grouplist = NIL;

	/* Preprocess the grouping clause, lookup TLEs */
	foreach(l, grouplist)
	{
		duckdb_libpgquery::PGList        *tl;
		duckdb_libpgquery::PGListCell    *tl_cell;
		duckdb_libpgquery::PGTargetEntry *tle;
		Oid			restype;
		duckdb_libpgquery::PGNode        *node;

		node = (duckdb_libpgquery::PGNode*)lfirst(l);
		tl = findListTargetlistEntries(pstate, node, targetlist, false, false, 
                                       exprKind, useSQL99);

		foreach(tl_cell, tl)
		{
			tle = (duckdb_libpgquery::PGTargetEntry*)lfirst(tl_cell);

			/* if tlist item is an UNKNOWN literal, change it to TEXT */
			restype = exprType((duckdb_libpgquery::PGNode *) tle->expr);

			if (restype == UNKNOWNOID)
				tle->expr = (duckdb_libpgquery::PGExpr *) coerce_type(pstate, (duckdb_libpgquery::PGNode *) tle->expr,
												 restype, TEXTOID, -1,
												 duckdb_libpgquery::PG_COERCION_IMPLICIT,
												 duckdb_libpgquery::PG_COERCE_IMPLICIT_CAST,
												 -1);

			/*
			 * The tle_list will be used to match with the ORDER by element below.
			 * We only append the tle to tle_list when node is not a
			 * GroupingClause or tle->expr is not a RowExpr.
			 */
			 if (node != NULL &&
				 !IsA(node, duckdb_libpgquery::PGGroupingClause) &&
				 !IsA(tle->expr, duckdb_libpgquery::PGRowExpr))
				 tle_list = lappend(tle_list, tle);
		}
	}

	/* create first group clauses based on sort clauses */
	duckdb_libpgquery::PGList *tle_list_remainder = NIL;
	result = create_group_clause(tle_list,
								*targetlist,
								sortClause,
								&tle_list_remainder);

	/*
	 * Now add all remaining elements of the GROUP BY list to the result list.
	 * The result list is a list of GroupClauses and/or GroupingClauses.
	 * In each grouping set, all GroupClauses will appear in front of
	 * GroupingClauses. See the following GROUP BY clause:
	 *
	 *   GROUP BY ROLLUP(b,c),a, ROLLUP(e,d)
	 *
	 * the result list can be roughly represented as follows.
	 *
	 *    GroupClause(a),
	 *    GroupingClause(ROLLUP,groupsets(GroupClause(b),GroupClause(c))),
	 *    GroupingClause(CUBE,groupsets(GroupClause(e),GroupClause(d)))
	 *
	 *   XXX: the above transformation doesn't make sense -gs
	 */
	reorder_grouplist = reorderGroupList(grouplist);

	foreach(l, reorder_grouplist)
	{
		duckdb_libpgquery::PGNode *node = (duckdb_libpgquery::PGNode*) lfirst(l);

		if (node == NULL) /* the empty grouping set */
			result = list_concat(result, list_make1(NIL));

		// else if (IsA(node, duckdb_libpgquery::PGGroupingClause))
		// {
		// 	GroupingClause *tmp = make_grouping_clause(pstate,
		// 											   (GroupingClause*)node,
		// 											   *targetlist);
		// 	result = lappend(result, tmp);
		// }

		else if (IsA(node, duckdb_libpgquery::PGRowExpr))
		{
			/* The top level RowExprs are handled differently with other expressions.
			 * We convert each argument into GroupClause and append them
			 * one by one into 'result' list.
			 *
			 * Note that RowExprs are not added into the final targetlist.
			 */
			result =
				transformRowExprToGroupClauses(pstate, (duckdb_libpgquery::PGRowExpr *)node,
											   result, *targetlist);
		}

		else
		{
			duckdb_libpgquery::PGTargetEntry *tle;
			duckdb_libpgquery::PGSortGroupClause *gc;
			Oid			sortop;
			Oid			eqop;
			bool		hashable;

			if (useSQL99)
				tle = findTargetlistEntrySQL99(pstate, node,
											   targetlist, exprKind);
			else
				tle = findTargetlistEntrySQL92(pstate, node,
											   targetlist, exprKind);

			/*
			 * Avoid making duplicate grouplist entries.  Note that we don't
			 * enforce a particular sortop here.  Along with the copying of sort
			 * information above, this means that if you write something like
			 * "GROUP BY foo ORDER BY foo USING <<<", the GROUP BY operation
			 * silently takes on the equality semantics implied by the ORDER BY.
			 */
			if (targetIsInSortList(tle, InvalidOid, result))
				continue;

			get_sort_group_operators(exprType((duckdb_libpgquery::PGNode *) tle->expr),
									 true, true, false,
									 &sortop, &eqop, NULL, &hashable);
			gc = make_group_clause(tle, *targetlist, eqop, sortop, false, hashable);
			result = lappend(result, gc);
		}
	}

	/* We're doing extended grouping for both ordinary grouping and grouping
	 * extensions.
	 */
	{
		duckdb_libpgquery::PGListCell *lc;

		/*
		 * Find all unique target entries appeared in reorder_grouplist.
		 * We stash them in the ParseState, to be processed later by
		 * processExtendedGrouping().
		 */
		foreach (lc, reorder_grouplist)
		{
			pstate->p_grp_tles = list_concat_unique(
				pstate->p_grp_tles,
				findListTargetlistEntries(pstate, lfirst(lc),
										  targetlist, false, true,
										  exprKind, useSQL99));
		}
	}

	list_free(tle_list);
	list_free(tle_list_remainder);
	freeGroupList(reorder_grouplist);

	return result;
};

duckdb_libpgquery::PGList *
ClauseParser::findListTargetlistEntries(PGParseState *pstate, duckdb_libpgquery::PGNode *node,
									   duckdb_libpgquery::PGList **tlist, bool in_grpext,
									   bool ignore_in_grpext,
									   PGParseExprKind exprKind,
                                       bool useSQL99)
{
	duckdb_libpgquery::PGList *result_list = NIL;

	/*
	 * In GROUP BY clauses, empty grouping set () is supported as 'NIL'
	 * in the list. If this is the case, we simply skip it.
	 */
	if (node == NULL)
		return result_list;

	// if (IsA(node, GroupingClause))
	// {
	// 	duckdb_libpgquery::PGListCell *gl;
	// 	GroupingClause *gc = (GroupingClause*)node;

	// 	foreach(gl, gc->groupsets)
	// 	{
	// 		duckdb_libpgquery::PGList *subresult_list;

	// 		subresult_list = findListTargetlistEntries(pstate, lfirst(gl),
	// 												   tlist, true, 
    //                                                    ignore_in_grpext,
	// 												   exprKind,
    //                                                    useSQL99);

	// 		result_list = list_concat(result_list, subresult_list);
	// 	}
	// }

	/*
	 * In GROUP BY clause, we handle RowExpr specially here. When
	 * RowExprs appears immediately inside a grouping extension, we do
	 * not want to append the target entries for their arguments into
	 * result_list. This is because we do not want the order of
	 * these target entries in the result list from transformGroupClause()
	 * to be affected by ORDER BY.
	 *
	 * However, if ignore_in_grpext is set, we will always append
	 * these target enties.
	 */
	if (IsA(node, duckdb_libpgquery::PGRowExpr))
	{
		duckdb_libpgquery::PGList *args = ((duckdb_libpgquery::PGRowExpr *)node)->args;
		duckdb_libpgquery::PGListCell *lc;

		foreach (lc, args)
		{
			duckdb_libpgquery::PGNode *rowexpr_arg = lfirst(lc);
			duckdb_libpgquery::PGTargetEntry *tle;

            if (useSQL99)
                tle = findTargetlistEntrySQL99(pstate, rowexpr_arg, tlist,
											   exprKind);
            else
                tle = findTargetlistEntrySQL92(pstate, rowexpr_arg, tlist,
                                               exprKind);

			/* If RowExpr does not appear immediately inside a GROUPING SETS,
			 * we append its targetlit to the given targetlist.
			 */
			if (ignore_in_grpext || !in_grpext)
				result_list = lappend(result_list, tle);
		}
	}

	else
	{
		duckdb_libpgquery::PGTargetEntry *tle;

        if (useSQL99)
            tle = findTargetlistEntrySQL99(pstate, node, tlist, exprKind);
        else
            tle = findTargetlistEntrySQL92(pstate, node, tlist, exprKind);

		result_list = lappend(result_list, tle);
	}

	return result_list;
};

duckdb_libpgquery::PGList *
ClauseParser::create_group_clause(duckdb_libpgquery::PGList *tlist_group, duckdb_libpgquery::PGList *targetlist,
					duckdb_libpgquery::PGList *sortClause, duckdb_libpgquery::PGList **tlist_remainder)
{
	duckdb_libpgquery::PGList	   *result = NIL;
	duckdb_libpgquery::PGListCell   *l;
	duckdb_libpgquery::PGList *tlist = list_copy(tlist_group);

	/*
	 * Iterate through the ORDER BY clause. If we find a grouping element
	 * that matches the ORDER BY element, append the grouping element to the
	 * result set immediately. Otherwise, stop iterating. The effect of this
	 * is to look for a prefix of the ORDER BY list in the grouping clauses,
	 * and to move that prefix to the front of the GROUP BY.
	 */
	foreach(l, sortClause)
	{
		duckdb_libpgquery::PGSortGroupClause *sc = (duckdb_libpgquery::PGSortGroupClause *) lfirst(l);
		duckdb_libpgquery::PGListCell   *prev = NULL;
		duckdb_libpgquery::PGListCell   *tl = NULL;
		bool		found = false;

		foreach(tl, tlist)
		{
			duckdb_libpgquery::PGNode        *node = (duckdb_libpgquery::PGNode*)lfirst(tl);

			if (IsA(node, duckdb_libpgquery::PGTargetEntry))
			{
				duckdb_libpgquery::PGTargetEntry *tle = (duckdb_libpgquery::PGTargetEntry *) lfirst(tl);

				if (!tle->resjunk &&
					sc->tleSortGroupRef == tle->ressortgroupref)
				{
					duckdb_libpgquery::PGSortGroupClause *gc;

					tlist = list_delete_cell(tlist, tl, prev);

					/* Use the sort clause's sorting information */
					gc = make_group_clause(tle, targetlist,
										   sc->eqop, sc->sortop, sc->nulls_first, sc->hashable);
					result = lappend(result, gc);
					found = true;
					break;
				}

				prev = tl;
			}
		}

		/* As soon as we've failed to match an ORDER BY element, stop */
		if (!found)
			break;
	}

	/* Save remaining GROUP-BY tle's */
	*tlist_remainder = tlist;

	return result;
};

duckdb_libpgquery::PGSortGroupClause *
ClauseParser::make_group_clause(duckdb_libpgquery::PGTargetEntry *tle, duckdb_libpgquery::PGList *targetlist,
				  Oid eqop, Oid sortop, bool nulls_first, bool hashable)
{
	duckdb_libpgquery::PGSortGroupClause *result;

	result = makeNode(duckdb_libpgquery::PGSortGroupClause);
	result->tleSortGroupRef = assignSortGroupRef(tle, targetlist);
	result->eqop = eqop;
	result->sortop = sortop;
	result->nulls_first = nulls_first;
	result->hashable = hashable;

	return result;
};

duckdb_libpgquery::PGList *
ClauseParser::reorderGroupList(duckdb_libpgquery::PGList *grouplist)
{
	duckdb_libpgquery::PGList *result = NIL;
	duckdb_libpgquery::PGListCell *gl;
	duckdb_libpgquery::PGList *sub_list = NIL;

	foreach(gl, grouplist)
	{
		duckdb_libpgquery::PGNode *node = (duckdb_libpgquery::PGNode*)lfirst(gl);

		if (node == NULL)
		{
			/* Append an empty set. */
			result = list_concat(result, list_make1(NIL));
		}

		// else if (IsA(node, GroupingClause))
		// {
		// 	GroupingClause *gc = (GroupingClause *)node;
		// 	GroupingClause *new_gc = makeNode(GroupingClause);

		// 	new_gc->groupType = gc->groupType;
		// 	new_gc->groupsets = reorderGroupList(gc->groupsets);

		// 	sub_list = lappend(sub_list, new_gc);
		// }
		else
		{
			duckdb_libpgquery::PGNode *new_node = (duckdb_libpgquery::PGNode *)copyObject(node);
			result = lappend(result, new_node);
		}
	}

	result = list_concat(result, sub_list);
	return result;
};

duckdb_libpgquery::PGList *
ClauseParser::transformRowExprToGroupClauses(PGParseState *pstate, duckdb_libpgquery::PGRowExpr *rowexpr,
							   duckdb_libpgquery::PGList *groupsets, duckdb_libpgquery::PGList *targetList)
{
	duckdb_libpgquery::PGList *args = rowexpr->args;
	duckdb_libpgquery::PGListCell *arglc;

	foreach (arglc, args)
	{
		duckdb_libpgquery::PGNode *node = lfirst(arglc);

		if (IsA(node, duckdb_libpgquery::PGRowExpr))
		{
			transformRowExprToGroupClauses(pstate, (duckdb_libpgquery::PGRowExpr *)node,
										   groupsets, targetList);
		}

		else
		{
			/* Find the TargetEntry for this argument. This should have been
			 * generated in findListTargetlistEntries().
			 */
			duckdb_libpgquery::PGTargetEntry *arg_tle =
				findTargetlistEntrySQL92(pstate, node, &targetList,
										 EXPR_KIND_GROUP_BY);
			Oid			sortop;
			Oid			eqop;
			bool		hashable;

			get_sort_group_operators(exprType((duckdb_libpgquery::PGNode *) arg_tle->expr),
									 true, true, false,
									 &sortop, &eqop, NULL, &hashable);

			/* avoid making duplicate expression entries */
			if (targetIsInSortList(arg_tle, sortop, groupsets))
				continue;

			groupsets = lappend(groupsets,
								make_group_clause(arg_tle, targetList, eqop, sortop, false, hashable));
		}
	}

	return groupsets;
};

duckdb_libpgquery::PGList *
ClauseParser::transformScatterClause(PGParseState *pstate,
					   duckdb_libpgquery::PGList *scatterlist,
					   duckdb_libpgquery::PGList **targetlist)
{
	duckdb_libpgquery::PGList	   *outlist = NIL;
	duckdb_libpgquery::PGListCell   *olitem;

	/* Special case handling for SCATTER RANDOMLY */
	if (list_length(scatterlist) == 1 && linitial(scatterlist) == NULL)
		return list_make1(NULL);
	
	/* preprocess the scatter clause, lookup TLEs */
	foreach(olitem, scatterlist)
	{
		duckdb_libpgquery::PGNode			*node = lfirst(olitem);
		duckdb_libpgquery::PGTargetEntry		*tle;

		tle = findTargetlistEntrySQL99(pstate, node, targetlist,
									   EXPR_KIND_SCATTER_BY);

		/* coerce unknown to text */
		if (duckdb_libpgquery::exprType((duckdb_libpgquery::PGNode *) tle->expr) == UNKNOWNOID)
		{
			tle->expr = (duckdb_libpgquery::PGExpr *) coerce_type(pstate, (duckdb_libpgquery::PGNode *) tle->expr,
											 UNKNOWNOID, TEXTOID, -1,
											 duckdb_libpgquery::PG_COERCION_IMPLICIT,
											 duckdb_libpgquery::PG_COERCE_IMPLICIT_CAST,
											 -1);
		}

		outlist = lappend(outlist, tle->expr);
	}
	return outlist;
};

duckdb_libpgquery::PGList *
ClauseParser::transformDistinctToGroupBy(PGParseState *pstate, duckdb_libpgquery::PGList **targetlist,
							duckdb_libpgquery::PGList **sortClause, duckdb_libpgquery::PGList **groupClause)
{
	duckdb_libpgquery::PGList *group_tlist = list_copy(*targetlist);

	/*
	 * create first group clauses based on matching sort clauses, if any
	 */
	duckdb_libpgquery::PGList *group_tlist_remainder = NIL;
	duckdb_libpgquery::PGList *group_clause_list = create_group_clause(group_tlist,
												*targetlist,
												*sortClause,
												&group_tlist_remainder);

	if (list_length(group_tlist_remainder) > 0)
	{
		/*
		 * append remaining group clauses to the end of group clause list
		 */
		duckdb_libpgquery::PGListCell *lc = NULL;

		foreach(lc, group_tlist_remainder)
		{
			duckdb_libpgquery::PGTargetEntry *tle = (duckdb_libpgquery::PGTargetEntry *) lfirst(lc);
			if (!tle->resjunk)
			{
				duckdb_libpgquery::PGSortBy sortby;

				sortby.type = duckdb_libpgquery::T_PGSortBy;
				sortby.sortby_dir = duckdb_libpgquery::PG_SORTBY_DEFAULT;
				sortby.sortby_nulls = duckdb_libpgquery::PG_SORTBY_NULLS_DEFAULT;
				sortby.useOp = NIL;
				sortby.location = -1;
				sortby.node = (duckdb_libpgquery::PGNode *) tle->expr;
				group_clause_list = addTargetToSortList(pstate, tle,
														group_clause_list, *targetlist,
														&sortby, true);
			}
		}
	}

	*groupClause = group_clause_list;

	list_free(group_tlist);
	list_free(group_tlist_remainder);

	/*
	 * return empty distinct list, since we have created a grouping clause to do the job
	 */
	return NIL;
};

duckdb_libpgquery::PGList *
ClauseParser::transformDistinctClause(PGParseState *pstate,
						duckdb_libpgquery::PGList **targetlist, duckdb_libpgquery::PGList *sortClause, bool is_agg)
{
	duckdb_libpgquery::PGList	   *result = NIL;
	duckdb_libpgquery::PGListCell   *slitem;
	duckdb_libpgquery::PGListCell   *tlitem;

	/*
	 * The distinctClause should consist of all ORDER BY items followed by all
	 * other non-resjunk targetlist items.  There must not be any resjunk
	 * ORDER BY items --- that would imply that we are sorting by a value that
	 * isn't necessarily unique within a DISTINCT group, so the results
	 * wouldn't be well-defined.  This construction ensures we follow the rule
	 * that sortClause and distinctClause match; in fact the sortClause will
	 * always be a prefix of distinctClause.
	 *
	 * Note a corner case: the same TLE could be in the ORDER BY list multiple
	 * times with different sortops.  We have to include it in the
	 * distinctClause the same way to preserve the prefix property. The net
	 * effect will be that the TLE value will be made unique according to both
	 * sortops.
	 */
	foreach(slitem, sortClause)
	{
		duckdb_libpgquery::PGSortGroupClause *scl = (duckdb_libpgquery::PGSortGroupClause *) lfirst(slitem);
		duckdb_libpgquery::PGTargetEntry *tle = get_sortgroupclause_tle(scl, *targetlist);

		if (tle->resjunk)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
					 is_agg ?
					 errmsg("in an aggregate with DISTINCT, ORDER BY expressions must appear in argument list") :
					 errmsg("for SELECT DISTINCT, ORDER BY expressions must appear in select list"),
					 parser_errposition(pstate,
										exprLocation((duckdb_libpgquery::PGNode *) tle->expr))));
		result = lappend(result, copyObject(scl));
	}

	/*
	 * Now add any remaining non-resjunk tlist items, using default sort/group
	 * semantics for their data types.
	 */
	foreach(tlitem, *targetlist)
	{
		duckdb_libpgquery::PGTargetEntry *tle = (duckdb_libpgquery::PGTargetEntry *) lfirst(tlitem);

		if (tle->resjunk)
			continue;			/* ignore junk */
		result = addTargetToGroupList(pstate, tle,
									  result, *targetlist,
									  exprLocation((duckdb_libpgquery::PGNode *) tle->expr),
									  true);
	}

	/*
	 * Complain if we found nothing to make DISTINCT.  Returning an empty list
	 * would cause the parsed Query to look like it didn't have DISTINCT, with
	 * results that would probably surprise the user.  Note: this case is
	 * presently impossible for aggregates because of grammar restrictions,
	 * but we check anyway.
	 */
	if (result == NIL)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 is_agg ?
		errmsg("an aggregate with DISTINCT must have at least one argument") :
				 errmsg("SELECT DISTINCT must have at least one column")));

	return result;
};

duckdb_libpgquery::PGList *
ClauseParser::addTargetToGroupList(PGParseState *pstate, duckdb_libpgquery::PGTargetEntry *tle,
					 duckdb_libpgquery::PGList *grouplist, duckdb_libpgquery::PGList *targetlist, int location,
					 bool resolveUnknown)
{
	Oid			restype = duckdb_libpgquery::exprType((duckdb_libpgquery::PGNode *) tle->expr);

	/* if tlist item is an UNKNOWN literal, change it to TEXT */
	if (restype == UNKNOWNOID && resolveUnknown)
	{
		tle->expr = (duckdb_libpgquery::PGExpr *) coerce_parser.coerce_type(pstate, (duckdb_libpgquery::PGNode *) tle->expr,
										 restype, TEXTOID, -1,
										 duckdb_libpgquery::PG_COERCION_IMPLICIT,
										 duckdb_libpgquery::PG_COERCE_IMPLICIT_CAST,
										 -1);
		restype = TEXTOID;
	}

	/* avoid making duplicate grouplist entries */
	if (!targetIsInSortList(tle, InvalidOid, grouplist))
	{
		duckdb_libpgquery::PGSortGroupClause *grpcl = makeNode(duckdb_libpgquery::PGSortGroupClause);
		Oid			sortop;
		Oid			eqop;
		bool		hashable;
		//ParseCallbackState pcbstate;

		//setup_parser_errposition_callback(&pcbstate, pstate, location);

		/* determine the eqop and optional sortop */
		get_sort_group_operators(restype,
								 false, true, false,
								 &sortop, &eqop, NULL,
								 &hashable);

		//cancel_parser_errposition_callback(&pcbstate);

		grpcl->tleSortGroupRef = assignSortGroupRef(tle, targetlist);
		grpcl->eqop = eqop;
		grpcl->sortop = sortop;
		grpcl->nulls_first = false;		/* OK with or without sortop */
		grpcl->hashable = hashable;

		grouplist = lappend(grouplist, grpcl);
	}

	return grouplist;
};

duckdb_libpgquery::PGList *
ClauseParser::transformDistinctOnClause(PGParseState *pstate, duckdb_libpgquery::PGList *distinctlist,
						  duckdb_libpgquery::PGList **targetlist, duckdb_libpgquery::PGList *sortClause)
{
	duckdb_libpgquery::PGList	   *result = NIL;
	duckdb_libpgquery::PGList	   *sortgrouprefs = NIL;
	bool		skipped_sortitem;
	duckdb_libpgquery::PGListCell   *lc;
	duckdb_libpgquery::PGListCell   *lc2;

	/*
	 * Add all the DISTINCT ON expressions to the tlist (if not already
	 * present, they are added as resjunk items).  Assign sortgroupref numbers
	 * to them, and make a list of these numbers.  (NB: we rely below on the
	 * sortgrouprefs list being one-for-one with the original distinctlist.
	 * Also notice that we could have duplicate DISTINCT ON expressions and
	 * hence duplicate entries in sortgrouprefs.)
	 */
	foreach(lc, distinctlist)
	{
		duckdb_libpgquery::PGNode	   *dexpr = (duckdb_libpgquery::PGNode *) lfirst(lc);
		int			sortgroupref;
		duckdb_libpgquery::PGTargetEntry *tle;

		tle = findTargetlistEntrySQL92(pstate, dexpr, targetlist,
									   EXPR_KIND_DISTINCT_ON);
		sortgroupref = assignSortGroupRef(tle, *targetlist);
		sortgrouprefs = lappend_int(sortgrouprefs, sortgroupref);
	}

	/*
	 * If the user writes both DISTINCT ON and ORDER BY, adopt the sorting
	 * semantics from ORDER BY items that match DISTINCT ON items, and also
	 * adopt their column sort order.  We insist that the distinctClause and
	 * sortClause match, so throw error if we find the need to add any more
	 * distinctClause items after we've skipped an ORDER BY item that wasn't
	 * in DISTINCT ON.
	 */
	skipped_sortitem = false;
	foreach(lc, sortClause)
	{
		duckdb_libpgquery::PGSortGroupClause *scl = (duckdb_libpgquery::PGSortGroupClause *) lfirst(lc);

		if (list_member_int(sortgrouprefs, scl->tleSortGroupRef))
		{
			if (skipped_sortitem)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
						 errmsg("SELECT DISTINCT ON expressions must match initial ORDER BY expressions"),
						 parser_errposition(pstate,
								  get_matching_location(scl->tleSortGroupRef,
														sortgrouprefs,
														distinctlist))));
			else
				result = lappend(result, copyObject(scl));
		}
		else
			skipped_sortitem = true;
	}

	/*
	 * Now add any remaining DISTINCT ON items, using default sort/group
	 * semantics for their data types.  (Note: this is pretty questionable; if
	 * the ORDER BY list doesn't include all the DISTINCT ON items and more
	 * besides, you certainly aren't using DISTINCT ON in the intended way,
	 * and you probably aren't going to get consistent results.  It might be
	 * better to throw an error or warning here.  But historically we've
	 * allowed it, so keep doing so.)
	 */
	forboth(lc, distinctlist, lc2, sortgrouprefs)
	{
		duckdb_libpgquery::PGNode	   *dexpr = (duckdb_libpgquery::PGNode *) lfirst(lc);
		int			sortgroupref = lfirst_int(lc2);
		duckdb_libpgquery::PGTargetEntry *tle = get_sortgroupref_tle(sortgroupref, *targetlist);

		if (targetIsInSortList(tle, InvalidOid, result))
			continue;			/* already in list (with some semantics) */
		if (skipped_sortitem)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
					 errmsg("SELECT DISTINCT ON expressions must match initial ORDER BY expressions"),
					 parser_errposition(pstate, exprLocation(dexpr))));
		result = addTargetToGroupList(pstate, tle,
									  result, *targetlist,
									  exprLocation(dexpr),
									  true);
	}

	/*
	 * An empty result list is impossible here because of grammar
	 * restrictions.
	 */
	Assert(result != NIL);

	return result;
};

duckdb_libpgquery::PGNode *
ClauseParser::transformLimitClause(PGParseState *pstate, duckdb_libpgquery::PGNode *clause,
					 PGParseExprKind exprKind, const char *constructName)
{
	duckdb_libpgquery::PGNode	   *qual;

	if (clause == NULL)
		return NULL;

	qual = expr_parser.transformExpr(pstate, clause, exprKind);

	qual = coerce_parser.coerce_to_specific_type(pstate, qual, INT8OID, constructName);

	/* LIMIT can't refer to any variables of the current query */
	checkExprIsVarFree(pstate, qual, constructName);

	return qual;
};

duckdb_libpgquery::PGList *
ClauseParser::transformWindowDefinitions(PGParseState *pstate,
						   duckdb_libpgquery::PGList *windowdefs,
						   duckdb_libpgquery::PGList **targetlist)
{
	duckdb_libpgquery::PGList	   *result = NIL;
	Index		winref = 0;
	duckdb_libpgquery::PGListCell   *lc;

	/*
	 * We have two lists of window specs: one in the ParseState -- put there
	 * when we find the OVER(...) clause in the targetlist and the other
	 * is windowClause, a list of named window clauses. So, we concatenate
	 * them together.
	 *
	 * Note that we're careful place those found in the target list at
	 * the end because the spec might refer to a named clause and we'll
	 * after to know about those first.
	 */
	foreach(lc, windowdefs)
	{
		duckdb_libpgquery::PGWindowDef  *windef = lfirst(lc);
		duckdb_libpgquery::PGWindowClause *refwc = NULL;
		duckdb_libpgquery::PGList	   *partitionClause;
		duckdb_libpgquery::PGList	   *orderClause;
		duckdb_libpgquery::PGWindowClause *wc;

		winref++;

		/*
		 * Check for duplicate window names.
		 */
		if (windef->name &&
			findWindowClause(result, windef->name) != NULL)
			ereport(ERROR,
					(errcode(ERRCODE_WINDOWING_ERROR),
					 errmsg("window \"%s\" is already defined", windef->name),
					 parser_errposition(pstate, windef->location)));

		/*
		 * If it references a previous window, look that up.
		 */
		if (windef->refname)
		{
			refwc = findWindowClause(result, windef->refname);
			if (refwc == NULL)
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_OBJECT),
						 errmsg("window \"%s\" does not exist",
								windef->refname),
						 parser_errposition(pstate, windef->location)));
		}

		/*
		 * Transform PARTITION and ORDER specs, if any.  These are treated
		 * almost exactly like top-level GROUP BY and ORDER BY clauses,
		 * including the special handling of nondefault operator semantics.
		 */
		orderClause = transformSortClause(pstate,
										  windef->orderClause,
										  targetlist,
										  EXPR_KIND_WINDOW_ORDER,
										  true /* fix unknowns */ ,
										  true /* force SQL99 rules */ );
		partitionClause = transformSortClause(pstate,
											  windef->partitionClause,
											  targetlist,
											  EXPR_KIND_WINDOW_PARTITION,
											  true /* fix unknowns */ ,
											  true /* force SQL99 rules */ );

		/*
		 * And prepare the new WindowClause.
		 */
		wc = makeNode(duckdb_libpgquery::PGWindowClause);
		wc->name = windef->name;
		wc->refname = windef->refname;

		/*
		 * Per spec, a windowdef that references a previous one copies the
		 * previous partition clause (and mustn't specify its own).  It can
		 * specify its own ordering clause, but only if the previous one had
		 * none.  It always specifies its own frame clause, and the previous
		 * one must not have a frame clause.  Yeah, it's bizarre that each of
		 * these cases works differently, but SQL:2008 says so; see 7.11
		 * <window clause> syntax rule 10 and general rule 1.  The frame
		 * clause rule is especially bizarre because it makes "OVER foo"
		 * different from "OVER (foo)", and requires the latter to throw an
		 * error if foo has a nondefault frame clause.  Well, ours not to
		 * reason why, but we do go out of our way to throw a useful error
		 * message for such cases.
		 */
		if (refwc)
		{
			if (partitionClause)
				ereport(ERROR,
						(errcode(ERRCODE_WINDOWING_ERROR),
				errmsg("cannot override PARTITION BY clause of window \"%s\"",
					   windef->refname),
						 parser_errposition(pstate, windef->location)));
			wc->partitionClause = copyObject(refwc->partitionClause);
		}
		else
			wc->partitionClause = partitionClause;
		if (refwc)
		{
			if (orderClause && refwc->orderClause)
				ereport(ERROR,
						(errcode(ERRCODE_WINDOWING_ERROR),
				   errmsg("cannot override ORDER BY clause of window \"%s\"",
						  windef->refname),
						 parser_errposition(pstate, windef->location)));
			if (orderClause)
			{
				wc->orderClause = orderClause;
				wc->copiedOrder = false;
			}
			else
			{
				wc->orderClause = copyObject(refwc->orderClause);
				wc->copiedOrder = true;
			}
		}
		else
		{
			wc->orderClause = orderClause;
			wc->copiedOrder = false;
		}
		if (refwc && refwc->frameOptions != FRAMEOPTION_DEFAULTS)
		{
			/*
			 * Use this message if this is a WINDOW clause, or if it's an OVER
			 * clause that includes ORDER BY or framing clauses.  (We already
			 * rejected PARTITION BY above, so no need to check that.)
			 */
			if (windef->name ||
				orderClause || windef->frameOptions != FRAMEOPTION_DEFAULTS)
				ereport(ERROR,
						(errcode(ERRCODE_WINDOWING_ERROR),
						 errmsg("cannot copy window \"%s\" because it has a frame clause",
								windef->refname),
						 parser_errposition(pstate, windef->location)));
			/* Else this clause is just OVER (foo), so say this: */
			ereport(ERROR,
					(errcode(ERRCODE_WINDOWING_ERROR),
					 errmsg("cannot copy window \"%s\" because it has a frame clause",
							windef->refname),
					 errhint("Omit the parentheses in this OVER clause."),
					 parser_errposition(pstate, windef->location)));
		}

		/*
		 * Finally, process the framing clause. parseProcessWindFunc() will
		 * have picked up window functions that do not support framing.
		 *
		 * What we do need to do is the following:
		 * - If BETWEEN has been specified, the trailing bound is not
		 *   UNBOUNDED FOLLOWING; the leading bound is not UNBOUNDED
		 *   PRECEDING; if the first bound specifies CURRENT ROW, the
		 *   second bound shall not specify a PRECEDING bound; if the
		 *   first bound specifies a FOLLOWING bound, the second bound
		 *   shall not specify a PRECEDING or CURRENT ROW bound.
		 *
		 * - If the user did not specify BETWEEN, the bound is assumed to be
		 *   a trailing bound and the leading bound is set to CURRENT ROW.
		 *   We're careful not to set is_between here because the user did not
		 *   specify it.
		 *
		 * - If RANGE is specified: the ORDER BY clause of the window spec
		 *   may specify only one column; the type of that column must support
		 *   +/- <integer> operations and must be merge-joinable.
		 */

		wc->frameOptions = windef->frameOptions;
		wc->winref = winref;
		/* Process frame offset expressions */
		wc->startOffset = transformFrameOffset(pstate, wc->frameOptions,
											   windef->startOffset, wc->orderClause,
											   *targetlist,
											   (windef->frameOptions & FRAMEOPTION_START_VALUE_FOLLOWING) != 0,
											   windef->location);
		wc->endOffset = transformFrameOffset(pstate, wc->frameOptions,
											 windef->endOffset, wc->orderClause,
											 *targetlist,
											 (windef->frameOptions & FRAMEOPTION_END_VALUE_FOLLOWING) != 0,
											 windef->location);

		/* finally, check function restriction with this spec. */
		winref_checkspec(pstate, *targetlist, winref,
						 PointerIsValid(wc->orderClause),
						 wc->frameOptions != FRAMEOPTION_DEFAULTS);

		result = lappend(result, wc);
	}

	/* If there are no window functions in the targetlist,
	 * forget the window clause.
	 */
	if (!pstate->p_hasWindowFuncs)
		pstate->p_windowdefs = NIL;

	return result;
};

duckdb_libpgquery::PGNode *
ClauseParser::transformFrameOffset(PGParseState *pstate, int frameOptions, duckdb_libpgquery::PGNode *clause,
					 duckdb_libpgquery::PGList *orderClause, duckdb_libpgquery::PGList *targetlist, bool isFollowing,
					 int location)
{
	const char *constructName = NULL;
	duckdb_libpgquery::PGNode	   *node;

	/* Quick exit if no offset expression */
	if (clause == NULL)
		return NULL;

	if (frameOptions & FRAMEOPTION_ROWS)
	{
		/* Transform the raw expression tree */
		node = expr_parser.transformExpr(pstate, clause, EXPR_KIND_WINDOW_FRAME_ROWS);

		/*
		 * Like LIMIT clause, simply coerce to int8
		 */
		constructName = "ROWS";
		node = coerce_parser.coerce_to_specific_type(pstate, node, INT8OID, constructName);
	}
	else if (frameOptions & FRAMEOPTION_RANGE)
	{
		duckdb_libpgquery::PGTargetEntry *te;
		Oid			otype;
		Oid			rtype;
		Oid			newrtype;
		duckdb_libpgquery::PGSortGroupClause *sort;
		Oid			oprresult;
		duckdb_libpgquery::PGList	   *oprname;
		Operator	tup;
		int32		typmod;

		/* Transform the raw expression tree */
		node = expr_parser.transformExpr(pstate, clause, EXPR_KIND_WINDOW_FRAME_RANGE);

		/*
		 * this needs a lot of thought to decide how to support in the context
		 * of Postgres' extensible datatype framework
		 */
		constructName = "RANGE";

		/* caller should've checked this already, but better safe than sorry */
		if (list_length(orderClause) == 0)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("window specifications with a framing clause must have an ORDER BY clause"),
					 parser_errposition(pstate, location)));
		if (list_length(orderClause) > 1)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("only one ORDER BY column may be specified when RANGE is used in a window specification"),
					 parser_errposition(pstate, location)));

		typmod = duckdb_libpgquery::exprTypmod(node);

		if (IsA(node, duckdb_libpgquery::PGConst))
		{
			duckdb_libpgquery::PGConst *con = (duckdb_libpgquery::PGConst *) node;

			if (con->constisnull)
				ereport(ERROR,
						(errcode(ERRCODE_WINDOWING_ERROR),
						 errmsg("RANGE parameter cannot be NULL"),
						 parser_errposition(pstate, con->location)));
		}

		sort = (duckdb_libpgquery::PGSortGroupClause *) linitial(orderClause);
		te = getTargetBySortGroupRef(sort->tleSortGroupRef,
									 targetlist);
		otype = duckdb_libpgquery::exprType((duckdb_libpgquery::PGNode *)te->expr);
		rtype = duckdb_libpgquery::exprType(node);

		/* XXX: Reverse these if user specified DESC */
		if (isFollowing)
			oprname = lappend(NIL, duckdb_libpgquery::makeString("+"));
		else
			oprname = lappend(NIL, duckdb_libpgquery::makeString("-"));

		tup = oper(pstate, oprname, otype, rtype, true, 0);

		if (!HeapTupleIsValid(tup))
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("window specification RANGE parameter type must be coercible to ORDER BY column type")));

		oprresult = ((Form_pg_operator)GETSTRUCT(tup))->oprresult;
		newrtype = ((Form_pg_operator)GETSTRUCT(tup))->oprright;
		ReleaseSysCache(tup);
		list_free_deep(oprname);

		if (rtype != newrtype)
		{
			/*
			 * We have to coerce the RHS to the new type so that we'll be
			 * able to trivially find an operator later on.
			 */

			/* XXX: we assume that the typmod for the new RHS type
			 * is the same as before... is that safe?
			 */
			duckdb_libpgquery::PGExpr *expr =
				(duckdb_libpgquery::PGExpr *)coerce_to_target_type(NULL,
											  node,
											  rtype,
											  newrtype, typmod,
											  duckdb_libpgquery::PG_COERCION_EXPLICIT,
											  duckdb_libpgquery::PG_COERCE_IMPLICIT_CAST,
											  -1);
			if (!PointerIsValid(expr))
			{
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("type mismatch between ORDER BY and RANGE "
								"parameter in window specification"),
						 errhint("Operations between window specification "
								 "the ORDER BY column and RANGE parameter "
								 "must result in a data type which can be "
								 "cast back to the ORDER BY column type"),
						 parser_errposition(pstate, exprLocation((duckdb_libpgquery::PGNode *) expr))));
			}

			node = (duckdb_libpgquery::PGNode *) expr;
		}

		if (oprresult != otype)
		{
			/*
			 * See if it can be coerced. The point of this is to just
			 * throw an error if the coercion is not possible. The
			 * actual coercion will be done later, in the executor.
			 *
			 * XXX: Why not do it here?
			 */
			if (!can_coerce_type(1, &oprresult, &otype, duckdb_libpgquery::PG_COERCION_EXPLICIT))
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("invalid RANGE parameter"),
						 errhint("Operations between window specification "
								 "the ORDER BY column and RANGE parameter "
								 "must result in a data type which can be "
								 "cast back to the ORDER BY column type")));
		}

		if (IsA(node, duckdb_libpgquery::PGConst))
		{
			/*
			 * see if RANGE parameter is negative
			 *
			 * Note: There's a similar check in nodeWindowAgg.c, for the
			 * case that the parameter is not a Const. Make sure it uses
			 * the same logic!
			 */
			duckdb_libpgquery::PGConst *con = (duckdb_libpgquery::PGConst *) node;
			Oid			sortop;

			get_sort_group_operators(newrtype,
									 false, false, false,
									 &sortop, NULL, NULL, NULL);

			if (OidIsValid(sortop))
			{
				Type typ = typeidType(newrtype);
				Oid funcoid = get_opcode(sortop);
				Datum zero;
				Datum result;

				zero = stringTypeDatum(typ, "0", exprTypmod(node));

				/*
				 * As we know the value is a const and since transformExpr()
				 * will have parsed the type into its internal format, we can
				 * just poke directly into the Const structure.
				 */
				result = OidFunctionCall2(funcoid, con->constvalue, zero);

				if (result)
					ereport(ERROR,
							(errcode(ERRCODE_WINDOWING_ERROR),
							 errmsg("RANGE parameter cannot be negative"),
							 parser_errposition(pstate, con->location)));

				ReleaseSysCache(typ);
			}
		}
	}
	else
	{
		Assert(false);
		node = NULL;
	}

	/* In GPDB, we allow this. */
#if 0
	/* Disallow variables in frame offsets */
	checkExprIsVarFree(pstate, node, constructName);
#endif

	return node;
};

}