#include <Interpreters/orcaopt/pgoptnew/ClauseParser.h>
//#include <Interpreters/orcaopt/pgoptnew/walkers.h>

using namespace duckdb_libpgquery;

namespace DB
{

ClauseParser::ClauseParser() : scalar_operator_provider(std::make_shared<ScalarOperatorProvider>())
{

};

PGRangeTblEntry *
ClauseParser::getRTEForSpecialRelationTypes(PGParseState *pstate, PGRangeVar *rv)
{
	PGCommonTableExpr *cte;
	Index		levelsup;
	PGRangeTblEntry *rte;

	/*
	 * if it is a qualified name, it can't be a CTE or tuplestore reference
	 */
	if (rv->schemaname)
		return NULL;

	cte = relation_parser.scanNameSpaceForCTE(pstate, rv->relname, &levelsup);
	if (cte)
		rte = relation_parser.addRangeTableEntryForCTE(pstate, cte, levelsup, rv, true);
	else if (relation_parser.scanNameSpaceForENR(pstate, rv->relname))
		rte = relation_parser.addRangeTableEntryForENR(pstate, rv, true);
	else
		rte = NULL;

	return rte;
};

PGRangeTblEntry *
ClauseParser::transformTableEntry(PGParseState *pstate, PGRangeVar *r)
{
	PGRangeTblEntry *rte;

	/* We need only build a range table entry */
	rte = relation_parser.addRangeTableEntry(pstate, r, r->alias, r->inh, true);

	return rte;
};

PGParseNamespaceItem *
ClauseParser::makeNamespaceItem(PGRangeTblEntry *rte, bool rel_visible, bool cols_visible,
				  bool lateral_only, bool lateral_ok)
{
	PGParseNamespaceItem *nsitem;

	nsitem = (PGParseNamespaceItem *) palloc(sizeof(PGParseNamespaceItem));
	nsitem->p_rte = rte;
	nsitem->p_rel_visible = rel_visible;
	nsitem->p_cols_visible = cols_visible;
	nsitem->p_lateral_only = lateral_only;
	nsitem->p_lateral_ok = lateral_ok;
	return nsitem;
};

PGRangeTblEntry *
ClauseParser::transformRangeSubselect(PGParseState *pstate, PGRangeSubselect *r)
{
	PGQuery	   *query;
	PGRangeTblEntry *rte;

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
							  relation_parser.getLockedRefname(pstate, r->alias->aliasname),
							  true);

	/* Restore state */
	pstate->p_lateral_active = false;
	pstate->p_expr_kind = EXPR_KIND_NONE;

	/*
	 * Check that we got a SELECT.  Anything else should be impossible given
	 * restrictions of the grammar, but check anyway.
	 */
	if (!IsA(query, PGQuery) ||
		query->commandType != PG_CMD_SELECT)
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
};

void
ClauseParser::setNamespaceLateralState(PGList *namespace_ptr,
                    bool lateral_only, bool lateral_ok)
{
	PGListCell   *lc;

	foreach(lc, namespace_ptr)
	{
		PGParseNamespaceItem *nsitem = (PGParseNamespaceItem *) lfirst(lc);

		nsitem->p_lateral_only = lateral_only;
		nsitem->p_lateral_ok = lateral_ok;
	}
};

PGNode *
ClauseParser::buildMergedJoinVar(PGParseState *pstate, PGJoinType jointype,
				   PGVar *l_colvar, PGVar *r_colvar)
{
	Oid			outcoltype;
	int32		outcoltypmod;
	PGNode	   *l_node,
			   *r_node,
			   *res_node;

	/*
	 * Choose output type if input types are dissimilar.
	 */
	outcoltype = l_colvar->vartype;
	outcoltypmod = l_colvar->vartypmod;
	if (outcoltype != r_colvar->vartype)
	{
		outcoltype = coerce_parser.select_common_type(pstate,
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
		l_node = coerce_parser.coerce_type(pstate, (PGNode *) l_colvar, l_colvar->vartype,
							 outcoltype, outcoltypmod,
							 PG_COERCION_IMPLICIT, PG_COERCE_IMPLICIT_CAST, -1);
	else if (l_colvar->vartypmod != outcoltypmod)
		l_node = (PGNode *) makeRelabelType((PGExpr *) l_colvar,
										  outcoltype, outcoltypmod,
										  InvalidOid,	/* fixed below */
										  PG_COERCE_IMPLICIT_CAST);
	else
		l_node = (PGNode *) l_colvar;

	if (r_colvar->vartype != outcoltype)
		r_node = coerce_parser.coerce_type(pstate, (PGNode *) r_colvar, r_colvar->vartype,
							 outcoltype, outcoltypmod,
							 PG_COERCION_IMPLICIT, PG_COERCE_IMPLICIT_CAST, -1);
	else if (r_colvar->vartypmod != outcoltypmod)
		r_node = (PGNode *) makeRelabelType((PGExpr *) r_colvar,
										  outcoltype, outcoltypmod,
										  InvalidOid,	/* fixed below */
										  PG_COERCE_IMPLICIT_CAST);
	else
		r_node = (PGNode *) r_colvar;

	/*
	 * Choose what to emit
	 */
	switch (jointype)
	{
		case PG_JOIN_INNER:

			/*
			 * We can use either var; prefer non-coerced one if available.
			 */
			if (IsA(l_node, PGVar))
				res_node = l_node;
			else if (IsA(r_node, PGVar))
				res_node = r_node;
			else
				res_node = l_node;
			break;
		case PG_JOIN_LEFT:
			/* Always use left var */
			res_node = l_node;
			break;
		case PG_JOIN_RIGHT:
			/* Always use right var */
			res_node = r_node;
			break;
		case PG_JOIN_FULL:
			{
				/*
				 * Here we must build a COALESCE expression to ensure that the
				 * join output is non-null if either input is.
				 */
				PGCoalesceExpr *c = makeNode(PGCoalesceExpr);

				c->coalescetype = outcoltype;
				/* coalescecollid will get set below */
				c->args = list_make2(l_node, r_node);
				c->location = -1;
				res_node = (PGNode *) c;
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
	collation_parser.assign_expr_collations(pstate, res_node);

	return res_node;
};

PGNode *
ClauseParser::transformJoinUsingClause(PGParseState *pstate,
						 PGRangeTblEntry *leftRTE, PGRangeTblEntry *rightRTE,
						 PGList *leftVars, PGList *rightVars)
{
	PGNode	   *result;
	PGList	   *andargs = NIL;
	PGListCell   *lvars,
			   *rvars;

	/*
	 * We cheat a little bit here by building an untransformed operator tree
	 * whose leaves are the already-transformed Vars.  This requires collusion
	 * from transformExpr(), which normally could be expected to complain
	 * about already-transformed subnodes.  However, this does mean that we
	 * have to mark the columns as requiring SELECT privilege for ourselves;
	 * transformExpr() won't do it.
	 */
	forboth(lvars, leftVars, rvars, rightVars)
	{
		PGVar		   *lvar = (PGVar *) lfirst(lvars);
		PGVar		   *rvar = (PGVar *) lfirst(rvars);
		PGAExpr	   *e;

		/* Require read access to the join variables */
		relation_parser.markVarForSelectPriv(pstate, lvar, leftRTE);
		relation_parser.markVarForSelectPriv(pstate, rvar, rightRTE);

		/* Now create the lvar = rvar join condition */
		e = makeSimpleAExpr(PG_AEXPR_OP, "=",
							 (PGNode *) copyObject(lvar), (PGNode *) copyObject(rvar),
							 -1);

		/* Prepare to combine into an AND clause, if multiple join columns */
		andargs = lappend(andargs, e);
	}

	/* Only need an AND if there's more than one join column */
	if (list_length(andargs) == 1)
		result = (PGNode *) linitial(andargs);
	else
		result = (PGNode *) makeBoolExpr(PG_AND_EXPR, andargs, -1);

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

PGNode *
ClauseParser::transformJoinOnClause(PGParseState *pstate, PGJoinExpr *j, PGList *namespace_ptr)
{
	PGNode	   *result;
	PGList	   *save_namespace;

	/*
	 * The namespace that the join expression should see is just the two
	 * subtrees of the JOIN plus any outer references from upper pstate
	 * levels.  Temporarily set this pstate's namespace accordingly.  (We need
	 * not check for refname conflicts, because transformFromClauseItem()
	 * already did.)  All namespace items are marked visible regardless of
	 * LATERAL state.
	 */
	setNamespaceLateralState(namespace_ptr, false, true);

	save_namespace = pstate->p_namespace;
	pstate->p_namespace = namespace_ptr;

	result = transformWhereClause(pstate, j->quals,
								  EXPR_KIND_JOIN_ON, "JOIN/ON");

	pstate->p_namespace = save_namespace;

	return result;
};

void
ClauseParser::extractRemainingColumns(PGList *common_colnames,
						PGList *src_colnames, PGList *src_colvars,
						PGList **res_colnames, PGList **res_colvars)
{
	PGList	   *new_colnames = NIL;
	PGList	   *new_colvars = NIL;
	PGListCell   *lnames,
			   *lvars;

	Assert(list_length(src_colnames) == list_length(src_colvars));

	forboth(lnames, src_colnames, lvars, src_colvars)
	{
		char	   *colname = strVal(lfirst(lnames));
		bool		match = false;
		PGListCell   *cnames;

		foreach(cnames, common_colnames)
		{
			char	   *ccolname = strVal(lfirst(cnames));

			if (strcmp(colname, ccolname) == 0)
			{
				match = true;
				break;
			}
		}

		if (!match)
		{
			new_colnames = lappend(new_colnames, lfirst(lnames));
			new_colvars = lappend(new_colvars, lfirst(lvars));
		}
	}

	*res_colnames = new_colnames;
	*res_colvars = new_colvars;
};

void
ClauseParser::setNamespaceColumnVisibility(PGList *namespace_ptr, bool cols_visible)
{
	PGListCell   *lc;

	foreach(lc, namespace_ptr)
	{
		PGParseNamespaceItem *nsitem = (PGParseNamespaceItem *) lfirst(lc);

		nsitem->p_cols_visible = cols_visible;
	}
};

PGNode *
ClauseParser::transformFromClauseItem(PGParseState *pstate, PGNode *n,
						PGRangeTblEntry **top_rte, int *top_rti,
						PGList **namespace_ptr)
{
	if (IsA(n, PGRangeVar))
	{
		/* Plain relation reference, or perhaps a CTE reference */
		PGRangeVar   *rv = (PGRangeVar *) n;
		PGRangeTblRef *rtr;
		PGRangeTblEntry *rte;
		int			rtindex;

		/* Check if it's a CTE or tuplestore reference */
		rte = getRTEForSpecialRelationTypes(pstate, rv);

		/* if not found above, must be a table reference */
		if (!rte)
			rte = transformTableEntry(pstate, rv);

		/* assume new rte is at end */
		rtindex = list_length(pstate->p_rtable);
		Assert(rte == rt_fetch(rtindex, pstate->p_rtable));
		*top_rte = rte;
		*top_rti = rtindex;
		*namespace_ptr = list_make1(makeNamespaceItem(rte, true, true, false, true));
		rtr = makeNode(PGRangeTblRef);
		rtr->rtindex = rtindex;
		return (PGNode *) rtr;
	}
	else if (IsA(n, PGRangeSubselect))
	{
		/* sub-SELECT is like a plain relation */
		PGRangeTblRef *rtr;
		PGRangeTblEntry *rte;
		int			rtindex;

		rte = transformRangeSubselect(pstate, (PGRangeSubselect *) n);
		/* assume new rte is at end */
		rtindex = list_length(pstate->p_rtable);
		Assert(rte == rt_fetch(rtindex, pstate->p_rtable));
		*top_rte = rte;
		*top_rti = rtindex;
		*namespace_ptr = list_make1(makeNamespaceItem(rte, true, true, false, true));
		rtr = makeNode(PGRangeTblRef);
		rtr->rtindex = rtindex;
		return (PGNode *) rtr;
	}
	// else if (IsA(n, PGRangeFunction))
	// {
	// 	/* function is like a plain relation */
	// 	PGRangeTblRef *rtr;
	// 	PGRangeTblEntry *rte;
	// 	int			rtindex;

	// 	rte = transformRangeFunction(pstate, (PGRangeFunction *) n);
	// 	/* assume new rte is at end */
	// 	rtindex = list_length(pstate->p_rtable);
	// 	Assert(rte == rt_fetch(rtindex, pstate->p_rtable));
	// 	*top_rte = rte;
	// 	*top_rti = rtindex;
	// 	*namespace_ptr = list_make1(makeDefaultNSItem(rte));
	// 	rtr = makeNode(PGRangeTblRef);
	// 	rtr->rtindex = rtindex;
	// 	return (PGNode *) rtr;
	// }
	// else if (IsA(n, PGRangeTableFunc))
	// {
	// 	/* table function is like a plain relation */
	// 	RangeTblRef *rtr;
	// 	RangeTblEntry *rte;
	// 	int			rtindex;

	// 	rte = transformRangeTableFunc(pstate, (RangeTableFunc *) n);
	// 	/* assume new rte is at end */
	// 	rtindex = list_length(pstate->p_rtable);
	// 	Assert(rte == rt_fetch(rtindex, pstate->p_rtable));
	// 	*top_rte = rte;
	// 	*top_rti = rtindex;
	// 	*namespace = list_make1(makeDefaultNSItem(rte));
	// 	rtr = makeNode(RangeTblRef);
	// 	rtr->rtindex = rtindex;
	// 	return (Node *) rtr;
	// }
	// else if (IsA(n, RangeTableSample))
	// {
	// 	/* TABLESAMPLE clause (wrapping some other valid FROM node) */
	// 	RangeTableSample *rts = (RangeTableSample *) n;
	// 	Node	   *rel;
	// 	RangeTblRef *rtr;
	// 	RangeTblEntry *rte;

	// 	/* Recursively transform the contained relation */
	// 	rel = transformFromClauseItem(pstate, rts->relation,
	// 								  top_rte, top_rti, namespace);
	// 	/* Currently, grammar could only return a RangeVar as contained rel */
	// 	rtr = castNode(RangeTblRef, rel);
	// 	rte = rt_fetch(rtr->rtindex, pstate->p_rtable);
	// 	/* We only support this on plain relations and matviews */
	// 	if (rte->relkind != RELKIND_RELATION &&
	// 		rte->relkind != RELKIND_MATVIEW &&
	// 		rte->relkind != RELKIND_PARTITIONED_TABLE)
	// 		ereport(ERROR,
	// 				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
	// 				 errmsg("TABLESAMPLE clause can only be applied to tables and materialized views"),
	// 				 parser_errposition(pstate, exprLocation(rts->relation))));

	// 	/* Transform TABLESAMPLE details and attach to the RTE */
	// 	rte->tablesample = transformRangeTableSample(pstate, rts);
	// 	return (Node *) rtr;
	// }
	else if (IsA(n, PGJoinExpr))
	{
		/* A newfangled join expression */
		PGJoinExpr   *j = (PGJoinExpr *) n;
		PGRangeTblEntry *l_rte;
		PGRangeTblEntry *r_rte;
		int			l_rtindex;
		int			r_rtindex;
		PGList	   *l_namespace,
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
		PGRangeTblEntry *rte;
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
		lateral_ok = (j->jointype == PG_JOIN_INNER || j->jointype == PG_JOIN_LEFT);
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
			PGList	   *rlist = NIL;
			PGListCell   *lx,
					   *rx;

			Assert(j->usingClause == NIL);	/* shouldn't have USING() too */

			foreach(lx, l_colnames)
			{
				char	   *l_colname = strVal(lfirst(lx));
				PGValue	   *m_name = NULL;

				foreach(rx, r_colnames)
				{
					char	   *r_colname = strVal(lfirst(rx));

					if (strcmp(l_colname, r_colname) == 0)
					{
						m_name = makeString(l_colname);
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
			PGList	   *ucols = j->usingClause;
			PGList	   *l_usingvars = NIL;
			PGList	   *r_usingvars = NIL;
			PGListCell   *ucol;

			Assert(j->quals == NULL);	/* shouldn't have ON() too */

			foreach(ucol, ucols)
			{
				char	   *u_colname = strVal(lfirst(ucol));
				PGListCell   *col;
				int			ndx;
				int			l_index = -1;
				int			r_index = -1;
				PGVar		   *l_colvar,
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

				l_colvar = (PGVar *)list_nth(l_colvars, l_index);
				l_usingvars = lappend(l_usingvars, l_colvar);
				r_colvar = (PGVar *)list_nth(r_colvars, r_index);
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
							(errcode(PG_ERRCODE_SYNTAX_ERROR),
							 errmsg("column alias list for \"%s\" has too many entries",
									j->alias->aliasname)));
			}
		}

		/*
		 * Now build an RTE for the result of the join
		 */
		rte = relation_parser.addRangeTableEntryForJoin(pstate,
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
		*namespace_ptr = lappend(my_namespace,
							 makeNamespaceItem(rte,
											   (j->alias != NULL),
											   true,
											   false,
											   true));

		return (PGNode *) j;
	}
	else
		elog(ERROR, "unrecognized node type: %d", (int) nodeTag(n));
	return NULL;				/* can't get here, keep compiler quiet */
};

void
ClauseParser::transformFromClause(PGParseState *pstate, PGList *frmList)
{
    PGListCell   *fl;

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
		PGNode	   *n = (PGNode *)lfirst(fl);
		PGRangeTblEntry *rte = NULL;
		int			rtindex = 0;
		PGList	   *namespace_ptr = NULL;

		n = transformFromClauseItem(pstate, n,
									&rte,
									&rtindex,
									&namespace_ptr);

		relation_parser.checkNameSpaceConflicts(pstate, pstate->p_namespace, namespace_ptr);

		/* Mark the new namespace items as visible only to LATERAL */
		setNamespaceLateralState(namespace_ptr, true, true);

		pstate->p_joinlist = lappend(pstate->p_joinlist, n);
		pstate->p_namespace = list_concat(pstate->p_namespace, namespace_ptr);
	}

	/*
	 * We're done parsing the FROM list, so make all namespace items
	 * unconditionally visible.  Note that this will also reset lateral_only
	 * for any namespace items that were already present when we were called;
	 * but those should have been that way already.
	 */
	setNamespaceLateralState(pstate->p_namespace, false, true);
};

PGNode *
ClauseParser::transformWhereClause(PGParseState *pstate, PGNode *clause,
					 PGParseExprKind exprKind, const char *constructName)
{
	PGNode	   *qual;

	if (clause == NULL)
		return NULL;

	qual = expr_parser.transformExpr(pstate, clause, exprKind);

	qual = coerce_parser.coerce_to_boolean(pstate, qual, constructName);

	return qual;
};

PGTargetEntry *
ClauseParser::findTargetlistEntrySQL99(PGParseState *pstate, PGNode *node, PGList **tlist,
						 PGParseExprKind exprKind)
{
	PGTargetEntry *target_result;
	PGListCell   *tl;
	PGNode	   *expr;

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
		PGTargetEntry *tle = (PGTargetEntry *) lfirst(tl);
		PGNode	   *texpr;

		/*
		 * Ignore any implicit cast on the existing tlist expression.
		 *
		 * This essentially allows the ORDER/GROUP/etc item to adopt the same
		 * datatype previously selected for a textually-equivalent tlist item.
		 * There can't be any implicit cast at top level in an ordinary SELECT
		 * tlist at this stage, but the case does arise with ORDER BY in an
		 * aggregate function.
		 */
		texpr = strip_implicit_coercions((PGNode *) tle->expr);

		if (equal(expr, texpr))
			return tle;
	}

	/*
	 * If no matches, construct a new target entry which is appended to the
	 * end of the target list.  This target is given resjunk = true so that it
	 * will not be projected into the final tuple.
	 */
	target_result = target_parser.transformTargetEntry(pstate, node, expr, exprKind,
										 NULL, true);

	*tlist = lappend(*tlist, target_result);

	return target_result;
};

void
ClauseParser::checkTargetlistEntrySQL92(PGParseState *pstate, PGTargetEntry *tle,
						  PGParseExprKind exprKind)
{
	switch (exprKind)
	{
		case EXPR_KIND_GROUP_BY:
			/* reject aggregates and window functions */
			if (pstate->p_hasAggs &&
				contain_aggs_of_level((PGNode *) tle->expr, 0))
				ereport(ERROR,
						(errcode(ERRCODE_GROUPING_ERROR),
				/* translator: %s is name of a SQL construct, eg GROUP BY */
						 errmsg("aggregate functions are not allowed in %s",
								expr_parser.ParseExprKindName(exprKind)),
						 parser_errposition(pstate,
											locate_agg_of_level((PGNode *) tle->expr, 0))));
			if (pstate->p_hasWindowFuncs &&
				contain_windowfuncs((PGNode *) tle->expr))
				ereport(ERROR,
						(errcode(PG_ERRCODE_WINDOWING_ERROR),
				/* translator: %s is name of a SQL construct, eg GROUP BY */
						 errmsg("window functions are not allowed in %s",
								expr_parser.ParseExprKindName(exprKind)),
						 parser_errposition(pstate,
											locate_windowfunc((PGNode *) tle->expr))));
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

PGTargetEntry *
ClauseParser::findTargetlistEntrySQL92(PGParseState *pstate, PGNode *node, PGList **tlist,
						 PGParseExprKind exprKind)
{
	PGListCell   *tl;

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
	if (IsA(node, PGColumnRef) &&
		list_length(((PGColumnRef *) node)->fields) == 1 &&
		IsA(linitial(((PGColumnRef *) node)->fields), PGString))
	{
		char	   *name = strVal(linitial(((PGColumnRef *) node)->fields));
		int			location = ((PGColumnRef *) node)->location;

		if (exprKind == PGParseExprKind::EXPR_KIND_GROUP_BY)
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
			PGTargetEntry *target_result = NULL;

			foreach(tl, *tlist)
			{
				PGTargetEntry *tle = (PGTargetEntry *) lfirst(tl);

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
											expr_parser.ParseExprKindName(exprKind),
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
	if (IsA(node, PGAConst))
	{
		PGValue	   *val = &((PGAConst *) node)->val;
		int			location = ((PGAConst *) node)->location;
		int			targetlist_pos = 0;
		int			target_pos;

		if (!IsA(val, PGInteger))
			ereport(ERROR,
					(errcode(PG_ERRCODE_SYNTAX_ERROR),
			/* translator: %s is name of a SQL construct, eg ORDER BY */
					 errmsg("non-integer constant in %s",
							expr_parser.ParseExprKindName(exprKind)),
					 parser_errposition(pstate, location)));

		target_pos = intVal(val);
		foreach(tl, *tlist)
		{
			PGTargetEntry *tle = (PGTargetEntry *) lfirst(tl);

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
						expr_parser.ParseExprKindName(exprKind), target_pos),
				 parser_errposition(pstate, location)));
	}

	/*
	 * Otherwise, we have an expression, so process it per SQL99 rules.
	 */
	return findTargetlistEntrySQL99(pstate, node, tlist, exprKind);
};

bool
ClauseParser::targetIsInSortList(PGTargetEntry *tle, Oid sortop, PGList *sortList)
{
	Index		ref = tle->ressortgroupref;
	PGListCell   *l;

	/* no need to scan list if tle has no marker */
	if (ref == 0)
		return false;

	foreach(l, sortList)
	{
		PGSortGroupClause *scl = (PGSortGroupClause *) lfirst(l);

		if (scl->tleSortGroupRef == ref &&
			(sortop == InvalidOid ||
			 sortop == scl->sortop ||
			 sortop == get_commutator(scl->sortop)))
			return true;
	}
	return false;
};

Index
ClauseParser::assignSortGroupRef(PGTargetEntry *tle, PGList *tlist)
{
	Index		maxRef;
	PGListCell   *l;

	if (tle->ressortgroupref)	/* already has one? */
		return tle->ressortgroupref;

	/* easiest way to pick an unused refnumber: max used + 1 */
	maxRef = 0;
	foreach(l, tlist)
	{
		Index		ref = ((PGTargetEntry *) lfirst(l))->ressortgroupref;

		if (ref > maxRef)
			maxRef = ref;
	}
	tle->ressortgroupref = maxRef + 1;
	return tle->ressortgroupref;
};

PGList *
ClauseParser::addTargetToSortList(PGParseState *pstate, PGTargetEntry *tle,
					PGList *sortlist, PGList *targetlist, PGSortBy *sortby)
{
	Oid			restype = exprType((PGNode *) tle->expr);
	Oid			sortop;
	Oid			eqop;
	bool		hashable;
	bool		reverse;
	int			location;
	ParseCallbackState pcbstate;

	/* if tlist item is an UNKNOWN literal, change it to TEXT */
	if (restype == UNKNOWNOID)
	{
		tle->expr = (PGExpr *) coerce_parser.coerce_type(pstate, (PGNode *) tle->expr,
										 restype, TEXTOID, -1,
										 PG_COERCION_IMPLICIT,
										 PG_COERCE_IMPLICIT_CAST,
										 exprLocation((PGNode *) tle->expr));
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
	setup_parser_errposition_callback(&pcbstate, pstate, location);

	/* determine the sortop, eqop, and directionality */
	switch (sortby->sortby_dir)
	{
		case PG_SORTBY_DEFAULT:
		case PG_SORTBY_ASC:
			oper_parser.get_sort_group_operators(restype,
									 true, true, false,
									 &sortop, &eqop, NULL,
									 &hashable);
			reverse = false;
			break;
		case PG_SORTBY_DESC:
			oper_parser.get_sort_group_operators(restype,
									 false, true, true,
									 NULL, &eqop, &sortop,
									 &hashable);
			reverse = true;
			break;
		case SORTBY_USING:
			Assert(sortby->useOp != NIL);
			sortop = oper_parser.compatible_oper_opid(sortby->useOp,
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

	cancel_parser_errposition_callback(&pcbstate);

	/* avoid making duplicate sortlist entries */
	if (!targetIsInSortList(tle, sortop, sortlist))
	{
		PGSortGroupClause *sortcl = makeNode(PGSortGroupClause);

		sortcl->tleSortGroupRef = assignSortGroupRef(tle, targetlist);

		sortcl->eqop = eqop;
		sortcl->sortop = sortop;
		sortcl->hashable = hashable;

		switch (sortby->sortby_nulls)
		{
			case PG_SORTBY_NULLS_DEFAULT:
				/* NULLS FIRST is default for DESC; other way for ASC */
				sortcl->nulls_first = reverse;
				break;
			case PG_SORTBY_NULLS_FIRST:
				sortcl->nulls_first = true;
				break;
			case PG_SORTBY_NULLS_LAST:
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

PGList *
ClauseParser::transformSortClause(PGParseState *pstate,
					PGList *orderlist,
					PGList **targetlist,
					PGParseExprKind exprKind,
					bool useSQL99)
{
	PGList	   *sortlist = NIL;
	PGListCell   *olitem;

	foreach(olitem, orderlist)
	{
		PGSortBy	   *sortby = (PGSortBy *) lfirst(olitem);
		PGTargetEntry *tle;

		if (useSQL99)
			tle = findTargetlistEntrySQL99(pstate, sortby->node,
										   targetlist, exprKind);
		else
			tle = findTargetlistEntrySQL92(pstate, sortby->node,
										   targetlist, exprKind);

		sortlist = addTargetToSortList(pstate, tle,
									   sortlist, *targetlist, sortby);
	}

	return sortlist;
};

PGList *
ClauseParser::addTargetToGroupList(PGParseState *pstate, PGTargetEntry *tle,
					 PGList *grouplist,
                     PGList *targetlist, int location)
{
	Oid			restype = exprType((PGNode *) tle->expr);

	/* if tlist item is an UNKNOWN literal, change it to TEXT */
	if (restype == UNKNOWNOID)
	{
		tle->expr = (PGExpr *) coerce_parser.coerce_type(pstate, (PGNode *) tle->expr,
										 restype, TEXTOID, -1,
										 PG_COERCION_IMPLICIT,
										 PG_COERCE_IMPLICIT_CAST,
										 -1);
		restype = TEXTOID;
	}

	/* avoid making duplicate grouplist entries */
	if (!targetIsInSortList(tle, InvalidOid, grouplist))
	{
		PGSortGroupClause *grpcl = makeNode(PGSortGroupClause);
		Oid			sortop;
		Oid			eqop;
		bool		hashable;
		ParseCallbackState pcbstate;

		setup_parser_errposition_callback(&pcbstate, pstate, location);

		/* determine the eqop and optional sortop */
		get_sort_group_operators(restype,
								 false, true, false,
								 &sortop, &eqop, NULL,
								 &hashable);

		cancel_parser_errposition_callback(&pcbstate);

		grpcl->tleSortGroupRef = assignSortGroupRef(tle, targetlist);
		grpcl->eqop = eqop;
		grpcl->sortop = sortop;
		grpcl->nulls_first = false; /* OK with or without sortop */
		grpcl->hashable = hashable;

		grouplist = lappend(grouplist, grpcl);
	}

	return grouplist;
};

PGList *
ClauseParser::transformDistinctClause(PGParseState *pstate,
						PGList **targetlist,
                        PGList *sortClause, bool is_agg)
{
	PGList	   *result = NIL;
	PGListCell   *slitem;
	PGListCell   *tlitem;

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
		PGSortGroupClause *scl = (PGSortGroupClause *) lfirst(slitem);
		PGTargetEntry *tle = get_sortgroupclause_tle(scl, *targetlist);

		if (tle->resjunk)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
					 is_agg ?
					 errmsg("in an aggregate with DISTINCT, ORDER BY expressions must appear in argument list") :
					 errmsg("for SELECT DISTINCT, ORDER BY expressions must appear in select list"),
					 parser_errposition(pstate,
										exprLocation((PGNode *) tle->expr))));
		result = lappend(result, copyObject(scl));
	}

	/*
	 * Now add any remaining non-resjunk tlist items, using default sort/group
	 * semantics for their data types.
	 */
	foreach(tlitem, *targetlist)
	{
		PGTargetEntry *tle = (PGTargetEntry *) lfirst(tlitem);

		if (tle->resjunk)
			continue;			/* ignore junk */
		result = addTargetToGroupList(pstate, tle,
									  result, *targetlist,
									  exprLocation((PGNode *) tle->expr));
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
				(errcode(PG_ERRCODE_SYNTAX_ERROR),
				 is_agg ?
				 errmsg("an aggregate with DISTINCT must have at least one argument") :
				 errmsg("SELECT DISTINCT must have at least one column")));

	return result;
};

}