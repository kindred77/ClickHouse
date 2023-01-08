#include <Interpreters/orcaopt/pgoptnew/ClauseParser.h>
#include <Interpreters/orcaopt/pgoptnew/walkers.h>

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
							(errcode(ERRCODE_SYNTAX_ERROR),
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
				pg_contain_aggs_of_level((PGNode *) tle->expr, 0))
			{
				node_parser.parser_errposition(pstate,
								pg_locate_agg_of_level((PGNode *) tle->expr, 0));
				ereport(ERROR,
						(errcode(ERRCODE_GROUPING_ERROR),
				/* translator: %s is name of a SQL construct, eg GROUP BY */
						 errmsg("aggregate functions are not allowed in %s",
								expr_parser.ParseExprKindName(exprKind))));
			}

			if (pstate->p_hasWindowFuncs &&
				pg_contain_windowfuncs((PGNode *) tle->expr))
			{
				node_parser.parser_errposition(pstate,
								pg_locate_windowfunc((PGNode *) tle->expr));
				ereport(ERROR,
						(errcode(ERRCODE_WINDOWING_ERROR),
				/* translator: %s is name of a SQL construct, eg GROUP BY */
						 errmsg("window functions are not allowed in %s",
								expr_parser.ParseExprKindName(exprKind))));
			}
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
						{
							node_parser.parser_errposition(pstate, location);
							ereport(ERROR,
									(errcode(ERRCODE_AMBIGUOUS_COLUMN),

							/*------
							  translator: first %s is name of a SQL construct, eg ORDER BY */
									 errmsg("%s \"%s\" is ambiguous",
											expr_parser.ParseExprKindName(exprKind),
											name)));
						}
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
		{
			node_parser.parser_errposition(pstate, location);
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
			/* translator: %s is name of a SQL construct, eg ORDER BY */
					 errmsg("non-integer constant in %s",
							expr_parser.ParseExprKindName(exprKind))));
		}

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
		node_parser.parser_errposition(pstate, location);
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
		/* translator: %s is name of a SQL construct, eg ORDER BY */
				 errmsg("%s position %d is not in select list",
						expr_parser.ParseExprKindName(exprKind), target_pos)));
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
	PGParseCallbackState pcbstate;

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
	node_parser.setup_parser_errposition_callback(&pcbstate, pstate, location);

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

	node_parser.cancel_parser_errposition_callback(&pcbstate);

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
		PGParseCallbackState pcbstate;

		node_parser.setup_parser_errposition_callback(&pcbstate, pstate, location);

		/* determine the eqop and optional sortop */
		oper_parser.get_sort_group_operators(restype,
								 false, true, false,
								 &sortop, &eqop, NULL,
								 &hashable);

		node_parser.cancel_parser_errposition_callback(&pcbstate);

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
		{
			node_parser.parser_errposition(pstate,
								exprLocation((PGNode *) tle->expr));
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
					 is_agg ?
					 errmsg("in an aggregate with DISTINCT, ORDER BY expressions must appear in argument list") :
					 errmsg("for SELECT DISTINCT, ORDER BY expressions must appear in select list")));
		}
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
				(errcode(ERRCODE_SYNTAX_ERROR),
				 is_agg ?
				 errmsg("an aggregate with DISTINCT must have at least one argument") :
				 errmsg("SELECT DISTINCT must have at least one column")));

	return result;
};

PGNode *
ClauseParser::flatten_grouping_sets(PGNode *expr, bool toplevel, bool *hasGroupingSets)
{
	/* just in case of pathological input */
	check_stack_depth();

	if (expr == (PGNode *) NIL)
		return (PGNode *) NIL;

	switch (expr->type)
	{
		case T_PGRowExpr:
			{
				PGRowExpr    *r = (PGRowExpr *) expr;

				if (r->row_format == PG_COERCE_IMPLICIT_CAST)
					return flatten_grouping_sets((PGNode *) r->args,
												 false, NULL);
			}
			break;
		case T_PGGroupingSet:
			{
				PGGroupingSet *gset = (PGGroupingSet *) expr;
				PGListCell   *l2;
				PGList	   *result_set = NIL;

				if (hasGroupingSets)
					*hasGroupingSets = true;

				/*
				 * at the top level, we skip over all empty grouping sets; the
				 * caller can supply the canonical GROUP BY () if nothing is
				 * left.
				 */

				if (toplevel && gset->kind == GROUPING_SET_EMPTY)
					return (PGNode *) NIL;

				foreach(l2, gset->content)
				{
					PGNode	   *n1 = (PGNode *)lfirst(l2);
					PGNode	   *n2 = flatten_grouping_sets(n1, false, NULL);

					if (IsA(n1, PGGroupingSet) &&
						((PGGroupingSet *) n1)->kind == GROUPING_SET_SETS)
					{
						result_set = list_concat(result_set, (PGList *) n2);
					}
					else
						result_set = lappend(result_set, n2);
				}

				/*
				 * At top level, keep the grouping set node; but if we're in a
				 * nested grouping set, then we need to concat the flattened
				 * result into the outer list if it's simply nested.
				 */

				if (toplevel || (gset->kind != GROUPING_SET_SETS))
				{
					return (PGNode *) makeGroupingSet(gset->kind, result_set, gset->location);
				}
				else
					return (PGNode *) result_set;
			}
		case T_PGList:
			{
				PGList	   *result = NIL;
				PGListCell   *l;

				foreach(l, (PGList *) expr)
				{
					PGNode	   *n = flatten_grouping_sets((PGNode *)lfirst(l), toplevel, hasGroupingSets);

					if (n != (PGNode *) NIL)
					{
						if (IsA(n, PGList))
							result = list_concat(result, (PGList *) n);
						else
							result = lappend(result, n);
					}
				}

				return (PGNode *) result;
			}
		default:
			break;
	}

	return expr;
};

Index
ClauseParser::transformGroupClauseExpr(PGList **flatresult,
                        PGBitmapset *seen_local,
						PGParseState *pstate, PGNode *gexpr,
						PGList **targetlist,
                        PGList *sortClause,
						PGParseExprKind exprKind, bool useSQL99, bool toplevel)
{
	PGTargetEntry *tle;
	bool		found = false;

	if (useSQL99)
		tle = findTargetlistEntrySQL99(pstate, gexpr,
									   targetlist, exprKind);
	else
		tle = findTargetlistEntrySQL92(pstate, gexpr,
									   targetlist, exprKind);

	if (tle->ressortgroupref > 0)
	{
		ListCell   *sl;

		/*
		 * Eliminate duplicates (GROUP BY x, x) but only at local level.
		 * (Duplicates in grouping sets can affect the number of returned
		 * rows, so can't be dropped indiscriminately.)
		 *
		 * Since we don't care about anything except the sortgroupref, we can
		 * use a bitmapset rather than scanning lists.
		 */
		if (bms_is_member(tle->ressortgroupref, seen_local))
			return 0;

		/*
		 * If we're already in the flat clause list, we don't need to consider
		 * adding ourselves again.
		 */
		found = targetIsInSortList(tle, InvalidOid, *flatresult);
		if (found)
			return tle->ressortgroupref;

		/*
		 * If the GROUP BY tlist entry also appears in ORDER BY, copy operator
		 * info from the (first) matching ORDER BY item.  This means that if
		 * you write something like "GROUP BY foo ORDER BY foo USING <<<", the
		 * GROUP BY operation silently takes on the equality semantics implied
		 * by the ORDER BY.  There are two reasons to do this: it improves the
		 * odds that we can implement both GROUP BY and ORDER BY with a single
		 * sort step, and it allows the user to choose the equality semantics
		 * used by GROUP BY, should she be working with a datatype that has
		 * more than one equality operator.
		 *
		 * If we're in a grouping set, though, we force our requested ordering
		 * to be NULLS LAST, because if we have any hope of using a sorted agg
		 * for the job, we're going to be tacking on generated NULL values
		 * after the corresponding groups. If the user demands nulls first,
		 * another sort step is going to be inevitable, but that's the
		 * planner's problem.
		 */

		foreach(sl, sortClause)
		{
			PGSortGroupClause *sc = (PGSortGroupClause *) lfirst(sl);

			if (sc->tleSortGroupRef == tle->ressortgroupref)
			{
				PGSortGroupClause *grpc = (PGSortGroupClause *)copyObject(sc);

				if (!toplevel)
					grpc->nulls_first = false;
				*flatresult = lappend(*flatresult, grpc);
				found = true;
				break;
			}
		}
	}

	/*
	 * If no match in ORDER BY, just add it to the result using default
	 * sort/group semantics.
	 */
	if (!found)
		*flatresult = addTargetToGroupList(pstate, tle,
										   *flatresult, *targetlist,
										   exprLocation(gexpr));

	/*
	 * _something_ must have assigned us a sortgroupref by now...
	 */

	return tle->ressortgroupref;
};

PGList *
ClauseParser::transformGroupClauseList(PGList **flatresult,
						 PGParseState *pstate, PGList *list,
						 PGList **targetlist,
                         PGList *sortClause,
						 PGParseExprKind exprKind, bool useSQL99, bool toplevel)
{
	PGBitmapset  *seen_local = NULL;
	PGList	   *result = NIL;
	PGListCell   *gl;

	foreach(gl, list)
	{
		PGNode	   *gexpr = (PGNode *) lfirst(gl);

		Index		ref = transformGroupClauseExpr(flatresult,
												   seen_local,
												   pstate,
												   gexpr,
												   targetlist,
												   sortClause,
												   exprKind,
												   useSQL99,
												   toplevel);

		if (ref > 0)
		{
			seen_local = bms_add_member(seen_local, ref);
			result = lappend_int(result, ref);
		}
	}

	return result;
};

PGNode *
ClauseParser::transformGroupingSet(PGList **flatresult,
					 PGParseState *pstate, PGGroupingSet *gset,
					 PGList **targetlist, PGList *sortClause,
					 PGParseExprKind exprKind, bool useSQL99, bool toplevel)
{
	PGListCell   *gl;
	PGList	   *content = NIL;

	Assert(toplevel || gset->kind != GROUPING_SET_SETS);

	foreach(gl, gset->content)
	{
		PGNode	   *n = (PGNode *)lfirst(gl);

		if (IsA(n, PGList))
		{
			PGList	   *l = transformGroupClauseList(flatresult,
													 pstate, (PGList *) n,
													 targetlist, sortClause,
													 exprKind, useSQL99, false);

			content = lappend(content, makeGroupingSet(GROUPING_SET_SIMPLE,
													   l,
													   exprLocation(n)));
		}
		else if (IsA(n, PGGroupingSet))
		{
			PGGroupingSet *gset2 = (PGGroupingSet *) lfirst(gl);

			content = lappend(content, transformGroupingSet(flatresult,
															pstate, gset2,
															targetlist, sortClause,
															exprKind, useSQL99, false));
		}
		else
		{
			Index		ref = transformGroupClauseExpr(flatresult,
													   NULL,
													   pstate,
													   n,
													   targetlist,
													   sortClause,
													   exprKind,
													   useSQL99,
													   false);

			content = lappend(content, makeGroupingSet(GROUPING_SET_SIMPLE,
													   list_make1_int(ref),
													   exprLocation(n)));
		}
	}

	/* Arbitrarily cap the size of CUBE, which has exponential growth */
	if (gset->kind == GROUPING_SET_CUBE)
	{
		if (list_length(content) > 12)
		{
			node_parser.parser_errposition(pstate, gset->location);
			ereport(ERROR,
					(errcode(ERRCODE_TOO_MANY_COLUMNS),
					 errmsg("CUBE is limited to 12 elements")));
		}
	}

	return (PGNode *) makeGroupingSet(gset->kind, content, gset->location);
};

PGList *
ClauseParser::transformGroupClause(PGParseState *pstate, PGList *grouplist,
                    PGList **groupingSets,
					PGList **targetlist,
                    PGList *sortClause,
					PGParseExprKind exprKind, bool useSQL99)
{
	PGList	   *result = NIL;
	PGList	   *flat_grouplist;
	PGList	   *gsets = NIL;
	PGListCell   *gl;
	bool		hasGroupingSets = false;
	PGBitmapset  *seen_local = NULL;

	/*
	 * Recursively flatten implicit RowExprs. (Technically this is only needed
	 * for GROUP BY, per the syntax rules for grouping sets, but we do it
	 * anyway.)
	 */
	flat_grouplist = (PGList *) flatten_grouping_sets((PGNode *) grouplist,
													true,
													&hasGroupingSets);

	/*
	 * If the list is now empty, but hasGroupingSets is true, it's because we
	 * elided redundant empty grouping sets. Restore a single empty grouping
	 * set to leave a canonical form: GROUP BY ()
	 */

	if (flat_grouplist == NIL && hasGroupingSets)
	{
		flat_grouplist = list_make1(makeGroupingSet(GROUPING_SET_EMPTY,
													NIL,
													exprLocation((PGNode *) grouplist)));
	}

	foreach(gl, flat_grouplist)
	{
		PGNode	   *gexpr = (PGNode *) lfirst(gl);

		if (IsA(gexpr, PGGroupingSet))
		{
			PGGroupingSet *gset = (PGGroupingSet *) gexpr;

			switch (gset->kind)
			{
				case GROUPING_SET_EMPTY:
					gsets = lappend(gsets, gset);
					break;
				case GROUPING_SET_SIMPLE:
					/* can't happen */
					Assert(false);
					break;
				case GROUPING_SET_SETS:
				case GROUPING_SET_CUBE:
				case GROUPING_SET_ROLLUP:
					gsets = lappend(gsets,
									transformGroupingSet(&result,
														 pstate, gset,
														 targetlist, sortClause,
														 exprKind, useSQL99, true));
					break;
			}
		}
		else
		{
			Index		ref = transformGroupClauseExpr(&result, seen_local,
													   pstate, gexpr,
													   targetlist, sortClause,
													   exprKind, useSQL99, true);

			if (ref > 0)
			{
				seen_local = bms_add_member(seen_local, ref);
				if (hasGroupingSets)
					gsets = lappend(gsets,
									makeGroupingSet(GROUPING_SET_SIMPLE,
													list_make1_int(ref),
													exprLocation(gexpr)));
			}
		}
	}

	/* parser should prevent this */
	Assert(gsets == NIL || groupingSets != NULL);

	if (groupingSets)
		*groupingSets = gsets;

	return result;
};

int
ClauseParser::get_matching_location(int sortgroupref, PGList *sortgrouprefs,
                    PGList *exprs)
{
	PGListCell   *lcs;
	PGListCell   *lce;

	forboth(lcs, sortgrouprefs, lce, exprs)
	{
		if (lfirst_int(lcs) == sortgroupref)
			return exprLocation((PGNode *) lfirst(lce));
	}
	/* if no match, caller blew it */
	elog(ERROR, "get_matching_location: no matching sortgroupref");
	return -1;					/* keep compiler quiet */
};

PGList *
ClauseParser::transformDistinctOnClause(PGParseState *pstate, PGList *distinctlist,
						  PGList **targetlist,
                          PGList *sortClause)
{
	PGList	   *result = NIL;
	PGList	   *sortgrouprefs = NIL;
	bool		skipped_sortitem;
	PGListCell   *lc;
	PGListCell   *lc2;

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
		PGNode	   *dexpr = (PGNode *) lfirst(lc);
		int			sortgroupref;
		PGTargetEntry *tle;

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
		PGSortGroupClause *scl = (PGSortGroupClause *) lfirst(lc);

		if (list_member_int(sortgrouprefs, scl->tleSortGroupRef))
		{
			if (skipped_sortitem)
			{
				node_parser.parser_errposition(pstate,
											get_matching_location(scl->tleSortGroupRef,
																  sortgrouprefs,
																  distinctlist));
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
						 errmsg("SELECT DISTINCT ON expressions must match initial ORDER BY expressions")));
			}
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
		PGNode	   *dexpr = (PGNode *) lfirst(lc);
		int			sortgroupref = lfirst_int(lc2);
		PGTargetEntry *tle = get_sortgroupref_tle(sortgroupref, *targetlist);

		if (targetIsInSortList(tle, InvalidOid, result))
			continue;			/* already in list (with some semantics) */
		if (skipped_sortitem)
		{
			node_parser.parser_errposition(pstate, exprLocation(dexpr));
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
					 errmsg("SELECT DISTINCT ON expressions must match initial ORDER BY expressions")));
		}
		result = addTargetToGroupList(pstate, tle,
									  result, *targetlist,
									  exprLocation(dexpr));
	}

	/*
	 * An empty result list is impossible here because of grammar
	 * restrictions.
	 */
	Assert(result != NIL);

	return result;
};

void
ClauseParser::checkExprIsVarFree(PGParseState *pstate, PGNode *n, const char *constructName)
{
	if (contain_vars_of_level(n, 0))
	{
		node_parser.parser_errposition(pstate,
						pg_locate_var_of_level(n, 0));
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
		/* translator: %s is name of a SQL construct, eg LIMIT */
				 errmsg("argument of %s must not contain variables",
						constructName)));
	}
};

PGNode *
ClauseParser::transformLimitClause(PGParseState *pstate, PGNode *clause,
					 PGParseExprKind exprKind, const char *constructName)
{
	PGNode	   *qual;

	if (clause == NULL)
		return NULL;

	qual = expr_parser.transformExpr(pstate, clause, exprKind);

	qual = coerce_parser.coerce_to_specific_type(pstate, qual, INT8OID, constructName);

	/* LIMIT can't refer to any variables of the current query */
	checkExprIsVarFree(pstate, qual, constructName);

	return qual;
};

PGWindowClause *
ClauseParser::findWindowClause(PGList *wclist, const char *name)
{
	PGListCell   *l;

	foreach(l, wclist)
	{
		PGWindowClause *wc = (PGWindowClause *) lfirst(l);

		if (wc->name && strcmp(wc->name, name) == 0)
			return wc;
	}

	return NULL;
};

PGList *
ClauseParser::transformWindowDefinitions(PGParseState *pstate,
						   PGList *windowdefs,
						   PGList **targetlist)
{
	PGList	   *result = NIL;
	Index		winref = 0;
	PGListCell   *lc;

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
		PGWindowDef  *windef = (PGWindowDef *) lfirst(lc);
		PGWindowClause *refwc = NULL;
		PGList	   *partitionClause;
		PGList	   *orderClause;
		Oid			rangeopfamily = InvalidOid;
		Oid			rangeopcintype = InvalidOid;
		PGWindowClause *wc;

		winref++;

		/*
		 * Check for duplicate window names.
		 */
		if (windef->name &&
			findWindowClause(result, windef->name) != NULL)
		{
			node_parser.parser_errposition(pstate, windef->location);
			ereport(ERROR,
					(errcode(ERRCODE_WINDOWING_ERROR),
					 errmsg("window \"%s\" is already defined", windef->name)));
		}

		/*
		 * If it references a previous window, look that up.
		 */
		if (windef->refname)
		{
			refwc = findWindowClause(result, windef->refname);
			if (refwc == NULL)
			{
				node_parser.parser_errposition(pstate, windef->location);
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_OBJECT),
						 errmsg("window \"%s\" does not exist",
								windef->refname)));
			}
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
										  true /* force SQL99 rules */ );
		partitionClause = transformGroupClause(pstate,
											   windef->partitionClause,
											   NULL,
											   targetlist,
											   orderClause,
											   EXPR_KIND_WINDOW_PARTITION,
											   true /* force SQL99 rules */ );

		/*
		 * And prepare the new WindowClause.
		 */
		wc = makeNode(PGWindowClause);
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
			{
				node_parser.parser_errposition(pstate, windef->location);
				ereport(ERROR,
						(errcode(ERRCODE_WINDOWING_ERROR),
						 errmsg("cannot override PARTITION BY clause of window \"%s\"",
								windef->refname)));
			}
			wc->partitionClause = (PGList *)copyObject(refwc->partitionClause);
		}
		else
			wc->partitionClause = partitionClause;
		if (refwc)
		{
			if (orderClause && refwc->orderClause)
			{
				node_parser.parser_errposition(pstate, windef->location);
				ereport(ERROR,
						(errcode(ERRCODE_WINDOWING_ERROR),
						 errmsg("cannot override ORDER BY clause of window \"%s\"",
								windef->refname)));
			}
			if (orderClause)
			{
				wc->orderClause = orderClause;
				wc->copiedOrder = false;
			}
			else
			{
				wc->orderClause = (PGList *)copyObject(refwc->orderClause);
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
			{
				node_parser.parser_errposition(pstate, windef->location);
				ereport(ERROR,
						(errcode(ERRCODE_WINDOWING_ERROR),
						 errmsg("cannot copy window \"%s\" because it has a frame clause",
								windef->refname)));
			}
			/* Else this clause is just OVER (foo), so say this: */
			node_parser.parser_errposition(pstate, windef->location);
			ereport(ERROR,
					(errcode(ERRCODE_WINDOWING_ERROR),
			errmsg("cannot copy window \"%s\" because it has a frame clause",
				   windef->refname),
					 errhint("Omit the parentheses in this OVER clause.")));
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

		/*
		 * RANGE offset PRECEDING/FOLLOWING requires exactly one ORDER BY
		 * column; check that and get its sort opfamily info.
		 */
		if ((wc->frameOptions & FRAMEOPTION_RANGE) &&
			(wc->frameOptions & (FRAMEOPTION_START_OFFSET |
								 FRAMEOPTION_END_OFFSET)))
		{
			PGSortGroupClause *sortcl;
			PGNode	   *sortkey;
			int16		rangestrategy;

			if (list_length(wc->orderClause) != 1)
			{
				node_parser.parser_errposition(pstate, windef->location);
				ereport(ERROR,
						(errcode(ERRCODE_WINDOWING_ERROR),
						 errmsg("RANGE with offset PRECEDING/FOLLOWING requires exactly one ORDER BY column")));
			}
			sortcl = castNode(PGSortGroupClause, linitial(wc->orderClause));
			sortkey = get_sortgroupclause_expr(sortcl, *targetlist);
			/* Find the sort operator in pg_amop */
			if (!get_ordering_op_properties(sortcl->sortop,
											&rangeopfamily,
											&rangeopcintype,
											&rangestrategy))
				elog(ERROR, "operator %u is not a valid ordering operator",
					 sortcl->sortop);
			/* Record properties of sort ordering */
			// wc->inRangeColl = exprCollation(sortkey);
			// wc->inRangeAsc = (rangestrategy == BTLessStrategyNumber);
			// wc->inRangeNullsFirst = sortcl->nulls_first;
		}

		/* Per spec, GROUPS mode requires an ORDER BY clause */
		if (wc->frameOptions & FRAMEOPTION_GROUPS)
		{
			if (wc->orderClause == NIL)
			{
				node_parser.parser_errposition(pstate, windef->location);
				ereport(ERROR,
						(errcode(ERRCODE_WINDOWING_ERROR),
						 errmsg("GROUPS mode requires an ORDER BY clause")));
			}
		}

		/* Process frame offset expressions */
		wc->startOffset = transformFrameOffset(pstate, wc->frameOptions,
											   rangeopfamily, rangeopcintype,
											   &wc->startInRangeFunc,
											   windef->startOffset);
		wc->endOffset = transformFrameOffset(pstate, wc->frameOptions,
											 rangeopfamily, rangeopcintype,
											 &wc->endInRangeFunc,
											 windef->endOffset);
		wc->winref = winref;

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

}