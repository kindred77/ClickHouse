#include <Interpreters/orcaopt/pgopt_hawq/ClauseParser.h>
#include <Interpreters/orcaopt/pgopt_hawq/walkers.h>

using namespace duckdb_libpgquery;

namespace DB
{

PGNode * ClauseParser::buildMergedJoinVar(PGParseState * pstate,
        PGJoinType jointype, PGVar * l_colvar,
        PGVar * r_colvar)
{
    Oid outcoltype;
    int32 outcoltypmod;
    PGNode *l_node, *r_node, *res_node;

    /*
	 * Choose output type if input types are dissimilar.
	 */
    outcoltype = l_colvar->vartype;
    outcoltypmod = l_colvar->vartypmod;
    if (outcoltype != r_colvar->vartype)
    {
        outcoltype = select_common_type(list_make2_oid(l_colvar->vartype, r_colvar->vartype), "JOIN/USING");
        outcoltypmod = -1; /* ie, unknown */
    }
    else if (outcoltypmod != r_colvar->vartypmod)
    {
        /* same type, but not same typmod */
        outcoltypmod = -1; /* ie, unknown */
    }

    /*
	 * Insert coercion functions if needed.  Note that a difference in typmod
	 * can only happen if input has typmod but outcoltypmod is -1. In that
	 * case we insert a RelabelType to clearly mark that result's typmod is
	 * not same as input.  We never need coerce_type_typmod.
	 */
    if (l_colvar->vartype != outcoltype)
        l_node = coerce_type(
            pstate, (PGNode *)l_colvar, l_colvar->vartype, outcoltype, outcoltypmod, COERCION_IMPLICIT, COERCE_IMPLICIT_CAST, -1);
    else if (l_colvar->vartypmod != outcoltypmod)
        l_node = (PGNode *)makeRelabelType((Expr *)l_colvar, outcoltype, outcoltypmod, COERCE_IMPLICIT_CAST);
    else
        l_node = (PGNode *)l_colvar;

    if (r_colvar->vartype != outcoltype)
        r_node = coerce_type(
            pstate, (PGNode *)r_colvar, r_colvar->vartype, outcoltype, outcoltypmod, COERCION_IMPLICIT, COERCE_IMPLICIT_CAST, -1);
    else if (r_colvar->vartypmod != outcoltypmod)
        r_node = (PGNode *)makeRelabelType((Expr *)r_colvar, outcoltype, outcoltypmod, COERCE_IMPLICIT_CAST);
    else
        r_node = (PGNode *)r_colvar;

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
        case PG_JOIN_FULL: {
            /*
				 * Here we must build a COALESCE expression to ensure that the
				 * join output is non-null if either input is.
				 */
            PGCoalesceExpr * c = makeNode(PGCoalesceExpr);

            c->coalescetype = outcoltype;
            c->args = list_make2(l_node, r_node);
            res_node = (PGNode *)c;
            break;
        }
        default:
            elog(ERROR, "unrecognized join type: %d", (int)jointype);
            res_node = NULL; /* keep compiler quiet */
            break;
    }

    return res_node;
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
};

PGRangeTblEntry * ClauseParser::transformRangeSubselect(PGParseState * pstate,
        PGRangeSubselect * r)
{
	PGList	   *parsetrees;
	PGQuery	   *query;
	PGRangeTblEntry *rte;

	/*
	 * We require user to supply an alias for a subselect, per SQL92. To relax
	 * this, we'd have to be prepared to gin up a unique alias for an
	 * unlabeled subselect.
	 */
	if (r->alias == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("subquery in FROM must have an alias")));

	/*
	 * Analyze and transform the subquery.
	 */
	parsetrees = select_parser.parse_sub_analyze(r->subquery, pstate);

	/*
	 * Check that we got something reasonable.	Most of these conditions are
	 * probably impossible given restrictions of the grammar, but check 'em
	 * anyway.
	 */
	if (list_length(parsetrees) != 1)
		elog(ERROR, "unexpected parse analysis result for subquery in FROM");
	query = (PGQuery *) linitial(parsetrees);
	if (query == NULL || !IsA(query, PGQuery))
		elog(ERROR, "unexpected parse analysis result for subquery in FROM");

	if (query->commandType != PG_CMD_SELECT)
		elog(ERROR, "expected SELECT query from subquery in FROM");
	if (query->intoClause != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("subquery in FROM may not have SELECT INTO")));

	/*
	 * The subquery cannot make use of any variables from FROM items created
	 * earlier in the current query.  Per SQL92, the scope of a FROM item does
	 * not include other FROM items.  Formerly we hacked the namespace so that
	 * the other variables weren't even visible, but it seems more useful to
	 * leave them visible and give a specific error message.
	 *
	 * XXX this will need further work to support SQL99's LATERAL() feature,
	 * wherein such references would indeed be legal.
	 *
	 * We can skip groveling through the subquery if there's not anything
	 * visible in the current query.  Also note that outer references are OK.
	 */
	if (pstate->p_relnamespace || pstate->p_varnamespace)
	{
		if (contain_vars_of_level((PGNode *) query, 1))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
					 errmsg("subquery in FROM may not refer to other relations of same query level"),
							 errOmitLocation(true)));
	}

	/*
	 * OK, build an RTE for the subquery.
	 */
	rte = relation_parser.addRangeTableEntryForSubquery(pstate, query, r->alias, true);

	return rte;
};

PGRangeTblEntry * ClauseParser::transformTableEntry(PGParseState * pstate,
        PGRangeVar * r)
{
    PGRangeTblEntry * rte;

    /*
	 * mark this entry to indicate it comes from the FROM clause. In SQL, the
	 * target list can only refer to range variables specified in the from
	 * clause but we follow the more powerful POSTQUEL semantics and
	 * automatically generate the range variable if not specified. However
	 * there are times we need to know whether the entries are legitimate.
	 */
    rte = relation_parser.addRangeTableEntry(pstate, r, r->alias, interpretInhOption(r->inhOpt), true);

    return rte;
};

PGNode * ClauseParser::transformWhereClause(PGParseState * pstate,
        PGNode * clause, const char * constructName)
{
    PGNode * qual;

    if (clause == NULL)
        return NULL;

    qual = expr_parser.transformExpr(pstate, clause);

    qual = coerce_parser.coerce_to_boolean(pstate, qual, constructName);

    return qual;
};

PGNode * ClauseParser::transformFromClauseItem(
        PGParseState * pstate, PGNode * n,
        PGRangeTblEntry ** top_rte, int * top_rti,
        PGList ** relnamespace, PGRelids * containedRels)
{
	PGNode                   *result;
    ParseStateBreadCrumb    savebreadcrumb;

    /* CDB: Push error location stack.  Must pop before return! */
    Assert(pstate);
    savebreadcrumb = pstate->p_breadcrumb;
    pstate->p_breadcrumb.pop = &savebreadcrumb;
    pstate->p_breadcrumb.node = n;

	if (IsA(n, PGRangeVar))
	{
		/* Plain relation reference */
		PGRangeTblRef *rtr;
		PGRangeTblEntry *rte = NULL;
		int	rtindex;
		PGRangeVar *rangeVar = (PGRangeVar *)n;

		/*
		 * If it is an unqualified name, it might be a CTE reference.
		 */
		if (rangeVar->schemaname == NULL)
		{
			PGCommonTableExpr *cte;
			Index levelsup;

			cte = relation_parser.scanNameSpaceForCTE(pstate, rangeVar->relname, &levelsup);
			if (cte)
			{
				rte = relation_parser.addRangeTableEntryForCTE(pstate, cte, levelsup, rangeVar, true);
			}
		}

		/* If it is not a CTE reference, it must be a simple relation reference. */
		if (rte == NULL)
		{
			rte = transformTableEntry(pstate, rangeVar);
		}
		
		/* assume new rte is at end */
		rtindex = list_length(pstate->p_rtable);
		Assert(rte == rt_fetch(rtindex, pstate->p_rtable));
		*top_rte = rte;
		*top_rti = rtindex;
		*relnamespace = list_make1(rte);
		*containedRels = bms_make_singleton(rtindex);
		rtr = makeNode(PGRangeTblRef);
		rtr->rtindex = rtindex;
		result = (PGNode *) rtr;
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
		*relnamespace = list_make1(rte);
		*containedRels = bms_make_singleton(rtindex);
		rtr = makeNode(PGRangeTblRef);
		rtr->rtindex = rtindex;
		result = (PGNode *) rtr;
	}
	else if (IsA(n, PGRangeFunction))
	{
		/* function is like a plain relation */
		PGRangeTblRef *rtr;
		PGRangeTblEntry *rte;
		int			rtindex;

		rte = transformRangeFunction(pstate, (PGRangeFunction *) n);
		/* assume new rte is at end */
		rtindex = list_length(pstate->p_rtable);
		Assert(rte == rt_fetch(rtindex, pstate->p_rtable));
		*top_rte = rte;
		*top_rti = rtindex;
		*relnamespace = list_make1(rte);
		*containedRels = bms_make_singleton(rtindex);
		rtr = makeNode(PGRangeTblRef);
		rtr->rtindex = rtindex;
		result = (PGNode *) rtr;
	}
	else if (IsA(n, PGJoinExpr))
	{
		/* A newfangled join expression */
		PGJoinExpr   *j = (PGJoinExpr *) n;
		PGRangeTblEntry *l_rte;
		PGRangeTblEntry *r_rte;
		int			l_rtindex;
		int			r_rtindex;
		PGRelids		l_containedRels,
					r_containedRels,
					my_containedRels;
		PGList	   *l_relnamespace,
				   *r_relnamespace,
				   *my_relnamespace,
				   *l_colnames,
				   *r_colnames,
				   *res_colnames,
				   *l_colvars,
				   *r_colvars,
				   *res_colvars;
		PGRangeTblEntry *rte;

		/*
		 * Recursively process the left and right subtrees
		 */
		j->larg = transformFromClauseItem(pstate, j->larg,
										  &l_rte,
										  &l_rtindex,
										  &l_relnamespace,
										  &l_containedRels);
		j->rarg = transformFromClauseItem(pstate, j->rarg,
										  &r_rte,
										  &r_rtindex,
										  &r_relnamespace,
										  &r_containedRels);

		/*
		 * Check for conflicting refnames in left and right subtrees. Must do
		 * this because higher levels will assume I hand back a self-
		 * consistent namespace subtree.
		 */
		relation_parser.checkNameSpaceConflicts(pstate, l_relnamespace, r_relnamespace);

		/*
		 * Generate combined relation membership info for possible use by
		 * transformJoinOnClause below.
		 */
		my_relnamespace = list_concat(l_relnamespace, r_relnamespace);
		my_containedRels = bms_join(l_containedRels, r_containedRels);

		pfree(r_relnamespace);	/* free unneeded list header */

		/*
		 * Extract column name and var lists from both subtrees
		 *
		 * Note: expandRTE returns new lists, safe for me to modify
		 */
		relation_parser.expandRTE(l_rte, l_rtindex, 0, false, -1,
				  &l_colnames, &l_colvars);
		relation_parser.expandRTE(r_rte, r_rtindex, 0, false, -1,
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
												l_usingvars,
												r_usingvars);
		}
		else if (j->quals)
		{
			/* User-written ON-condition; transform it */
			j->quals = transformJoinOnClause(pstate, j,
											 l_rte, r_rte,
											 my_relnamespace,
											 my_containedRels);
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

		/*
		 * Prepare returned namespace list.  If the JOIN has an alias then it
		 * hides the contained RTEs as far as the relnamespace goes;
		 * otherwise, put the contained RTEs and *not* the JOIN into
		 * relnamespace.
		 */
		if (j->alias)
		{
			*relnamespace = list_make1(rte);
			list_free(my_relnamespace);
		}
		else
			*relnamespace = my_relnamespace;

		/*
		 * Include join RTE in returned containedRels set
		 */
		*containedRels = bms_add_member(my_containedRels, j->rtindex);

		result = (PGNode *) j;
	}
	else
    {
        result = NULL;
		elog(ERROR, "unrecognized node type: %d", (int) nodeTag(n));
    }

    /* CDB: Pop error location stack. */
    Assert(pstate->p_breadcrumb.pop == &savebreadcrumb);
    pstate->p_breadcrumb = savebreadcrumb;

	return result;
};

void ClauseParser::transformFromClause(PGParseState * pstate, PGList * frmList)
{
    PGListCell * fl = NULL;

    /*
	 * The grammar will have produced a list of RangeVars, RangeSubselects,
	 * RangeFunctions, and/or JoinExprs. Transform each one (possibly adding
	 * entries to the rtable), check for duplicate refnames, and then add it
	 * to the joinlist and namespaces.
	 */
    foreach (fl, frmList)
    {
        PGNode * n = lfirst(fl);
        PGRangeTblEntry * rte = NULL;
        int rtindex = 0;
        PGList * relnamespace = NULL;
        PGRelids containedRels = NULL;

        n = transformFromClauseItem(pstate, n, &rte, &rtindex, &relnamespace, &containedRels);
        checkNameSpaceConflicts(pstate, pstate->p_relnamespace, relnamespace);
        pstate->p_joinlist = lappend(pstate->p_joinlist, n);
        pstate->p_relnamespace = list_concat(pstate->p_relnamespace, relnamespace);
        pstate->p_varnamespace = lappend(pstate->p_varnamespace, rte);
        bms_free(containedRels);
    }
};

PGTargetEntry * ClauseParser::findTargetlistEntrySQL99(PGParseState * pstate,
        PGNode * node, PGList ** tlist)
{
    PGTargetEntry * target_result;
    PGListCell * tl;
    PGNode * expr;

    /*
	 * Convert the untransformed node to a transformed expression, and search
	 * for a match in the tlist.  NOTE: it doesn't really matter whether there
	 * is more than one match.	Also, we are willing to match an existing
	 * resjunk target here, though the SQL92 cases above must ignore resjunk
	 * targets.
	 */
    expr = expr_parser.transformExpr(pstate, node);

    foreach (tl, *tlist)
    {
        PGTargetEntry * tle = (PGTargetEntry *)lfirst(tl);

        if (equal(expr, tle->expr))
            return tle;
    }

    /*
	 * If no matches, construct a new target entry which is appended to the
	 * end of the target list.	This target is given resjunk = TRUE so that it
	 * will not be projected into the final tuple.
	 */
    target_result = transformTargetEntry(pstate, node, expr, NULL, true);

    *tlist = lappend(*tlist, target_result);

    return target_result;
};

PGTargetEntry * ClauseParser::findTargetlistEntrySQL92(PGParseState * pstate,
        PGNode * node, PGList ** tlist, int clause)
{
    PGTargetEntry * target_result = NULL;
    PGListCell * tl;

    /* CDB: Drop a breadcrumb in case of error. */
    pstate->p_breadcrumb.node = node;

    /*----------
	 * Handle two special cases as mandated by the SQL92 spec:
	 *
	 * 1. Bare ColumnName (no qualifier or subscripts)
	 *	  For a bare identifier, we search for a matching column name
	 *	  in the existing target list.	Multiple matches are an error
	 *	  unless they refer to identical values; for example,
	 *	  we allow	SELECT a, a FROM table ORDER BY a
	 *	  but not	SELECT a AS b, b FROM table ORDER BY b
	 *	  If no match is found, we fall through and treat the identifier
	 *	  as an expression.
	 *	  For GROUP BY, it is incorrect to match the grouping item against
	 *	  targetlist entries: according to SQL92, an identifier in GROUP BY
	 *	  is a reference to a column name exposed by FROM, not to a target
	 *	  list column.	However, many implementations (including pre-7.0
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
	 *	  actual constant, so this does not create a conflict with our
	 *	  extension to order/group by an expression.
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
    if (IsA(node, PGColumnRef) && list_length(((PGColumnRef *)node)->fields) == 1)
    {
        char * name = strVal(linitial(((PGColumnRef *)node)->fields));
        int location = ((PGColumnRef *)node)->location;

        if (clause == GROUP_CLAUSE)
        {
            /*
			 * In GROUP BY, we must prefer a match against a FROM-clause
			 * column to one against the targetlist.  Look to see if there is
			 * a matching column.  If so, fall through to use SQL99 rules
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
            foreach (tl, *tlist)
            {
                PGTargetEntry * tle = (PGTargetEntry *)lfirst(tl);

                if (!tle->resjunk && strcmp(tle->resname, name) == 0)
                {
                    if (target_result != NULL)
                    {
                        if (!equal(target_result->expr, tle->expr))
                            ereport(
                                ERROR,
                                (errcode(ERRCODE_AMBIGUOUS_COLUMN),

                                 /*------
							  translator: first %s is name of a SQL construct, eg ORDER BY */
                                 errmsg("%s \"%s\" is ambiguous", clauseText[clause], name),
                                 parser_errposition(pstate, location)));
                    }
                    else
                        target_result = tle;
                    /* Stay in loop to check for ambiguity */
                }
            }
            if (target_result != NULL)
                return target_result; /* return the first match */
        }
    }
    if (IsA(node, PGAConst))
    {
        PGValue * val = &((PGAConst *)node)->val;
        int targetlist_pos = 0;
        int target_pos;

        if (!IsA(val, Integer))
            ereport(
                ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                 /* translator: %s is name of a SQL construct, eg ORDER BY */
                 errmsg("non-integer constant in %s", clauseText[clause])));
        target_pos = intVal(val);
        foreach (tl, *tlist)
        {
            PGTargetEntry * tle = (PGTargetEntry *)lfirst(tl);

            if (!tle->resjunk)
            {
                if (++targetlist_pos == target_pos)
                    return tle; /* return the unique match */
            }
        }
        ereport(
            ERROR,
            (errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
             /* translator: %s is name of a SQL construct, eg ORDER BY */
             errmsg("%s position %d is not in select list", clauseText[clause], target_pos),
             errOmitLocation(true)));
    }


    /*
	 * Otherwise, we have an expression, so process it per SQL99 rules.
	 */
    return findTargetlistEntrySQL99(pstate, node, tlist);
};

bool ClauseParser::targetIsInSortGroupList(PGTargetEntry * tle,
        PGList * sortgroupList)
{
    Index ref = tle->ressortgroupref;
    PGListCell * l;

    /* no need to scan list if tle has no marker */
    if (ref == 0)
        return false;

    foreach (l, sortgroupList)
    {
        PGNode * node = (PGNode *)lfirst(l);

        /* Skip the empty grouping set */
        if (node == NULL)
            continue;

        if (IsA(node, GroupClause) || IsA(node, SortClause))
        {
            GroupClause * gc = (GroupClause *)node;
            if (gc->tleSortGroupRef == ref)
                return true;
        }
    }
    return false;
};

PGList * ClauseParser::addTargetToSortList(
        PGParseState * pstate,
        PGTargetEntry * tle,
        PGList * sortlist,
        PGList * targetlist,
        int sortby_kind,
        PGList * sortby_opname,
        bool resolveUnknown)
{
    /* avoid making duplicate sortlist entries */
    if (!targetIsInSortGroupList(tle, sortlist))
    {
        PGSortClause * sortcl = makeNode(PGSortClause);
        Oid restype = exprType((PGNode *)tle->expr);

        /* if tlist item is an UNKNOWN literal, change it to TEXT */
        if (restype == UNKNOWNOID && resolveUnknown)
        {
            Oid tobe_type = InvalidOid;
            int32 tobe_typmod;

            if (pstate->p_setopTypes)
            {
                /* UNION, etc. case. */
                int idx = tle->resno - 1;

                Assert(pstate->p_setopTypmods);
                tobe_type = list_nth_oid(pstate->p_setopTypes, idx);
                tobe_typmod = list_nth_int(pstate->p_setopTypmods, idx);
            }

            if (!OidIsValid(tobe_type))
            {
                tobe_type = TEXTOID;
                tobe_typmod = -1;
            }
            tle->expr = (PGExpr *)coerce_parser.coerce_type(
                pstate, (PGNode *)tle->expr, restype, tobe_type, tobe_typmod, PG_COERCION_IMPLICIT, PG_COERCE_IMPLICIT_CAST, -1);
            restype = tobe_type;
        }

        sortcl->tleSortGroupRef = assignSortGroupRef(tle, targetlist);

        switch (sortby_kind)
        {
            case PG_SORTBY_ASC:
                sortcl->sortop = ordering_oper_opid(restype);
                break;
            case PG_SORTBY_DESC:
                sortcl->sortop = reverse_ordering_oper_opid(restype);
                break;
            case SORTBY_USING:
                Assert(sortby_opname != NIL);
                sortcl->sortop = compatible_oper_opid(sortby_opname, restype, restype, false);
                if (!sort_op_can_sort(sortcl->sortop, false))
                    ereport(
                        ERROR,
                        (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                         errmsg("operator %s is not a valid ordering operator", strVal(llast(sortby_opname))),
                         errhint("Ordering operators must be \"<\" or \">\" members of btree operator families."),
                         errOmitLocation(true)));

                break;
            default:
                elog(ERROR, "unrecognized sortby_kind: %d", sortby_kind);
                break;
        }

        sortlist = lappend(sortlist, sortcl);
    }
    return sortlist;
};

PGList * ClauseParser::transformSortClause(PGParseState * pstate,
        PGList * orderlist, PGList ** targetlist,
        bool resolveUnknown, bool useSQL99)
{
    PGList * sortlist = NIL;
    PGListCell * olitem;

    foreach (olitem, orderlist)
    {
        PGSortBy * sortby = lfirst(olitem);
        PGTargetEntry * tle;

        if (useSQL99)
            tle = findTargetlistEntrySQL99(pstate, sortby->node, targetlist);
        else
            tle = findTargetlistEntrySQL92(pstate, sortby->node, targetlist, ORDER_CLAUSE);

        sortlist = addTargetToSortList(pstate, tle, sortlist, *targetlist, sortby->sortby_kind, sortby->useOp, resolveUnknown);
    }

    return sortlist;
};

PGList * ClauseParser::transformGroupClause(PGParseState * pstate,
        PGList * grouplist, PGList ** targetlist,
        PGList * sortClause, bool useSQL99)
{
    PGList * result = NIL;
    PGList * tle_list = NIL;
    PGListCell * l;
    PGList * reorder_grouplist = NIL;

    /* Preprocess the grouping clause, lookup TLEs */
    foreach (l, grouplist)
    {
        PGList * tl;
        PGListCell * tl_cell;
        PGTargetEntry * tle;
        Oid restype;
        PGNode * node;

        node = (PGNode *)lfirst(l);
        tl = findListTargetlistEntries(pstate, node, targetlist, false, false, useSQL99);

        /* CDB: Cursor position not available for errors below this point. */
        pstate->p_breadcrumb.node = NULL;

        foreach (tl_cell, tl)
        {
            tle = (PGTargetEntry *)lfirst(tl_cell);

            /* if tlist item is an UNKNOWN literal, change it to TEXT */
            restype = exprType((PGNode *)tle->expr);

            if (restype == UNKNOWNOID)
                tle->expr
                    = (PGExpr *)coerce_parser.coerce_type(pstate, (PGNode *)tle->expr, restype, TEXTOID, -1, PG_COERCION_IMPLICIT, PG_COERCE_IMPLICIT_CAST, -1);

            /*
			 * The tle_list will be used to match with the ORDER by element below.
			 * We only append the tle to tle_list when node is not a
			 * GroupingClause or tle->expr is not a RowExpr.
			 */
            if (node != NULL && !IsA(node, GroupingClause) && !IsA(tle->expr, PGRowExpr))
                tle_list = lappend(tle_list, tle);
        }
    }

    /* create first group clauses based on sort clauses */
    PGList * tle_list_remainder = NIL;
    result = create_group_clause(tle_list, *targetlist, sortClause, &tle_list_remainder);

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

    foreach (l, reorder_grouplist)
    {
        PGNode * node = (PGNode *)lfirst(l);
        PGTargetEntry * tle;
        GroupClause * gc;
        Oid sort_op;

        if (node == NULL) /* the empty grouping set */
            result = list_concat(result, list_make1(NIL));

        else if (IsA(node, GroupingClause))
        {
            GroupingClause * tmp = make_grouping_clause(pstate, (GroupingClause *)node, *targetlist);
            result = lappend(result, tmp);
        }

        else if (IsA(node, PGRowExpr))
        {
            /* The top level RowExprs are handled differently with other expressions.
			 * We convert each argument into GroupClause and append them
			 * one by one into 'result' list.
			 *
			 * Note that RowExprs are not added into the final targetlist.
			 */
            result = transformRowExprToGroupClauses(pstate, (PGRowExpr *)node, result, *targetlist);
        }

        else
        {
            if (useSQL99)
                tle = findTargetlistEntrySQL99(pstate, node, targetlist);
            else
                tle = findTargetlistEntrySQL92(pstate, node, targetlist, GROUP_CLAUSE);

            /* avoid making duplicate expression entries */
            if (targetIsInSortGroupList(tle, result))
                continue;

            sort_op = ordering_oper_opid(exprType((PGNode *)tle->expr));
            gc = make_group_clause(tle, *targetlist, sort_op);
            result = lappend(result, gc);
        }
    }

    /* We're doing extended grouping for both ordinary grouping and grouping
	 * extensions.
	 */
    {
        PGList * grp_tles = NIL;
        PGListCell * lc;
        grouping_rewrite_ctx ctx;

        /* Find all unique target entries appeared in reorder_grouplist. */
        foreach (lc, reorder_grouplist)
        {
            grp_tles = list_concat_unique(grp_tles, findListTargetlistEntries(pstate, lfirst(lc), targetlist, false, true, useSQL99));
        }

        /* CDB: Cursor position not available for errors below this point. */
        pstate->p_breadcrumb.node = NULL;

        /*
		 * For each GROUPING function, check if its argument(s) appear in the
		 * GROUP BY clause. We also set ngrpcols, nargs and grpargs values for
		 * each GROUPING function here. These values are used together with
		 * GROUPING_ID to calculate the final value for each GROUPING function
		 * in the executor.
		 */

        ctx.grp_tles = grp_tles;
        ctx.pstate = pstate;
        expression_tree_walker((PGNode *)*targetlist, grouping_rewrite_walker, (void *)&ctx);

        /*
		 * The expression might be present in a window clause as well
		 * so process those.
		 */
        expression_tree_walker((PGNode *)pstate->p_win_clauses, grouping_rewrite_walker, (void *)&ctx);

        /*
		 * The expression might be present in the having clause as well.
		 */
        expression_tree_walker(pstate->having_qual, grouping_rewrite_walker, (void *)&ctx);
    }

    list_free(tle_list);
    list_free(tle_list_remainder);
    freeGroupList(reorder_grouplist);

    return result;
};

PGList * ClauseParser::transformScatterClause(PGParseState * pstate,
        PGList * scatterlist, PGList ** targetlist)
{
    PGList * outlist = NIL;
    PGListCell * olitem;

    /* Special case handling for SCATTER RANDOMLY */
    if (list_length(scatterlist) == 1 && linitial(scatterlist) == NULL)
        return list_make1(NULL);

    /* preprocess the scatter clause, lookup TLEs */
    foreach (olitem, scatterlist)
    {
        PGNode * node = lfirst(olitem);
        PGTargetEntry * tle;

        tle = findTargetlistEntrySQL99(pstate, node, targetlist);

        /* coerce unknown to text */
        if (exprType((PGNode *)tle->expr) == UNKNOWNOID)
        {
            tle->expr
                = (PGExpr *)coerce_parser.coerce_type(pstate, (PGNode *)tle->expr, UNKNOWNOID, TEXTOID, -1, PG_COERCION_IMPLICIT, PG_COERCE_IMPLICIT_CAST, -1);
        }

        outlist = lappend(outlist, tle->expr);
    }
    return outlist;
};

void ClauseParser::transformWindowClause(PGParseState * pstate, PGQuery * qry)
{
    PGListCell * w;
    PGList * winout = NIL;
    PGList * winin = pstate->p_win_clauses;
    Index clauseno = -1;

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

    foreach (w, winin)
    {
        WindowSpec * ws = lfirst(w);
        WindowSpec * newspec = makeNode(WindowSpec);
        PGListCell * tmp;
        bool found = false;

        clauseno++;

        /* Include this WindowSpec's location in error messages. */
        pstate->p_breadcrumb.node = (PGNode *)ws;

        if (checkExprHasWindFuncs((PGNode *)ws))
        {
            ereport(
                ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                 errmsg("cannot use window function in a window "
                        "specification"),
                 errOmitLocation(true)));
        }
        /*
		 * Loop through those clauses we've already processed to
		 * a) check that the name passed in is not already taken and
		 * b) look up the parent window spec.
		 *
		 * This is obvious O(n^2) but n is small.
		 */
        if (ws->parent || ws->name)
        {
            /*
			 * Firstly, check that the parent is not a reference to this
			 * window specification.
			 */
            if (ws->parent && ws->name && strcmp(ws->parent, ws->name) == 0)
                ereport(
                    ERROR,
                    (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                     errmsg("window \"%s\" cannot reference itself", ws->name),
                     errOmitLocation(true)));

            foreach (tmp, winout)
            {
                WindowSpec * ws2 = lfirst(tmp);

                /* Only check if the name exists if wc->name is not NULL */
                if (ws->name != NULL && strcmp(ws2->name, ws->name) == 0)
                    ereport(
                        ERROR,
                        (errcode(ERRCODE_DUPLICATE_OBJECT),
                         errmsg(
                             "window name \"%s\" occurs more than once "
                             "in WINDOW clause",
                             ws->name),
                         errOmitLocation(true)));

                /*
				 * If this spec has a parent reference, we need to test that
				 * the following rules are met. Given the following:
				 *
				 * 		OVER (myspec ...) ... WINDOW myspec AS (...)
				 *
				 * the OVER clause cannot have a partitioning clause; only
				 * the OVER clause or myspec can have an ORDER clause; myspec
				 * cannot have a framing clause.
				 */

                /*
				 * XXX: these errors could apply to any number of clauses in the
				 * query and may be considered ambiguous by the user. Perhaps a
				 * location (see FuncCall) would help?
				 */
                if (ws->parent && ws2->name && strcmp(ws->parent, ws2->name) == 0)
                {
                    found = true;
                    if (ws->partition != NIL)
                        ereport(
                            ERROR,
                            (errcode(ERRCODE_SYNTAX_ERROR),
                             errmsg("PARTITION BY not allowed when "
                                    "an existing window name is specified"),
                             errOmitLocation(true)));

                    if (ws->order != NIL && ws2->order != NIL)
                        ereport(
                            ERROR,
                            (errcode(ERRCODE_SYNTAX_ERROR),
                             errmsg("conflicting ORDER BY clauses in window "
                                    "specification"),
                             errOmitLocation(true)));

                    /*
					 * We only want to disallow the specification of a
					 * framing clause when the target list form is like:
					 *
					 *  foo() OVER (w1 ORDER BY baz) ...
					 */
                    if (!(ws->partition == NIL && ws->order == NIL && ws->name == NULL) && ws2->frame)
                        ereport(
                            ERROR,
                            (errcode(ERRCODE_SYNTAX_ERROR),
                             errmsg(
                                 "window specification \"%s\" cannot have "
                                 "a framing clause",
                                 ws2->name),
                             errhint("Window specifications which are "
                                     "referenced by other window "
                                     "specifications cannot have framing "
                                     "clauses"),
                             errOmitLocation(true),
                             parser_errposition(pstate, ws2->location)));

                    /*
					 * The specifications are valid so just copy the details
					 * from the parent spec.
					 */
                    newspec->parent = ws2->name;
                    /* XXX: some parameters might not be processed! */
                    newspec->partition = copyObject(ws2->partition);

                    if (ws->order == NIL && ws2->order != NIL)
                        newspec->order = copyObject(ws2->order);

                    newspec->frame = copyObject(ws2->frame);
                }
            }

            if (!found && ws->parent)
                ereport(
                    ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR), errmsg("window specification \"%s\" not found", ws->parent), errOmitLocation(true)));
        }

        newspec->name = ws->name;
        newspec->location = ws->location;

        /*
		 * Process partition, if one is defined and if it isn't already
		 * in newspec.
		 */
        if (!newspec->partition && ws->partition)
        {
            newspec->partition
                = transformSortClause(pstate, ws->partition, &qry->targetList, true /* fix unknowns */, false /* use SQL92 rules */);
        }
        /* order is just like partition */
        if (ws->order || newspec->order)
        {
            /*
			 * Only do this if it came from the new definition
			 */
            if (ws->order != NIL && newspec->order == NIL)
            {
                newspec->order
                    = transformSortClause(pstate, ws->order, &qry->targetList, true /* fix unknowns */, false /* use SQL92 rules */);
            }
        }

        /* Refresh our breadcrumb in case transformSortClause stepped on it. */
        pstate->p_breadcrumb.node = (PGNode *)ws;

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
        if (ws->frame)
        {
            /* with that out of the way, we may proceed */
            WindowFrame * nf = copyObject(ws->frame);

            /*
			 * Framing is only supported on specifications with an ordering
			 * clause.
			 */
            if (!ws->order && !newspec->order)
                ereport(
                    ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                     errmsg("window specifications with a framing clause "
                            "must have an ORDER BY clause"),
                     errOmitLocation(true)));

            if (nf->is_between)
            {
                Assert(PointerIsValid(nf->trail));
                Assert(PointerIsValid(nf->lead));

                if (nf->trail->kind == WINDOW_UNBOUND_FOLLOWING)
                    ereport(
                        ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                         errmsg("conflicting bounds in window framing "
                                "clause"),
                         errhint("First bound of BETWEEN clause in window "
                                 "specification cannot be UNBOUNDED FOLLOWING"),
                         errOmitLocation(true)));
                if (nf->lead->kind == WINDOW_UNBOUND_PRECEDING)
                    ereport(
                        ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                         errmsg("conflicting bounds in window framing "
                                "clause"),
                         errhint("Second bound of BETWEEN clause in window "
                                 "specification cannot be UNBOUNDED PRECEDING"),
                         errOmitLocation(true)));
                if (nf->trail->kind == WINDOW_CURRENT_ROW
                    && (nf->lead->kind == WINDOW_BOUND_PRECEDING || nf->lead->kind == WINDOW_UNBOUND_PRECEDING))
                    ereport(
                        ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                         errmsg("conflicting bounds in window framing "
                                "clause"),
                         errhint("Second bound cannot be PRECEDING "
                                 "when first bound is CURRENT ROW"),
                         errOmitLocation(true)));
                if ((nf->trail->kind == WINDOW_BOUND_FOLLOWING || nf->trail->kind == WINDOW_UNBOUND_FOLLOWING)
                    && !(nf->lead->kind == WINDOW_BOUND_FOLLOWING || nf->lead->kind == WINDOW_UNBOUND_FOLLOWING))
                    ereport(
                        ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                         errmsg("conflicting bounds in window framing "
                                "clause"),
                         errhint("Second bound must be FOLLOWING if first "
                                 "bound is FOLLOWING"),
                         errOmitLocation(true)));
            }
            else
            {
                /*
				 * If only a single bound has been specified, set the
				 * leading bound to CURRENT ROW
				 */
                WindowFrameEdge * e = makeNode(WindowFrameEdge);

                Assert(!PointerIsValid(nf->lead));

                e->kind = WINDOW_CURRENT_ROW;
                nf->lead = e;
            }

            transformWindowFrameEdge(pstate, nf->trail, newspec, qry, nf->is_rows);
            transformWindowFrameEdge(pstate, nf->lead, newspec, qry, nf->is_rows);
            newspec->frame = nf;
        }

        /* finally, check function restriction with this spec. */
        winref_checkspec(pstate, qry, clauseno, PointerIsValid(newspec->order), PointerIsValid(newspec->frame));

        winout = lappend(winout, newspec);
    }

    /* If there are no window functions in the targetlist,
	 * forget the window clause.
	 */
    if (!pstate->p_hasWindFuncs)
    {
        pstate->p_win_clauses = NIL;
        qry->windowClause = NIL;
    }
    else
    {
        qry->windowClause = winout;
    }
};

PGList * ClauseParser::transformDistinctClause(PGParseState * pstate,
        PGList * distinctlist, PGList ** targetlist,
        PGList ** sortClause, PGList ** groupClause)
{
    PGList * result = NIL;
    PGListCell * slitem;
    PGListCell * dlitem;

    /* No work if there was no DISTINCT clause */
    if (distinctlist == NIL)
        return NIL;

    if (linitial(distinctlist) == NULL)
    {
        /* We had SELECT DISTINCT */

        if (!pstate->p_hasAggs && !pstate->p_hasWindFuncs && *groupClause == NIL)
        {
            /*
			 * MPP-15040
			 * turn distinct clause into grouping clause to make both sort-based
			 * and hash-based grouping implementations viable plan options
			 */

            return transformDistinctToGroupBy(pstate, targetlist, sortClause, groupClause);
        }

        /*
		 * All non-resjunk elements from target list that are not already in
		 * the sort list should be added to it.  (We don't really care what
		 * order the DISTINCT fields are checked in, so we can leave the
		 * user's ORDER BY spec alone, and just add additional sort keys to it
		 * to ensure that all targetlist items get sorted.)
		 */
        *sortClause = addAllTargetsToSortList(pstate, *sortClause, *targetlist, true);

        /*
		 * Now, DISTINCT list consists of all non-resjunk sortlist items.
		 * Actually, all the sortlist items had better be non-resjunk!
		 * Otherwise, user wrote SELECT DISTINCT with an ORDER BY item that
		 * does not appear anywhere in the SELECT targetlist, and we can't
		 * implement that with only one sorting pass...
		 */
        foreach (slitem, *sortClause)
        {
            SortClause * scl = (SortClause *)lfirst(slitem);
            PGTargetEntry * tle = get_sortgroupclause_tle(scl, *targetlist);

            if (tle->resjunk)
                ereport(
                    ERROR,
                    (errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
                     errmsg("for SELECT DISTINCT, ORDER BY expressions must appear in select list"),
                     errOmitLocation(true)));
            else
                result = lappend(result, copyObject(scl));
        }
    }
    else
    {
        /* We had SELECT DISTINCT ON (expr, ...) */

        /*
		 * If the user writes both DISTINCT ON and ORDER BY, then the two
		 * expression lists must match (until one or the other runs out).
		 * Otherwise the ORDER BY requires a different sort order than the
		 * DISTINCT does, and we can't implement that with only one sort pass
		 * (and if we do two passes, the results will be rather
		 * unpredictable). However, it's OK to have more DISTINCT ON
		 * expressions than ORDER BY expressions; we can just add the extra
		 * DISTINCT values to the sort list, much as we did above for ordinary
		 * DISTINCT fields.
		 *
		 * Actually, it'd be OK for the common prefixes of the two lists to
		 * match in any order, but implementing that check seems like more
		 * trouble than it's worth.
		 */
        PGListCell * nextsortlist = list_head(*sortClause);

        foreach (dlitem, distinctlist)
        {
            PGTargetEntry * tle;

            tle = findTargetlistEntrySQL92(pstate, lfirst(dlitem), targetlist, DISTINCT_ON_CLAUSE);

            if (nextsortlist != NULL)
            {
                SortClause * scl = (SortClause *)lfirst(nextsortlist);

                if (tle->ressortgroupref != scl->tleSortGroupRef)
                    ereport(
                        ERROR,
                        (errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
                         errmsg("SELECT DISTINCT ON expressions must match initial ORDER BY expressions"),
                         errOmitLocation(true)));
                result = lappend(result, copyObject(scl));
                nextsortlist = lnext(nextsortlist);
            }
            else
            {
                *sortClause = addTargetToSortList(pstate, tle, *sortClause, *targetlist, SORTBY_ASC, NIL, true);

                /*
				 * Probably, the tle should always have been added at the end
				 * of the sort list ... but search to be safe.
				 */
                foreach (slitem, *sortClause)
                {
                    SortClause * scl = (SortClause *)lfirst(slitem);

                    if (tle->ressortgroupref == scl->tleSortGroupRef)
                    {
                        result = lappend(result, copyObject(scl));
                        break;
                    }
                }
                if (slitem == NULL) /* should not happen */
                    elog(ERROR, "failed to add DISTINCT ON clause to target list");
            }
        }
    }

    return result;
};

PGNode * ClauseParser::transformLimitClause(PGParseState * pstate,
        PGNode * clause, const char * constructName)
{
    PGNode * qual;

    if (clause == NULL)
        return NULL;

    qual = expr_parser.transformExpr(pstate, clause);

    qual = coerce_parser.coerce_to_bigint(pstate, qual, constructName);

    /*
	 * LIMIT can't refer to any vars or aggregates of the current query; we
	 * don't allow subselects either (though that case would at least be
	 * sensible)
	 */
    if (contain_vars_of_level(qual, 0))
    {
        ereport(
            ERROR,
            (errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
             /* translator: %s is name of a SQL construct, eg LIMIT */
             errmsg("argument of %s must not contain variables", constructName)));
    }
    if (checkExprHasAggs(qual))
    {
        ereport(
            ERROR,
            (errcode(ERRCODE_GROUPING_ERROR),
             /* translator: %s is name of a SQL construct, eg LIMIT */
             errmsg("argument of %s must not contain aggregates", constructName)));
    }
    if (contain_subplans(qual))
    {
        ereport(
            ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
             /* translator: %s is name of a SQL construct, eg LIMIT */
             errmsg("argument of %s must not contain subqueries", constructName)));
    }

    return qual;
};

}