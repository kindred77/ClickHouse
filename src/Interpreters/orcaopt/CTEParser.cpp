#include <Interpreters/orcaopt/CTEParser.h>

#include <Interpreters/orcaopt/RelationParser.h>
#include <Interpreters/orcaopt/NodeParser.h>
#include <Interpreters/orcaopt/SelectParser.h>
#include <Interpreters/orcaopt/provider/TypeProvider.h>

#include <Interpreters/Context.h>

using namespace duckdb_libpgquery;

namespace DB
{

CTEParser::CTEParser(const ContextPtr& context_) : context(context_)
{
	relation_parser = std::make_shared<RelationParser>(context);
	node_parser = std::make_shared<NodeParser>(context);
	select_parser = std::make_shared<SelectParser>(context);
	type_provider = std::make_shared<TypeProvider>(context);
};

void CTEParser::analyzeCTETargetList(PGParseState * pstate, PGCommonTableExpr * cte, PGList * tlist)
{
    int numaliases;
    int varattno;
    PGListCell * tlistitem;

    /* Not done already ... */
    Assert(cte->ctecolnames == NIL)

    /*
	 * We need to determine column names, types, and collations.  The alias
	 * column names override anything coming from the query itself.  (Note:
	 * the SQL spec says that the alias list must be empty or exactly as long
	 * as the output column set; but we allow it to be shorter for consistency
	 * with Alias handling.)
	 */
    cte->ctecolnames = (PGList *)copyObject(cte->aliascolnames);
    cte->ctecoltypes = cte->ctecoltypmods = cte->ctecolcollations = NIL;
    numaliases = list_length(cte->aliascolnames);
    varattno = 0;
    foreach (tlistitem, tlist)
    {
        PGTargetEntry * te = (PGTargetEntry *)lfirst(tlistitem);
        PGOid coltype;
        int32 coltypmod;
        PGOid colcoll;

        if (te->resjunk)
            continue;
        varattno++;
        Assert(varattno == te->resno)
        if (varattno > numaliases)
        {
            char * attrname;

            attrname = pstrdup(te->resname);
            cte->ctecolnames = lappend(cte->ctecolnames, makeString(attrname));
        }
        coltype = exprType((PGNode *)te->expr);
        coltypmod = exprTypmod((PGNode *)te->expr);
        colcoll = exprCollation((PGNode *)te->expr);

        /*
		 * If the CTE is recursive, force the exposed column type of any
		 * "unknown" column to "text".  This corresponds to the fact that
		 * SELECT 'foo' UNION SELECT 'bar' will ultimately produce text. We
		 * might see "unknown" as a result of an untyped literal in the
		 * non-recursive term's select list, and if we don't convert to text
		 * then we'll have a mismatch against the UNION result.
		 *
		 * The column might contain 'foo' COLLATE "bar", so don't override
		 * collation if it's already set.
		 */
        if (cte->cterecursive && coltype == UNKNOWNOID)
        {
            coltype = TEXTOID;
            coltypmod = -1; /* should be -1 already, but be sure */
            if (!OidIsValid(colcoll))
                colcoll = DEFAULT_COLLATION_OID;
        }
        cte->ctecoltypes = lappend_oid(cte->ctecoltypes, coltype);
        cte->ctecoltypmods = lappend_int(cte->ctecoltypmods, coltypmod);
        cte->ctecolcollations = lappend_oid(cte->ctecolcollations, colcoll);
    }
    if (varattno < numaliases)
        ereport(
            ERROR,
            (errcode(PG_ERRCODE_INVALID_COLUMN_REFERENCE),
             errmsg("WITH query \"%s\" has %d columns available but %d columns specified", cte->ctename, varattno, numaliases),
             parser_errposition(pstate, cte->location)));
};

void CTEParser::analyzeCTE(PGParseState * pstate, PGCommonTableExpr * cte)
{
    PGQuery * query;

    /* Analysis not done already */
    Assert(!IsA(cte->ctequery, PGQuery))

    query = select_parser->parse_sub_analyze(cte->ctequery, pstate, cte, NULL);
    cte->ctequery = (PGNode *)query;

    /*
	 * Check that we got something reasonable.  These first two cases should
	 * be prevented by the grammar.
	 */
    if (!IsA(query, PGQuery))
        elog(ERROR, "unexpected non-Query statement in WITH");
    if (query->utilityStmt != NULL)
        elog(ERROR, "unexpected utility statement in WITH");

    /*
	 * We disallow data-modifying WITH except at the top level of a query,
	 * because it's not clear when such a modification should be executed.
	 */
    if (query->commandType != PG_CMD_SELECT && pstate->parentParseState != NULL)
        ereport(
            ERROR,
            (errcode(PG_ERRCODE_FEATURE_NOT_SUPPORTED),
             errmsg("WITH clause containing a data-modifying statement must be at the top level"),
             parser_errposition(pstate, cte->location)));

    /*
	 * CTE queries are always marked not canSetTag.  (Currently this only
	 * matters for data-modifying statements, for which the flag will be
	 * propagated to the ModifyTable plan node.)
	 */
    query->canSetTag = false;

    if (!cte->cterecursive)
    {
        /* Compute the output column names/types if not done yet */
        analyzeCTETargetList(pstate, cte, GetCTETargetList(cte));
    }
    else
    {
        /*
		 * Verify that the previously determined output column types and
		 * collations match what the query really produced.  We have to check
		 * this because the recursive term could have overridden the
		 * non-recursive term, and we don't have any easy way to fix that.
		 */
        ListCell *lctlist, *lctyp, *lctypmod, *lccoll;
        int varattno;

        lctyp = list_head(cte->ctecoltypes);
        lctypmod = list_head(cte->ctecoltypmods);
        lccoll = list_head(cte->ctecolcollations);
        varattno = 0;
        foreach (lctlist, GetCTETargetList(cte))
        {
            PGTargetEntry * te = (PGTargetEntry *)lfirst(lctlist);
            PGNode * texpr;

            if (te->resjunk)
                continue;
            varattno++;
            Assert(varattno == te->resno)
            if (lctyp == NULL || lctypmod == NULL || lccoll == NULL) /* shouldn't happen */
                elog(ERROR, "wrong number of output columns in WITH");
            texpr = (PGNode *)te->expr;
            if (exprType(texpr) != lfirst_oid(lctyp) || exprTypmod(texpr) != lfirst_int(lctypmod))
                ereport(
                    ERROR,
                    (errcode(PG_ERRCODE_DATATYPE_MISMATCH),
                     errmsg(
                         "recursive query \"%s\" column %d has type %s in non-recursive term but type %s overall",
                         cte->ctename,
                         varattno,
                         type_provider->format_type_with_typemod(lfirst_oid(lctyp), lfirst_int(lctypmod)).c_str(),
                         type_provider->format_type_with_typemod(exprType(texpr), exprTypmod(texpr)).c_str()),
                     errhint("Cast the output of the non-recursive term to the correct type."),
                     parser_errposition(pstate, exprLocation(texpr))));
            if (exprCollation(texpr) != lfirst_oid(lccoll))
                ereport(
                    ERROR,
                    (errcode(PG_ERRCODE_COLLATION_MISMATCH),
                     errmsg(
                         "recursive query \"%s\" column %d has collation \"%s\" in non-recursive term but collation \"%s\" overall",
                         cte->ctename,
                         varattno/* ,
                         get_collation_name(lfirst_oid(lccoll)),
                         get_collation_name(exprCollation(texpr)) */),
                     errhint("Use the COLLATE clause to set the collation of the non-recursive term."),
                     parser_errposition(pstate, exprLocation(texpr))));
            lctyp = lnext(lctyp);
            lctypmod = lnext(lctypmod);
            lccoll = lnext(lccoll);
        }
        if (lctyp != NULL || lctypmod != NULL || lccoll != NULL) /* shouldn't happen */
            elog(ERROR, "wrong number of output columns in WITH");
    }
};

PGList *
CTEParser::transformWithClause(PGParseState *pstate, PGWithClause *withClause)
{
	PGListCell   *lc;

	/* Only one WITH clause per query level */
	Assert(pstate->p_ctenamespace == NIL)
	Assert(pstate->p_future_ctes == NIL)

	/*
	 * WITH RECURSIVE is disabled if gp_recursive_cte is not set
	 * to allow recursive CTEs.
	 */
	if (withClause->recursive/*  && !gp_recursive_cte */)
		ereport(ERROR,
				(errcode(PG_ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("RECURSIVE clauses in WITH queries are currently disabled"),
				 errhint("In order to use recursive CTEs, \"gp_recursive_cte\" must be turned on.")));

	/*
	 * For either type of WITH, there must not be duplicate CTE names in the
	 * list.  Check this right away so we needn't worry later.
	 *
	 * Also, tentatively mark each CTE as non-recursive, and initialize its
	 * reference count to zero, and set pstate->p_hasModifyingCTE if needed.
	 */
	foreach(lc, withClause->ctes)
	{
		PGCommonTableExpr *cte = (PGCommonTableExpr *) lfirst(lc);
		PGListCell   *rest;

		for_each_cell(rest, lnext(lc))
		{
			PGCommonTableExpr *cte2 = (PGCommonTableExpr *) lfirst(rest);

			if (strcmp(cte->ctename, cte2->ctename) == 0)
			{
				parser_errposition(pstate, cte2->location);
				ereport(ERROR,
						(errcode(PG_ERRCODE_DUPLICATE_ALIAS),
						 errmsg("WITH query name \"%s\" specified more than once",
								cte2->ctename)));
			}
		}

		cte->cterecursive = false;
		cte->cterefcount = 0;

		if (!IsA(cte->ctequery, PGSelectStmt))
		{
			/* must be a data-modifying statement */
			Assert(IsA(cte->ctequery, PGInsertStmt) ||
				   IsA(cte->ctequery, PGUpdateStmt) ||
				   IsA(cte->ctequery, PGDeleteStmt))


			/*
			 * Since GPDB currently only support a single writer gang, only one
			 * writable clause is permitted per CTE. Once we get flexible gangs
			 * with more than one writer gang we can lift this restriction.
			 */
			if (pstate->p_hasModifyingCTE)
				ereport(ERROR,
						(errcode(PG_ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("only one modifying WITH clause allowed per query"),
						 errdetail("Greenplum Database currently only support CTEs with one writable clause."),
						 errhint("Rewrite the query to only include one writable CTE clause.")));

			pstate->p_hasModifyingCTE = true;
		}
	}

	//TODO kindred
	//not support cte recursive yet
	// if (withClause->recursive)
	// {
	// 	/*
	// 	 * For WITH RECURSIVE, we rearrange the list elements if needed to
	// 	 * eliminate forward references.  First, build a work array and set up
	// 	 * the data structure needed by the tree walkers.
	// 	 */
	// 	PGCteState	cstate;
	// 	int			i;

	// 	cstate.pstate = pstate;
	// 	cstate.numitems = list_length(withClause->ctes);
	// 	cstate.items = (PGCteItem *) palloc(cstate.numitems * sizeof(PGCteItem));
	// 	i = 0;
	// 	foreach(lc, withClause->ctes)
	// 	{
	// 		cstate.items[i].cte = (PGCommonTableExpr *) lfirst(lc);
	// 		cstate.items[i].id = i;
	// 		i++;
	// 	}

	// 	/*
	// 	 * Find all the dependencies and sort the CteItems into a safe
	// 	 * processing order.  Also, mark CTEs that contain self-references.
	// 	 */
	// 	makeDependencyGraph(&cstate);

	// 	/*
	// 	 * Check that recursive queries are well-formed.
	// 	 */
	// 	checkWellFormedRecursion(&cstate);

	// 	/*
	// 	 * Set up the ctenamespace for parse analysis.  Per spec, all the WITH
	// 	 * items are visible to all others, so stuff them all in before parse
	// 	 * analysis.  We build the list in safe processing order so that the
	// 	 * planner can process the queries in sequence.
	// 	 */
	// 	for (i = 0; i < cstate.numitems; i++)
	// 	{
	// 		PGCommonTableExpr *cte = cstate.items[i].cte;

	// 		pstate->p_ctenamespace = lappend(pstate->p_ctenamespace, cte);
	// 	}

	// 	/*
	// 	 * Do parse analysis in the order determined by the topological sort.
	// 	 */
	// 	for (i = 0; i < cstate.numitems; i++)
	// 	{
	// 		PGCommonTableExpr *cte = cstate.items[i].cte;

	// 		analyzeCTE(pstate, cte);
	// 	}
	// }
	// else
	// {
		/*
		 * For non-recursive WITH, just analyze each CTE in sequence and then
		 * add it to the ctenamespace.  This corresponds to the spec's
		 * definition of the scope of each WITH name.  However, to allow error
		 * reports to be aware of the possibility of an erroneous reference,
		 * we maintain a list in p_future_ctes of the not-yet-visible CTEs.
		 */
		pstate->p_future_ctes = list_copy(withClause->ctes);

		foreach (lc, withClause->ctes)
		{
			PGCommonTableExpr *cte = (PGCommonTableExpr *) lfirst(lc);

			analyzeCTE(pstate, cte);
			pstate->p_ctenamespace = lappend(pstate->p_ctenamespace, cte);
			pstate->p_future_ctes = list_delete_first(pstate->p_future_ctes);
		}
	// }

	return pstate->p_ctenamespace;
};

}
