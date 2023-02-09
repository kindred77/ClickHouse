#include <Interpreters/orcaopt/CTEParser.h>

#include <Interpreters/orcaopt/RelationParser.h>
#include <Interpreters/orcaopt/NodeParser.h>
#include <Interpreters/orcaopt/SelectParser.h>

using namespace duckdb_libpgquery;

namespace DB
{
void CTEParser::reportDuplicateNames(const char *queryName, PGList *names)
{
    if (names == NULL)
        return;

    PGListCell * lc;
    foreach (lc, names)
    {
        PGValue * string = (PGValue *)lfirst(lc);
        Assert(IsA(string, PGString));

        PGListCell * rest;
        for_each_cell(rest, lnext(lc))
        {
            PGValue * string2 = (PGValue *)lfirst(rest);
            Assert(IsA(string, PGString));

            if (strcmp(strVal(string), strVal(string2)) == 0)
            {
                ereport(
                    ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                     errmsg("WITH query \"%s\" must not have duplicate column name: %s", queryName, strVal(string)),
                     errhint("Specify a column list without duplicate names")));
            }
        }
    }
};

void CTEParser::analyzeCTETargetList(PGParseState *pstate, PGCommonTableExpr *cte,
		PGList *tlist)
{
    Assert(cte->ctecolnames == NIL);

    /*
	 * We need to determine column names, types, and typmods.  The alias
	 * column names override anything coming from the query itself.  (Note:
	 * the SQL spec says that the alias list must be empty or exactly as long
	 * as the output column set. Also, the alias can not have the same name.
	 * We report errors if this is not the case.)
	 * 
	 */
    cte->ctecolnames = copyObject(cte->aliascolnames);
    cte->ctecoltypes = cte->ctecoltypmods = NIL;
    int numaliases = list_length(cte->aliascolnames);

    int varattno = 0;
    PGListCell * tlistitem;
    foreach (tlistitem, tlist)
    {
        PGTargetEntry * te = (PGTargetEntry *)lfirst(tlistitem);
        Oid coltype;
        int32 coltypmod;

        if (te->resjunk)
            continue;
        varattno++;
        Assert(varattno == te->resno);
        if (varattno > numaliases)
        {
            if (numaliases > 0)
            {
                ereport(
                    ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                     errmsg(ERRMSG_GP_WITH_COLUMNS_MISMATCH, cte->ctename),
                     parser_errposition(pstate, cte->location)));
            }

            char * attrname;

            attrname = pstrdup(te->resname);
            cte->ctecolnames = lappend(cte->ctecolnames, makeString(attrname));
        }
        coltype = exprType((Node *)te->expr);
        coltypmod = exprTypmod((Node *)te->expr);

        cte->ctecoltypes = lappend_oid(cte->ctecoltypes, coltype);
        cte->ctecoltypmods = lappend_int(cte->ctecoltypmods, coltypmod);
    }

    if (varattno < numaliases)
    {
        ereport(
            ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR),
             errmsg(ERRMSG_GP_WITH_COLUMNS_MISMATCH, cte->ctename),
             parser_errposition(pstate, cte->location)));
    }

    reportDuplicateNames(cte->ctename, cte->ctecolnames);
};

void CTEParser::analyzeCTE(PGParseState *pstate, PGCommonTableExpr *cte)
{
    Assert(cte != NULL);
    Assert(cte->ctequery != NULL);
    Assert(!IsA(cte->ctequery, PGQuery));

    PGList * queryList;

    queryList = select_parser_ptr->parse_sub_analyze(cte->ctequery, pstate);
    Assert(list_length(queryList) == 1);

    PGQuery * query = (PGQuery *)linitial(queryList);
    cte->ctequery = (PGNode *)query;

    /* Check if the query is what we expected. */
    if (!IsA(query, PGQuery))
        elog(ERROR, "unexpected non-Query statement in WITH clause");
    if (query->utilityStmt != NULL)
        elog(ERROR, "unexpected utility statement in WITH clause");

    if (query->intoClause)
        ereport(
            ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR),
             errmsg("query defined in WITH clause cannot have SELECT INTO"),
             parser_errposition(pstate, exprLocation((Node *)query->intoClause))));

    /* CTE queries are always marked as not canSetTag */
    query->canSetTag = false;

    /* Compute the column types, typmods. */
    analyzeCTETargetList(pstate, cte, GetCTETargetList(cte));
};

PGList *
CTEParser::transformWithClause(PGParseState *pstate, PGWithClause *withClause)
{
    PGListCell * lc;

    /* Only one WITH clause per query level */
    Assert(pstate->p_ctenamespace == NIL);
    Assert(pstate->p_future_ctes == NIL);

    /*
	 * WITH RECURSIVE is disabled if gp_recursive_cte is not set
	 * to allow recursive CTEs.
	 */
    if (withClause->recursive && !gp_recursive_cte)
        ereport(
            ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
             errmsg("RECURSIVE clauses in WITH queries are currently disabled"),
             errhint("In order to use recursive CTEs, \"gp_recursive_cte\" must be turned on.")));

    /*
	 * For either type of WITH, there must not be duplicate CTE names in the
	 * list.  Check this right away so we needn't worry later.
	 *
	 * Also, tentatively mark each CTE as non-recursive, and initialize its
	 * reference count to zero, and set pstate->p_hasModifyingCTE if needed.
	 */
    foreach (lc, withClause->ctes)
    {
        PGCommonTableExpr * cte = (PGCommonTableExpr *)lfirst(lc);
        PGListCell * rest;

        for_each_cell(rest, lnext(lc))
        {
            PGCommonTableExpr * cte2 = (PGCommonTableExpr *)lfirst(rest);

            if (strcmp(cte->ctename, cte2->ctename) == 0)
                ereport(
                    ERROR,
                    (errcode(ERRCODE_DUPLICATE_ALIAS),
                     errmsg("WITH query name \"%s\" specified more than once", cte2->ctename),
                     parser_errposition(pstate, cte2->location)));
        }

        cte->cterecursive = false;
        cte->cterefcount = 0;

        if (!IsA(cte->ctequery, PGSelectStmt))
        {
            /* must be a data-modifying statement */
            Assert(IsA(cte->ctequery, PGInsertStmt) || IsA(cte->ctequery, PGUpdateStmt) || IsA(cte->ctequery, PGDeleteStmt));


            /*
			 * Since GPDB currently only support a single writer gang, only one
			 * writable clause is permitted per CTE. Once we get flexible gangs
			 * with more than one writer gang we can lift this restriction.
			 */
            if (pstate->p_hasModifyingCTE)
                ereport(
                    ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg("only one modifying WITH clause allowed per query"),
                     errdetail("Greenplum Database currently only support CTEs with one writable clause."),
                     errhint("Rewrite the query to only include one writable CTE clause.")));

            pstate->p_hasModifyingCTE = true;
        }
    }

    if (withClause->recursive)
    {
        /*
		 * For WITH RECURSIVE, we rearrange the list elements if needed to
		 * eliminate forward references.  First, build a work array and set up
		 * the data structure needed by the tree walkers.
		 */
        CteState cstate;
        int i;

        cstate.pstate = pstate;
        cstate.numitems = list_length(withClause->ctes);
        cstate.items = (CteItem *)palloc0(cstate.numitems * sizeof(CteItem));
        i = 0;
        foreach (lc, withClause->ctes)
        {
            cstate.items[i].cte = (PGCommonTableExpr *)lfirst(lc);
            cstate.items[i].id = i;
            i++;
        }

        /*
		 * Find all the dependencies and sort the CteItems into a safe
		 * processing order.  Also, mark CTEs that contain self-references.
		 */
        makeDependencyGraph(&cstate);

        /*
		 * Check that recursive queries are well-formed.
		 */
        checkWellFormedRecursion(&cstate);

        /*
		 * Set up the ctenamespace for parse analysis.  Per spec, all the WITH
		 * items are visible to all others, so stuff them all in before parse
		 * analysis.  We build the list in safe processing order so that the
		 * planner can process the queries in sequence.
		 */
        for (i = 0; i < cstate.numitems; i++)
        {
            PGCommonTableExpr * cte = cstate.items[i].cte;

            pstate->p_ctenamespace = lappend(pstate->p_ctenamespace, cte);
        }

        /*
		 * Do parse analysis in the order determined by the topological sort.
		 */
        for (i = 0; i < cstate.numitems; i++)
        {
            PGCommonTableExpr * cte = cstate.items[i].cte;

            analyzeCTE(pstate, cte);
        }
    }
    else
    {
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
            PGCommonTableExpr * cte = (PGCommonTableExpr *)lfirst(lc);

            analyzeCTE(pstate, cte);
            pstate->p_ctenamespace = lappend(pstate->p_ctenamespace, cte);
            pstate->p_future_ctes = list_delete_first(pstate->p_future_ctes);
        }
    }

    return pstate->p_ctenamespace;
};

PGCommonTableExpr * CTEParser::GetCTEForRTE(PGParseState * pstate,
		PGRangeTblEntry * rte, int rtelevelsup)
{
    Assert(pstate != NULL && rte != NULL);
    Assert(rte->rtekind == PG_RTE_CTE);

    /* Determine RTE's levelsup if caller didn't know it */
    if (rtelevelsup < 0)
        (void)relation_parser_ptr->RTERangeTablePosn(pstate, rte, &rtelevelsup);

    Index levelsup = rte->ctelevelsup + rtelevelsup;
    while (levelsup > 0)
    {
        pstate = pstate->parentParseState;
        Assert(pstate != NULL);
        levelsup--;
    }

    if (pstate->p_ctenamespace == NULL)
        return NULL;

    PGListCell * lc;
    foreach (lc, pstate->p_ctenamespace)
    {
        PGCommonTableExpr * cte = (PGCommonTableExpr *)lfirst(lc);
        if (strcmp(cte->ctename, rte->ctename) == 0)
        {
            return cte;
        }
    }

    /* shouldn't happen */
    elog(ERROR, "unexpected error while parsing WITH query \"%s\"", rte->ctename);
    return NULL;
};

}
