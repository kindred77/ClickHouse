#include <Interpreters/orcaopt/pgopt_hawq/CTEParser.h>

namespace DB
{
using namespace duckdb_libpgquery;

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

    queryList = select_parser.parse_sub_analyze(cte->ctequery, pstate);
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
    if (withClause == NULL)
        return NULL;

    if (withClause->recursive)
    {
        ereport(ERROR, (errcode(ERRCODE_GP_FEATURE_NOT_SUPPORTED), errmsg("RECURSIVE option in WITH clause is not supported")));
    }

    /* Only one WITH clause per query level */
    Assert(pstate->p_ctenamespace == NIL);
    Assert(pstate->p_future_ctes == NIL);

    /*
	 * Check if CTE list in the WITH clause contains duplicate query names.
	 * If so, error out.
	 *
	 * Also, initialize other variables in CommonTableExpr.
	 */
    PGListCell * lc;
    foreach (lc, withClause->ctes)
    {
        PGCommonTableExpr * cte = (PGCommonTableExpr *)lfirst(lc);

        PGListCell * lc2;
        for_each_cell(lc2, lnext(lc))
        {
            PGCommonTableExpr * cte2 = (PGCommonTableExpr *)lfirst(lc2);
            Assert(cte != NULL && cte2 != NULL && cte->ctename != NULL && cte2->ctename != NULL);

            if (strcmp(cte->ctename, cte2->ctename) == 0)
            {
                ereport(
                    ERROR,
                    (errcode(ERRCODE_DUPLICATE_ALIAS),
                     errmsg("query name \"%s\" in WITH clause must not be specified more than once", cte2->ctename),
                     parser_errposition(pstate, cte2->location)));
            }
        }

        cte->cterecursive = false;
        cte->cterefcount = 0;
    }

    /*
	 * For non-recursive WITH, just analyze each CTE in sequence and then
	 * add it to the ctenamespace.	This corresponds to the spec's
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

    return pstate->p_ctenamespace;
};

PGCommonTableExpr * CTEParser::GetCTEForRTE(PGParseState * pstate,
		PGRangeTblEntry * rte, int rtelevelsup)
{
    Assert(pstate != NULL && rte != NULL);
    Assert(rte->rtekind == PG_RTE_CTE);

    /* Determine RTE's levelsup if caller didn't know it */
    if (rtelevelsup < 0)
        (void)relation_parser.RTERangeTablePosn(pstate, rte, &rtelevelsup);

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