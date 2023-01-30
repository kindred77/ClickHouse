#include <Interpreters/orcaopt/pgoptnew/SelectParser.h>

using namespace duckdb_libpgquery;

namespace DB
{

PGFromExpr * SelectParser::makeFromExpr(PGList *fromlist,
        PGNode *quals)
{
    PGFromExpr * f = makeNode(PGFromExpr);

    f->fromlist = fromlist;
    f->quals = quals;
    return f;
};

PGQuery * SelectParser::transformSelectStmt(PGParseState * pstate,
        PGSelectStmt * stmt)
{
    PGQuery * qry = makeNode(PGQuery);
    PGNode * qual;
    PGListCell * l;

    qry->commandType = PG_CMD_SELECT;

    /* setup database name for use of magma operations */
    MemoryContext oldContext = MemoryContextSwitchTo(MessageContext);
    database = get_database_name(MyDatabaseId);
    MemoryContextSwitchTo(oldContext);

    /* process the WITH clause */
    if (stmt->withClause != NULL)
    {
        qry->hasRecursive = stmt->withClause->recursive;
        qry->cteList = cte_parser.transformWithClause(pstate, stmt->withClause);
        qry->hasModifyingCTE = pstate->p_hasModifyingCTE;
    }

    /* make FOR UPDATE/FOR SHARE info available to addRangeTableEntry */
    pstate->p_locking_clause = stmt->lockingClause;

    /*
   * Put WINDOW clause data into pstate so that window references know
   * about them.
   */
    pstate->p_win_clauses = stmt->windowClause;

    /* process the FROM clause */
    clause_parser.transformFromClause(pstate, stmt->fromClause);

    /* tidy up expressions in window clauses */
    agg_parser.transformWindowSpecExprs(pstate);

    /* transform targetlist */
    qry->targetList = target_parser.transformTargetList(pstate, stmt->targetList);

    /* mark column origins */
    target_parser.markTargetListOrigins(pstate, qry->targetList);

    /* transform WHERE */
    qual = clause_parser.transformWhereClause(pstate, stmt->whereClause, "WHERE");

    /*
   * Initial processing of HAVING clause is just like WHERE clause.
   */
    pstate->having_qual = clause_parser.transformWhereClause(pstate, stmt->havingClause, "HAVING");

    /*
   * CDB: Untyped Const or Param nodes in a subquery in the FROM clause
   * might have been assigned proper types when we transformed the WHERE
   * clause, targetlist, etc.  Bring targetlist Var types up to date.
   */
    coerce_parser.fixup_unknown_vars_in_targetlist(pstate, qry->targetList);

    /*
   * Transform sorting/grouping stuff.  Do ORDER BY first because both
   * transformGroupClause and transformDistinctClause need the results.
   */
    qry->sortClause = clause_parser.transformSortClause(
        pstate,
        stmt->sortClause,
        &qry->targetList,
        true, /* fix unknowns */
        false /* use SQL92 rules */);

    qry->groupClause = clause_parser.transformGroupClause(pstate, stmt->groupClause, &qry->targetList, qry->sortClause, false /* useSQL92 rules */);

    /*
   * SCATTER BY clause on a table function TableValueExpr subquery.
   *
   * Note: a given subquery cannot have both a SCATTER clause and an INTO
   * clause, because both of those control distribution.  This should not
   * possible due to grammar restrictions on where a SCATTER clause is
   * allowed.
   */
    Insist(!(stmt->scatterClause && stmt->intoClause));
    qry->scatterClause = clause_parser.transformScatterClause(pstate, stmt->scatterClause, &qry->targetList);

    /* Having clause */
    qry->havingQual = pstate->having_qual;
    pstate->having_qual = NULL;

    /*
   * Process WINDOW clause.
   */
    clause_parser.transformWindowClause(pstate, qry);

    qry->distinctClause = clause_parser.transformDistinctClause(pstate, stmt->distinctClause, &qry->targetList, &qry->sortClause, &qry->groupClause);

    qry->limitOffset = clause_parser.transformLimitClause(pstate, stmt->limitOffset, "OFFSET");
    qry->limitCount = clause_parser.transformLimitClause(pstate, stmt->limitCount, "LIMIT");

    /* CDB: Cursor position not available for errors below this point. */
    pstate->p_breadcrumb.node = NULL;

    /* handle any SELECT INTO/CREATE TABLE AS spec */
    qry->intoClause = NULL;
    if (stmt->intoClause)
    {
        qry->intoClause = stmt->intoClause;
        if (stmt->intoClause->colNames)
            applyColumnNames(qry->targetList, stmt->intoClause->colNames);
        /* XXX XXX:		qry->partitionBy = stmt->partitionBy; */
    }

    /*
   * Generally, we'll only have a distributedBy clause if stmt->into is set,
   * with the exception of set op queries, since transformSetOperationStmt()
   * sets stmt->into to NULL to avoid complications elsewhere.
   */
    if (Gp_role == GP_ROLE_DISPATCH)
        setQryDistributionPolicy(stmt, qry);

    qry->rtable = pstate->p_rtable;
    qry->jointree = makeFromExpr(pstate->p_joinlist, qual);

    qry->hasSubLinks = pstate->p_hasSubLinks;
    qry->hasAggs = pstate->p_hasAggs;
    if (pstate->p_hasAggs || qry->groupClause || qry->havingQual)
        parseCheckAggregates(pstate, qry);

    if (pstate->p_hasTblValueExpr)
        parseCheckTableFunctions(pstate, qry);

    qry->hasWindFuncs = pstate->p_hasWindFuncs;
    if (pstate->p_hasWindFuncs)
        parseProcessWindFuncs(pstate, qry);

    foreach (l, stmt->lockingClause)
    {
        /* disable select for update/share for gpsql */
        ereport(ERROR, (errcode(ERRCODE_CDB_FEATURE_NOT_YET), errmsg("Cannot support select for update/share statement yet")));
        /*transformLockingClause(qry, (LockingClause *) lfirst(l));*/
    }

    /*
   * If the query mixes window functions and aggregates, we need to
   * transform it such that the grouped query appears as a subquery
   */
    if (qry->hasWindFuncs && (qry->groupClause || qry->hasAggs))
        transformGroupedWindows(qry);

    return qry;
};

PGQuery * SelectParser::transformStmt(
        PGParseState * pstate,
        PGNode * parseTree,
        PGList ** extras_before,
        PGList ** extras_after)
{
    PGQuery * result = NULL;

    switch (nodeTag(parseTree))
    {
        /*
     * Non-optimizable statements
     */
        // case T_CreateStmt:
        //     result = transformCreateStmt(pstate, (CreateStmt *)parseTree, extras_before, extras_after);
        //     break;

        // case T_CreateExternalStmt:
        //     result = transformCreateExternalStmt(pstate, (CreateExternalStmt *)parseTree, extras_before, extras_after);
        //     break;

        // case T_CreateForeignStmt:
        //     result = transformCreateForeignStmt(pstate, (CreateForeignStmt *)parseTree, extras_before, extras_after);
        //     break;

        // case T_IndexStmt:
        //     result = transformIndexStmt(pstate, (IndexStmt *)parseTree, extras_before, extras_after);
        //     break;

        // case T_RuleStmt:
        //     result = transformRuleStmt(pstate, (RuleStmt *)parseTree, extras_before, extras_after);
        //     break;

        // case T_ViewStmt:
        //     result = transformViewStmt(pstate, (ViewStmt *)parseTree, extras_before, extras_after);
        //     break;

        // case T_ExplainStmt: {
        //     ExplainStmt * n = (ExplainStmt *)parseTree;

        //     result = makeNode(Query);
        //     result->commandType = CMD_UTILITY;
        //     n->query = transformStmt(pstate, (Node *)n->query, extras_before, extras_after);
        //     result->utilityStmt = (Node *)parseTree;
        // }
        // break;

    //     case T_CopyStmt: {
    //         CopyStmt * n = (CopyStmt *)parseTree;

    //         /*
    //    * Check if we need to create an error table. If so, add it to the
    //    * before list.
    //    */
    //         if (n->sreh && ((SingleRowErrorDesc *)n->sreh)->errtable)
    //         {
    //             CreateStmtContext cxt;
    //             cxt.blist = NIL;
    //             cxt.alist = NIL;

    //             transformSingleRowErrorHandling(pstate, &cxt, (SingleRowErrorDesc *)n->sreh);
    //             *extras_before = list_concat(*extras_before, cxt.blist);
    //         }

    //         result = makeNode(Query);
    //         result->commandType = CMD_UTILITY;
    //         if (n->query)
    //             n->query = transformStmt(pstate, (Node *)n->query, extras_before, extras_after);
    //         result->utilityStmt = (Node *)parseTree;
    //     }
    //     break;

    //     case T_AlterTableStmt:
    //         result = transformAlterTableStmt(pstate, (AlterTableStmt *)parseTree, extras_before, extras_after);
    //         break;

    //     case T_PrepareStmt:
    //         result = transformPrepareStmt(pstate, (PrepareStmt *)parseTree);
    //         break;

    //     case T_ExecuteStmt:
    //         result = transformExecuteStmt(pstate, (ExecuteStmt *)parseTree);
    //         break;

        /*
     * Optimizable statements
     */
        // case T_InsertStmt:
        //     result = transformInsertStmt(pstate, (InsertStmt *)parseTree, extras_before, extras_after);
        //     break;

        // case T_DeleteStmt:
        //     result = transformDeleteStmt(pstate, (DeleteStmt *)parseTree);
        //     break;

        // case T_UpdateStmt:
        //     result = transformUpdateStmt(pstate, (UpdateStmt *)parseTree);
        //     break;

        case T_SelectStmt: {
            PGSelectStmt * n = (PGSelectStmt *)parseTree;

            if (n->valuesLists)
                //result = transformValuesClause(pstate, n);
            else if (n->op == SETOP_NONE)
                result = transformSelectStmt(pstate, n);
            else
                //result = transformSetOperationStmt(pstate, n);
        }
        break;

        // case T_DeclareCursorStmt:
        //     result = transformDeclareCursorStmt(pstate, (DeclareCursorStmt *)parseTree);
        //     break;

        default:

            /*
       * other statements don't require any transformation; just return
       * the original parsetree with a Query node plastered on top.
       */
            result = makeNode(PGQuery);
            result->commandType = PG_CMD_UTILITY;
            result->utilityStmt = (PGNode *)parseTree;
            break;
    }

    /* Mark as original query until we learn differently */
    result->querySource = PG_QSRC_ORIGINAL;
    result->canSetTag = true;

    /*
   * Check that we did not produce too many resnos; at the very least we
   * cannot allow more than 2^16, since that would exceed the range of a
   * AttrNumber. It seems safest to use MaxTupleAttributeNumber.
   */
    if (pstate->p_next_resno - 1 > MaxTupleAttributeNumber)
        ereport(
            ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED), errmsg("target lists can have at most %d entries", MaxTupleAttributeNumber)));

    return result;
};

PGList *
SelectParser::do_parse_analyze(PGNode *parseTree, PGParseState *pstate)
{
    PGList * result = NIL;

    /* Lists to return extra commands from transformation */
    PGList * extras_before = NIL;
    PGList * extras_after = NIL;
    PGList * tmp = NIL;
    PGQuery * query;
    PGListCell * l;

    ErrorContextCallback errcontext;

    /* CDB: Request a callback in case ereport or elog is called. */
    errcontext.callback = parse_analyze_error_callback;
    errcontext.arg = pstate;
    errcontext.previous = error_context_stack;
    error_context_stack = &errcontext;

    query = transformStmt(pstate, parseTree, &extras_before, &extras_after);

    /* CDB: Pop error context callback stack. */
    error_context_stack = errcontext.previous;

    /* CDB: All breadcrumbs should have been popped. */
    Assert(!pstate->p_breadcrumb.pop);

    /* don't need to access result relation any more */
    release_pstate_resources(pstate);

    foreach (l, extras_before)
        result = list_concat(result, parse_sub_analyze(lfirst(l), pstate));

    result = lappend(result, query);

    foreach (l, extras_after)
        tmp = list_concat(tmp, parse_sub_analyze(lfirst(l), pstate));

    /*
   * If this is the top level query and it is a CreateStmt/CreateExternalStmt
   * and it has a partition by clause, reorder the expanded extras_after so
   * that AlterTable is able to build the partitioning hierarchy
   * better. The problem with the existing list is that for
   * subpartitioned tables, the subpartitions will be added to the
   * hierarchy before the root, which means we cannot get the parent
   * oid of rules.
   *
   * nefarious: special KeepMe case in cdbpartition.c:atpxPart_validate_spec
   */
    if (pstate->parentParseState == NULL && query->utilityStmt
        && (IsA(query->utilityStmt, CreateStmt) && ((CreateStmt *)query->utilityStmt)->base.partitionBy
            || IsA(query->utilityStmt, CreateExternalStmt) && ((CreateExternalStmt *)query->utilityStmt)->base.partitionBy))
    {
        /*
     * We just break the statements into two lists: alter statements and
     * other statements.
     */
        PGList * alters = NIL;
        PGList * others = NIL;
        PGQuery ** stmts;
        int i = 0;
        int j;

        foreach (l, tmp)
        {
            PGQuery * q = lfirst(l);

            Assert(IsA(q, PGQuery));

            if (IsA(q->utilityStmt, AlterTableStmt))
                alters = lappend(alters, q);
            else
                others = lappend(others, q);
        }

        Assert(list_length(alters));

        /*
     * Now, sort the ALTER statements so that the deeper partition members
     * are processed last.
     */
        stmts = palloc(list_length(alters) * sizeof(PGQuery *));
        foreach (l, alters)
            stmts[i++] = (PGQuery *)lfirst(l);

        qsort(stmts, i, sizeof(void *), alter_cmp);

        list_free(alters);
        alters = NIL;
        for (j = 0; j < i; j++)
        {
            AlterTableStmt * n;
            alters = lappend(alters, stmts[j]);

            n = (AlterTableStmt *)((PGQuery *)stmts[j])->utilityStmt;
        }
        result = list_concat(result, others);
        result = list_concat(result, alters);
    }
    else
        result = list_concat(result, tmp);

    /*
   * Make sure that only the original query is marked original. We have to
   * do this explicitly since recursive calls of do_parse_analyze will have
   * marked some of the added-on queries as "original".  Also mark only the
   * original query as allowed to set the command-result tag.
   */
    foreach (l, result)
    {
        PGQuery * q = lfirst(l);

        if (q == query)
        {
            q->querySource = QSRC_ORIGINAL;
            q->canSetTag = true;
        }
        else
        {
            q->querySource = QSRC_PARSER;
            q->canSetTag = false;
        }
    }

    return result;
};

PGList *
SelectParser::parse_analyze(PGNode *parseTree, const char *sourceText, Oid *paramTypes,
                    int numParams)
{
	PGParseState *pstate = palloc0(sizeof(PGParseState));
    pstate->parentParseState = parentParseState;

    /* Fill in fields that don't start at null/false/zero */
    pstate->p_next_resno = 1;

    if (parentParseState)
    {
        pstate->p_sourcetext = parentParseState->p_sourcetext;
        pstate->p_variableparams = parentParseState->p_variableparams;
        pstate->p_setopTypes = parentParseState->p_setopTypes;
        pstate->p_setopTypmods = parentParseState->p_setopTypmods;
    }

    PGList * result;

    pstate->p_sourcetext = sourceText;
    pstate->p_paramtypes = paramTypes;
    pstate->p_numparams = numParams;
    pstate->p_variableparams = false;

    result = do_parse_analyze(parseTree, pstate);

    free_parsestate(&pstate);

    return result;
};

}