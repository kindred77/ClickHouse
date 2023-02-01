#include <Interpreters/orcaopt/pgopt_hawq/RelationParser.h>

using namespace duckdb_libpgquery;

namespace DB
{

int RelationParser::RTERangeTablePosn(PGParseState * pstate, PGRangeTblEntry * rte, int * sublevels_up)
{
    int index;
    PGListCell * l;

    if (sublevels_up)
        *sublevels_up = 0;

    while (pstate != NULL)
    {
        index = 1;
        foreach (l, pstate->p_rtable)
        {
            if (rte == (PGRangeTblEntry *)lfirst(l))
                return index;
            index++;
        }
        pstate = pstate->parentParseState;
        if (sublevels_up)
            (*sublevels_up)++;
        else
            break;
    }

    elog(ERROR, "RTE not found (internal error)");
    return 0; /* keep compiler quiet */
};

PGTargetEntry * RelationParser::get_tle_by_resno(PGList * tlist,
		PGAttrNumber resno)
{
	PGListCell   *l;

	foreach(l, tlist)
	{
		PGTargetEntry *tle = (PGTargetEntry *) lfirst(l);

		if (tle->resno == resno)
			return tle;
	}
	return NULL;
};

PGNode * RelationParser::scanRTEForColumn(PGParseState * pstate,
      PGRangeTblEntry * rte, char * colname, int location)
{
    PGNode * result = NULL;
    int attnum = 0;
    PGListCell * c;

    /*
	 * Scan the user column names (or aliases) for a match. Complain if
	 * multiple matches.
	 *
	 * Note: eref->colnames may include entries for dropped columns, but those
	 * will be empty strings that cannot match any legal SQL identifier, so we
	 * don't bother to test for that case here.
	 *
	 * Should this somehow go wrong and we try to access a dropped column,
	 * we'll still catch it by virtue of the checks in
	 * get_rte_attribute_type(), which is called by make_var().  That routine
	 * has to do a cache lookup anyway, so the check there is cheap.
	 */
    foreach (c, rte->eref->colnames)
    {
        attnum++;
        if (strcmp(strVal(lfirst(c)), colname) == 0)
        {
            if (result)
                ereport(
                    ERROR,
                    (errcode(ERRCODE_AMBIGUOUS_COLUMN),
                     errmsg("column reference \"%s\" is ambiguous", colname),
                     errOmitLocation(true),
                     parser_errposition(pstate, location)));
            result = (PGNode *)make_var(pstate, rte, attnum, location);
            /* Require read access */
            rte->requiredPerms |= ACL_SELECT;
        }
    }

    /*
	 * If we have a unique match, return it.  Note that this allows a user
	 * alias to override a system column name (such as OID) without error.
	 */
    if (result)
        return result;

    /*
	 * If the RTE represents a real table, consider system column names.
	 */
    if (rte->rtekind == PG_RTE_RELATION)
    {
        /* quick check to see if name could be a system column */
        attnum = specialAttNum(colname);
        if (attnum != InvalidAttrNumber)
        {
            /* now check to see if column actually is defined */
            if (caql_getcount(
                    NULL,
                    cql("SELECT COUNT(*) FROM pg_attribute "
                        " WHERE attrelid = :1 "
                        " AND attnum = :2 ",
                        ObjectIdGetDatum(rte->relid),
                        Int16GetDatum(attnum))))
            {
                result = (PGNode *)make_var(pstate, rte, attnum, location);
                /* Require read access */
                rte->requiredPerms |= ACL_SELECT;
            }
        }
    }

    return result;
};

PGNode * RelationParser::colNameToVar(PGParseState * pstate, char * colname,
    bool localonly, int location)
{
    PGNode * result = NULL;
    PGParseState * orig_pstate = pstate;

    while (pstate != NULL)
    {
        PGListCell * l;

        foreach (l, pstate->p_varnamespace)
        {
            PGRangeTblEntry * rte = (PGRangeTblEntry *)lfirst(l);
            PGNode * newresult;

            /* use orig_pstate here to get the right sublevels_up */
            newresult = scanRTEForColumn(orig_pstate, rte, colname, location);

            if (newresult)
            {
                if (result)
                    ereport(
                        ERROR,
                        (errcode(ERRCODE_AMBIGUOUS_COLUMN),
                         errmsg("column reference \"%s\" is ambiguous", colname),
                         errOmitLocation(true),
                         parser_errposition(orig_pstate, location)));
                result = newresult;
            }
        }

        if (result != NULL || localonly)
            break; /* found, or don't want to look at parent */

        pstate = pstate->parentParseState;
    }

    return result;
};

PGRangeTblEntry * RelationParser::GetRTEByRangeTablePosn(
	PGParseState * pstate,
	int varno, int sublevels_up)
{
    while (sublevels_up-- > 0)
    {
        pstate = pstate->parentParseState;
        Assert(pstate != NULL);
    }
    Assert(varno > 0 && varno <= list_length(pstate->p_rtable));
    return rt_fetch(varno, pstate->p_rtable);
};

void RelationParser::expandRTE(PGRangeTblEntry * rte, int rtindex, int sublevels_up,
		int location, bool include_dropped, PGList ** colnames,
		PGList ** colvars)
{
    int varattno;

    if (colnames)
        *colnames = NIL;
    if (colvars)
        *colvars = NIL;

    switch (rte->rtekind)
    {
        case PG_RTE_RELATION:
            /* Ordinary relation RTE */
            expandRelation(rte->relid, rte->eref, rtindex, sublevels_up, include_dropped, colnames, colvars);
            break;
        case PG_RTE_SUBQUERY: {
            /* Subquery RTE */
            PGListCell * aliasp_item = list_head(rte->eref->colnames);
            PGListCell * tlistitem;

            varattno = 0;
            foreach (tlistitem, rte->subquery->targetList)
            {
                PGTargetEntry * te = (PGTargetEntry *)lfirst(tlistitem);

                if (te->resjunk)
                    continue;
                varattno++;
                Assert(varattno == te->resno);

                if (colnames)
                {
                    /* Assume there is one alias per target item */
                    char * label = strVal(lfirst(aliasp_item));

                    *colnames = lappend(*colnames, makeString(pstrdup(label)));
                    aliasp_item = lnext(aliasp_item);
                }

                if (colvars)
                {
                    PGVar * varnode;

                    varnode = makeVar(rtindex, varattno, exprType((PGNode *)te->expr), exprTypmod((PGNode *)te->expr), sublevels_up);

                    *colvars = lappend(*colvars, varnode);
                }
            }
        }
        break;
        case PG_RTE_CTE: {
            PGListCell * aliasp_item = list_head(rte->eref->colnames);
            PGListCell * lct;
            PGListCell * lcm;

            varattno = 0;
            forboth(lct, rte->ctecoltypes, lcm, rte->ctecoltypmods)
            {
                Oid coltype = lfirst_oid(lct);
                int32 coltypmod = lfirst_int(lcm);

                varattno++;

                if (colnames)
                {
                    /* Assume there is one alias per output column */
                    Assert(IsA(lfirst(aliasp_item), String));
                    char * label = strVal(lfirst(aliasp_item));

                    *colnames = lappend(*colnames, makeString(pstrdup(label)));
                    aliasp_item = lnext(aliasp_item);
                }

                if (colvars)
                {
                    PGVar * varnode;

                    varnode = makeVar(rtindex, varattno, coltype, coltypmod, sublevels_up);
                    *colvars = lappend(*colvars, varnode);
                }
            }
        }
        break;

        // case PG_RTE_TABLEFUNCTION:
        case PG_RTE_FUNCTION: {
            /* Function RTE */
            TypeFuncClass functypclass;
            Oid funcrettype;
            TupleDesc tupdesc;

            functypclass = get_expr_result_type(rte->funcexpr, &funcrettype, &tupdesc);
            if (functypclass == TYPEFUNC_COMPOSITE)
            {
                /* Composite data type, e.g. a table's row type */
                Assert(tupdesc);
                expandTupleDesc(tupdesc, rte->eref, rtindex, sublevels_up, include_dropped, colnames, colvars);
            }
            else if (functypclass == TYPEFUNC_SCALAR)
            {
                /* Base data type, i.e. scalar */
                if (colnames)
                    *colnames = lappend(*colnames, linitial(rte->eref->colnames));

                if (colvars)
                {
                    PGVar * varnode;

                    varnode = makeVar(rtindex, 1, funcrettype, -1, sublevels_up);

                    *colvars = lappend(*colvars, varnode);
                }
            }
            else if (functypclass == TYPEFUNC_RECORD)
            {
                if (colnames)
                    *colnames = copyObject(rte->eref->colnames);
                if (colvars)
                {
                    PGListCell * l1;
                    PGListCell * l2;
                    int attnum = 0;

                    forboth(l1, rte->funccoltypes, l2, rte->funccoltypmods)
                    {
                        Oid attrtype = lfirst_oid(l1);
                        int32 attrtypmod = lfirst_int(l2);
                        PGVar * varnode;

                        attnum++;
                        varnode = makeVar(rtindex, attnum, attrtype, attrtypmod, sublevels_up);
                        *colvars = lappend(*colvars, varnode);
                    }
                }
            }
            else
            {
                /* addRangeTableEntryForFunction should've caught this */
                elog(ERROR, "function in FROM has unsupported return type");
            }
        }
        break;
        case PG_RTE_VALUES: {
            /* Values RTE */
            PGListCell * aliasp_item = list_head(rte->eref->colnames);
            PGListCell * lc;

            varattno = 0;
            foreach (lc, (List *)linitial(rte->values_lists))
            {
                PGNode * col = (PGNode *)lfirst(lc);

                varattno++;
                if (colnames)
                {
                    /* Assume there is one alias per column */
                    char * label = strVal(lfirst(aliasp_item));

                    *colnames = lappend(*colnames, makeString(pstrdup(label)));
                    aliasp_item = lnext(aliasp_item);
                }

                if (colvars)
                {
                    PGVar * varnode;

                    varnode = makeVar(rtindex, varattno, exprType(col), exprTypmod(col), sublevels_up);
                    *colvars = lappend(*colvars, varnode);
                }
            }
        }
        break;
        case PG_RTE_JOIN: {
            /* Join RTE */
            PGListCell * colname;
            PGListCell * aliasvar;

            Assert(list_length(rte->eref->colnames) == list_length(rte->joinaliasvars));

            varattno = 0;
            forboth(colname, rte->eref->colnames, aliasvar, rte->joinaliasvars)
            {
                PGNode * avar = (PGNode *)lfirst(aliasvar);

                varattno++;

                /*
					 * During ordinary parsing, there will never be any
					 * deleted columns in the join; but we have to check since
					 * this routine is also used by the rewriter, and joins
					 * found in stored rules might have join columns for
					 * since-deleted columns.  This will be signaled by a NULL
					 * Const in the alias-vars list.
					 */
                if (IsA(avar, PGConst))
                {
                    if (include_dropped)
                    {
                        if (colnames)
                            *colnames = lappend(*colnames, makeString(pstrdup("")));
                        if (colvars)
                            *colvars = lappend(*colvars, copyObject(avar));
                    }
                    continue;
                }

                if (colnames)
                {
                    char * label = strVal(lfirst(colname));

                    *colnames = lappend(*colnames, makeString(pstrdup(label)));
                }

                if (colvars)
                {
                    PGVar * varnode;

                    varnode = makeVar(rtindex, varattno, exprType(avar), exprTypmod(avar), sublevels_up);

                    *colvars = lappend(*colvars, varnode);
                }
            }
        }
        break;
        default:
            elog(ERROR, "unrecognized RTE kind: %d", (int)rte->rtekind);
    }
};

void RelationParser::checkNameSpaceConflicts(PGParseState * pstate, PGList * namespace1,
		PGList * namespace2)
{
    PGListCell * l1;

    foreach (l1, namespace1)
    {
        PGRangeTblEntry * rte1 = (PGRangeTblEntry *)lfirst(l1);
        const char * aliasname1 = rte1->eref->aliasname;
        PGListCell * l2;

        foreach (l2, namespace2)
        {
            PGRangeTblEntry * rte2 = (PGRangeTblEntry *)lfirst(l2);

            if (strcmp(rte2->eref->aliasname, aliasname1) != 0)
                continue; /* definitely no conflict */
            if (rte1->rtekind == PG_RTE_RELATION && rte1->alias == NULL && rte2->rtekind == PG_RTE_RELATION && rte2->alias == NULL
                && rte1->relid != rte2->relid)
                continue; /* no conflict per SQL92 rule */
            ereport(ERROR, (errcode(ERRCODE_DUPLICATE_ALIAS), errmsg("table name \"%s\" specified more than once", aliasname1)));
        }
    }
};

PGCommonTableExpr * RelationParser::scanNameSpaceForCTE(PGParseState * pstate,
		const char * refname, Index * ctelevelsup)
{
	Assert(refname != NULL);

	Index levelsup;

	for (levelsup = 0;
		 pstate != NULL;
		 pstate = pstate->parentParseState, levelsup++)
	{
		PGListCell *lc;

		foreach(lc, pstate->p_ctenamespace)
		{
			PGCommonTableExpr *cte = (PGCommonTableExpr *) lfirst(lc);
			Assert(cte != NULL && cte->ctename != NULL);

			if (strcmp(cte->ctename, refname) == 0)
			{
				*ctelevelsup = levelsup;
				return cte;
			}
		}
	}

	return NULL;
};

RangeTblEntry * RelationParser::addRangeTableEntryForCTE(PGParseState * pstate,
		PGCommonTableExpr * cte, Index levelsup,
		PGRangeVar * rangeVar, bool inFromCl)
{
	PGRangeTblEntry *rte = makeNode(PGRangeTblEntry);

	rte->rtekind = PG_RTE_CTE;
	rte->ctename = cte->ctename;
	rte->ctelevelsup = levelsup;

	/* Self-reference if and only if CTE's parse analysis isn't completed */
	rte->self_reference = !IsA(cte->ctequery, PGQuery);
	Assert(cte->cterecursive || !rte->self_reference);
	/* Bump the CTE's refcount if this isn't a self-reference */
	if (!rte->self_reference)
		cte->cterefcount++;

	/* Currently, we only support SELECT in WITH query */
	AssertImply(IsA(cte->ctequery, PGQuery),
				((PGQuery *) cte->ctequery)->commandType == PG_CMD_SELECT);

	rte->ctecoltypes = cte->ctecoltypes;
	rte->ctecoltypmods = cte->ctecoltypmods;

	rte->alias = rangeVar->alias;
	char *refname = rte->alias ? rte->alias->aliasname : cte->ctename;
	PGAlias *eref;

	if (rte->alias)
		eref = copyObject(rte->alias);
	else
		eref = makeAlias(refname, NIL);
	int numaliases = list_length(eref->colnames);

	/* fill in any unspecified alias columns */
	int varattno = 0;
	PGListCell *lc;
	foreach(lc, cte->ctecolnames)
	{
		varattno++;
		if (varattno > numaliases)
			eref->colnames = lappend(eref->colnames, lfirst(lc));
	}
	if (varattno < numaliases)
	{
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg(ERRMSG_GP_WITH_COLUMNS_MISMATCH, refname)));
	}

	rte->eref = eref;

	/*----------
	 * Flags:
	 * - this RTE should be expanded to include descendant tables,
	 * - this RTE is in the FROM clause,
	 * - this RTE should be checked for appropriate access rights.
	 *
	 * Subqueries are never checked for access rights.
	 *----------
	 */
	rte->inh = false;			/* never true for subqueries */
	rte->inFromCl = inFromCl;

	rte->requiredPerms = 0;
	rte->checkAsUser = InvalidOid;

	/*
	 * Add completed RTE to pstate's range table list, but not to join list
	 * nor namespace --- caller must do that if appropriate.
	 */
	if (pstate != NULL)
		pstate->p_rtable = lappend(pstate->p_rtable, rte);

	return rte;
};

PGRangeTblEntry * RelationParser::addRangeTableEntry(PGParseState * pstate,
		PGRangeVar * relation, PGAlias * alias,
		bool inh, bool inFromCl)
{
	PGRangeTblEntry		*rte	 = makeNode(PGRangeTblEntry);
	char				*refname = alias ? alias->aliasname : relation->relname;
	LOCKMODE             lockmode = AccessShareLock;
	bool                 nowait = false;
	LockingClause		*locking;
	Relation			 rel;
	
	/* 
	 * CDB: lock promotion around the locking clause is a little different
	 * from postgres to allow for required lock promotion for distributed
	 * tables.
	 */
	locking = getLockingClause(pstate, refname);
	if (locking)
	{
		lockmode = locking->forUpdate ? RowExclusiveLock : RowShareLock;
		nowait	 = locking->noWait;
	}
	rel = parserOpenTable(pstate, relation, lockmode, nowait, NULL);
	
	/*
	 * Get the rel's OID.  This access also ensures that we have an up-to-date
	 * relcache entry for the rel.	Since this is typically the first access
	 * to a rel in a statement, be careful to get the right access level
	 * depending on whether we're doing SELECT FOR UPDATE/SHARE.
	 */
	rte->relid = RelationGetRelid(rel);
	rte->alias = alias;
	rte->rtekind = PG_RTE_RELATION;

	/* external tables except for pluggable storage don't allow inheritance */
	if(RelationIsExternal(rel) && !RelationIsPluggableStorage(rte->relid))
		inh = false;

	/*
	 * Build the list of effective column names using user-supplied aliases
	 * and/or actual column names.
	 */
	rte->eref = makeAlias(refname, NIL);
	buildRelationAliases(rel->rd_att, alias, rte->eref);

	/*
	 * Drop the rel refcount, but keep the access lock till end of transaction
	 * so that the table can't be deleted or have its schema modified
	 * underneath us.
	 */
	heap_close(rel, NoLock);

	/*----------
	 * Flags:
	 * - this RTE should be expanded to include descendant tables,
	 * - this RTE is in the FROM clause,
	 * - this RTE should be checked for appropriate access rights.
	 *
	 * The initial default on access checks is always check-for-READ-access,
	 * which is the right thing for all except target tables.
	 *----------
	 */
	rte->inh = inh;
	rte->inFromCl = inFromCl;

	rte->requiredPerms = ACL_SELECT;
	rte->checkAsUser = InvalidOid;		/* not set-uid by default, either */

	/*
	 * Add completed RTE to pstate's range table list, but not to join list
	 * nor namespace --- caller must do that if appropriate.
	 */
	if (pstate != NULL)
		pstate->p_rtable = lappend(pstate->p_rtable, rte);

	return rte;
};

PGRangeTblEntry * RelationParser::addRangeTableEntryForSubquery(PGParseState * pstate,
		PGQuery * subquery, PGAlias * alias, bool inFromCl)
{
	PGRangeTblEntry *rte = makeNode(PGRangeTblEntry);
	char	   *refname = alias->aliasname;
	PGAlias	   *eref;
	int			numaliases;
	int			varattno;
	PGListCell   *tlistitem;

	rte->rtekind = PG_RTE_SUBQUERY;
	rte->relid = InvalidOid;
	rte->subquery = subquery;
	rte->alias = alias;

	eref = copyObject(alias);
	numaliases = list_length(eref->colnames);

	/* fill in any unspecified alias columns */
	varattno = 0;
	foreach(tlistitem, subquery->targetList)
	{
		PGTargetEntry *te = (PGTargetEntry *) lfirst(tlistitem);

		if (te->resjunk)
			continue;
		varattno++;
		Assert(varattno == te->resno);
		if (varattno > numaliases)
		{
			char	   *attrname;

			attrname = pstrdup(te->resname);
			eref->colnames = lappend(eref->colnames, makeString(attrname));
		}
	}
	if (varattno < numaliases)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
				 errmsg("table \"%s\" has %d columns available but %d columns specified",
						refname, varattno, numaliases)));

	rte->eref = eref;

	/*----------
	 * Flags:
	 * - this RTE should be expanded to include descendant tables,
	 * - this RTE is in the FROM clause,
	 * - this RTE should be checked for appropriate access rights.
	 *
	 * Subqueries are never checked for access rights.
	 *----------
	 */
	rte->inh = false;			/* never true for subqueries */
	rte->inFromCl = inFromCl;

	rte->requiredPerms = 0;
	rte->checkAsUser = InvalidOid;

	/*
	 * Add completed RTE to pstate's range table list, but not to join list
	 * nor namespace --- caller must do that if appropriate.
	 */
	if (pstate != NULL)
		pstate->p_rtable = lappend(pstate->p_rtable, rte);

	return rte;
};

PGRangeTblEntry * RelationParser::scanNameSpaceForRelid(PGParseState * pstate, Oid relid)
{
    PGRangeTblEntry * result = NULL;
    PGListCell * l;

    foreach (l, pstate->p_relnamespace)
    {
        PGRangeTblEntry * rte = (PGRangeTblEntry *)lfirst(l);

        /* yes, the test for alias == NULL should be there... */
        if (rte->rtekind == PG_RTE_RELATION && rte->relid == relid && rte->alias == NULL)
        {
            if (result)
                ereport(ERROR, (errcode(ERRCODE_AMBIGUOUS_ALIAS), errmsg("table reference %u is ambiguous", relid)));
            result = rte;
        }
    }
    return result;
};

PGRangeTblEntry * RelationParser::scanNameSpaceForRefname(PGParseState * pstate, const char * refname)
{
    PGRangeTblEntry * result = NULL;
    PGListCell * l;

    foreach (l, pstate->p_relnamespace)
    {
        PGRangeTblEntry * rte = (PGRangeTblEntry *)lfirst(l);

        if (strcmp(rte->eref->aliasname, refname) == 0)
        {
            if (result)
                ereport(ERROR, (errcode(ERRCODE_AMBIGUOUS_ALIAS), errmsg("table reference \"%s\" is ambiguous", refname)));
            result = rte;
        }
    }
    return result;
};

PGRangeTblEntry * RelationParser::refnameRangeTblEntryHelper(
        PGParseState * pstate, const char * refname, Oid relId, int * sublevels_up)
{
    while (pstate != NULL)
    {
        PGRangeTblEntry * result = NULL;

        if (OidIsValid(relId))
        {
            result = scanNameSpaceForRelid(pstate, relId);
        }
        else
        {
            result = scanNameSpaceForRefname(pstate, refname);
        }

        if (result)
        {
            return result;
        }

        if (NULL == sublevels_up)
        {
            break;
        }

        (*sublevels_up)++;
        pstate = pstate->parentParseState;
    }

    return NULL;
};

PGRangeTblEntry * RelationParser::refnameRangeTblEntryHelperSchemaQualified(
        PGParseState * pstate, Oid dboid, const char * nspname, const char * refname,
        int * sublevels_up)
{
    Oid nspid = LookupNamespaceId(nspname, dboid);
    if (!OidIsValid(nspid))
    {
        return NULL;
    }
    Oid relid = get_relname_relid(refname, nspid);
    if (!OidIsValid(relid))
    {
        return NULL;
    }

    return refnameRangeTblEntryHelper(pstate, refname, relid, sublevels_up);
};

PGRangeTblEntry * RelationParser::refnameRangeTblEntry(
        PGParseState * pstate, const char * catalogname,
        const char * schemaname, const char * refname, int location, int * sublevels_up)
{
    Oid relId = InvalidOid;

    if (sublevels_up)
    {
        *sublevels_up = 0;
    }

    if (NULL == catalogname && NULL == schemaname)
    {
        /* simple unqualified name: search the rangevars of the query */
        return refnameRangeTblEntryHelper(pstate, refname, InvalidOid /*relId*/, sublevels_up);
    }

    if (NULL != catalogname && NULL != schemaname)
    {
        /* fully qualified table name: catalog.schema.refname */
        Oid dboid = GetCatalogId(catalogname);
        Oid namespaceId = LookupExplicitNamespace(schemaname, dboid);
        relId = get_relname_relid(refname, namespaceId);

        if (!OidIsValid(relId))
        {
            return NULL;
        }

        return refnameRangeTblEntryHelper(pstate, refname, relId, sublevels_up);
    }

    /* namespace-qualified name: schema.refname: consider both the current database and HCatalog */
    Assert(NULL != schemaname);

    Oid dboidCurrent = GetCatalogId(NULL /*catalogname*/);
    int slevelsUpCurrent = 0;

    int * pslevelsUpCurrent = NULL;

    if (NULL != sublevels_up)
        pslevelsUpCurrent = &slevelsUpCurrent;

    PGRangeTblEntry * rteCurrent = refnameRangeTblEntryHelperSchemaQualified(pstate, dboidCurrent, schemaname, refname, pslevelsUpCurrent);

    if (NULL == rteCurrent)
        return NULL;

    if (NULL != rteCurrent)
    {
        if (NULL != sublevels_up)
        {
            *sublevels_up = *pslevelsUpCurrent;
        }
        return rteCurrent;
    }
};

PGRangeTblEntry * RelationParser::searchRangeTable(PGParseState * pstate,
        PGRangeVar * relation)
{
    Oid relId = InvalidOid;
    const char * refname = relation->relname;
    PGCommonTableExpr * cte = NULL;
    Index ctelevelsup = 0;

    /*
	 * If it's an unqualified name, check for possible CTE matches. A CTE
	 * hides any real relation matches.  If no CTE, look for a matching
	 * relation.
	 */
    if (!relation->schemaname)
        cte = scanNameSpaceForCTE(pstate, refname, &ctelevelsup);
    if (!cte)
        relId = RangeVarGetRelid(relation, true, true /*allowHcatalog*/);

    Index levelsup = 0;
    while (pstate != NULL)
    {
        PGListCell * l;

        foreach (l, pstate->p_rtable)
        {
            PGRangeTblEntry * rte = (PGRangeTblEntry *)lfirst(l);

            if (OidIsValid(relId) && rte->rtekind == PG_RTE_RELATION && rte->relid == relId)
                return rte;

            if (rte->rtekind == PG_RTE_CTE && cte != NULL && rte->ctelevelsup + levelsup == ctelevelsup && strcmp(rte->ctename, refname) == 0)
                return rte;

            if (rte->eref != NULL && rte->eref->aliasname != NULL && strcmp(rte->eref->aliasname, refname) == 0)
                return rte;
        }

        pstate = pstate->parentParseState;
        levelsup++;
    }
    return NULL;
};

void RelationParser::warnAutoRange(PGParseState * pstate, PGRangeVar * relation, int location)
{
    PGRangeTblEntry * rte;
    int sublevels_up;
    const char * badAlias = NULL;

    /*
	 * Check to see if there are any potential matches in the query's
	 * rangetable.	This affects the message we provide.
	 */
    rte = searchRangeTable(pstate, relation);

    /*
	 * If we found a match that has an alias and the alias is visible in the
	 * namespace, then the problem is probably use of the relation's real name
	 * instead of its alias, ie "SELECT foo.* FROM foo f". This mistake is
	 * common enough to justify a specific hint.
	 *
	 * If we found a match that doesn't meet those criteria, assume the
	 * problem is illegal use of a relation outside its scope, as in the
	 * MySQL-ism "SELECT ... FROM a, b LEFT JOIN c ON (a.x = c.y)".
	 */
    if (rte && rte->alias && strcmp(rte->eref->aliasname, relation->relname) != 0
        && refnameRangeTblEntry(pstate, NULL /*catalogname*/, NULL, rte->eref->aliasname, location, &sublevels_up) == rte)
        badAlias = rte->eref->aliasname;

    if (!add_missing_from)
    {
        if (rte)
            ereport(
                ERROR,
                (errcode(ERRCODE_UNDEFINED_TABLE),
                 errmsg("invalid reference to FROM-clause entry for table \"%s\"", relation->relname),
                 (badAlias ? errhint("Perhaps you meant to reference the table alias \"%s\".", badAlias)
                           : errhint(
                               "There is an entry for table \"%s\", but it cannot be referenced from this part of the query.",
                               rte->eref->aliasname)),
                 errOmitLocation(true),
                 parser_errposition(pstate, location)));
        else
            ereport(
                ERROR,
                (errcode(ERRCODE_UNDEFINED_TABLE),
                 (pstate->parentParseState ? errmsg("missing FROM-clause entry in subquery for table \"%s\"", relation->relname)
                                           : errmsg("missing FROM-clause entry for table \"%s\"", relation->relname)),
                 errOmitLocation(true),
                 parser_errposition(pstate, location)));
    }
    else
    {
        /* just issue a warning */
        ereport(
            NOTICE,
            (errcode(ERRCODE_UNDEFINED_TABLE),
             (pstate->parentParseState ? errmsg("adding missing FROM-clause entry in subquery for table \"%s\"", relation->relname)
                                       : errmsg("adding missing FROM-clause entry for table \"%s\"", relation->relname)),
             (badAlias ? errhint("Perhaps you meant to reference the table alias \"%s\".", badAlias)
                       : (rte ? errhint(
                              "There is an entry for table \"%s\", but it cannot be referenced from this part of the query.",
                              rte->eref->aliasname)
                              : 0)),
             errOmitLocation(true),
             parser_errposition(pstate, location)));
    }
};

void RelationParser::addRTEtoQuery(PGParseState * pstate, PGRangeTblEntry * rte,
        bool addToJoinList, bool addToRelNameSpace, bool addToVarNameSpace)
{
    if (addToJoinList)
    {
        int rtindex = RTERangeTablePosn(pstate, rte, NULL);
        PGRangeTblRef * rtr = makeNode(PGRangeTblRef);

        rtr->rtindex = rtindex;
        pstate->p_joinlist = lappend(pstate->p_joinlist, rtr);
    }
    if (addToRelNameSpace)
        pstate->p_relnamespace = lappend(pstate->p_relnamespace, rte);
    if (addToVarNameSpace)
        pstate->p_varnamespace = lappend(pstate->p_varnamespace, rte);
};

PGRangeTblEntry * RelationParser::addImplicitRTE(PGParseState * pstate, PGRangeVar * relation,
        int location)
{
    PGRangeTblEntry * rte;

    /* issue warning or error as needed */
    warnAutoRange(pstate, relation, location);

    /*
	 * Note that we set inFromCl true, so that the RTE will be listed
	 * explicitly if the parsetree is ever decompiled by ruleutils.c. This
	 * provides a migration path for views/rules that were originally written
	 * with implicit-RTE syntax.
	 */
    rte = addRangeTableEntry(pstate, relation, NULL, false, true);
    /* Add to joinlist and relnamespace, but not varnamespace */
    addRTEtoQuery(pstate, rte, true, true, false);

    return rte;
};

PGList * RelationParser::expandRelAttrs(PGParseState * pstate, PGRangeTblEntry * rte,
        int rtindex, int sublevels_up, int location)
{
    PGList *names, *vars;
    PGListCell *name, *var;
    PGList * te_list = NIL;

    expandRTE(rte, rtindex, sublevels_up, location, false, &names, &vars);

    forboth(name, names, var, vars)
    {
        char * label = strVal(lfirst(name));
        PGNode * varnode = (PGNode *)lfirst(var);
        PGTargetEntry * te;

        te = makeTargetEntry((PGExpr *)varnode, (PGAttrNumber)pstate->p_next_resno++, label, false);
        te_list = lappend(te_list, te);
    }

    Assert(name == NULL && var == NULL); /* lists not the same length? */

    return te_list;
};

}