#include <Interpreters/orcaopt/pgoptnew/RelationParser.h>

using namespace duckdb_libpgquery;

namespace DB
{

RelationParser::RelationParser()
{
	relation_provider = std::make_shared<RelationProvider>();
};

PGCommonTableExpr *
RelationParser::scanNameSpaceForCTE(PGParseState *pstate, const char *refname,
					Index *ctelevelsup)
{
	Index		levelsup;

	for (levelsup = 0;
		 pstate != NULL;
		 pstate = pstate->parentParseState, levelsup++)
	{
		PGListCell   *lc;

		foreach(lc, pstate->p_ctenamespace)
		{
			PGCommonTableExpr *cte = (PGCommonTableExpr *) lfirst(lc);

			if (strcmp(cte->ctename, refname) == 0)
			{
				*ctelevelsup = levelsup;
				return cte;
			}
		}
	}
	return NULL;
};

PGRangeTblEntry *
RelationParser::addRangeTableEntryForCTE(PGParseState *pstate,
						 PGCommonTableExpr *cte,
						 Index levelsup,
						 PGRangeVar *rv,
						 bool inFromCl)
{
	PGRangeTblEntry *rte = makeNode(PGRangeTblEntry);
	PGAlias	   *alias = rv->alias;
	char	   *refname = alias ? alias->aliasname : cte->ctename;
	PGAlias	   *eref;
	int			numaliases;
	int			varattno;
	ListCell   *lc;

	Assert(pstate != NULL);

	rte->rtekind = PG_RTE_CTE;
	rte->ctename = cte->ctename;
	rte->ctelevelsup = levelsup;

	/* Self-reference if and only if CTE's parse analysis isn't completed */
	rte->self_reference = !IsA(cte->ctequery, PGQuery);
	Assert(cte->cterecursive || !rte->self_reference);
	/* Bump the CTE's refcount if this isn't a self-reference */
	if (!rte->self_reference)
		cte->cterefcount++;

	/*
	 * We throw error if the CTE is INSERT/UPDATE/DELETE without RETURNING.
	 * This won't get checked in case of a self-reference, but that's OK
	 * because data-modifying CTEs aren't allowed to be recursive anyhow.
	 */
	if (IsA(cte->ctequery, PGQuery))
	{
		PGQuery	   *ctequery = (PGQuery *) cte->ctequery;

		if (ctequery->commandType != PG_CMD_SELECT &&
			ctequery->returningList == NIL)
			ereport(ERROR,
					(errcode(PG_ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("WITH query \"%s\" does not have a RETURNING clause",
							cte->ctename),
					 parser_errposition(pstate, rv->location)));
	}

	rte->coltypes = cte->ctecoltypes;
	rte->coltypmods = cte->ctecoltypmods;
	rte->colcollations = cte->ctecolcollations;

	rte->alias = alias;
	if (alias)
		eref = (PGAlias *)copyObject(alias);
	else
		eref = makeAlias(refname, NIL);
	numaliases = list_length(eref->colnames);

	/* fill in any unspecified alias columns */
	varattno = 0;
	foreach(lc, cte->ctecolnames)
	{
		varattno++;
		if (varattno > numaliases)
			eref->colnames = lappend(eref->colnames, lfirst(lc));
	}
	if (varattno < numaliases)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
				 errmsg("table \"%s\" has %d columns available but %d columns specified",
						refname, varattno, numaliases)));

	rte->eref = eref;

	/*
	 * Set flags and access permissions.
	 *
	 * Subqueries are never checked for access rights.
	 */
	rte->lateral = false;
	rte->inh = false;			/* never true for subqueries */
	rte->inFromCl = inFromCl;

	// rte->requiredPerms = 0;
	// rte->checkAsUser = InvalidOid;
	// rte->selectedCols = NULL;
	// rte->insertedCols = NULL;
	// rte->updatedCols = NULL;
	// rte->extraUpdatedCols = NULL;

	/*
	 * Add completed RTE to pstate's range table list, but not to join list
	 * nor namespace --- caller must do that if appropriate.
	 */
	pstate->p_rtable = lappend(pstate->p_rtable, rte);

	return rte;
};

bool
RelationParser::scanNameSpaceForENR(PGParseState *pstate, const char *refname)
{
	return enr_parser.name_matches_visible_ENR(pstate, refname);
};

void
RelationParser::buildRelationAliases(TupleDesc tupdesc, PGAlias *alias, PGAlias *eref)
{
	int			maxattrs = tupdesc->natts;
	PGListCell   *aliaslc;
	int			numaliases;
	int			varattno;
	int			numdropped = 0;

	Assert(eref->colnames == NIL);

	if (alias)
	{
		aliaslc = list_head(alias->colnames);
		numaliases = list_length(alias->colnames);
		/* We'll rebuild the alias colname list */
		alias->colnames = NIL;
	}
	else
	{
		aliaslc = NULL;
		numaliases = 0;
	}

	for (varattno = 0; varattno < maxattrs; varattno++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, varattno);
		PGValue	   *attrname;

		if (attr->attisdropped)
		{
			/* Always insert an empty string for a dropped column */
			attrname = makeString(pstrdup(""));
			if (aliaslc)
				alias->colnames = lappend(alias->colnames, attrname);
			numdropped++;
		}
		else if (aliaslc)
		{
			/* Use the next user-supplied alias */
			attrname = (PGValue *) lfirst(aliaslc);
			aliaslc = lnext(aliaslc);
			alias->colnames = lappend(alias->colnames, attrname);
		}
		else
		{
			attrname = makeString(pstrdup(attr->attname.data));
			/* we're done with the alias if any */
		}

		eref->colnames = lappend(eref->colnames, attrname);
	}

	/* Too many user-supplied aliases? */
	if (aliaslc)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
				 errmsg("table \"%s\" has %d columns available but %d columns specified",
						eref->aliasname, maxattrs - numdropped, numaliases)));
};

PGRangeTblEntry *
RelationParser::searchRangeTableForRel(PGParseState *pstate, PGRangeVar *relation)
{
	const char *refname = relation->relname;
	Oid			relId = InvalidOid;
	PGCommonTableExpr *cte = NULL;
	bool		isenr = false;
	Index		ctelevelsup = 0;
	Index		levelsup;

	/*
	 * If it's an unqualified name, check for possible CTE matches. A CTE
	 * hides any real relation matches.  If no CTE, look for a matching
	 * relation.
	 *
	 * NB: It's not critical that RangeVarGetRelid return the correct answer
	 * here in the face of concurrent DDL.  If it doesn't, the worst case
	 * scenario is a less-clear error message.  Also, the tables involved in
	 * the query are already locked, which reduces the number of cases in
	 * which surprising behavior can occur.  So we do the name lookup
	 * unlocked.
	 */
	if (!relation->schemaname)
	{
		cte = scanNameSpaceForCTE(pstate, refname, &ctelevelsup);
		if (!cte)
			isenr = scanNameSpaceForENR(pstate, refname);
	}

	if (!cte && !isenr)
		relId = RangeVarGetRelid(relation, NoLock, true);

	/* Now look for RTEs matching either the relation/CTE/ENR or the alias */
	for (levelsup = 0;
		 pstate != NULL;
		 pstate = pstate->parentParseState, levelsup++)
	{
		ListCell   *l;

		foreach(l, pstate->p_rtable)
		{
			PGRangeTblEntry *rte = (PGRangeTblEntry *) lfirst(l);

			if (rte->rtekind == PG_RTE_RELATION &&
				OidIsValid(relId) &&
				rte->relid == relId)
				return rte;
			if (rte->rtekind == PG_RTE_CTE &&
				cte != NULL &&
				rte->ctelevelsup + levelsup == ctelevelsup &&
				strcmp(rte->ctename, refname) == 0)
				return rte;
			if (rte->rtekind == RTE_NAMEDTUPLESTORE &&
				isenr &&
				strcmp(rte->enrname, refname) == 0)
				return rte;

			if (rte->eref != NULL &&
                rte->eref->aliasname != NULL &&
                strcmp(rte->eref->aliasname, refname) == 0)
				return rte;
		}
	}
	return NULL;
};

void
RelationParser::errorMissingRTE(PGParseState *pstate, PGRangeVar *relation)
{
	PGRangeTblEntry *rte;
	int			sublevels_up;
	const char *badAlias = NULL;

	/*
	 * Check to see if there are any potential matches in the query's
	 * rangetable.  (Note: cases involving a bad schema name in the RangeVar
	 * will throw error immediately here.  That seems OK.)
	 */
	rte = searchRangeTableForRel(pstate, relation);

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
	if (rte && rte->alias &&
		strcmp(rte->eref->aliasname, relation->relname) != 0 &&
		refnameRangeTblEntry(pstate, NULL, rte->eref->aliasname,
							 relation->location,
							 &sublevels_up) == rte)
		badAlias = rte->eref->aliasname;

	if (rte)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_TABLE),
				 errmsg("invalid reference to FROM-clause entry for table \"%s\"",
						relation->relname),
				 (badAlias ?
				  errhint("Perhaps you meant to reference the table alias \"%s\".",
						  badAlias) :
				  errhint("There is an entry for table \"%s\", but it cannot be referenced from this part of the query.",
						  rte->eref->aliasname)),
				 parser_errposition(pstate, relation->location)));
	else
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_TABLE),
				 errmsg("missing FROM-clause entry for table \"%s\"",
						relation->relname),
				 parser_errposition(pstate, relation->location)));
};

void
RelationParser::errorMissingColumn(PGParseState *pstate,
				   const char *relname, const char *colname, int location)
{
	FuzzyAttrMatchState *state;
	char	   *closestfirst = NULL;

	/*
	 * Search the entire rtable looking for possible matches.  If we find one,
	 * emit a hint about it.
	 *
	 * TODO: improve this code (and also errorMissingRTE) to mention using
	 * LATERAL if appropriate.
	 */
	state = searchRangeTableForCol(pstate, relname, colname, location);

	/*
	 * Extract closest col string for best match, if any.
	 *
	 * Infer an exact match referenced despite not being visible from the fact
	 * that an attribute number was not present in state passed back -- this
	 * is what is reported when !closestfirst.  There might also be an exact
	 * match that was qualified with an incorrect alias, in which case
	 * closestfirst will be set (so hint is the same as generic fuzzy case).
	 */
	if (state->rfirst && AttributeNumberIsValid(state->first))
		closestfirst = strVal(list_nth(state->rfirst->eref->colnames,
									   state->first - 1));

	if (!state->rsecond)
	{
		/*
		 * Handle case where there is zero or one column suggestions to hint,
		 * including exact matches referenced but not visible.
		 */
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_COLUMN),
				 relname ?
				 errmsg("column %s.%s does not exist", relname, colname) :
				 errmsg("column \"%s\" does not exist", colname),
				 state->rfirst ? closestfirst ?
				 errhint("Perhaps you meant to reference the column \"%s.%s\".",
						 state->rfirst->eref->aliasname, closestfirst) :
				 errhint("There is a column named \"%s\" in table \"%s\", but it cannot be referenced from this part of the query.",
						 colname, state->rfirst->eref->aliasname) : 0,
				 parser_errposition(pstate, location)));
	}
	else
	{
		/* Handle case where there are two equally useful column hints */
		char	   *closestsecond;

		closestsecond = strVal(list_nth(state->rsecond->eref->colnames,
										state->second - 1));

		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_COLUMN),
				 relname ?
				 errmsg("column %s.%s does not exist", relname, colname) :
				 errmsg("column \"%s\" does not exist", colname),
				 errhint("Perhaps you meant to reference the column \"%s.%s\" or the column \"%s.%s\".",
						 state->rfirst->eref->aliasname, closestfirst,
						 state->rsecond->eref->aliasname, closestsecond),
				 parser_errposition(pstate, location)));
	}
};

FuzzyAttrMatchState *
RelationParser::searchRangeTableForCol(PGParseState *pstate, const char *alias, const char *colname,
					   int location)
{
	PGParseState *orig_pstate = pstate;
	FuzzyAttrMatchState *fuzzystate = (FuzzyAttrMatchState *)palloc(sizeof(FuzzyAttrMatchState));

	fuzzystate->distance = MAX_FUZZY_DISTANCE + 1;
	fuzzystate->rfirst = NULL;
	fuzzystate->rsecond = NULL;
	fuzzystate->first = InvalidAttrNumber;
	fuzzystate->second = InvalidAttrNumber;

	while (pstate != NULL)
	{
		ListCell   *l;

		foreach(l, pstate->p_rtable)
		{
			PGRangeTblEntry *rte = (PGRangeTblEntry *) lfirst(l);
			int			fuzzy_rte_penalty = 0;

			/*
			 * Typically, it is not useful to look for matches within join
			 * RTEs; they effectively duplicate other RTEs for our purposes,
			 * and if a match is chosen from a join RTE, an unhelpful alias is
			 * displayed in the final diagnostic message.
			 */
			if (rte->rtekind == PG_RTE_JOIN)
				continue;

			/*
			 * If the user didn't specify an alias, then matches against one
			 * RTE are as good as another.  But if the user did specify an
			 * alias, then we want at least a fuzzy - and preferably an exact
			 * - match for the range table entry.
			 */
			if (alias != NULL)
				fuzzy_rte_penalty =
					varstr_levenshtein_less_equal(alias, strlen(alias),
												  rte->eref->aliasname,
												  strlen(rte->eref->aliasname),
												  1, 1, 1,
												  MAX_FUZZY_DISTANCE + 1,
												  true);

			/*
			 * Scan for a matching column; if we find an exact match, we're
			 * done.  Otherwise, update fuzzystate.
			 */
			if (scanRTEForColumn(orig_pstate, rte, colname, location,
								 fuzzy_rte_penalty, fuzzystate)
				&& fuzzy_rte_penalty == 0)
			{
				fuzzystate->rfirst = rte;
				fuzzystate->first = InvalidAttrNumber;
				fuzzystate->rsecond = NULL;
				fuzzystate->second = InvalidAttrNumber;
				return fuzzystate;
			}
		}

		pstate = pstate->parentParseState;
	}

	return fuzzystate;
};

PGRangeTblEntry *
RelationParser::addRangeTableEntryForENR(PGParseState *pstate,
						 PGRangeVar *rv,
						 bool inFromCl)
{
	PGRangeTblEntry *rte = makeNode(PGRangeTblEntry);
	PGAlias	   *alias = rv->alias;
	char	   *refname = alias ? alias->aliasname : rv->relname;
	EphemeralNamedRelationMetadata enrmd;
	TupleDesc	tupdesc;
	int			attno;

	Assert(pstate != NULL);
	enrmd = enr_parser.get_visible_ENR(pstate, rv->relname);
	Assert(enrmd != NULL);

	switch (enrmd->enrtype)
	{
		case ENR_NAMED_TUPLESTORE:
			rte->rtekind = RTE_NAMEDTUPLESTORE;
			break;

		default:
			elog(ERROR, "unexpected enrtype: %d", enrmd->enrtype);
			return NULL;		/* for fussy compilers */
	}

	/*
	 * Record dependency on a relation.  This allows plans to be invalidated
	 * if they access transition tables linked to a table that is altered.
	 */
	rte->relid = enrmd->reliddesc;

	/*
	 * Build the list of effective column names using user-supplied aliases
	 * and/or actual column names.
	 */
	tupdesc = ENRMetadataGetTupDesc(enrmd);
	rte->eref = makeAlias(refname, NIL);
	buildRelationAliases(tupdesc, alias, rte->eref);

	/* Record additional data for ENR, including column type info */
	rte->enrname = enrmd->name;
	rte->enrtuples = enrmd->enrtuples;
	rte->coltypes = NIL;
	rte->coltypmods = NIL;
	rte->colcollations = NIL;
	for (attno = 1; attno <= tupdesc->natts; ++attno)
	{
		Form_pg_attribute att = TupleDescAttr(tupdesc, attno - 1);

		if (att->attisdropped)
		{
			/* Record zeroes for a dropped column */
			rte->coltypes = lappend_oid(rte->coltypes, InvalidOid);
			rte->coltypmods = lappend_int(rte->coltypmods, 0);
			rte->colcollations = lappend_oid(rte->colcollations, InvalidOid);
		}
		else
		{
			/* Let's just make sure we can tell this isn't dropped */
			if (att->atttypid == InvalidOid)
				elog(ERROR, "atttypid is invalid for non-dropped column in \"%s\"",
					 rv->relname);
			rte->coltypes = lappend_oid(rte->coltypes, att->atttypid);
			rte->coltypmods = lappend_int(rte->coltypmods, att->atttypmod);
			rte->colcollations = lappend_oid(rte->colcollations,
											 att->attcollation);
		}
	}

	/*
	 * Set flags and access permissions.
	 *
	 * ENRs are never checked for access rights.
	 */
	rte->lateral = false;
	rte->inh = false;			/* never true for ENRs */
	rte->inFromCl = inFromCl;

	// rte->requiredPerms = 0;
	// rte->checkAsUser = InvalidOid;
	// rte->selectedCols = NULL;

	/*
	 * Add completed RTE to pstate's range table list, but not to join list
	 * nor namespace --- caller must do that if appropriate.
	 */
	pstate->p_rtable = lappend(pstate->p_rtable, rte);

	return rte;
};

void
RelationParser::expandTupleDesc(TupleDesc tupdesc, PGAlias *eref, int count, int offset,
				int rtindex, int sublevels_up,
				int location, bool include_dropped,
				PGList **colnames, PGList **colvars)
{
	PGListCell   *aliascell = list_head(eref->colnames);
	int			varattno;

	if (colnames)
	{
		int			i;

		for (i = 0; i < offset; i++)
		{
			if (aliascell)
				aliascell = lnext(aliascell);
		}
	}

	Assert(count <= tupdesc->natts);
	for (varattno = 0; varattno < count; varattno++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, varattno);

		if (attr->attisdropped)
		{
			if (include_dropped)
			{
				if (colnames)
					*colnames = lappend(*colnames, makeString(pstrdup("")));
				if (colvars)
				{
					/*
					 * can't use atttypid here, but it doesn't really matter
					 * what type the Const claims to be.
					 */
					*colvars = lappend(*colvars,
									   makeNullConst(INT4OID, -1, InvalidOid));
				}
			}
			if (aliascell)
				aliascell = lnext(aliascell);
			continue;
		}

		if (colnames)
		{
			char	   *label;

			if (aliascell)
			{
				label = strVal(lfirst(aliascell));
				aliascell = lnext(aliascell);
			}
			else
			{
				/* If we run out of aliases, use the underlying name */
				label = ((PGValue *)(attr->attname))->val.str;
			}
			*colnames = lappend(*colnames, makeString(pstrdup(label)));
		}

		if (colvars)
		{
			PGVar		   *varnode;

			varnode = makeVar(rtindex, varattno + offset + 1,
							  attr->atttypid, attr->atttypmod,
							  attr->attcollation,
							  sublevels_up);
			varnode->location = location;

			*colvars = lappend(*colvars, varnode);
		}
	}
};

PGList *
RelationParser::expandRelAttrs(PGParseState *pstate, PGRangeTblEntry *rte,
			   int rtindex, int sublevels_up, int location)
{
	PGList	   *names,
			   *vars;
	PGListCell   *name,
			   *var;
	PGList	   *te_list = NIL;

	expandRTE(rte, rtindex, sublevels_up, location, false,
			  &names, &vars);

	/*
	 * Require read access to the table.  This is normally redundant with the
	 * markVarForSelectPriv calls below, but not if the table has zero
	 * columns.
	 */
	// rte->requiredPerms |= ACL_SELECT;

	forboth(name, names, var, vars)
	{
		char	   *label = strVal(lfirst(name));
		PGVar		   *varnode = (PGVar *) lfirst(var);
		PGTargetEntry *te;

		te = makeTargetEntry((PGExpr *) varnode,
							 (PGAttrNumber) pstate->p_next_resno++,
							 label,
							 false);
		te_list = lappend(te_list, te);

		/* Require read access to each column */
		markVarForSelectPriv(pstate, varnode, rte);
	}

	Assert(name == NULL && var == NULL);	/* lists not the same length? */

	return te_list;
};

PGTargetEntry *
RelationParser::get_tle_by_resno(PGList *tlist, PGAttrNumber resno)
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

PGCommonTableExpr *
RelationParser::GetCTEForRTE(PGParseState *pstate, PGRangeTblEntry *rte, int rtelevelsup)
{
	Index		levelsup;
	PGListCell   *lc;

	/* Determine RTE's levelsup if caller didn't know it */
	if (rtelevelsup < 0)
		(void) RTERangeTablePosn(pstate, rte, &rtelevelsup);

	Assert(rte->rtekind == PG_RTE_CTE);
	levelsup = rte->ctelevelsup + rtelevelsup;
	while (levelsup-- > 0)
	{
		pstate = pstate->parentParseState;
		if (!pstate)			/* shouldn't happen */
			elog(ERROR, "bad levelsup for CTE \"%s\"", rte->ctename);
	}
	foreach(lc, pstate->p_ctenamespace)
	{
		PGCommonTableExpr *cte = (PGCommonTableExpr *) lfirst(lc);

		if (strcmp(cte->ctename, rte->ctename) == 0)
			return cte;
	}
	/* shouldn't happen */
	elog(ERROR, "could not find CTE \"%s\"", rte->ctename);
	return NULL;				/* keep compiler quiet */
};

int
RelationParser::RTERangeTablePosn(PGParseState *pstate, PGRangeTblEntry *rte, int *sublevels_up)
{
	int			index;
	PGListCell   *l;

	if (sublevels_up)
		*sublevels_up = 0;

	while (pstate != NULL)
	{
		index = 1;
		foreach(l, pstate->p_rtable)
		{
			if (rte == (PGRangeTblEntry *) lfirst(l))
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
	return 0;					/* keep compiler quiet */
};

void
RelationParser::expandRelation(Oid relid, PGAlias *eref, int rtindex, int sublevels_up,
			   int location, bool include_dropped,
			   PGList **colnames, PGList **colvars)
{
	PGRelation	rel;

	/* Get the tupledesc and turn it over to expandTupleDesc */
	rel = relation_open(relid, AccessShareLock);
	expandTupleDesc(rel->rd_att, eref, rel->rd_att->natts, 0,
					rtindex, sublevels_up,
					location, include_dropped,
					colnames, colvars);
	relation_close(rel, AccessShareLock);
};

void
RelationParser::expandRTE(PGRangeTblEntry *rte, int rtindex, int sublevels_up,
		  int location, bool include_dropped,
		  PGList **colnames, PGList **colvars)
{
	int			varattno;

	if (colnames)
		*colnames = NIL;
	if (colvars)
		*colvars = NIL;

	switch (rte->rtekind)
	{
		case PG_RTE_RELATION:
			/* Ordinary relation RTE */
			expandRelation(rte->relid, rte->eref,
						   rtindex, sublevels_up, location,
						   include_dropped, colnames, colvars);
			break;
		case PG_RTE_SUBQUERY:
			{
				/* Subquery RTE */
				PGListCell   *aliasp_item = list_head(rte->eref->colnames);
				PGListCell   *tlistitem;

				varattno = 0;
				foreach(tlistitem, rte->subquery->targetList)
				{
					PGTargetEntry *te = (PGTargetEntry *) lfirst(tlistitem);

					if (te->resjunk)
						continue;
					varattno++;
					Assert(varattno == te->resno);

					/*
					 * In scenarios where columns have been added to a view
					 * since the outer query was originally parsed, there can
					 * be more items in the subquery tlist than the outer
					 * query expects.  We should ignore such extra column(s)
					 * --- compare the behavior for composite-returning
					 * functions, in the RTE_FUNCTION case below.
					 */
					if (!aliasp_item)
						break;

					if (colnames)
					{
						char	   *label = strVal(lfirst(aliasp_item));

						*colnames = lappend(*colnames, makeString(pstrdup(label)));
					}

					if (colvars)
					{
						PGVar		   *varnode;

						varnode = makeVar(rtindex, varattno,
										  exprType((PGNode *) te->expr),
										  exprTypmod((PGNode *) te->expr),
										  exprCollation((PGNode *) te->expr),
										  sublevels_up);
						varnode->location = location;

						*colvars = lappend(*colvars, varnode);
					}

					aliasp_item = lnext(aliasp_item);
				}
			}
			break;
		// case RTE_TABLEFUNCTION:
		case PG_RTE_FUNCTION:
			{
				/* Function RTE */
				int			atts_done = 0;
				PGListCell   *lc;

				foreach(lc, rte->functions)
				{
					PGRangeTblFunction *rtfunc = (PGRangeTblFunction *) lfirst(lc);
					TypeFuncClass functypclass;
					Oid			funcrettype;
					TupleDesc	tupdesc;

					functypclass = get_expr_result_type(rtfunc->funcexpr,
														&funcrettype,
														&tupdesc);
					if (functypclass == TYPEFUNC_COMPOSITE /* ||
						functypclass == TYPEFUNC_COMPOSITE_DOMAIN */)
					{
						/* Composite data type, e.g. a table's row type */
						Assert(tupdesc);
						expandTupleDesc(tupdesc, rte->eref,
										rtfunc->funccolcount, atts_done,
										rtindex, sublevels_up, location,
										include_dropped, colnames, colvars);
					}
					else if (functypclass == TYPEFUNC_SCALAR)
					{
						/* Base data type, i.e. scalar */
						if (colnames)
							*colnames = lappend(*colnames,
												list_nth(rte->eref->colnames,
														 atts_done));

						if (colvars)
						{
							PGVar		   *varnode;

							varnode = makeVar(rtindex, atts_done + 1,
											  funcrettype, -1,
											  exprCollation(rtfunc->funcexpr),
											  sublevels_up);
							varnode->location = location;

							*colvars = lappend(*colvars, varnode);
						}
					}
					else if (functypclass == TYPEFUNC_RECORD)
					{
						if (colnames)
						{
							PGList	   *namelist;

							/* extract appropriate subset of column list */
							namelist = list_copy_tail(rte->eref->colnames,
													  atts_done);
							namelist = list_truncate(namelist,
													 rtfunc->funccolcount);
							*colnames = list_concat(*colnames, namelist);
						}

						if (colvars)
						{
							PGListCell   *l1;
							PGListCell   *l2;
							PGListCell   *l3;
							int			attnum = atts_done;

							forthree(l1, rtfunc->funccoltypes,
									 l2, rtfunc->funccoltypmods,
									 l3, rtfunc->funccolcollations)
							{
								Oid			attrtype = lfirst_oid(l1);
								int32		attrtypmod = lfirst_int(l2);
								Oid			attrcollation = lfirst_oid(l3);
								PGVar		   *varnode;

								attnum++;
								varnode = makeVar(rtindex,
												  attnum,
												  attrtype,
												  attrtypmod,
												  attrcollation,
												  sublevels_up);
								varnode->location = location;
								*colvars = lappend(*colvars, varnode);
							}
						}
					}
					else
					{
						/* addRangeTableEntryForFunction should've caught this */
						elog(ERROR, "function in FROM has unsupported return type");
					}
					atts_done += rtfunc->funccolcount;
				}

				/* Append the ordinality column if any */
				if (rte->funcordinality)
				{
					if (colnames)
						*colnames = lappend(*colnames,
											llast(rte->eref->colnames));

					if (colvars)
					{
						PGVar		   *varnode = makeVar(rtindex,
													  atts_done + 1,
													  INT8OID,
													  -1,
													  InvalidOid,
													  sublevels_up);

						*colvars = lappend(*colvars, varnode);
					}
				}
			}
			break;
		case PG_RTE_JOIN:
			{
				/* Join RTE */
				ListCell   *colname;
				ListCell   *aliasvar;

				Assert(list_length(rte->eref->colnames) == list_length(rte->joinaliasvars));

				varattno = 0;
				forboth(colname, rte->eref->colnames, aliasvar, rte->joinaliasvars)
				{
					PGNode	   *avar = (PGNode *) lfirst(aliasvar);

					varattno++;

					/*
					 * During ordinary parsing, there will never be any
					 * deleted columns in the join; but we have to check since
					 * this routine is also used by the rewriter, and joins
					 * found in stored rules might have join columns for
					 * since-deleted columns.  This will be signaled by a null
					 * pointer in the alias-vars list.
					 */
					if (avar == NULL)
					{
						if (include_dropped)
						{
							if (colnames)
								*colnames = lappend(*colnames,
													makeString(pstrdup("")));
							if (colvars)
							{
								/*
								 * Can't use join's column type here (it might
								 * be dropped!); but it doesn't really matter
								 * what type the Const claims to be.
								 */
								*colvars = lappend(*colvars,
												   makeNullConst(INT4OID, -1,
																 InvalidOid));
							}
						}
						continue;
					}

					if (colnames)
					{
						char	   *label = strVal(lfirst(colname));

						*colnames = lappend(*colnames,
											makeString(pstrdup(label)));
					}

					if (colvars)
					{
						PGVar		   *varnode;

						varnode = makeVar(rtindex, varattno,
										  exprType(avar),
										  exprTypmod(avar),
										  exprCollation(avar),
										  sublevels_up);
						varnode->location = location;

						*colvars = lappend(*colvars, varnode);
					}
				}
			}
			break;
		case PG_RTE_TABLEFUNC:
		case PG_RTE_VALUES:
		case PG_RTE_CTE:
		case RTE_NAMEDTUPLESTORE:
			{
				/* Tablefunc, Values, CTE, or ENR RTE */
				PGListCell   *aliasp_item = list_head(rte->eref->colnames);
				PGListCell   *lct;
				PGListCell   *lcm;
				PGListCell   *lcc;

				varattno = 0;
				forthree(lct, rte->coltypes,
						 lcm, rte->coltypmods,
						 lcc, rte->colcollations)
				{
					Oid			coltype = lfirst_oid(lct);
					int32		coltypmod = lfirst_int(lcm);
					Oid			colcoll = lfirst_oid(lcc);

					varattno++;

					if (colnames)
					{
						/* Assume there is one alias per output column */
						if (OidIsValid(coltype))
						{
							char	   *label = strVal(lfirst(aliasp_item));

							*colnames = lappend(*colnames,
												makeString(pstrdup(label)));
						}
						else if (include_dropped)
							*colnames = lappend(*colnames,
												makeString(pstrdup("")));

						aliasp_item = lnext(aliasp_item);
					}

					if (colvars)
					{
						if (OidIsValid(coltype))
						{
							PGVar		   *varnode;

							varnode = makeVar(rtindex, varattno,
											  coltype, coltypmod, colcoll,
											  sublevels_up);
							varnode->location = location;

							*colvars = lappend(*colvars, varnode);
						}
						else if (include_dropped)
						{
							/*
							 * It doesn't really matter what type the Const
							 * claims to be.
							 */
							*colvars = lappend(*colvars,
											   makeNullConst(INT4OID, -1,
															 InvalidOid));
						}
					}
				}
			}
			break;
		// case RTE_RESULT:
		// 	/* These expose no columns, so nothing to do */
		// 	break;
		default:
			elog(ERROR, "unrecognized RTE kind: %d", (int) rte->rtekind);
	}
};

PGRangeTblEntry *
RelationParser::addRangeTableEntry(PGParseState *pstate,
				   PGRangeVar *relation,
				   PGAlias *alias,
				   bool inh,
				   bool inFromCl)
{
	PGRangeTblEntry *rte = makeNode(PGRangeTblEntry);
	char	   *refname = alias ? alias->aliasname : relation->relname;
	LOCKMODE	lockmode = AccessShareLock;
	PGLockingClause *locking;
	PGRelation	rel;

	Assert(pstate != NULL);

	rte->alias = alias;
	rte->rtekind = PG_RTE_RELATION;

	/*
	 * Identify the type of lock we'll need on this relation.  It's not the
	 * query's target table (that case is handled elsewhere), so we need
	 * either RowShareLock if it's locked by FOR UPDATE/SHARE, or plain
	 * AccessShareLock otherwise.
	 */
	/*
	 * Greenplum specific behavior:
	 * The implementation of select statement with locking clause
	 * (for update | no key update | share | key share) in postgres
	 * is to hold RowShareLock on tables during parsing stage, and
	 * generate a LockRows plan node for executor to lock the tuples.
	 * It is not easy to lock tuples in Greenplum database, since
	 * tuples may be fetched through motion nodes.
	 *
	 * But when Global Deadlock Detector is enabled, and the select
	 * statement with locking clause contains only one table, we are
	 * sure that there are no motions. For such simple cases, we could
	 * make the behavior just the same as Postgres.
	 */
	locking = getLockedRefname(pstate, refname);
	if (locking)
	{
		Oid relid;

		relid = RangeVarGetRelid(relation, lockmode, false);

		rel = try_table_open(relid, NoLock, true);
		if (!rel)
			elog(ERROR, "open relation(%u) fail", relid);

		if (rel->rd_rel->relkind != RELKIND_RELATION ||
			GpPolicyIsReplicated(rel->rd_cdbpolicy) ||
			RelationIsAppendOptimized(rel))
			pstate->p_canOptSelectLockingClause = false;

		if (rel->rd_rel->relkind == RELKIND_MATVIEW)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot lock rows in materialized view \"%s\"",
							RelationGetRelationName(rel))));

		lockmode = pstate->p_canOptSelectLockingClause ? RowShareLock : ExclusiveLock;
		if (lockmode == ExclusiveLock && locking->waitPolicy != LockWaitBlock)
			ereport(WARNING,
					(errmsg("Upgrade the lockmode to ExclusiveLock on table(%s) and ingore the wait policy.",
					 RelationGetRelationName(rel))));

		heap_close(rel, NoLock);

	}
	else
		lockmode = AccessShareLock;

	/*
	 * Get the rel's OID.  This access also ensures that we have an up-to-date
	 * relcache entry for the rel.  Since this is typically the first access
	 * to a rel in a statement, we must open the rel with the proper lockmode.
	 */
	rel = parserOpenTable(pstate, relation, lockmode, NULL);
	rte->relid = RelationGetRelid(rel);
	rte->relkind = rel->rd_rel->relkind;
	rte->rellockmode = lockmode;

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
	table_close(rel, NoLock);

	/*
	 * Set flags and access permissions.
	 *
	 * The initial default on access checks is always check-for-READ-access,
	 * which is the right thing for all except target tables.
	 */
	rte->lateral = false;
	rte->inh = inh;
	rte->inFromCl = inFromCl;

	// rte->requiredPerms = ACL_SELECT;
	// rte->checkAsUser = InvalidOid;	/* not set-uid by default, either */
	// rte->selectedCols = NULL;
	// rte->insertedCols = NULL;
	// rte->updatedCols = NULL;
	// rte->extraUpdatedCols = NULL;

	/*
	 * Add completed RTE to pstate's range table list, but not to join list
	 * nor namespace --- caller must do that if appropriate.
	 */
	pstate->p_rtable = lappend(pstate->p_rtable, rte);

	return rte;
};

PGRangeTblEntry *
RelationParser::addRangeTableEntryForSubquery(PGParseState *pstate,
							  PGQuery *subquery,
							  PGAlias *alias,
							  bool lateral,
							  bool inFromCl)
{
	PGRangeTblEntry *rte = makeNode(PGRangeTblEntry);
	char	   *refname = alias->aliasname;
	PGAlias	   *eref;
	int			numaliases;
	int			varattno;
	ListCell   *tlistitem;

	rte->rtekind = PG_RTE_SUBQUERY;
	rte->subquery = subquery;
	rte->alias = alias;

	eref = (PGAlias *)copyObject(alias);
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

	/*
	 * Set flags and access permissions.
	 *
	 * Subqueries are never checked for access rights.
	 */
	rte->lateral = lateral;
	rte->inh = false;			/* never true for subqueries */
	rte->inFromCl = inFromCl;

	// rte->requiredPerms = 0;
	// rte->checkAsUser = InvalidOid;
	// rte->selectedCols = NULL;
	// rte->insertedCols = NULL;
	// rte->updatedCols = NULL;
	// rte->extraUpdatedCols = NULL;

	/*
	 * Add completed RTE to pstate's range table list, but not to join list
	 * nor namespace --- caller must do that if appropriate.
	 */
	if (pstate != NULL)
		pstate->p_rtable = lappend(pstate->p_rtable, rte);

	return rte;
};

void
RelationParser::checkNameSpaceConflicts(PGParseState *pstate, PGList *namespace1,
						PGList *namespace2)
{
	PGListCell   *l1;

	foreach(l1, namespace1)
	{
		PGParseNamespaceItem *nsitem1 = (PGParseNamespaceItem *) lfirst(l1);
		PGRangeTblEntry *rte1 = nsitem1->p_rte;
		const char *aliasname1 = rte1->eref->aliasname;
		ListCell   *l2;

		if (!nsitem1->p_rel_visible)
			continue;

		foreach(l2, namespace2)
		{
			PGParseNamespaceItem *nsitem2 = (PGParseNamespaceItem *) lfirst(l2);
			PGRangeTblEntry *rte2 = nsitem2->p_rte;

			if (!nsitem2->p_rel_visible)
				continue;
			if (strcmp(rte2->eref->aliasname, aliasname1) != 0)
				continue;		/* definitely no conflict */
			if (rte1->rtekind == PG_RTE_RELATION && rte1->alias == NULL &&
				rte2->rtekind == PG_RTE_RELATION && rte2->alias == NULL &&
				rte1->relid != rte2->relid)
				continue;		/* no conflict per SQL rule */
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_ALIAS),
					 errmsg("table name \"%s\" specified more than once",
							aliasname1)));
		}
	}
};

PGRangeTblEntry *
RelationParser::GetRTEByRangeTablePosn(PGParseState *pstate,
					   int varno,
					   int sublevels_up)
{
	while (sublevels_up-- > 0)
	{
		pstate = pstate->parentParseState;
		Assert(pstate != NULL);
	}
	Assert(varno > 0 && varno <= list_length(pstate->p_rtable));
	return rt_fetch(varno, pstate->p_rtable);
};

PGLockingClause *
RelationParser::getLockedRefname(PGParseState *pstate, const char *refname)
{
	PGListCell   *l;

	/*
	 * If we are in a subquery specified as locked FOR UPDATE/SHARE from
	 * parent level, then act as though there's a generic FOR UPDATE here.
	 */
	if (pstate->p_lockclause_from_parent)
		return pstate->p_lockclause_from_parent;

	foreach(l, pstate->p_locking_clause)
	{
		PGLockingClause *lc = (PGLockingClause *) lfirst(l);

		if (lc->lockedRels == NIL)
		{
			/* all tables used in query */
			return lc;
		}
		else
		{
			/* just the named tables */
			ListCell   *l2;

			foreach(l2, lc->lockedRels)
			{
				PGRangeVar   *thisrel = (PGRangeVar *) lfirst(l2);

				if (strcmp(refname, thisrel->relname) == 0)
					return lc;
			}
		}
	}

	return NULL;
};

PGRangeTblEntry *
RelationParser::scanNameSpaceForRelid(PGParseState *pstate, Oid relid, int location)
{
	PGRangeTblEntry *result = NULL;
	PGListCell   *l;

	foreach(l, pstate->p_namespace)
	{
		PGParseNamespaceItem *nsitem = (PGParseNamespaceItem *) lfirst(l);
		PGRangeTblEntry *rte = nsitem->p_rte;

		/* Ignore columns-only items */
		if (!nsitem->p_rel_visible)
			continue;
		/* If not inside LATERAL, ignore lateral-only items */
		if (nsitem->p_lateral_only && !pstate->p_lateral_active)
			continue;

		/* yes, the test for alias == NULL should be there... */
		if (rte->rtekind == PG_RTE_RELATION &&
			rte->relid == relid &&
			rte->alias == NULL)
		{
			if (result)
				ereport(ERROR,
						(errcode(ERRCODE_AMBIGUOUS_ALIAS),
						 errmsg("table reference %u is ambiguous",
								relid),
						 parser_errposition(pstate, location)));
			check_lateral_ref_ok(pstate, nsitem, location);
			result = rte;
		}
	}
	return result;
};

PGRangeTblEntry *
RelationParser::scanNameSpaceForRefname(PGParseState *pstate, const char *refname, int location)
{
	PGRangeTblEntry *result = NULL;
	PGListCell   *l;

	foreach(l, pstate->p_namespace)
	{
		PGParseNamespaceItem *nsitem = (PGParseNamespaceItem *) lfirst(l);
		PGRangeTblEntry *rte = nsitem->p_rte;

		/* Ignore columns-only items */
		if (!nsitem->p_rel_visible)
			continue;
		/* If not inside LATERAL, ignore lateral-only items */
		if (nsitem->p_lateral_only && !pstate->p_lateral_active)
			continue;

		if (strcmp(rte->eref->aliasname, refname) == 0)
		{
			if (result)
				ereport(ERROR,
						(errcode(ERRCODE_AMBIGUOUS_ALIAS),
						 errmsg("table reference \"%s\" is ambiguous",
								refname),
						 parser_errposition(pstate, location)));
			check_lateral_ref_ok(pstate, nsitem, location);
			result = rte;
		}
	}
	return result;
};

PGRangeTblEntry *
RelationParser::refnameRangeTblEntry(PGParseState *pstate,
					 const char *schemaname,
					 const char *refname,
					 int location,
					 int *sublevels_up)
{
	Oid			relId = InvalidOid;

	if (sublevels_up)
		*sublevels_up = 0;

	if (schemaname != NULL)
	{
		Oid			namespaceId;

		/*
		 * We can use LookupNamespaceNoError() here because we are only
		 * interested in finding existing RTEs.  Checking USAGE permission on
		 * the schema is unnecessary since it would have already been checked
		 * when the RTE was made.  Furthermore, we want to report "RTE not
		 * found", not "no permissions for schema", if the name happens to
		 * match a schema name the user hasn't got access to.
		 */
		namespaceId = LookupNamespaceNoError(schemaname);
		if (!OidIsValid(namespaceId))
			return NULL;
		relId = get_relname_relid(refname, namespaceId);
		if (!OidIsValid(relId))
			return NULL;
	}

	while (pstate != NULL)
	{
		PGRangeTblEntry *result;

		if (OidIsValid(relId))
			result = scanNameSpaceForRelid(pstate, relId, location);
		else
			result = scanNameSpaceForRefname(pstate, refname, location);

		if (result)
			return result;

		if (sublevels_up)
			(*sublevels_up)++;
		else
			break;

		pstate = pstate->parentParseState;
	}
	return NULL;
};

PGRangeTblEntry *
RelationParser::addRangeTableEntryForJoin(PGParseState *pstate,
						  PGList *colnames,
						  PGJoinType jointype,
						  PGList *aliasvars,
						  PGAlias *alias,
						  bool inFromCl)
{
	PGRangeTblEntry *rte = makeNode(PGRangeTblEntry);
	PGAlias	   *eref;
	int			numaliases;

	Assert(pstate != NULL);

	/*
	 * Fail if join has too many columns --- we must be able to reference any
	 * of the columns with an AttrNumber.
	 */
	if (list_length(aliasvars) > MaxAttrNumber)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("joins can have at most %d columns",
						MaxAttrNumber)));

	rte->rtekind = PG_RTE_JOIN;
	rte->relid = InvalidOid;
	rte->subquery = NULL;
	rte->jointype = jointype;
	rte->joinaliasvars = aliasvars;
	rte->alias = alias;

	eref = alias ? (PGAlias *)copyObject(alias) : makeAlias("unnamed_join", NIL);
	numaliases = list_length(eref->colnames);

	/* fill in any unspecified alias columns */
	if (numaliases < list_length(colnames))
		eref->colnames = list_concat(eref->colnames,
									 list_copy_tail(colnames, numaliases));

	rte->eref = eref;

	/*
	 * Set flags and access permissions.
	 *
	 * Joins are never checked for access rights.
	 */
	rte->lateral = false;
	rte->inh = false;			/* never true for joins */
	rte->inFromCl = inFromCl;

	// rte->requiredPerms = 0;
	// rte->checkAsUser = InvalidOid;
	// rte->selectedCols = NULL;
	// rte->insertedCols = NULL;
	// rte->updatedCols = NULL;
	// rte->extraUpdatedCols = NULL;

	/*
	 * Add completed RTE to pstate's range table list, but not to join list
	 * nor namespace --- caller must do that if appropriate.
	 */
	pstate->p_rtable = lappend(pstate->p_rtable, rte);

	return rte;
};

void
RelationParser::markRTEForSelectPriv(PGParseState *pstate, PGRangeTblEntry *rte,
					 int rtindex, PGAttrNumber col)
{
	if (rte == NULL)
		rte = rt_fetch(rtindex, pstate->p_rtable);

	if (rte->rtekind == PG_RTE_RELATION)
	{
		// /* Make sure the rel as a whole is marked for SELECT access */
		// rte->requiredPerms |= PG_ACL_SELECT;
		// /* Must offset the attnum to fit in a bitmapset */
		// rte->selectedCols = bms_add_member(rte->selectedCols,
		// 								   col - FirstLowInvalidHeapAttributeNumber);
	}
	else if (rte->rtekind == PG_RTE_JOIN)
	{
		if (col == InvalidAttrNumber)
		{
			/*
			 * A whole-row reference to a join has to be treated as whole-row
			 * references to the two inputs.
			 */
			PGJoinExpr   *j;

			if (rtindex > 0 && rtindex <= list_length(pstate->p_joinexprs))
				j = list_nth_node(PGJoinExpr, pstate->p_joinexprs, rtindex - 1);
			else
				j = NULL;
			if (j == NULL)
				elog(ERROR, "could not find JoinExpr for whole-row reference");

			/* Note: we can't see FromExpr here */
			if (IsA(j->larg, PGRangeTblRef))
			{
				int			varno = ((PGRangeTblRef *) j->larg)->rtindex;

				markRTEForSelectPriv(pstate, NULL, varno, InvalidAttrNumber);
			}
			else if (IsA(j->larg, PGJoinExpr))
			{
				int			varno = ((PGJoinExpr *) j->larg)->rtindex;

				markRTEForSelectPriv(pstate, NULL, varno, InvalidAttrNumber);
			}
			else
				elog(ERROR, "unrecognized node type: %d",
					 (int) nodeTag(j->larg));
			if (IsA(j->rarg, PGRangeTblRef))
			{
				int			varno = ((PGRangeTblRef *) j->rarg)->rtindex;

				markRTEForSelectPriv(pstate, NULL, varno, InvalidAttrNumber);
			}
			else if (IsA(j->rarg, PGJoinExpr))
			{
				int			varno = ((PGJoinExpr *) j->rarg)->rtindex;

				markRTEForSelectPriv(pstate, NULL, varno, InvalidAttrNumber);
			}
			else
				elog(ERROR, "unrecognized node type: %d",
					 (int) nodeTag(j->rarg));
		}
		else
		{
			/*
			 * Regular join attribute, look at the alias-variable list.
			 *
			 * The aliasvar could be either a Var or a COALESCE expression,
			 * but in the latter case we should already have marked the two
			 * referent variables as being selected, due to their use in the
			 * JOIN clause.  So we need only be concerned with the Var case.
			 * But we do need to drill down through implicit coercions.
			 */
			PGVar		   *aliasvar;

			Assert(col > 0 && col <= list_length(rte->joinaliasvars));
			aliasvar = (PGVar *) list_nth(rte->joinaliasvars, col - 1);
			aliasvar = (PGVar *) strip_implicit_coercions((PGNode *) aliasvar);
			if (aliasvar && IsA(aliasvar, PGVar))
				markVarForSelectPriv(pstate, aliasvar, NULL);
		}
	}
	/* other RTE types don't require privilege marking */
};

void
RelationParser::markVarForSelectPriv(PGParseState *pstate, PGVar *var, PGRangeTblEntry *rte)
{
	Index		lv;

	Assert(IsA(var, PGVar));
	/* Find the appropriate pstate if it's an uplevel Var */
	for (lv = 0; lv < var->varlevelsup; lv++)
		pstate = pstate->parentParseState;
	markRTEForSelectPriv(pstate, rte, var->varno, var->varattno);
};

void
RelationParser::updateFuzzyAttrMatchState(int fuzzy_rte_penalty,
						  FuzzyAttrMatchState *fuzzystate, PGRangeTblEntry *rte,
						  const char *actual, const char *match, int attnum)
{
	int			columndistance;
	int			matchlen;

	/* Bail before computing the Levenshtein distance if there's no hope. */
	if (fuzzy_rte_penalty > fuzzystate->distance)
		return;

	/*
	 * Outright reject dropped columns, which can appear here with apparent
	 * empty actual names, per remarks within scanRTEForColumn().
	 */
	if (actual[0] == '\0')
		return;

	/* Use Levenshtein to compute match distance. */
	matchlen = strlen(match);
	columndistance =
		varstr_levenshtein_less_equal(actual, strlen(actual), match, matchlen,
									  1, 1, 1,
									  fuzzystate->distance + 1
									  - fuzzy_rte_penalty,
									  true);

	/*
	 * If more than half the characters are different, don't treat it as a
	 * match, to avoid making ridiculous suggestions.
	 */
	if (columndistance > matchlen / 2)
		return;

	/*
	 * From this point on, we can ignore the distinction between the RTE-name
	 * distance and the column-name distance.
	 */
	columndistance += fuzzy_rte_penalty;

	/*
	 * If the new distance is less than or equal to that of the best match
	 * found so far, update fuzzystate.
	 */
	if (columndistance < fuzzystate->distance)
	{
		/* Store new lowest observed distance for RTE */
		fuzzystate->distance = columndistance;
		fuzzystate->rfirst = rte;
		fuzzystate->first = attnum;
		fuzzystate->rsecond = NULL;
		fuzzystate->second = InvalidAttrNumber;
	}
	else if (columndistance == fuzzystate->distance)
	{
		/*
		 * This match distance may equal a prior match within this same range
		 * table.  When that happens, the prior match may also be given, but
		 * only if there is no more than two equally distant matches from the
		 * RTE (in turn, our caller will only accept two equally distant
		 * matches overall).
		 */
		if (AttributeNumberIsValid(fuzzystate->second))
		{
			/* Too many RTE-level matches */
			fuzzystate->rfirst = NULL;
			fuzzystate->first = InvalidAttrNumber;
			fuzzystate->rsecond = NULL;
			fuzzystate->second = InvalidAttrNumber;
			/* Clearly, distance is too low a bar (for *any* RTE) */
			fuzzystate->distance = columndistance - 1;
		}
		else if (AttributeNumberIsValid(fuzzystate->first))
		{
			/* Record as provisional second match for RTE */
			fuzzystate->rsecond = rte;
			fuzzystate->second = attnum;
		}
		else if (fuzzystate->distance <= MAX_FUZZY_DISTANCE)
		{
			/*
			 * Record as provisional first match (this can occasionally occur
			 * because previous lowest distance was "too low a bar", rather
			 * than being associated with a real match)
			 */
			fuzzystate->rfirst = rte;
			fuzzystate->first = attnum;
		}
	}
};

PGNode *
RelationParser::scanRTEForColumn(PGParseState *pstate, PGRangeTblEntry *rte, const char *colname,
				 int location, int fuzzy_rte_penalty,
				 FuzzyAttrMatchState *fuzzystate)
{
	PGNode	   *result = NULL;
	int			attnum = 0;
	PGVar		   *var;
	PGListCell   *c;

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
	 * has to do a cache lookup anyway, so the check there is cheap.  Callers
	 * interested in finding match with shortest distance need to defend
	 * against this directly, though.
	 */
	foreach(c, rte->eref->colnames)
	{
		const char *attcolname = strVal(lfirst(c));

		attnum++;
		if (strcmp(attcolname, colname) == 0)
		{
			if (result)
				ereport(ERROR,
						(errcode(ERRCODE_AMBIGUOUS_COLUMN),
						 errmsg("column reference \"%s\" is ambiguous",
								colname),
						 parser_errposition(pstate, location)));
			var = make_var(pstate, rte, attnum, location);
			/* Require read access to the column */
			markVarForSelectPriv(pstate, var, rte);
			result = (PGNode *) var;
		}

		/* Updating fuzzy match state, if provided. */
		if (fuzzystate != NULL)
			updateFuzzyAttrMatchState(fuzzy_rte_penalty, fuzzystate,
									  rte, attcolname, colname, attnum);
	}

	/*
	 * If we have a unique match, return it.  Note that this allows a user
	 * alias to override a system column name (such as OID) without error.
	 */
	if (result)
		return result;

	/*
	 * If the RTE represents a real relation, consider system column names.
	 * Composites are only used for pseudo-relations like ON CONFLICT's
	 * excluded.
	 */
	if (rte->rtekind == PG_RTE_RELATION &&
		rte->relkind != RELKIND_COMPOSITE_TYPE)
	{
		/* In GPDB, system columns like gp_segment_id, ctid, xmin/xmax seem to be
		 * ambiguous for replicated table, replica in each segment has different
		 * value of those columns, between sessions, different replicas are chosen
		 * to provide data, so it's weird for users to see different system columns
		 * between sessions. So for replicated table, we don't expose system columns
		 * unless it's GP_ROLE_UTILITY for debug purpose.
		 */
		if (GpPolicyIsReplicated(GpPolicyFetch(rte->relid)) &&
			Gp_role != GP_ROLE_UTILITY)
			return result;

		/* quick check to see if name could be a system column */
		attnum = specialAttNum(colname);

		/* In constraint check, no system column is allowed except tableOid */
		if (pstate->p_expr_kind == EXPR_KIND_CHECK_CONSTRAINT &&
			attnum < InvalidAttrNumber && attnum != TableOidAttributeNumber)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
					 errmsg("system column \"%s\" reference in check constraint is invalid",
							colname),
					 parser_errposition(pstate, location)));

		/*
		 * In generated column, no system column is allowed except tableOid.
		 */
		if (pstate->p_expr_kind == EXPR_KIND_GENERATED_COLUMN &&
			attnum < InvalidAttrNumber && attnum != TableOidAttributeNumber)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
					 errmsg("cannot use system column \"%s\" in column generation expression",
							colname),
					 parser_errposition(pstate, location)));

		if (attnum != InvalidAttrNumber)
		{
			/*
			 * Now check to see if column actually is defined.  Because of
			 * an ancient oversight in DefineQueryRewrite, it's possible that
			 * pg_attribute contains entries for system columns for a view,
			 * even though views should not have such --- so we also check
			 * the relkind.  This kluge will not be needed in 9.3 and later.
			 */
			if (SearchSysCacheExists2(ATTNUM,
									  ObjectIdGetDatum(rte->relid),
									  Int16GetDatum(attnum)) &&
				get_rel_relkind(rte->relid) != RELKIND_VIEW)
			{
				var = make_var(pstate, rte, attnum, location);
				/* Require read access to the column */
				markVarForSelectPriv(pstate, var, rte);
				result = (PGNode *) var;
			}
		}
	}

	return result;
};

PGNode *
RelationParser::colNameToVar(PGParseState *pstate, const char *colname, bool localonly,
			 int location)
{
	PGNode	   *result = NULL;
	PGParseState *orig_pstate = pstate;

	while (pstate != NULL)
	{
		ListCell   *l;

		foreach(l, pstate->p_namespace)
		{
			PGParseNamespaceItem *nsitem = (PGParseNamespaceItem *) lfirst(l);
			PGRangeTblEntry *rte = nsitem->p_rte;
			PGNode	   *newresult;

			/* Ignore table-only items */
			if (!nsitem->p_cols_visible)
				continue;
			/* If not inside LATERAL, ignore lateral-only items */
			if (nsitem->p_lateral_only && !pstate->p_lateral_active)
				continue;

			/* use orig_pstate here to get the right sublevels_up */
			newresult = scanRTEForColumn(orig_pstate, rte, colname, location,
										 0, NULL);

			if (newresult)
			{
				if (result)
					ereport(ERROR,
							(errcode(ERRCODE_AMBIGUOUS_COLUMN),
							 errmsg("column reference \"%s\" is ambiguous",
									colname),
							 parser_errposition(pstate, location)));
				check_lateral_ref_ok(pstate, nsitem, location);
				result = newresult;
			}
		}

		if (result != NULL || localonly)
			break;				/* found, or don't want to look at parent */

		pstate = pstate->parentParseState;
	}

	return result;
};

}