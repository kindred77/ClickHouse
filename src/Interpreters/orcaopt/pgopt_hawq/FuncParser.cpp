#include <Interpreters/orcaopt/pgopt_hawq/FuncParser.h>

namespace DB
{
using namespace duckdb_libpgquery;

bool FuncParser::typeInheritsFrom(Oid subclassTypeId, Oid superclassTypeId)
{
    bool result = false;
    Oid relid;
    Relation inhrel;
    PGList *visited, *queue;
    PGListCell * queue_item;

    if (!ISCOMPLEX(subclassTypeId) || !ISCOMPLEX(superclassTypeId))
        return false;
    relid = typeidTypeRelid(subclassTypeId);
    if (relid == InvalidOid)
        return false;

    /*
	 * Begin the search at the relation itself, so add relid to the queue.
	 */
    queue = list_make1_oid(relid);
    visited = NIL;

    inhrel = heap_open(InheritsRelationId, AccessShareLock);

    /*
	 * Use queue to do a breadth-first traversal of the inheritance graph from
	 * the relid supplied up to the root.  Notice that we append to the queue
	 * inside the loop --- this is okay because the foreach() macro doesn't
	 * advance queue_item until the next loop iteration begins.
	 */
    foreach (queue_item, queue)
    {
        Oid this_relid = lfirst_oid(queue_item);
        cqContext * pcqCtx;
        cqContext cqc;
        HeapTuple inhtup;

        /* If we've seen this relid already, skip it */
        if (list_member_oid(visited, this_relid))
            continue;

        /*
		 * Okay, this is a not-yet-seen relid. Add it to the list of
		 * already-visited OIDs, then find all the types this relid inherits
		 * from and add them to the queue. The one exception is we don't add
		 * the original relation to 'visited'.
		 */
        if (queue_item != list_head(queue))
            visited = lappend_oid(visited, this_relid);

        pcqCtx = caql_beginscan(
            caql_addrel(cqclr(&cqc), inhrel),
            cql("SELECT * FROM pg_inherits "
                " WHERE inhrelid = :1 ",
                ObjectIdGetDatum(this_relid)));

        while (HeapTupleIsValid(inhtup = caql_getnext(pcqCtx)))
        {
            Form_pg_inherits inh = (Form_pg_inherits)GETSTRUCT(inhtup);
            Oid inhparent = inh->inhparent;

            /* If this is the target superclass, we're done */
            if (get_rel_type_id(inhparent) == superclassTypeId)
            {
                result = true;
                break;
            }

            /* Else add to queue */
            queue = lappend_oid(queue, inhparent);
        }

        caql_endscan(pcqCtx);

        if (result)
            break;
    }

    heap_close(inhrel, AccessShareLock);

    list_free(visited);
    list_free(queue);

    return result;
};

void FuncParser::make_fn_arguments(PGParseState * pstate, duckdb_libpgquery::PGList * fargs,
        Oid * actual_arg_types, Oid * declared_arg_types)
{
    PGListCell * current_fargs;
    int i = 0;

    foreach (current_fargs, fargs)
    {
        /* types don't match? then force coercion using a function call... */
        if (actual_arg_types[i] != declared_arg_types[i])
        {
            lfirst(current_fargs) = coerce_parser.coerce_type(
                pstate, lfirst(current_fargs), actual_arg_types[i], declared_arg_types[i], -1, PG_COERCION_IMPLICIT, PG_COERCE_IMPLICIT_CAST, -1);
        }
        i++;
    }
};

PGNode * FuncParser::ParseFuncOrColumn(
        PGParseState * pstate,
        PGList * funcname,
        PGList * fargs,
        PGList * agg_order,
        bool agg_star,
        bool agg_distinct,
        bool func_variadic,
        bool is_column,
        WindowSpec * over,
        int location,
        PGNode * agg_filter)
{
    Oid rettype = InvalidOid;
    Oid funcid = InvalidOid;
    PGListCell * l;
    PGListCell * nextl;
    PGNode * first_arg = NULL;
    int nargs;
    Oid actual_arg_types[FUNC_MAX_ARGS];
    Oid * declared_arg_types = NULL;
    PGNode * retval = NULL;
    bool retset = false;
    bool retstrict = false;
    bool retordered = false;
    FuncDetailCode fdresult;
    int nvargs;

    /*
	 * Most of the rest of the parser just assumes that functions do not have
	 * more than FUNC_MAX_ARGS parameters.	We have to test here to protect
	 * against array overruns, etc.  Of course, this may not be a function,
	 * but the test doesn't hurt.
	 */
    if (list_length(fargs) > FUNC_MAX_ARGS)
        ereport(
            ERROR,
            (errcode(ERRCODE_TOO_MANY_ARGUMENTS),
             errmsg("cannot pass more than %d arguments to a function", FUNC_MAX_ARGS),
             parser_errposition(pstate, location)));

    /* 
	 * Perform the FILTER -> CASE transform.
	 *    FUNC(expr) FILTER (WHERE cond)  =>  FUNC(CASE WHEN cond THEN expr END)
	 * This must be done for every parameter of the function and special handling
	 * is needed for FUNC(*).  
	 *
	 * For this to be a valid transform we must assume that NULLs passed into
	 * the function will not change the result.  This assumption is not valid
	 * for count(*), which is why we need special processing for this case.  If
	 * it is not a valid assumption for other cases we may need to rethink how
	 * we implement FILTER.
	 */
    if (agg_filter)
    {
        PGList * newfargs = NULL;

        if (agg_star || !fargs)
        {
            /*
			 * FUNC(*) => assume that datatype doesn't matter 
			 * By converting agg_star into a conditional constant boolean 
			 * expression we get the correct results for count(*) since it
			 * will then supress the NULLs returned by the CASE statement.
			 */
            PGCaseExpr * c = makeNode(PGCaseExpr);
            PGCaseWhen * w = makeNode(PGCaseWhen);
            PGAConst * a = makeNode(PGAConst);
            a->val.type = T_PGInteger;
            a->val.val.ival = 1; /* Actual value shouldn't matter */
            w->expr = (PGExpr *)agg_filter;
            w->result = (PGExpr *)a;
            c->casetype = InvalidOid; /* will analyze in a moment */
            c->arg = (PGExpr *)NULL;
            c->defresult = (PGExpr *)NULL;
            c->args = list_make1(w);
            newfargs = list_make1(c);

            /* 
			 * Since we haven't checked the compatability of our function with
			 * agg_star we can not clear the local bit yet, otherwise we would
			 * loose track of the fact that this was an agg_star operation prior
			 * to transformation.
			 */
        }
        else
        {
            Assert(fargs && list_length(fargs) > 0);

            foreach (l, fargs)
            {
                PGCaseExpr * c = makeNode(PGCaseExpr);
                PGCaseWhen * w = makeNode(PGCaseWhen);
                w->expr = (PGExpr *)agg_filter;
                w->result = (PGExpr *)lfirst(l);
                c->casetype = InvalidOid; /* will analyze in a moment */
                c->arg = (PGExpr *)NULL;
                c->defresult = (PGExpr *)NULL;
                c->args = list_make1(w);

                if (newfargs)
                    lappend(newfargs, c);
                else
                    newfargs = list_make1(c);
            }
        }
        fargs = transformExpressionList(pstate, newfargs);
    }

    /*
	 * Extract arg type info in preparation for function lookup.
	 *
	 * If any arguments are Param markers of type VOID, we discard them from
	 * the parameter list.	This is a hack to allow the JDBC driver to not
	 * have to distinguish "input" and "output" parameter symbols while
	 * parsing function-call constructs.  We can't use foreach() because we
	 * may modify the list ...
	 */
    nargs = 0;
    for (l = list_head(fargs); l != NULL; l = nextl)
    {
        PGNode * arg = lfirst(l);
        Oid argtype = exprType(arg);

        nextl = lnext(l);

        if (argtype == VOIDOID && IsA(arg, PGParam) && !is_column)
        {
            fargs = list_delete_ptr(fargs, arg);
            continue;
        }

        actual_arg_types[nargs++] = argtype;
    }

    if (fargs)
    {
        first_arg = linitial(fargs);
        Assert(first_arg != NULL);
    }

    /*
	 * Check for column projection: if function has one argument, and that
	 * argument is of complex type, and function name is not qualified, then
	 * the "function call" could be a projection.  We also check that there
	 * wasn't any aggregate or variadic decoration.
	 */
    if (nargs == 1 && agg_order == NIL && !agg_star && !agg_distinct && !agg_filter && list_length(funcname) == 1 && !func_variadic)
    {
        Oid argtype = actual_arg_types[0];

        if (argtype == RECORDOID || ISCOMPLEX(argtype))
        {
            retval = ParseComplexProjection(pstate, strVal(linitial(funcname)), first_arg, location);
            if (retval)
                return retval;

            /*
			 * If ParseComplexProjection doesn't recognize it as a projection,
			 * just press on.
			 */
        }
    }

    /*
	 * Okay, it's not a column projection, so it must really be a function.
	 * func_get_detail looks up the function in the catalogs, does
	 * disambiguation for polymorphic functions, handles inheritance, and
	 * returns the funcid and type and set or singleton status of the
   * function's return value.  It also returns the true argument types to
   * the function.     * the function.  (In the case of a variadic function call, the reported
   * "true" types aren't really what is in pg_proc: the variadic argument is
   * replaced by a suitable number of copies of its element type.  We'll fix
   * it up below.)
	 */
    fdresult = func_get_detail(
        funcname,
        fargs,
        nargs,
        actual_arg_types,
        !func_variadic,
        &funcid,
        &rettype,
        &retset,
        &retstrict,
        &retordered,
        &nvargs,
        &declared_arg_types);
    if (fdresult == FUNCDETAIL_COERCION)
    {
        /*
		 * We can do it as a trivial coercion. coerce_type can handle these
		 * cases, so why duplicate code...
		 */
        return coerce_type(pstate, linitial(fargs), actual_arg_types[0], rettype, -1, PG_COERCION_EXPLICIT, PG_COERCE_EXPLICIT_CALL, -1);
    }
    else if (fdresult == FUNCDETAIL_NORMAL)
    {
        /*
		 * Normal function found; was there anything indicating it must be an
		 * aggregate?
		 */
        if (agg_star)
            ereport(
                ERROR,
                (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                 errmsg("%s(*) specified, but %s is not an aggregate function", NameListToString(funcname), NameListToString(funcname)),
                 parser_errposition(pstate, location)));
        if (agg_distinct)
            ereport(
                ERROR,
                (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                 errmsg("DISTINCT specified, but %s is not an aggregate function", NameListToString(funcname)),
                 errOmitLocation(true),
                 parser_errposition(pstate, location)));
        if (agg_order)
            ereport(
                ERROR,
                (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                 errmsg("ORDER BY specified, but %s is not an ordered aggregate function", NameListToString(funcname)),
                 errOmitLocation(true),
                 parser_errposition(pstate, location)));
        if (agg_filter)
            ereport(
                ERROR,
                (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                 errmsg(
                     "filter clause specified, but "
                     "%s is not an aggregate function",
                     NameListToString(funcname)),
                 errOmitLocation(true),
                 parser_errposition(pstate, location)));
    }
    else if (fdresult != FUNCDETAIL_AGGREGATE)
    {
        /*
		 * Oops.  Time to die.
		 *
		 * If we are dealing with the attribute notation rel.function, give an
		 * error message that is appropriate for that case.
		 */
        if (is_column)
        {
            Assert(nargs == 1);
            Assert(list_length(funcname) == 1);
            unknown_attribute(pstate, first_arg, strVal(linitial(funcname)), location);
        }

        /*
		 * Else generate a detailed complaint for a function
		 */
        if (fdresult == FUNCDETAIL_MULTIPLE)
            ereport(
                ERROR,
                (errcode(ERRCODE_AMBIGUOUS_FUNCTION),
                 errmsg("function %s is not unique", func_signature_string(funcname, nargs, actual_arg_types)),
                 errhint("Could not choose a best candidate function. "
                         "You may need to add explicit type casts."),
                 errOmitLocation(true),
                 parser_errposition(pstate, location)));
        else
            ereport(
                ERROR,
                (errcode(ERRCODE_UNDEFINED_FUNCTION),
                 errmsg("function %s does not exist", func_signature_string(funcname, nargs, actual_arg_types)),
                 errhint("No function matches the given name and argument types. "
                         "You may need to add explicit type casts."),
                 errOmitLocation(true),
                 parser_errposition(pstate, location)));
    }

    /*
	 * The agg_filter rewrite in the case of agg_star is only valid for count(*)
	 * otherwise we need to throw an error.
	 */
    if (agg_star && agg_filter && funcid != COUNT_ANY_OID)
    {
        ereport(
            ERROR,
            (errcode(ERRCODE_UNDEFINED_FUNCTION),
             errmsg("function %s() does not exist", NameListToString(funcname)),
             errhint("No function matches the given name and argument types. "
                     "You may need to add explicit type casts."),
             errOmitLocation(true),
             parser_errposition(pstate, location)));
    }

    /*
	 * enforce consistency with ANYARRAY and ANYELEMENT argument and return
	 * types, possibly adjusting return type or declared_arg_types (which will
	 * be used as the cast destination by make_fn_arguments)
	 */
    rettype = enforce_generic_type_consistency(actual_arg_types, declared_arg_types, nargs, rettype);

    /* perform the necessary typecasting of arguments */
    make_fn_arguments(pstate, fargs, actual_arg_types, declared_arg_types);

    /*
   * If it's a variadic function call, transform the last nvargs arguments
   * into an array --- unless it's an "any" variadic.
   */
    if (nvargs > 0 && declared_arg_types[nargs - 1] != ANYOID)
    {
        PGArrayExpr * newa = makeNode(PGArrayExpr);
        int non_var_args = nargs - nvargs;
        PGList * vargs;

        Assert(non_var_args >= 0);
        vargs = list_copy_tail(fargs, non_var_args);
        fargs = list_truncate(fargs, non_var_args);

        newa->elements = vargs;
        /* assume all the variadic arguments were coerced to the same type */
        newa->element_typeid = exprType((PGNode *)linitial(vargs));
        newa->array_typeid = get_array_type(newa->element_typeid);
        if (!OidIsValid(newa->array_typeid))
            ereport(
                ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                 errmsg("could not find array type for data type %s", format_type_be(newa->element_typeid))));
        newa->multidims = false;

        fargs = lappend(fargs, newa);
    }


    /* build the appropriate output structure */
    if (fdresult == FUNCDETAIL_NORMAL && over == NULL)
    {
        PGFuncExpr * funcexpr = makeNode(PGFuncExpr);

        funcexpr->funcid = funcid;
        funcexpr->funcresulttype = rettype;
        funcexpr->funcretset = retset;
        funcexpr->funcformat = PG_COERCE_EXPLICIT_CALL;
        funcexpr->args = fargs;

        retval = (PGNode *)funcexpr;
    }
    else if (over != NULL)
    {
        /* must be a window function call */
        WindowRef * winref = makeNode(WindowRef);
        HeapTuple tuple;
        cqContext * wincqCtx;

        if (retset)
            ereport(
                ERROR,
                (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
                 errmsg("window functions may not return sets"),
                 parser_errposition(pstate, location)));

        if (agg_order)
            ereport(
                ERROR,
                (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                 errmsg("aggregate ORDER BY is not implemented for window functions"),
                 parser_errposition(pstate, location)));


        /*
         * If this is a "true" window function, rather than an aggregate
         * derived window function then it will have a tuple in pg_window
         */

        wincqCtx = caql_beginscan(
            NULL,
            cql("SELECT * FROM pg_window "
                " WHERE winfnoid = :1 ",
                ObjectIdGetDatum(funcid)));

        tuple = caql_getnext(wincqCtx);

        if (HeapTupleIsValid(tuple))
        {
            if (agg_filter)
                ereport(
                    ERROR,
                    (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                     errmsg(
                         "window function \"%s\" can not be used with a "
                         "filter clause",
                         NameListToString(funcname)),
                     parser_errposition(pstate, location)));

            /*
			 * We perform more checks – such as whether the window
			 * function requires ordering or permits a frame specification –
			 * later in transformWindowClause(). It's too early at this stage.
			 */
        }
        caql_endscan(wincqCtx);


        winref->winfnoid = funcid;
        winref->restype = rettype;
        winref->args = fargs;

        {
            /*
			 * Find if this "over" clause has already existed. If so,
			 * We let the "winspec" for this WindowRef point to
			 * the existing "over" clause. In this way, we will be able
			 * to determine if two WindowRef nodes are actually equal,
			 * see MPP-4268.
			 */
            int winspec = 0;
            PGListCell * over_lc = NULL;

            transformWindowSpec(pstate, over);

            foreach (over_lc, pstate->p_win_clauses)
            {
                PGNode * over1 = lfirst(over_lc);
                if (equal(over1, over))
                    break;
                winspec++;
            }

            if (over_lc == NULL)
                pstate->p_win_clauses = lappend(pstate->p_win_clauses, over);
            winref->winspec = winspec;
        }

        winref->windistinct = agg_distinct;
        winref->location = location;

        transformWindowFuncCall(pstate, winref);
        retval = (PGNode *)winref;
    }
    else
    {
        /* aggregate function */
        PGAggref * aggref;

        /*
		 * Reject attempt to call a parameterless aggregate without (*)
		 * syntax.	This is mere pedantry but some folks insisted ...
		 */
        if (fargs == NIL && !agg_star)
            ereport(
                ERROR,
                (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                 errmsg("%s(*) must be used to call a parameterless aggregate function", NameListToString(funcname)),
                 parser_errposition(pstate, location)));

        /* 
		 * We only support FILTER clauses over STRICT aggegation functions.
		 *
		 * All built in aggregations are strict except for int2_sum, 
         * int4_sum, and int8_sum, all of which are logically strict, but are
		 * simply defined as non-strict to bootstrap their calculations.  
		 * Since they are logically strict we will not change their results 
		 * by including extra nulls in the calculation so the rewrite won't 
		 * produce incorrect results.
		 *
		 * For user defined functions we must enforce this restriction since
		 * passing "extra" nulls back to a non-strict function may cause it
		 * to return an incorrect answer, eg: count_null(i) filter (...) 
		 * wouldn't differeniate between data nulls vs filtered values.
		 */
        if (agg_filter && !retstrict && (funcid < SUM_OID_MIN || funcid > SUM_OID_MAX))
        {
            ereport(
                ERROR,
                (errcode(ERRCODE_GP_FEATURE_NOT_SUPPORTED),
                 errmsg("function %s is not defined as STRICT", func_signature_string(funcname, nargs, actual_arg_types)),
                 errhint("The filter clause is only supported over functions "
                         "defined as STRICT."),
                 errOmitLocation(true)));
        }

        if (retset)
            ereport(
                ERROR,
                (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
                 errmsg("aggregates may not return sets"),
                 parser_errposition(pstate, location),
                 errOmitLocation(true)));

        /* 
		 * If this is not an ordered aggregate, but it was called with an
		 * aggregate order by specification then we must raise an error.
		 */
        if (!retordered && agg_order != NIL)
        {
            ereport(
                ERROR,
                (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                 errmsg("ORDER BY specified, but %s is not an ordered aggregate function", NameListToString(funcname)),
                 errOmitLocation(true),
                 parser_errposition(pstate, location)));
        }

        /* 
		 * ordered aggregates are not compatible with distinct
		 */
        if (agg_distinct && agg_order != NIL)
        {
            ereport(
                ERROR,
                (errcode(ERRCODE_GP_FEATURE_NOT_SUPPORTED),
                 errmsg("ORDER BY and DISTINCT are mutually exclusive"),
                 errOmitLocation(true),
                 parser_errposition(pstate, location)));
        }

        /* 
         * Build the aggregate node and transform it
         *
         * Note: aggorder is handled inside transformAggregateCall()
         */
        aggref = makeNode(PGAggref);
        aggref->aggfnoid = funcid;
        aggref->aggtype = rettype;
        aggref->args = fargs;
        aggref->aggstar = agg_star;
        aggref->aggdistinct = agg_distinct;

        transformAggregateCall(pstate, aggref, agg_order);

        retval = (PGNode *)aggref;
    }

    /*
	 * Mark the context if this is a dynamic typed function, if so we mustn't
	 * allow views to be created from this statement because we cannot 
	 * guarantee that the future return type will be the same as the current
	 * return type.
	 */
    if (TypeSupportsDescribe(rettype))
    {
        Oid DescribeFuncOid = lookupProcCallback(funcid, PROMETHOD_DESCRIBE);
        if (OidIsValid(DescribeFuncOid))
        {
            PGParseState * state = pstate;

            for (state = pstate; state; state = state->parentParseState)
                state->p_hasDynamicFunction = true;
        }
    }

    return retval;
};

int FuncParser::func_match_argtypes(
        int nargs, Oid * input_typeids, FuncCandidateList raw_candidates, FuncCandidateList * candidates)
{
    FuncCandidateList current_candidate;
    FuncCandidateList next_candidate;
    int ncandidates = 0;

    *candidates = NULL;

    for (current_candidate = raw_candidates; current_candidate != NULL; current_candidate = next_candidate)
    {
        next_candidate = current_candidate->next;
        if (coerce_parser.can_coerce_type(nargs, input_typeids, current_candidate->args, PG_COERCION_IMPLICIT))
        {
            current_candidate->next = *candidates;
            *candidates = current_candidate;
            ncandidates++;
        }
    }

    return ncandidates;
};

FuncCandidateList FuncParser::func_select_candidate(int nargs, Oid * input_typeids, FuncCandidateList candidates)
{
    FuncCandidateList current_candidate;
    FuncCandidateList last_candidate;
    Oid * current_typeids;
    Oid current_type;
    int i;
    int ncandidates;
    int nbestMatch, nmatch;
    Oid input_base_typeids[FUNC_MAX_ARGS];
    CATEGORY slot_category[FUNC_MAX_ARGS], current_category;
    bool slot_has_preferred_type[FUNC_MAX_ARGS];
    bool resolved_unknowns;

    /* protect local fixed-size arrays */
    if (nargs > FUNC_MAX_ARGS)
        ereport(ERROR, (errcode(ERRCODE_TOO_MANY_ARGUMENTS), errmsg("cannot pass more than %d arguments to a function", FUNC_MAX_ARGS)));

    /*
	 * If any input types are domains, reduce them to their base types. This
	 * ensures that we will consider functions on the base type to be "exact
	 * matches" in the exact-match heuristic; it also makes it possible to do
	 * something useful with the type-category heuristics. Note that this
	 * makes it difficult, but not impossible, to use functions declared to
	 * take a domain as an input datatype.	Such a function will be selected
	 * over the base-type function only if it is an exact match at all
	 * argument positions, and so was already chosen by our caller.
	 */
    for (i = 0; i < nargs; i++)
        input_base_typeids[i] = getBaseType(input_typeids[i]);

    /*
	 * Run through all candidates and keep those with the most matches on
	 * exact types. Keep all candidates if none match.
	 */
    ncandidates = 0;
    nbestMatch = 0;
    last_candidate = NULL;
    for (current_candidate = candidates; current_candidate != NULL; current_candidate = current_candidate->next)
    {
        current_typeids = current_candidate->args;
        nmatch = 0;
        for (i = 0; i < nargs; i++)
        {
            if (input_base_typeids[i] != UNKNOWNOID && current_typeids[i] == input_base_typeids[i])
                nmatch++;
        }

        /* take this one as the best choice so far? */
        if ((nmatch > nbestMatch) || (last_candidate == NULL))
        {
            nbestMatch = nmatch;
            candidates = current_candidate;
            last_candidate = current_candidate;
            ncandidates = 1;
        }
        /* no worse than the last choice, so keep this one too? */
        else if (nmatch == nbestMatch)
        {
            last_candidate->next = current_candidate;
            last_candidate = current_candidate;
            ncandidates++;
        }
        /* otherwise, don't bother keeping this one... */
    }

    if (last_candidate) /* terminate rebuilt list */
        last_candidate->next = NULL;

    if (ncandidates == 1)
        return candidates;

    /*
	 * Still too many candidates? Now look for candidates which have either
	 * exact matches or preferred types at the args that will require
	 * coercion. (Restriction added in 7.4: preferred type must be of same
	 * category as input type; give no preference to cross-category
	 * conversions to preferred types.)  Keep all candidates if none match.
	 */
    for (i = 0; i < nargs; i++) /* avoid multiple lookups */
        slot_category[i] = coerce_parser.TypeCategory(input_base_typeids[i]);
    ncandidates = 0;
    nbestMatch = 0;
    last_candidate = NULL;
    for (current_candidate = candidates; current_candidate != NULL; current_candidate = current_candidate->next)
    {
        current_typeids = current_candidate->args;
        nmatch = 0;
        for (i = 0; i < nargs; i++)
        {
            if (input_base_typeids[i] != UNKNOWNOID)
            {
                if (current_typeids[i] == input_base_typeids[i] || coerce_parser.IsPreferredType(slot_category[i], current_typeids[i]))
                    nmatch++;
            }
        }

        if ((nmatch > nbestMatch) || (last_candidate == NULL))
        {
            nbestMatch = nmatch;
            candidates = current_candidate;
            last_candidate = current_candidate;
            ncandidates = 1;
        }
        else if (nmatch == nbestMatch)
        {
            last_candidate->next = current_candidate;
            last_candidate = current_candidate;
            ncandidates++;
        }
    }

    if (last_candidate) /* terminate rebuilt list */
        last_candidate->next = NULL;

    if (ncandidates == 1)
        return candidates;

    /*
	 * Still too many candidates? Try assigning types for the unknown columns.
	 *
	 * NOTE: for a binary operator with one unknown and one non-unknown input,
	 * we already tried the heuristic of looking for a candidate with the
	 * known input type on both sides (see binary_oper_exact()). That's
	 * essentially a special case of the general algorithm we try next.
	 *
	 * We do this by examining each unknown argument position to see if we can
	 * determine a "type category" for it.	If any candidate has an input
	 * datatype of STRING category, use STRING category (this bias towards
	 * STRING is appropriate since unknown-type literals look like strings).
	 * Otherwise, if all the candidates agree on the type category of this
	 * argument position, use that category.  Otherwise, fail because we
	 * cannot determine a category.
	 *
	 * If we are able to determine a type category, also notice whether any of
	 * the candidates takes a preferred datatype within the category.
	 *
	 * Having completed this examination, remove candidates that accept the
	 * wrong category at any unknown position.	Also, if at least one
	 * candidate accepted a preferred type at a position, remove candidates
	 * that accept non-preferred types.
	 *
	 * If we are down to one candidate at the end, we win.
	 */
    resolved_unknowns = false;
    for (i = 0; i < nargs; i++)
    {
        bool have_conflict;

        if (input_base_typeids[i] != UNKNOWNOID)
            continue;
        resolved_unknowns = true; /* assume we can do it */
        slot_category[i] = INVALID_TYPE;
        slot_has_preferred_type[i] = false;
        have_conflict = false;
        for (current_candidate = candidates; current_candidate != NULL; current_candidate = current_candidate->next)
        {
            current_typeids = current_candidate->args;
            current_type = current_typeids[i];
            current_category = coerce_parser.TypeCategory(current_type);
            if (slot_category[i] == INVALID_TYPE)
            {
                /* first candidate */
                slot_category[i] = current_category;
                slot_has_preferred_type[i] = coerce_parser.IsPreferredType(current_category, current_type);
            }
            else if (current_category == slot_category[i])
            {
                /* more candidates in same category */
                slot_has_preferred_type[i] |= coerce_parser.IsPreferredType(current_category, current_type);
            }
            else
            {
                /* category conflict! */
                if (current_category == STRING_TYPE)
                {
                    /* STRING always wins if available */
                    slot_category[i] = current_category;
                    slot_has_preferred_type[i] = IsPreferredType(current_category, current_type);
                }
                else
                {
                    /*
					 * Remember conflict, but keep going (might find STRING)
					 */
                    have_conflict = true;
                }
            }
        }
        if (have_conflict && slot_category[i] != STRING_TYPE)
        {
            /* Failed to resolve category conflict at this position */
            resolved_unknowns = false;
            break;
        }
    }

    if (resolved_unknowns)
    {
        /* Strip non-matching candidates */
        ncandidates = 0;
        last_candidate = NULL;
        for (current_candidate = candidates; current_candidate != NULL; current_candidate = current_candidate->next)
        {
            bool keepit = true;

            current_typeids = current_candidate->args;
            for (i = 0; i < nargs; i++)
            {
                if (input_base_typeids[i] != UNKNOWNOID)
                    continue;
                current_type = current_typeids[i];
                current_category = coerce_parser.TypeCategory(current_type);
                if (current_category != slot_category[i])
                {
                    keepit = false;
                    break;
                }
                if (slot_has_preferred_type[i] && !coerce_parser.IsPreferredType(current_category, current_type))
                {
                    keepit = false;
                    break;
                }
            }
            if (keepit)
            {
                /* keep this candidate */
                last_candidate = current_candidate;
                ncandidates++;
            }
            else
            {
                /* forget this candidate */
                if (last_candidate)
                    last_candidate->next = current_candidate->next;
                else
                    candidates = current_candidate->next;
            }
        }
        if (last_candidate) /* terminate rebuilt list */
            last_candidate->next = NULL;
    }

    if (ncandidates == 1)
        return candidates;

    return NULL; /* failed to select a best candidate */
};

}