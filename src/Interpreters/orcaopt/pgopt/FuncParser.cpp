#include <Interpreters/orcaopt/pgopt/FuncParser.h>

namespace DB
{
using namespace duckdb_libpgquery;

PGNode *
FuncParser::ParseFuncOrColumn(PGParseState *pstate, PGList *funcname, PGList *fargs,
				  PGFuncCall *fn, int location)
{
	bool		is_column = (fn == NULL);
	PGList	   *agg_order = (fn ? fn->agg_order : NIL);
	PGExpr	   *agg_filter = NULL;
	bool		agg_within_group = (fn ? fn->agg_within_group : false);
	bool		agg_star = (fn ? fn->agg_star : false);
	bool		agg_distinct = (fn ? fn->agg_distinct : false);
	bool		func_variadic = (fn ? fn->func_variadic : false);
	PGWindowDef  *over = (fn ? fn->over : NULL);
	Oid			rettype;
	Oid			funcid;
	ListCell   *l;
	ListCell   *nextl;
	PGNode	   *first_arg = NULL;
	int			nargs;
	int			nargsplusdefs;
	Oid			actual_arg_types[FUNC_MAX_ARGS];
	Oid		   *declared_arg_types;
	PGList	   *argnames;
	PGList	   *argdefaults;
	PGNode	   *retval;
	bool		retset;
	int			nvargs;
	Oid			vatype;
	FuncDetailCode fdresult;
	char		aggkind = 0;

	/*
	 * If there's an aggregate filter, transform it using transformWhereClause
	 */
	if (fn && fn->agg_filter != NULL)
		agg_filter = (PGExpr *) clause_parser.transformWhereClause(pstate, fn->agg_filter,
												   PGParseExprKind::EXPR_KIND_FILTER,
												   "FILTER");

	/*
	 * Most of the rest of the parser just assumes that functions do not have
	 * more than FUNC_MAX_ARGS parameters.  We have to test here to protect
	 * against array overruns, etc.  Of course, this may not be a function,
	 * but the test doesn't hurt.
	 */
	if (list_length(fargs) > FUNC_MAX_ARGS)
		ereport(ERROR,
				(errcode(PG_ERRCODE_SYNTAX_ERROR),
			 errmsg_plural("cannot pass more than %d argument to a function",
						   "cannot pass more than %d arguments to a function",
						   FUNC_MAX_ARGS,
						   FUNC_MAX_ARGS),
				 parser_errposition(pstate, location)));

	/*
	 * Extract arg type info in preparation for function lookup.
	 *
	 * If any arguments are Param markers of type VOID, we discard them from
	 * the parameter list. This is a hack to allow the JDBC driver to not have
	 * to distinguish "input" and "output" parameter symbols while parsing
	 * function-call constructs.  Don't do this if dealing with column syntax,
	 * nor if we had WITHIN GROUP (because in that case it's critical to keep
	 * the argument count unchanged).  We can't use foreach() because we may
	 * modify the list ...
	 */
	nargs = 0;
	for (l = list_head(fargs); l != NULL; l = nextl)
	{
		PGNode	   *arg = (PGNode *)lfirst(l);
		Oid			argtype = exprType(arg);

		nextl = lnext(l);

		if (argtype == VOIDOID && IsA(arg, PGParam) &&
			!is_column && !agg_within_group)
		{
			fargs = list_delete_ptr(fargs, arg);
			continue;
		}

		actual_arg_types[nargs++] = argtype;
	}

	/*
	 * Check for named arguments; if there are any, build a list of names.
	 *
	 * We allow mixed notation (some named and some not), but only with all
	 * the named parameters after all the unnamed ones.  So the name list
	 * corresponds to the last N actual parameters and we don't need any extra
	 * bookkeeping to match things up.
	 */
	argnames = NIL;
	foreach(l, fargs)
	{
		PGNode	   *arg = (PGNode *)lfirst(l);

		if (IsA(arg, PGNamedArgExpr))
		{
			PGNamedArgExpr *na = (PGNamedArgExpr *) arg;
			ListCell   *lc;

			/* Reject duplicate arg names */
			foreach(lc, argnames)
			{
				if (strcmp(na->name, (char *) lfirst(lc)) == 0)
					ereport(ERROR,
							(errcode(PG_ERRCODE_SYNTAX_ERROR),
						   errmsg("argument name \"%s\" used more than once",
								  na->name),
							 parser_errposition(pstate, na->location)));
			}
			argnames = lappend(argnames, na->name);
		}
		else
		{
			if (argnames != NIL)
				ereport(ERROR,
						(errcode(PG_ERRCODE_SYNTAX_ERROR),
				  errmsg("positional argument cannot follow named argument"),
						 parser_errposition(pstate, exprLocation(arg))));
		}
	}

	if (fargs)
	{
		first_arg = (PGNode *)linitial(fargs);
		Assert(first_arg != NULL);
	}

	/*
	 * Check for column projection: if function has one argument, and that
	 * argument is of complex type, and function name is not qualified, then
	 * the "function call" could be a projection.  We also check that there
	 * wasn't any aggregate or variadic decoration, nor an argument name.
	 */
	if (nargs == 1 && agg_order == NIL && agg_filter == NULL && !agg_star &&
		!agg_distinct && over == NULL && !func_variadic && argnames == NIL &&
		list_length(funcname) == 1)
	{
		Oid			argtype = actual_arg_types[0];

		if (argtype == RECORDOID || ISCOMPLEX(argtype))
		{
			retval = ParseComplexProjection(pstate,
											strVal(linitial(funcname)),
											first_arg,
											location);
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
	 * the function.
	 *
	 * Note: for a named-notation or variadic function call, the reported
	 * "true" types aren't really what is in pg_proc: the types are reordered
	 * to match the given argument order of named arguments, and a variadic
	 * argument is replaced by a suitable number of copies of its element
	 * type.  We'll fix up the variadic case below.  We may also have to deal
	 * with default arguments.
	 */
	fdresult = func_get_detail(funcname, fargs, argnames, nargs,
							   actual_arg_types,
							   !func_variadic, true,
							   &funcid, &rettype, &retset,
							   &nvargs, &vatype,
							   &declared_arg_types, &argdefaults);
	if (fdresult == FUNCDETAIL_COERCION)
	{
		/*
		 * We interpreted it as a type coercion. coerce_type can handle these
		 * cases, so why duplicate code...
		 */
		return coerce_parser.coerce_type(pstate, (PGNode *)linitial(fargs),
						   actual_arg_types[0], rettype, -1,
						   PG_COERCION_EXPLICIT, PG_COERCE_EXPLICIT_CALL, location);
	}
	else if (fdresult == FUNCDETAIL_NORMAL)
	{
		/*
		 * Normal function found; was there anything indicating it must be an
		 * aggregate?
		 */
		if (agg_star)
			ereport(ERROR,
					(errcode(PG_ERRCODE_SYNTAX_ERROR),
					 errmsg("%s(*) specified, but %s is not an aggregate function",
							NameListToString(funcname),
							NameListToString(funcname)),
					 parser_errposition(pstate, location)));
		if (agg_distinct)
			ereport(ERROR,
					(errcode(PG_ERRCODE_SYNTAX_ERROR),
					 errmsg("DISTINCT specified, but %s is not an aggregate function",
							NameListToString(funcname)),
					 parser_errposition(pstate, location)));
		if (agg_within_group)
			ereport(ERROR,
					(errcode(PG_ERRCODE_SYNTAX_ERROR),
					 errmsg("WITHIN GROUP specified, but %s is not an aggregate function",
							NameListToString(funcname)),
					 parser_errposition(pstate, location)));
		if (agg_within_group)
			ereport(ERROR,
					(errcode(PG_ERRCODE_SYNTAX_ERROR),
					 errmsg("WITHIN GROUP specified, but %s is not an aggregate function",
							NameListToString(funcname)),
					 parser_errposition(pstate, location)));
		if (agg_order != NIL)
			ereport(ERROR,
					(errcode(PG_ERRCODE_SYNTAX_ERROR),
			errmsg("ORDER BY specified, but %s is not an aggregate function",
				   NameListToString(funcname)),
					 parser_errposition(pstate, location)));
		if (agg_filter)
			ereport(ERROR,
					(errcode(PG_ERRCODE_SYNTAX_ERROR),
			  errmsg("FILTER specified, but %s is not an aggregate function",
					 NameListToString(funcname)),
					 parser_errposition(pstate, location)));
		if (over)
			ereport(ERROR,
					(errcode(PG_ERRCODE_SYNTAX_ERROR),
					 errmsg("OVER specified, but %s is not a window function nor an aggregate function",
							NameListToString(funcname)),
					 parser_errposition(pstate, location)));
	}
	else if (fdresult == FUNCDETAIL_AGGREGATE)
	{
		/*
		 * It's an aggregate; fetch needed info from the pg_aggregate entry.
		 */
		HeapTuple	tup;
		Form_pg_aggregate classForm;
		int			catDirectArgs;

		tup = SearchSysCache1(AGGFNOID, ObjectIdGetDatum(funcid));
		if (!HeapTupleIsValid(tup))		/* should not happen */
			elog(ERROR, "cache lookup failed for aggregate %u", funcid);
		classForm = (Form_pg_aggregate) GETSTRUCT(tup);
		aggkind = classForm->aggkind;
		catDirectArgs = classForm->aggnumdirectargs;
		ReleaseSysCache(tup);

		/* Now check various disallowed cases. */
		if (AGGKIND_IS_ORDERED_SET(aggkind))
		{
			int			numAggregatedArgs;
			int			numDirectArgs;

			if (!agg_within_group)
				ereport(ERROR,
						(errcode(PG_ERRCODE_SYNTAX_ERROR),
						 errmsg("WITHIN GROUP is required for ordered-set aggregate %s",
								NameListToString(funcname)),
						 parser_errposition(pstate, location)));
			if (over)
				ereport(ERROR,
						(errcode(PG_ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("OVER is not supported for ordered-set aggregate %s",
						NameListToString(funcname)),
						 parser_errposition(pstate, location)));
			/* gram.y rejects DISTINCT + WITHIN GROUP */
			Assert(!agg_distinct);
			/* gram.y rejects VARIADIC + WITHIN GROUP */
			Assert(!func_variadic);

			/*
			 * Since func_get_detail was working with an undifferentiated list
			 * of arguments, it might have selected an aggregate that doesn't
			 * really match because it requires a different division of direct
			 * and aggregated arguments.  Check that the number of direct
			 * arguments is actually OK; if not, throw an "undefined function"
			 * error, similarly to the case where a misplaced ORDER BY is used
			 * in a regular aggregate call.
			 */
			numAggregatedArgs = list_length(agg_order);
			numDirectArgs = nargs - numAggregatedArgs;
			Assert(numDirectArgs >= 0);

			if (vatype == InvalidOid)
			{
				/* Test is simple if aggregate isn't variadic */
				if (numDirectArgs != catDirectArgs)
					ereport(ERROR,
							(errcode(PG_ERRCODE_SYNTAX_ERROR),
							 errmsg("function %s does not exist",
									func_signature_string(funcname, nargs,
														  argnames,
														  actual_arg_types)),
							 errhint("There is an ordered-set aggregate %s, but it requires %d direct arguments, not %d.",
									 NameListToString(funcname),
									 catDirectArgs, numDirectArgs),
							 parser_errposition(pstate, location)));
			}
			else
			{
				/*
				 * If it's variadic, we have two cases depending on whether
				 * the agg was "... ORDER BY VARIADIC" or "..., VARIADIC ORDER
				 * BY VARIADIC".  It's the latter if catDirectArgs equals
				 * pronargs; to save a catalog lookup, we reverse-engineer
				 * pronargs from the info we got from func_get_detail.
				 */
				int			pronargs;

				pronargs = nargs;
				if (nvargs > 1)
					pronargs -= nvargs - 1;
				if (catDirectArgs < pronargs)
				{
					/* VARIADIC isn't part of direct args, so still easy */
					if (numDirectArgs != catDirectArgs)
						ereport(ERROR,
								(errcode(PG_ERRCODE_SYNTAX_ERROR),
								 errmsg("function %s does not exist",
										func_signature_string(funcname, nargs,
															  argnames,
														  actual_arg_types)),
								 errhint("There is an ordered-set aggregate %s, but it requires %d direct arguments, not %d.",
										 NameListToString(funcname),
										 catDirectArgs, numDirectArgs),
								 parser_errposition(pstate, location)));
				}
				else
				{
					/*
					 * Both direct and aggregated args were declared variadic.
					 * For a standard ordered-set aggregate, it's okay as long
					 * as there aren't too few direct args.  For a
					 * hypothetical-set aggregate, we assume that the
					 * hypothetical arguments are those that matched the
					 * variadic parameter; there must be just as many of them
					 * as there are aggregated arguments.
					 */
					if (aggkind == AGGKIND_HYPOTHETICAL)
					{
						if (nvargs != 2 * numAggregatedArgs)
							ereport(ERROR,
									(errcode(PG_ERRCODE_SYNTAX_ERROR),
									 errmsg("function %s does not exist",
									   func_signature_string(funcname, nargs,
															 argnames,
														  actual_arg_types)),
									 errhint("To use the hypothetical-set aggregate %s, the number of hypothetical direct arguments (here %d) must match the number of ordering columns (here %d).",
											 NameListToString(funcname),
							  nvargs - numAggregatedArgs, numAggregatedArgs),
									 parser_errposition(pstate, location)));
					}
					else
					{
						if (nvargs <= numAggregatedArgs)
							ereport(ERROR,
									(errcode(PG_ERRCODE_SYNTAX_ERROR),
									 errmsg("function %s does not exist",
									   func_signature_string(funcname, nargs,
															 argnames,
														  actual_arg_types)),
									 errhint("There is an ordered-set aggregate %s, but it requires at least %d direct arguments.",
											 NameListToString(funcname),
											 catDirectArgs),
									 parser_errposition(pstate, location)));
					}
				}
			}

			/* Check type matching of hypothetical arguments */
			if (aggkind == AGGKIND_HYPOTHETICAL)
				unify_hypothetical_args(pstate, fargs, numAggregatedArgs,
										actual_arg_types, declared_arg_types);
		}
		else
		{
			/* Normal aggregate, so it can't have WITHIN GROUP */
			if (agg_within_group)
				ereport(ERROR,
						(errcode(PG_ERRCODE_SYNTAX_ERROR),
						 errmsg("%s is not an ordered-set aggregate, so it cannot have WITHIN GROUP",
								NameListToString(funcname)),
						 parser_errposition(pstate, location)));
		}
	}
	else if (fdresult == FUNCDETAIL_WINDOWFUNC)
	{
		/*
		 * True window functions must be called with a window definition.
		 */
		if (!over)
			ereport(ERROR,
					(errcode(PG_ERRCODE_SYNTAX_ERROR),
					 errmsg("window function %s requires an OVER clause",
							NameListToString(funcname)),
					 parser_errposition(pstate, location)));
		/* And, per spec, WITHIN GROUP isn't allowed */
		if (agg_within_group)
			ereport(ERROR,
					(errcode(PG_ERRCODE_SYNTAX_ERROR),
					 errmsg("window function %s cannot have WITHIN GROUP",
							NameListToString(funcname)),
					 parser_errposition(pstate, location)));
	}
	else
	{
		/*
		 * Oops.  Time to die.
		 *
		 * If we are dealing with the attribute notation rel.function, let the
		 * caller handle failure.
		 */
		if (is_column)
			return NULL;

		/*
		 * Else generate a detailed complaint for a function
		 */
		if (fdresult == FUNCDETAIL_MULTIPLE)
			ereport(ERROR,
					(errcode(PG_ERRCODE_SYNTAX_ERROR),
					 errmsg("function %s is not unique",
							func_signature_string(funcname, nargs, argnames,
												  actual_arg_types)),
					 errhint("Could not choose a best candidate function. "
							 "You might need to add explicit type casts."),
					 parser_errposition(pstate, location)));
		else if (list_length(agg_order) > 1 && !agg_within_group)
		{
			/* It's agg(x, ORDER BY y,z) ... perhaps misplaced ORDER BY */
			ereport(ERROR,
					(errcode(PG_ERRCODE_SYNTAX_ERROR),
					 errmsg("function %s does not exist",
							func_signature_string(funcname, nargs, argnames,
												  actual_arg_types)),
					 errhint("No aggregate function matches the given name and argument types. "
					  "Perhaps you misplaced ORDER BY; ORDER BY must appear "
							 "after all regular arguments of the aggregate."),
					 parser_errposition(pstate, location)));
		}
		else
			ereport(ERROR,
					(errcode(PG_ERRCODE_SYNTAX_ERROR),
					 errmsg("function %s does not exist",
							func_signature_string(funcname, nargs, argnames,
												  actual_arg_types)),
					 errhint("No function matches the given name and argument types. "
							 "You might need to add explicit type casts."),
					 parser_errposition(pstate, location)));
	}

	/*
	 * If there are default arguments, we have to include their types in
	 * actual_arg_types for the purpose of checking generic type consistency.
	 * However, we do NOT put them into the generated parse node, because
	 * their actual values might change before the query gets run.  The
	 * planner has to insert the up-to-date values at plan time.
	 */
	nargsplusdefs = nargs;
	foreach(l, argdefaults)
	{
		PGNode	   *expr = (PGNode *) lfirst(l);

		/* probably shouldn't happen ... */
		if (nargsplusdefs >= FUNC_MAX_ARGS)
			ereport(ERROR,
					(errcode(PG_ERRCODE_SYNTAX_ERROR),
			 errmsg_plural("cannot pass more than %d argument to a function",
						   "cannot pass more than %d arguments to a function",
						   FUNC_MAX_ARGS,
						   FUNC_MAX_ARGS),
					 parser_errposition(pstate, location)));

		actual_arg_types[nargsplusdefs++] = exprType(expr);
	}

	/*
	 * enforce consistency with polymorphic argument and return types,
	 * possibly adjusting return type or declared_arg_types (which will be
	 * used as the cast destination by make_fn_arguments)
	 */
	rettype = enforce_generic_type_consistency(actual_arg_types,
											   declared_arg_types,
											   nargsplusdefs,
											   rettype,
											   false);

	/* perform the necessary typecasting of arguments */
	make_fn_arguments(pstate, fargs, actual_arg_types, declared_arg_types);

	/*
	 * If the function isn't actually variadic, forget any VARIADIC decoration
	 * on the call.  (Perhaps we should throw an error instead, but
	 * historically we've allowed people to write that.)
	 */
	if (vatype == InvalidOid)
	{
		Assert(nvargs == 0);
		func_variadic = false;
	}

	/*
	 * If it's a variadic function call, transform the last nvargs arguments
	 * into an array --- unless it's an "any" variadic.
	 */
	if (nvargs > 0 && vatype != ANYOID)
	{
		PGArrayExpr  *newa = makeNode(PGArrayExpr);
		int			non_var_args = nargs - nvargs;
		PGList	   *vargs;

		Assert(non_var_args >= 0);
		vargs = list_copy_tail(fargs, non_var_args);
		fargs = list_truncate(fargs, non_var_args);

		newa->elements = vargs;
		/* assume all the variadic arguments were coerced to the same type */
		newa->element_typeid = exprType((PGNode *) linitial(vargs));
		newa->array_typeid = get_array_type(newa->element_typeid);
		if (newa->array_typeid == InvalidOid)
			ereport(ERROR,
					(errcode(PG_ERRCODE_SYNTAX_ERROR),
					 errmsg("could not find array type for data type %s",
							format_type_be(newa->element_typeid)),
				  parser_errposition(pstate, exprLocation((PGNode *) vargs))));
		/* array_collid will be set by parse_collate.c */
		newa->multidims = false;
		newa->location = exprLocation((PGNode *) vargs);

		fargs = lappend(fargs, newa);

		/* We could not have had VARIADIC marking before ... */
		Assert(!func_variadic);
		/* ... but now, it's a VARIADIC call */
		func_variadic = true;
	}

	/*
	 * If an "any" variadic is called with explicit VARIADIC marking, insist
	 * that the variadic parameter be of some array type.
	 */
	if (nargs > 0 && vatype == ANYOID && func_variadic)
	{
		Oid			va_arr_typid = actual_arg_types[nargs - 1];

		if (get_base_element_type(va_arr_typid) == InvalidOid)
			ereport(ERROR,
					(errcode(PG_ERRCODE_SYNTAX_ERROR),
					 errmsg("VARIADIC argument must be an array"),
					 parser_errposition(pstate,
									  exprLocation((PGNode *) llast(fargs)))));
	}

	if (retset)
		check_srf_call_placement(pstate, location);

	/* build the appropriate output structure */
	if (fdresult == FUNCDETAIL_NORMAL)
	{
		PGFuncExpr   *funcexpr = makeNode(PGFuncExpr);

		funcexpr->funcid = funcid;
		funcexpr->funcresulttype = rettype;
		funcexpr->funcretset = retset;
		funcexpr->funcvariadic = func_variadic;
		funcexpr->funcformat = PG_COERCE_EXPLICIT_CALL;
		/* funccollid and inputcollid will be set by parse_collate.c */
		funcexpr->args = fargs;
		funcexpr->location = location;

		retval = (PGNode *) funcexpr;
	}
	else if (fdresult == FUNCDETAIL_AGGREGATE && !over)
	{
		/* aggregate function */
		PGAggref	   *aggref = makeNode(PGAggref);

		aggref->aggfnoid = funcid;
		aggref->aggtype = rettype;
		/* aggcollid and inputcollid will be set by parse_collate.c */
		/* aggdirectargs and args will be set by transformAggregateCall */
		/* aggorder and aggdistinct will be set by transformAggregateCall */
		aggref->aggfilter = agg_filter;
		aggref->aggstar = agg_star;
		aggref->aggvariadic = func_variadic;
		aggref->aggkind = aggkind;
		/* agglevelsup will be set by transformAggregateCall */
		aggref->location = location;

		/*
		 * Reject attempt to call a parameterless aggregate without (*)
		 * syntax.  This is mere pedantry but some folks insisted ...
		 *
		 * GPDB: We allow this in GPDB.
		 */
#if 0
		if (fargs == NIL && !agg_star && !agg_within_group)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("%s(*) must be used to call a parameterless aggregate function",
							NameListToString(funcname)),
					 parser_errposition(pstate, location)));
#endif

		if (retset)
			ereport(ERROR,
					(errcode(PG_ERRCODE_SYNTAX_ERROR),
					 errmsg("aggregates cannot return sets"),
					 parser_errposition(pstate, location)));

		/*
		 * We might want to support named arguments later, but disallow it for
		 * now.  We'd need to figure out the parsed representation (should the
		 * NamedArgExprs go above or below the TargetEntry nodes?) and then
		 * teach the planner to reorder the list properly.  Or maybe we could
		 * make transformAggregateCall do that?  However, if you'd also like
		 * to allow default arguments for aggregates, we'd need to do it in
		 * planning to avoid semantic problems.
		 */
		if (argnames != NIL)
			ereport(ERROR,
					(errcode(PG_ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("aggregates cannot use named arguments"),
					 parser_errposition(pstate, location)));

		/* parse_agg.c does additional aggregate-specific processing */
		agg_parser.transformAggregateCall(pstate, aggref, fargs, agg_order, agg_distinct);

		retval = (PGNode *) aggref;
	}
	else
	{
		/* window function */
		PGWindowFunc *wfunc = makeNode(PGWindowFunc);

		Assert(over);			/* lack of this was checked above */
		Assert(!agg_within_group);		/* also checked above */

		wfunc->winfnoid = funcid;
		wfunc->wintype = rettype;
		/* wincollid and inputcollid will be set by parse_collate.c */
		wfunc->args = fargs;
		/* winref will be set by transformWindowFuncCall */
		wfunc->winstar = agg_star;
		wfunc->winagg = (fdresult == FUNCDETAIL_AGGREGATE);
		wfunc->aggfilter = agg_filter;
		wfunc->location = location;

		wfunc->windistinct = agg_distinct;

		/*
		 * agg_star is allowed for aggregate functions but distinct isn't
		 *
		 * GPDB: We have implemented this in GPDB, with some limitations.
		 */
		if (agg_distinct)
		{
			if (fdresult == FUNCDETAIL_WINDOWFUNC)
				ereport(ERROR,
						(errcode(PG_ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("DISTINCT is not implemented for window functions"),
						 parser_errposition(pstate, location)));

			if (list_length(fargs) != 1)
				ereport(ERROR,
						(errcode(PG_ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("DISTINCT is supported only for single-argument window aggregates")));
		}

		/*
		 * Reject attempt to call a parameterless aggregate without (*)
		 * syntax.  This is mere pedantry but some folks insisted ...
		 *
		 * GPDB: We allow this in GPDB.
		 */
#if 0
		if (wfunc->winagg && fargs == NIL && !agg_star)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("%s(*) must be used to call a parameterless aggregate function",
							NameListToString(funcname)),
					 parser_errposition(pstate, location)));
#endif

		/*
		 * ordered aggs not allowed in windows yet
		 */
		if (agg_order != NIL)
			ereport(ERROR,
					(errcode(PG_ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("aggregate ORDER BY is not implemented for window functions"),
					 parser_errposition(pstate, location)));

		/*
		 * FILTER is not yet supported with true window functions
		 */
		if (!wfunc->winagg && agg_filter)
			ereport(ERROR,
					(errcode(PG_ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("FILTER is not implemented for non-aggregate window functions"),
					 parser_errposition(pstate, location)));

		if (retset)
			ereport(ERROR,
					(errcode(PG_ERRCODE_SYNTAX_ERROR),
					 errmsg("window functions cannot return sets"),
					 parser_errposition(pstate, location)));

		/* parse_agg.c does additional window-func-specific processing */
		agg_parser.transformWindowFuncCall(pstate, wfunc, over);

		retval = (PGNode *) wfunc;
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
		if (DescribeFuncOid != InvalidOid)
		{
			PGParseState *state = pstate;

			for (state = pstate; state; state = state->parentParseState)
				state->p_hasDynamicFunction = true;
		}
	}

	/*
	 * If this function has restrictions on where it can be executed
	 * (EXECUTE ON MASTER or EXECUTE ON ALL SEGMENTS), make note of that,
	 * so that the planner knows to be prepared for it.
	 */
	if (func_exec_location(funcid) != PROEXECLOCATION_ANY)
		pstate->p_hasFuncsWithExecRestrictions = true;

	return retval;
};

void
FuncParser::make_fn_arguments(PGParseState *pstate,
				  PGList *fargs,
				  Oid *actual_arg_types,
				  Oid *declared_arg_types)
{
    PGListCell   *current_fargs;
	int			i = 0;

	foreach(current_fargs, fargs)
	{
		/* types don't match? then force coercion using a function call... */
		if (actual_arg_types[i] != declared_arg_types[i])
		{
			PGNode	   *node = (PGNode *) lfirst(current_fargs);

			/*
			 * If arg is a NamedArgExpr, coerce its input expr instead --- we
			 * want the NamedArgExpr to stay at the top level of the list.
			 */
			if (IsA(node, PGNamedArgExpr))
			{
				PGNamedArgExpr *na = (PGNamedArgExpr *) node;

				node = coerce_parser.coerce_type(pstate,
								   (PGNode *) na->arg,
								   actual_arg_types[i],
								   declared_arg_types[i], -1,
								   PG_COERCION_IMPLICIT,
								   PG_COERCE_IMPLICIT_CAST,
								   -1);
				na->arg = (PGExpr *) node;
			}
			else
			{
				node = coerce_parser.coerce_type(pstate,
								   node,
								   actual_arg_types[i],
								   declared_arg_types[i], -1,
								   PG_COERCION_IMPLICIT,
								   PG_COERCE_IMPLICIT_CAST,
								   -1);
				lfirst(current_fargs) = node;
			}
		}
		i++;
	}
};

}