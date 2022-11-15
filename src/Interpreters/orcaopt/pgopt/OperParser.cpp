#include <Interpreters/orcaopt/pgopt/OperParser.h>

namespace DB
{

using namespace duckdb_libpgquery;

void
OperParser::get_sort_group_operators(Oid argtype,
						 bool needLT, bool needEQ, bool needGT,
						 Oid *ltOpr, Oid *eqOpr, Oid *gtOpr,
						 bool *isHashable)
{
    TypeCacheEntry *typentry;
	int			cache_flags;
	Oid			lt_opr;
	Oid			eq_opr;
	Oid			gt_opr;
	bool		hashable;

	/*
	 * Look up the operators using the type cache.
	 *
	 * Note: the search algorithm used by typcache.c ensures that the results
	 * are consistent, ie all from matching opclasses.
	 */
	if (isHashable != NULL)
		cache_flags = TYPECACHE_LT_OPR | TYPECACHE_EQ_OPR | TYPECACHE_GT_OPR |
			TYPECACHE_HASH_PROC;
	else
		cache_flags = TYPECACHE_LT_OPR | TYPECACHE_EQ_OPR | TYPECACHE_GT_OPR;

	typentry = lookup_type_cache(argtype, cache_flags);
	lt_opr = typentry->lt_opr;
	eq_opr = typentry->eq_opr;
	gt_opr = typentry->gt_opr;
	hashable = (typentry->hash_proc != InvalidOid);

	/* Report errors if needed */
	if ((needLT && !OidIsValid(lt_opr)) ||
		(needGT && !OidIsValid(gt_opr)))
		ereport(ERROR,
				(errcode(PG_ERRCODE_SYNTAX_ERROR),
				 errmsg("could not identify an ordering operator for type %s",
						format_type_be(argtype)),
		 errhint("Use an explicit ordering operator or modify the query.")));
	if (needEQ && !OidIsValid(eq_opr))
		ereport(ERROR,
				(errcode(PG_ERRCODE_SYNTAX_ERROR),
				 errmsg("could not identify an equality operator for type %s",
						format_type_be(argtype))));

	/* Return results as needed */
	if (ltOpr)
		*ltOpr = lt_opr;
	if (eqOpr)
		*eqOpr = eq_opr;
	if (gtOpr)
		*gtOpr = gt_opr;
	if (isHashable)
		*isHashable = hashable;
};

Oid
OperParser::binary_oper_exact(PGList *opname, Oid arg1, Oid arg2)
{
	Oid			result;
	bool		was_unknown = false;

	/* Unspecified type for one of the arguments? then use the other */
	if ((arg1 == UNKNOWNOID) && (arg2 != InvalidOid))
	{
		arg1 = arg2;
		was_unknown = true;
	}
	else if ((arg2 == UNKNOWNOID) && (arg1 != InvalidOid))
	{
		arg2 = arg1;
		was_unknown = true;
	}

	result = OpernameGetOprid(opname, arg1, arg2);
	if (OidIsValid(result))
		return result;

	if (was_unknown)
	{
		/* arg1 and arg2 are the same here, need only look at arg1 */
		Oid			basetype = getBaseType(arg1);

		if (basetype != arg1)
		{
			result = OpernameGetOprid(opname, basetype, basetype);
			if (OidIsValid(result))
				return result;
		}
	}

	return InvalidOid;
};

FuncDetailCode
OperParser::oper_select_candidate(int nargs,
					  Oid *input_typeids,
					  FuncCandidateList candidates,
					  Oid *operOid)
{
	int			ncandidates;

	/*
	 * Delete any candidates that cannot actually accept the given input
	 * types, whether directly or by coercion.
	 */
	ncandidates = func_parser.func_match_argtypes(nargs, input_typeids,
									  candidates, &candidates);

	/* Done if no candidate or only one candidate survives */
	if (ncandidates == 0)
	{
		*operOid = InvalidOid;
		return FuncDetailCode::FUNCDETAIL_NOTFOUND;
	}
	if (ncandidates == 1)
	{
		*operOid = candidates->oid;
		return FuncDetailCode::FUNCDETAIL_NORMAL;
	}

	/*
	 * Use the same heuristics as for ambiguous functions to resolve the
	 * conflict.
	 */
	candidates = func_parser.func_select_candidate(nargs, input_typeids, candidates);

	if (candidates)
	{
		*operOid = candidates->oid;
		return FuncDetailCode::FUNCDETAIL_NORMAL;
	}

	*operOid = InvalidOid;
	return FuncDetailCode::FUNCDETAIL_MULTIPLE; /* failed to select a best candidate */
};

HeapTuple
OperParser::oper(PGParseState *pstate, PGList *opname, Oid ltypeId, Oid rtypeId,
		bool noError, int location)
{
	Oid			operOid;
	OprCacheKey key;
	bool		key_ok;
	FuncDetailCode fdresult = FuncDetailCode::FUNCDETAIL_NOTFOUND;
	HeapTuple	tup = NULL;

	/*
	 * Try to find the mapping in the lookaside cache.
	 */
	key_ok = make_oper_cache_key(&key, opname, ltypeId, rtypeId);
	if (key_ok)
	{
		operOid = find_oper_cache_entry(&key);
		if (OidIsValid(operOid))
		{
			tup = SearchSysCache1(OPEROID, ObjectIdGetDatum(operOid));
			if (HeapTupleIsValid(tup))
				return (Operator) tup;
		}
	}

	/*
	 * First try for an "exact" match.
	 */
	operOid = binary_oper_exact(opname, ltypeId, rtypeId);
	if (!OidIsValid(operOid))
	{
		/*
		 * Otherwise, search for the most suitable candidate.
		 */
		FuncCandidateList clist;

		/* Get binary operators of given name */
		clist = OpernameGetCandidates(opname, 'b', false);

		/* No operators found? Then fail... */
		if (clist != NULL)
		{
			/*
			 * Unspecified type for one of the arguments? then use the other
			 * (XXX this is probably dead code?)
			 */
			Oid			inputOids[2];

			if (rtypeId == InvalidOid)
				rtypeId = ltypeId;
			else if (ltypeId == InvalidOid)
				ltypeId = rtypeId;
			inputOids[0] = ltypeId;
			inputOids[1] = rtypeId;
			fdresult = oper_select_candidate(2, inputOids, clist, &operOid);
		}
	}

	if (OidIsValid(operOid))
		tup = SearchSysCache1(OPEROID, ObjectIdGetDatum(operOid));

	if (HeapTupleIsValid(tup))
	{
		if (key_ok)
			make_oper_cache_entry(&key, operOid);
	}
	else if (!noError)
		op_error(pstate, opname, 'b', ltypeId, rtypeId, fdresult, location);

	return (Operator) tup;
};

Operator
OperParser::right_oper(PGParseState *pstate, PGList *op, Oid arg, bool noError, int location)
{
	Oid			operOid;
	OprCacheKey key;
	bool		key_ok;
	FuncDetailCode fdresult = FuncDetailCode::FUNCDETAIL_NOTFOUND;
	HeapTuple	tup = NULL;

	/*
	 * Try to find the mapping in the lookaside cache.
	 */
	key_ok = make_oper_cache_key(&key, op, arg, InvalidOid);
	if (key_ok)
	{
		operOid = find_oper_cache_entry(&key);
		if (OidIsValid(operOid))
		{
			tup = SearchSysCache1(OPEROID, ObjectIdGetDatum(operOid));
			if (HeapTupleIsValid(tup))
				return (Operator) tup;
		}
	}

	/*
	 * First try for an "exact" match.
	 */
	operOid = OpernameGetOprid(op, arg, InvalidOid);
	if (!OidIsValid(operOid))
	{
		/*
		 * Otherwise, search for the most suitable candidate.
		 */
		FuncCandidateList clist;

		/* Get postfix operators of given name */
		clist = OpernameGetCandidates(op, 'r', false);

		/* No operators found? Then fail... */
		if (clist != NULL)
		{
			/*
			 * We must run oper_select_candidate even if only one candidate,
			 * otherwise we may falsely return a non-type-compatible operator.
			 */
			fdresult = oper_select_candidate(1, &arg, clist, &operOid);
		}
	}

	if (OidIsValid(operOid))
		tup = SearchSysCache1(OPEROID, ObjectIdGetDatum(operOid));

	if (HeapTupleIsValid(tup))
	{
		if (key_ok)
			make_oper_cache_entry(&key, operOid);
	}
	else if (!noError)
		op_error(pstate, op, 'r', arg, InvalidOid, fdresult, location);

	return (Operator) tup;
};

Operator
OperParser::left_oper(PGParseState *pstate, PGList *op, Oid arg, bool noError, int location)
{
	Oid			operOid;
	OprCacheKey key;
	bool		key_ok;
	FuncDetailCode fdresult = FuncDetailCode::FUNCDETAIL_NOTFOUND;
	HeapTuple	tup = NULL;

	/*
	 * Try to find the mapping in the lookaside cache.
	 */
	key_ok = make_oper_cache_key(&key, op, InvalidOid, arg);
	if (key_ok)
	{
		operOid = find_oper_cache_entry(&key);
		if (OidIsValid(operOid))
		{
			tup = SearchSysCache1(OPEROID, ObjectIdGetDatum(operOid));
			if (HeapTupleIsValid(tup))
				return (Operator) tup;
		}
	}

	/*
	 * First try for an "exact" match.
	 */
	operOid = OpernameGetOprid(op, InvalidOid, arg);
	if (!OidIsValid(operOid))
	{
		/*
		 * Otherwise, search for the most suitable candidate.
		 */
		FuncCandidateList clist;

		/* Get prefix operators of given name */
		clist = OpernameGetCandidates(op, 'l', false);

		/* No operators found? Then fail... */
		if (clist != NULL)
		{
			/*
			 * The returned list has args in the form (0, oprright). Move the
			 * useful data into args[0] to keep oper_select_candidate simple.
			 * XXX we are assuming here that we may scribble on the list!
			 */
			FuncCandidateList clisti;

			for (clisti = clist; clisti != NULL; clisti = clisti->next)
			{
				clisti->args[0] = clisti->args[1];
			}

			/*
			 * We must run oper_select_candidate even if only one candidate,
			 * otherwise we may falsely return a non-type-compatible operator.
			 */
			fdresult = oper_select_candidate(1, &arg, clist, &operOid);
		}
	}

	if (OidIsValid(operOid))
		tup = SearchSysCache1(OPEROID, ObjectIdGetDatum(operOid));

	if (HeapTupleIsValid(tup))
	{
		if (key_ok)
			make_oper_cache_entry(&key, operOid);
	}
	else if (!noError)
		op_error(pstate, op, 'l', InvalidOid, arg, fdresult, location);

	return (Operator) tup;
};

PGExpr *
OperParser::make_scalar_array_op(PGParseState *pstate, PGList *opname,
					 bool useOr,
					 PGNode *ltree, PGNode *rtree,
					 int location)
{
	Oid			ltypeId,
				rtypeId,
				atypeId,
				res_atypeId;
	Operator	tup;
	Form_pg_operator opform;
	Oid			actual_arg_types[2];
	Oid			declared_arg_types[2];
	PGList	   *args;
	Oid			rettype;
	PGScalarArrayOpExpr *result;

	ltypeId = exprType(ltree);
	atypeId = exprType(rtree);

	/*
	 * The right-hand input of the operator will be the element type of the
	 * array.  However, if we currently have just an untyped literal on the
	 * right, stay with that and hope we can resolve the operator.
	 */
	if (atypeId == UNKNOWNOID)
		rtypeId = UNKNOWNOID;
	else
	{
		rtypeId = get_base_element_type(atypeId);
		if (!OidIsValid(rtypeId))
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				   errmsg("op ANY/ALL (array) requires array on right side"),
					 node_parser.parser_errposition(pstate, location)));
	}

	/* Now resolve the operator */
	tup = oper(pstate, opname, ltypeId, rtypeId, false, location);
	opform = (Form_pg_operator) GETSTRUCT(tup);

	/* Check it's not a shell */
	if (!RegProcedureIsValid(opform->oprcode))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FUNCTION),
				 errmsg("operator is only a shell: %s",
						op_signature_string(opname,
											opform->oprkind,
											opform->oprleft,
											opform->oprright)),
				 node_parser.parser_errposition(pstate, location)));

	args = list_make2(ltree, rtree);
	actual_arg_types[0] = ltypeId;
	actual_arg_types[1] = rtypeId;
	declared_arg_types[0] = opform->oprleft;
	declared_arg_types[1] = opform->oprright;

	/*
	 * enforce consistency with polymorphic argument and return types,
	 * possibly adjusting return type or declared_arg_types (which will be
	 * used as the cast destination by make_fn_arguments)
	 */
	rettype = enforce_generic_type_consistency(actual_arg_types,
											   declared_arg_types,
											   2,
											   opform->oprresult,
											   false);

	/*
	 * Check that operator result is boolean
	 */
	if (rettype != BOOLOID)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
			 errmsg("op ANY/ALL (array) requires operator to yield boolean"),
				 node_parser.parser_errposition(pstate, location)));
	if (get_func_retset(opform->oprcode))
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
		  errmsg("op ANY/ALL (array) requires operator not to return a set"),
				 node_parser.parser_errposition(pstate, location)));

	/*
	 * Now switch back to the array type on the right, arranging for any
	 * needed cast to be applied.  Beware of polymorphic operators here;
	 * enforce_generic_type_consistency may or may not have replaced a
	 * polymorphic type with a real one.
	 */
	if (IsPolymorphicType(declared_arg_types[1]))
	{
		/* assume the actual array type is OK */
		res_atypeId = atypeId;
	}
	else
	{
		res_atypeId = get_array_type(declared_arg_types[1]);
		if (!OidIsValid(res_atypeId))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("could not find array type for data type %s",
							format_type_be(declared_arg_types[1])),
					 node_parser.parser_errposition(pstate, location)));
	}
	actual_arg_types[1] = atypeId;
	declared_arg_types[1] = res_atypeId;

	/* perform the necessary typecasting of arguments */
	func_parser.make_fn_arguments(pstate, args, actual_arg_types, declared_arg_types);

	/* and build the expression node */
	result = makeNode(PGScalarArrayOpExpr);
	result->opno = oprid(tup);
	result->opfuncid = opform->oprcode;
	result->useOr = useOr;
	/* inputcollid will be set by parse_collate.c */
	result->args = args;
	result->location = location;

	ReleaseSysCache(tup);

	return (PGExpr *) result;
};

PGExpr *
OperParser::make_op(PGParseState *pstate, PGList *opname, PGNode *ltree, PGNode *rtree,
		int location)
{
	Oid			ltypeId,
				rtypeId;
	Operator	tup;
	Form_pg_operator opform;
	Oid			actual_arg_types[2];
	Oid			declared_arg_types[2];
	int			nargs;
	PGList	   *args;
	Oid			rettype;
	PGOpExpr	   *result;

	/* Select the operator */
	if (rtree == NULL)
	{
		/* right operator */
		ltypeId = exprType(ltree);
		rtypeId = InvalidOid;
		tup = right_oper(pstate, opname, ltypeId, false, location);
	}
	else if (ltree == NULL)
	{
		/* left operator */
		rtypeId = exprType(rtree);
		ltypeId = InvalidOid;
		tup = left_oper(pstate, opname, rtypeId, false, location);
	}
	else
	{
		/* otherwise, binary operator */
		ltypeId = exprType(ltree);
		rtypeId = exprType(rtree);
		tup = oper(pstate, opname, ltypeId, rtypeId, false, location);
	}

	opform = (Form_pg_operator) GETSTRUCT(tup);

	/* Check it's not a shell */
	if (!RegProcedureIsValid(opform->oprcode))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FUNCTION),
				 errmsg("operator is only a shell: %s",
						op_signature_string(opname,
											opform->oprkind,
											opform->oprleft,
											opform->oprright)),
				 node_parser.parser_errposition(pstate, location)));

	/* Do typecasting and build the expression tree */
	if (rtree == NULL)
	{
		/* right operator */
		args = list_make1(ltree);
		actual_arg_types[0] = ltypeId;
		declared_arg_types[0] = opform->oprleft;
		nargs = 1;
	}
	else if (ltree == NULL)
	{
		/* left operator */
		args = list_make1(rtree);
		actual_arg_types[0] = rtypeId;
		declared_arg_types[0] = opform->oprright;
		nargs = 1;
	}
	else
	{
		/* otherwise, binary operator */
		args = list_make2(ltree, rtree);
		actual_arg_types[0] = ltypeId;
		actual_arg_types[1] = rtypeId;
		declared_arg_types[0] = opform->oprleft;
		declared_arg_types[1] = opform->oprright;
		nargs = 2;
	}

	/*
	 * enforce consistency with polymorphic argument and return types,
	 * possibly adjusting return type or declared_arg_types (which will be
	 * used as the cast destination by make_fn_arguments)
	 */
	rettype = enforce_generic_type_consistency(actual_arg_types,
											   declared_arg_types,
											   nargs,
											   opform->oprresult,
											   false);

	/* perform the necessary typecasting of arguments */
	func_parser.make_fn_arguments(pstate, args, actual_arg_types, declared_arg_types);

	/* and build the expression node */
	result = makeNode(PGOpExpr);
	result->opno = oprid(tup);
	result->opfuncid = opform->oprcode;
	result->opresulttype = rettype;
	result->opretset = get_func_retset(opform->oprcode);
	/* opcollid and inputcollid will be set by parse_collate.c */
	result->args = args;
	result->location = location;

	ReleaseSysCache(tup);

	return (PGExpr *) result;
};

}