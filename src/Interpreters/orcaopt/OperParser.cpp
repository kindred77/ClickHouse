#include <Interpreters/orcaopt/OperParser.h>

#include <Interpreters/orcaopt/FuncParser.h>
#include <Interpreters/orcaopt/NodeParser.h>
#include <Interpreters/orcaopt/CoerceParser.h>

namespace DB
{

using namespace duckdb_libpgquery;

Oid
OperParser::compatible_oper_opid(PGList *op, Oid arg1, Oid arg2, bool noError)
{
    Operator	optup;
	Oid			result;

	optup = compatible_oper(NULL, op, arg1, arg2, noError, -1);
	if (optup != NULL)
	{
		result = oprid(optup);
		ReleaseSysCache(optup);
		return result;
	}
	return InvalidOid;
};

Operator
OperParser::compatible_oper(PGParseState *pstate, PGList *op, Oid arg1, Oid arg2,
				bool noError, int location)
{
    Operator	optup;
	Form_pg_operator opform;

	/* oper() will find the best available match */
	optup = oper(pstate, op, arg1, arg2, noError, location);
	if (optup == (Operator) NULL)
		return (Operator) NULL; /* must be noError case */

	/* but is it good enough? */
	opform = (Form_pg_operator) GETSTRUCT(optup);
	if (IsBinaryCoercible(arg1, opform->oprleft) &&
		IsBinaryCoercible(arg2, opform->oprright))
		return optup;

	/* nope... */
	ReleaseSysCache(optup);

	if (!noError)
	{
		node_parser.parser_errposition(pstate, location);
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FUNCTION),
				 errmsg("operator requires run-time type coercion: %s",
						op_signature_string(op, 'b', arg1, arg2))));
	}

	return (Operator) NULL;
};

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
	hashable = OidIsValid(typentry->hash_proc);

	/* Report errors if needed */
	if ((needLT && !OidIsValid(lt_opr)) ||
		(needGT && !OidIsValid(gt_opr)))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FUNCTION),
				 errmsg("could not identify an ordering operator for type %s",
						format_type_be(argtype)),
				 errhint("Use an explicit ordering operator or modify the query.")));
	if (needEQ && !OidIsValid(eq_opr))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FUNCTION),
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

Operator
OperParser::right_oper(PGParseState *pstate, PGList *op,
			Oid arg, bool noError, int location)
{
	Oid			operOid;
	OprCacheKey key;
	bool		key_ok;
	FuncDetailCode fdresult = FUNCDETAIL_NOTFOUND;
	HeapTuple	tup = NULL;

	/*
	 * Try to find the mapping in the lookaside cache.
	 */
	key_ok = make_oper_cache_key(pstate, &key, op, arg, InvalidOid, location);

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
OperParser::left_oper(PGParseState *pstate, PGList *op,
			Oid arg, bool noError, int location)
{
	Oid			operOid;
	OprCacheKey key;
	bool		key_ok;
	FuncDetailCode fdresult = FUNCDETAIL_NOTFOUND;
	HeapTuple	tup = NULL;

	/*
	 * Try to find the mapping in the lookaside cache.
	 */
	key_ok = make_oper_cache_key(pstate, &key, op, InvalidOid, arg, location);

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

bool
OperParser::make_oper_cache_key(PGParseState *pstate, OprCacheKey *key, PGList *opname,
					Oid ltypeId, Oid rtypeId, int location)
{
	char	   *schemaname;
	char	   *opername;

	/* deconstruct the name list */
	DeconstructQualifiedName(opname, &schemaname, &opername);

	/* ensure zero-fill for stable hashing */
	MemSet(key, 0, sizeof(OprCacheKey));

	/* save operator name and input types into key */
	strlcpy(key->oprname, opername, NAMEDATALEN);
	key->left_arg = ltypeId;
	key->right_arg = rtypeId;

	if (schemaname)
	{
		PGParseCallbackState pcbstate;

		/* search only in exact schema given */
		node_parser.setup_parser_errposition_callback(&pcbstate, pstate, location);
		key->search_path[0] = LookupExplicitNamespace(schemaname, false);
		node_parser.cancel_parser_errposition_callback(&pcbstate);
	}
	else
	{
		/* get the active search path */
		if (fetch_search_path_array(key->search_path,
									MAX_CACHED_PATH_LEN) > MAX_CACHED_PATH_LEN)
			return false;		/* oops, didn't fit */
	}

	return true;
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
		{
			node_parser.parser_errposition(pstate, location);
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("op ANY/ALL (array) requires array on right side")));
		}
	}

	/* Now resolve the operator */
	tup = oper(pstate, opname, ltypeId, rtypeId, false, location);
	opform = (Form_pg_operator) GETSTRUCT(tup);

	/* Check it's not a shell */
	if (!OidIsValid(opform->oprcode))
	{
		node_parser.parser_errposition(pstate, location);
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FUNCTION),
				 errmsg("operator is only a shell: %s",
						op_signature_string(opname,
											opform->oprkind,
											opform->oprleft,
											opform->oprright))));
	}

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
	rettype = coerce_parser.enforce_generic_type_consistency(actual_arg_types,
											   declared_arg_types,
											   2,
											   opform->oprresult,
											   false);

	/*
	 * Check that operator result is boolean
	 */
	if (rettype != BOOLOID)
	{
		node_parser.parser_errposition(pstate, location);
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("op ANY/ALL (array) requires operator to yield boolean")));
	}
	if (get_func_retset(opform->oprcode))
	{
		node_parser.parser_errposition(pstate, location);
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("op ANY/ALL (array) requires operator not to return a set")));
	}

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
		{
			node_parser.parser_errposition(pstate, location);
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("could not find array type for data type %s",
							format_type_be(declared_arg_types[1]))));
		}
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
		return FUNCDETAIL_NOTFOUND;
	}
	if (ncandidates == 1)
	{
		*operOid = candidates->oid;
		return FUNCDETAIL_NORMAL;
	}

	/*
	 * Use the same heuristics as for ambiguous functions to resolve the
	 * conflict.
	 */
	candidates = func_parser.func_select_candidate(nargs, input_typeids, candidates);

	if (candidates)
	{
		*operOid = candidates->oid;
		return FUNCDETAIL_NORMAL;
	}

	*operOid = InvalidOid;
	return FUNCDETAIL_MULTIPLE; /* failed to select a best candidate */
};

Oid
OperParser::find_oper_cache_entry(OprCacheKey *key)
{
	OprCacheEntry *oprentry;

	if (OprCacheHash == NULL)
	{
		/* First time through: initialize the hash table */
		HASHCTL		ctl;

		MemSet(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(OprCacheKey);
		ctl.entrysize = sizeof(OprCacheEntry);
		OprCacheHash = hash_create("Operator lookup cache", 256,
								   &ctl, HASH_ELEM | HASH_BLOBS);

		/* Arrange to flush cache on pg_operator and pg_cast changes */
		CacheRegisterSyscacheCallback(OPERNAMENSP,
									  InvalidateOprCacheCallBack,
									  (Datum) 0);
		CacheRegisterSyscacheCallback(CASTSOURCETARGET,
									  InvalidateOprCacheCallBack,
									  (Datum) 0);
	}

	/* Look for an existing entry */
	oprentry = (OprCacheEntry *) hash_search(OprCacheHash,
											 (void *) key,
											 HASH_FIND, NULL);
	if (oprentry == NULL)
		return InvalidOid;

	return oprentry->opr_oid;
};

void
OperParser::make_oper_cache_entry(OprCacheKey *key, Oid opr_oid)
{
	OprCacheEntry *oprentry;

	Assert(OprCacheHash != NULL);

	oprentry = (OprCacheEntry *) hash_search(OprCacheHash,
											 (void *) key,
											 HASH_ENTER, NULL);
	oprentry->opr_oid = opr_oid;
};

Operator
OperParser::oper(PGParseState *pstate, PGList *opname,
		Oid ltypeId, Oid rtypeId,bool noError, int location)
{
	Oid			operOid;
	OprCacheKey key;
	bool		key_ok;
	FuncDetailCode fdresult = FUNCDETAIL_NOTFOUND;
	HeapTuple	tup = NULL;

	/*
	 * Try to find the mapping in the lookaside cache.
	 */
	key_ok = make_oper_cache_key(pstate, &key, opname, ltypeId, rtypeId, location);

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

void
OperParser::op_error(PGParseState *pstate, PGList *op, char oprkind,
		 Oid arg1, Oid arg2,
		 FuncDetailCode fdresult, int location)
{
	if (fdresult == FUNCDETAIL_MULTIPLE)
	{
		node_parser.parser_errposition(pstate, location);
		ereport(ERROR,
				(errcode(ERRCODE_AMBIGUOUS_FUNCTION),
				 errmsg("operator is not unique: %s",
						op_signature_string(op, oprkind, arg1, arg2)),
				 errhint("Could not choose a best candidate operator. "
						 "You might need to add explicit type casts.")));
	}
	else
	{
		node_parser.parser_errposition(pstate, location);
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FUNCTION),
				 errmsg("operator does not exist: %s",
						op_signature_string(op, oprkind, arg1, arg2)),
				 (!arg1 || !arg2) ?
				 errhint("No operator matches the given name and argument type. "
						 "You might need to add an explicit type cast.") :
				 errhint("No operator matches the given name and argument types. "
						 "You might need to add explicit type casts.")));
	}
};

const char *
OperParser::op_signature_string(PGList *op, char oprkind, Oid arg1, Oid arg2)
{
	StringInfoData argbuf;

	initStringInfo(&argbuf);

	if (oprkind != 'l')
		appendStringInfo(&argbuf, "%s ", format_type_be(arg1));

	appendStringInfoString(&argbuf, NameListToString(op));

	if (oprkind != 'r')
		appendStringInfo(&argbuf, " %s", format_type_be(arg2));

	return argbuf.data;			/* return palloc'd string buffer */
};

Oid
OperParser::oprid(Operator op)
{
	return ((Form_pg_operator) GETSTRUCT(op))->oid;
};

PGExpr *
OperParser::make_op(PGParseState *pstate, PGList *opname, 
		PGNode *ltree, PGNode *rtree,
		PGNode *last_srf, int location)
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
	if (!OidIsValid(opform->oprcode))
	{
		node_parser.parser_errposition(pstate, location);
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FUNCTION),
				 errmsg("operator is only a shell: %s",
						op_signature_string(opname,
											opform->oprkind,
											opform->oprleft,
											opform->oprright))));
	}

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
	rettype = coerce_parser.enforce_generic_type_consistency(actual_arg_types,
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

	/* if it returns a set, check that's OK */
	if (result->opretset)
	{
		func_parser.check_srf_call_placement(pstate, last_srf, location);
		/* ... and remember it for error checks at higher levels */
		pstate->p_last_srf = (PGNode *) result;
	}

	ReleaseSysCache(tup);

	return (PGExpr *) result;
};

}
