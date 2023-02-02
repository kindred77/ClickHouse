#include <Interpreters/orcaopt/pgopt_hawq/OperParser.h>

namespace DB
{

using namespace duckdb_libpgquery;

FuncDetailCode OperParser::oper_select_candidate(int nargs, Oid * input_typeids, FuncCandidateList candidates,
		Oid * operOid)
{
    int ncandidates;

    /*
	 * Delete any candidates that cannot actually accept the given input
	 * types, whether directly or by coercion.
	 */
    ncandidates = func_parser.func_match_argtypes(nargs, input_typeids, candidates, &candidates);

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

Operator OperParser::right_oper(PGParseState * pstate, PGList * op,
		Oid arg, bool noError, int location)
{
    Oid operOid;
    FuncDetailCode fdresult = FUNCDETAIL_NOTFOUND;
    HeapTuple tup = NULL;

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
        clist = OpernameGetCandidates(op, 'r');

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

    tup = fetch_op_tup(operOid, false);

    if (!HeapTupleIsValid(tup) && !noError)
        op_error(pstate, op, 'r', arg, InvalidOid, fdresult, location);

    return (Operator)tup;
};

Operator OperParser::left_oper(PGParseState * pstate, PGList * op,
		Oid arg, bool noError, int location)
{
    Oid operOid;
    FuncDetailCode fdresult = FUNCDETAIL_NOTFOUND;
    HeapTuple tup = NULL;

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
        clist = OpernameGetCandidates(op, 'l');

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

    tup = fetch_op_tup(operOid, false);

    if (!HeapTupleIsValid(tup) && !noError)
        op_error(pstate, op, 'l', InvalidOid, arg, fdresult, location);

    return (Operator)tup;
};

Operator OperParser::oper(PGParseState * pstate, PGList * opname,
		Oid ltypeId, Oid rtypeId, bool noError, int location)
{
    Oid operOid;
    FuncDetailCode fdresult = FUNCDETAIL_NOTFOUND;
    HeapTuple tup = NULL;

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
        clist = OpernameGetCandidates(opname, 'b');

        /* No operators found? Then fail... */
        if (clist != NULL)
        {
            /*
			 * Unspecified type for one of the arguments? then use the other
			 * (XXX this is probably dead code?)
			 */
            Oid inputOids[2];

            if (!OidIsValid(rtypeId))
                rtypeId = ltypeId;
            else if (!OidIsValid(ltypeId))
                ltypeId = rtypeId;
            inputOids[0] = ltypeId;
            inputOids[1] = rtypeId;
            fdresult = oper_select_candidate(2, inputOids, clist, &operOid);
        }
    }

    tup = fetch_op_tup(operOid, false);

    if (!HeapTupleIsValid(tup) && !noError)
        op_error(pstate, opname, 'b', ltypeId, rtypeId, fdresult, location);

    return (Operator)tup;
};

PGExpr * OperParser::make_op_expr(PGParseState * pstate, Operator op,
		PGNode * ltree, PGNode * rtree,
		Oid ltypeId, Oid rtypeId)
{
    Form_pg_operator opform = (Form_pg_operator)GETSTRUCT(op);
    Oid actual_arg_types[2];
    Oid declared_arg_types[2];
    int nargs;
    PGList * args;
    Oid rettype;
    PGOpExpr * result;

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
	 * enforce consistency with ANYARRAY and ANYELEMENT argument and return
	 * types, possibly adjusting return type or declared_arg_types (which will
	 * be used as the cast destination by make_fn_arguments)
	 */
    rettype = coerce_parser.enforce_generic_type_consistency(actual_arg_types, declared_arg_types, nargs, opform->oprresult);

    /* perform the necessary typecasting of arguments */
    func_parser.make_fn_arguments(pstate, args, actual_arg_types, declared_arg_types);

    /* and build the expression node */
    result = makeNode(PGOpExpr);
    result->opno = oprid(op);
    result->opfuncid = InvalidOid;
    result->opresulttype = rettype;
    result->opretset = get_func_retset(opform->oprcode);
    result->args = args;

    return (PGExpr *)result;
};

PGExpr * OperParser::make_op(PGParseState * pstate, PGList * opname,
		PGNode * ltree, PGNode * rtree, int location)
{
    Oid ltypeId, rtypeId;
    Operator tup;
    PGExpr * result;

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

    /* Do typecasting and build the expression tree */
    result = make_op_expr(pstate, tup, ltree, rtree, ltypeId, rtypeId);

    ReleaseOperator(tup);

    return result;
};

PGExpr * OperParser::make_scalar_array_op(PGParseState * pstate, PGList * opname,
		bool useOr, PGNode * ltree, PGNode * rtree,
		int location)
{
    Oid ltypeId, rtypeId, atypeId, res_atypeId;
    Operator tup;
    Form_pg_operator opform;
    Oid actual_arg_types[2];
    Oid declared_arg_types[2];
    PGList * args;
    Oid rettype;
    PGScalarArrayOpExpr * result;

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
        rtypeId = get_element_type(atypeId);
        if (!OidIsValid(rtypeId))
            ereport(
                ERROR,
                (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                 errmsg("op ANY/ALL (array) requires array on right side"),
                 errOmitLocation(true),
                 parser_errposition(pstate, location)));
    }

    /* Now resolve the operator */
    tup = oper(pstate, opname, ltypeId, rtypeId, false, location);
    opform = (Form_pg_operator)GETSTRUCT(tup);

    args = list_make2(ltree, rtree);
    actual_arg_types[0] = ltypeId;
    actual_arg_types[1] = rtypeId;
    declared_arg_types[0] = opform->oprleft;
    declared_arg_types[1] = opform->oprright;

    /*
	 * enforce consistency with ANYARRAY and ANYELEMENT argument and return
	 * types, possibly adjusting return type or declared_arg_types (which will
	 * be used as the cast destination by make_fn_arguments)
	 */
    rettype = coerce_parser.enforce_generic_type_consistency(actual_arg_types, declared_arg_types, 2, opform->oprresult);

    /*
	 * Check that operator result is boolean
	 */
    if (rettype != BOOLOID)
        ereport(
            ERROR,
            (errcode(ERRCODE_WRONG_OBJECT_TYPE),
             errmsg("op ANY/ALL (array) requires operator to yield boolean"),
             errOmitLocation(true),
             parser_errposition(pstate, location)));
    if (get_func_retset(opform->oprcode))
        ereport(
            ERROR,
            (errcode(ERRCODE_WRONG_OBJECT_TYPE),
             errmsg("op ANY/ALL (array) requires operator not to return a set"),
             errOmitLocation(true),
             parser_errposition(pstate, location)));

    /*
	 * Now switch back to the array type on the right, arranging for any
	 * needed cast to be applied.
	 */
    res_atypeId = get_array_type(declared_arg_types[1]);
    if (!OidIsValid(res_atypeId))
        ereport(
            ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT),
             errmsg("could not find array type for data type %s", format_type_be(declared_arg_types[1])),
             errOmitLocation(true),
             parser_errposition(pstate, location)));
    actual_arg_types[1] = atypeId;
    declared_arg_types[1] = res_atypeId;

    /* perform the necessary typecasting of arguments */
    func_parser.make_fn_arguments(pstate, args, actual_arg_types, declared_arg_types);

    /* and build the expression node */
    result = makeNode(PGScalarArrayOpExpr);
    result->opno = oprid(tup);
    result->opfuncid = InvalidOid;
    result->useOr = useOr;
    result->args = args;

    ReleaseOperator(tup);

    return (PGExpr *)result;
};

}