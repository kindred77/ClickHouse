#include <Interpreters/orcaopt/pgopt_hawq/ExprParser.h>

using namespace duckdb_libpgquery;

namespace DB
{

PGNode * ExprParser::typecast_expression(PGParseState * pstate,
        PGNode * expr, PGTypeName * typname)
{
    Oid inputType = exprType(expr);
    Oid targetType;

    targetType = type_parser.typenameTypeId(pstate, typname);

    if (inputType == InvalidOid)
        return expr; /* do nothing if NULL input */

    expr = coerce_parser.coerce_to_target_type(pstate, expr, inputType, targetType, typname->typmod, PG_COERCION_EXPLICIT, COERCE_EXPLICIT_CAST, -1);
    if (expr == NULL)
        ereport(
            ERROR,
            (errcode(ERRCODE_CANNOT_COERCE),
             errmsg("cannot cast type %s to %s", format_type_be(inputType), format_type_be(targetType)),
             errOmitLocation(true),
             parser_errposition(pstate, typname->location)));

    return expr;
};

PGNode * ExprParser::transformColumnRef(PGParseState * pstate,
        PGColumnRef * cref)
{
    int numnames = list_length(cref->fields);
    PGNode * node;
    int levels_up;
    ParseStateBreadCrumb savebreadcrumb;

    /* CDB: Push error location stack.  Must pop before return! */
    Assert(pstate);
    savebreadcrumb = pstate->p_breadcrumb;
    pstate->p_breadcrumb.pop = &savebreadcrumb;
    pstate->p_breadcrumb.node = (PGNode *)cref;

    /*----------
	 * The allowed syntaxes are:
	 *
	 * A		First try to resolve as unqualified column name;
	 *			if no luck, try to resolve as unqualified table name (A.*).
	 * A.B		A is an unqualified table name; B is either a
	 *			column or function name (trying column name first).
	 * A.B.C	schema A, table B, col or func name C.
	 * A.B.C.D	catalog A, schema B, table C, col or func D.
	 * A.*		A is an unqualified table name; means whole-row value.
	 * A.B.*	whole-row value of table B in schema A.
	 * A.B.C.*	whole-row value of table C in schema B in catalog A.
	 *
	 * We do not need to cope with bare "*"; that will only be accepted by
	 * the grammar at the top level of a SELECT list, and transformTargetList
	 * will take care of it before it ever gets here.  Also, "A.*" etc will
	 * be expanded by transformTargetList if they appear at SELECT top level,
	 * so here we are only going to see them as function or operator inputs.
	 *
	 * Currently, if a catalog name is given then it must equal the current
	 * database name; we check it here and then discard it.
	 *----------
	 */
    switch (numnames)
    {
        case 1: {
            char * name = strVal(linitial(cref->fields));

            /* Try to identify as an unqualified column */
            node = relation_parser.colNameToVar(pstate, name, false, cref->location);

            if (node == NULL)
            {
                /*
					 * Not known as a column of any range-table entry.
					 *
					 * Consider the possibility that it's VALUE in a domain
					 * check expression.  (We handle VALUE as a name, not a
					 * keyword, to avoid breaking a lot of applications that
					 * have used VALUE as a column name in the past.)
					 */
                if (pstate->p_value_substitute != NULL && strcmp(name, "value") == 0)
                {
                    node = (PGNode *)copyObject(pstate->p_value_substitute);
                    break;
                }

                /*
					 * Try to find the name as a relation.	Note that only
					 * relations already entered into the rangetable will be
					 * recognized.
					 *
					 * This is a hack for backwards compatibility with
					 * PostQUEL-inspired syntax.  The preferred form now is
					 * "rel.*".
					 */
                if (relation_parser.refnameRangeTblEntry(pstate, NULL /*catalogname*/, NULL /*schemaname*/, name, cref->location, &levels_up) != NULL)
                    node = transformWholeRowRef(pstate, NULL /*catalogname*/, NULL, name, cref->location);
                else
                    ereport(
                        ERROR,
                        (errcode(ERRCODE_UNDEFINED_COLUMN),
                         errmsg("column \"%s\" does not exist", name),
                         errOmitLocation(true),
                         parser_errposition(pstate, cref->location)));
            }
            break;
        }
        case 2: {
            char * name1 = strVal(linitial(cref->fields));
            char * name2 = strVal(lsecond(cref->fields));

            /* Whole-row reference? */
            if (strcmp(name2, "*") == 0)
            {
                node = transformWholeRowRef(pstate, NULL /*catalogname*/, NULL /*schemaname*/, name1, cref->location);
                break;
            }

            /* Try to identify as a once-qualified column */
            node = qualifiedNameToVar(pstate, NULL /*catalogname*/, NULL /*schemaname*/, name1, name2, true, cref->location);
            if (node == NULL)
            {
                /*
					 * Not known as a column of any range-table entry, so try
					 * it as a function call.  Here, we will create an
					 * implicit RTE for tables not already entered.
					 */
                node = transformWholeRowRef(pstate, NULL /*catalogname*/, NULL, name1, cref->location);
                node = ParseFuncOrColumn(
                    pstate, list_make1(makeString(name2)), list_make1(node), NIL, false, false, false, true, NULL, cref->location, NULL);
            }
            break;
        }
        case 3: {
            char * name1 = strVal(linitial(cref->fields));
            char * name2 = strVal(lsecond(cref->fields));
            char * name3 = strVal(lthird(cref->fields));

            /* Whole-row reference? */
            if (strcmp(name3, "*") == 0)
            {
                node = transformWholeRowRef(pstate, NULL /*catalogname*/, name1, name2, cref->location);
                break;
            }

            /* Try to identify as a twice-qualified column */
            node = qualifiedNameToVar(pstate, NULL /*catalogname*/, name1, name2, name3, true, cref->location);
            if (node == NULL)
            {
                /* Try it as a function call */
                node = transformWholeRowRef(pstate, NULL /*catalogname*/, name1, name2, cref->location);
                node = ParseFuncOrColumn(
                    pstate, list_make1(makeString(name3)), list_make1(node), NIL, false, false, false, true, NULL, cref->location, NULL);
            }
            break;
        }
        case 4: {
            char * name1 = strVal(linitial(cref->fields));
            char * name2 = strVal(lsecond(cref->fields));
            char * name3 = strVal(lthird(cref->fields));
            char * name4 = strVal(lfourth(cref->fields));

            /* Whole-row reference? */
            if (strcmp(name4, "*") == 0)
            {
                node = transformWholeRowRef(pstate, name1, name2, name3, cref->location);
                break;
            }

            /* Try to identify as a twice-qualified column */
            node = qualifiedNameToVar(pstate, name1, name2, name3, name4, true, cref->location);
            if (node == NULL)
            {
                /* Try it as a function call */
                node = transformWholeRowRef(pstate, name1, name2, name3, cref->location);
                node = ParseFuncOrColumn(
                    pstate, list_make1(makeString(name4)), list_make1(node), NIL, false, false, false, true, NULL, cref->location, NULL);
            }
            break;
        }
        default:
            ereport(
                ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                 errmsg("improper qualified name (too many dotted names): %s", NameListToString(cref->fields)),
                 parser_errposition(pstate, cref->location)));
            node = NULL; /* keep compiler quiet */
            break;
    }

    /* CDB: Pop error location stack. */
    Assert(pstate->p_breadcrumb.pop == &savebreadcrumb);
    pstate->p_breadcrumb = savebreadcrumb;

    return node;
};

PGNode * ExprParser::transformParamRef(PGParseState * pstate,
        PGParamRef * pref)
{
    int paramno = pref->number;
    PGParseState * toppstate;
    PGParam * param;

    /*
	 * Find topmost ParseState, which is where paramtype info lives.
	 */
    toppstate = pstate;
    while (toppstate->parentParseState != NULL)
        toppstate = toppstate->parentParseState;

    /* Check parameter number is in range */
    if (paramno <= 0) /* probably can't happen? */
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_PARAMETER), errmsg("there is no parameter $%d", paramno)));
    if (paramno > toppstate->p_numparams)
    {
        if (!toppstate->p_variableparams)
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_PARAMETER), errmsg("there is no parameter $%d", paramno), errOmitLocation(true)));
        /* Okay to enlarge param array */
        if (toppstate->p_paramtypes)
            toppstate->p_paramtypes = (Oid *)repalloc(toppstate->p_paramtypes, paramno * sizeof(Oid));
        else
            toppstate->p_paramtypes = (Oid *)palloc(paramno * sizeof(Oid));
        /* Zero out the previously-unreferenced slots */
        MemSet(toppstate->p_paramtypes + toppstate->p_numparams, 0, (paramno - toppstate->p_numparams) * sizeof(Oid));
        toppstate->p_numparams = paramno;
    }
    if (toppstate->p_variableparams)
    {
        /* If not seen before, initialize to UNKNOWN type */
        if (toppstate->p_paramtypes[paramno - 1] == InvalidOid)
            toppstate->p_paramtypes[paramno - 1] = UNKNOWNOID;
    }

    param = makeNode(PGParam);
    param->paramkind = PG_PARAM_EXTERN;
    param->paramid = paramno;
    param->paramtype = toppstate->p_paramtypes[paramno - 1];

    return (PGNode *)param;
};

PGNode * ExprParser::transformWholeRowRef(PGParseState * pstate, char * catalogname,
        char * schemaname, char * relname, int location)
{
    PGNode * result;
    PGRangeTblEntry * rte;
    int vnum;
    int sublevels_up;
    Oid toid;

    /* Look up the referenced RTE, creating it if needed */

    rte = relation_parser.refnameRangeTblEntry(pstate, catalogname, schemaname, relname, location, &sublevels_up);

    if (rte == NULL)
        rte = relation_parser.addImplicitRTE(pstate, makeRangeVar(catalogname, schemaname, relname, location), location);

    vnum = relation_parser.RTERangeTablePosn(pstate, rte, &sublevels_up);

    /* Build the appropriate referencing node */

    switch (rte->rtekind)
    {
        case PG_RTE_RELATION:
            /* relation: the rowtype is a named composite type */
            toid = get_rel_type_id(rte->relid);
            if (!OidIsValid(toid))
                elog(ERROR, "could not find type OID for relation %u", rte->relid);
            result = (Node *)makeVar(vnum, InvalidAttrNumber, toid, -1, sublevels_up);
            break;
        // case RTE_TABLEFUNCTION:
        case PG_RTE_FUNCTION:
            toid = exprType(rte->funcexpr);
            if (toid == RECORDOID || get_typtype(toid) == 'c')
            {
                /* func returns composite; same as relation case */
                result = (PGNode *)makeVar(vnum, InvalidAttrNumber, toid, -1, sublevels_up);
            }
            else
            {
                /*
				 * func returns scalar; instead of making a whole-row Var,
				 * just reference the function's scalar output.  (XXX this
				 * seems a tad inconsistent, especially if "f.*" was
				 * explicitly written ...)
				 */
                result = (PGNode *)makeVar(vnum, 1, toid, -1, sublevels_up);
            }
            break;
        case PG_RTE_VALUES:
            toid = RECORDOID;
            /* returns composite; same as relation case */
            result = (PGNode *)makeVar(vnum, InvalidAttrNumber, toid, -1, sublevels_up);
            break;
        default:

            /*
			 * RTE is a join or subselect.	We represent this as a whole-row
			 * Var of RECORD type.	(Note that in most cases the Var will be
			 * expanded to a RowExpr during planning, but that is not our
			 * concern here.)
			 */
            result = (PGNode *)makeVar(vnum, InvalidAttrNumber, RECORDOID, -1, sublevels_up);
            break;
    }

    return result;
};

bool ExprParser::exprIsNullConstant(PGNode * arg)
{
    if (arg && IsA(arg, PGAConst))
    {
        PGAConst * con = (PGAConst *)arg;

        if (con->val.type == T_PGNull && con->typname == NULL)
            return true;
    }
    return false;
}

PGNode * ExprParser::make_row_comparison_op(PGParseState * pstate,
        PGList * opname, PGList * largs,
        PGList * rargs, int location)
{
    PGRowCompareExpr * rcexpr;
    PGRowCompareType rctype;
    PGList * opexprs;
    PGList * opnos;
    PGList * opclasses;
    PGListCell *l, *r;
    PGList ** opclass_lists;
    PGList ** opstrat_lists;
    PGBitmapset * strats;
    int nopers;
    int i;

    nopers = list_length(largs);
    if (nopers != list_length(rargs))
        ereport(
            ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR),
             errmsg("unequal number of entries in row expressions"),
             errOmitLocation(true),
             parser_errposition(pstate, location)));

    /*
	 * We can't compare zero-length rows because there is no principled basis
	 * for figuring out what the operator is.
	 */
    if (nopers == 0)
        ereport(
            ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
             errmsg("cannot compare rows of zero length"),
             errOmitLocation(true),
             parser_errposition(pstate, location)));

    /*
	 * Identify all the pairwise operators, using make_op so that behavior is
	 * the same as in the simple scalar case.
	 */
    opexprs = NIL;
    forboth(l, largs, r, rargs)
    {
        PGNode * larg = (PGNode *)lfirst(l);
        PGNode * rarg = (PGNode *)lfirst(r);
        PGOpExpr * cmp;

        cmp = (PGOpExpr *)oper_parser.make_op(pstate, opname, larg, rarg, location);
        Assert(IsA(cmp, PGOpExpr));

        /*
		 * We don't use coerce_to_boolean here because we insist on the
		 * operator yielding boolean directly, not via coercion.  If it
		 * doesn't yield bool it won't be in any index opclasses...
		 */
        if (cmp->opresulttype != BOOLOID)
            ereport(
                ERROR,
                (errcode(ERRCODE_DATATYPE_MISMATCH),
                 errmsg(
                     "row comparison operator must yield type boolean, "
                     "not type %s",
                     format_type_be(cmp->opresulttype)),
                 errOmitLocation(true),
                 parser_errposition(pstate, location)));
        if (expression_returns_set((Node *)cmp))
            ereport(
                ERROR,
                (errcode(ERRCODE_DATATYPE_MISMATCH),
                 errmsg("row comparison operator must not return a set"),
                 errOmitLocation(true),
                 parser_errposition(pstate, location)));
        opexprs = lappend(opexprs, cmp);
    }

    /*
	 * If rows are length 1, just return the single operator.  In this case we
	 * don't insist on identifying btree semantics for the operator (but we
	 * still require it to return boolean).
	 */
    if (nopers == 1)
        return (PGNode *)linitial(opexprs);

    /*
	 * Now we must determine which row comparison semantics (= <> < <= > >=)
	 * apply to this set of operators.	We look for btree opclasses containing
	 * the operators, and see which interpretations (strategy numbers) exist
	 * for each operator.
	 */
    opclass_lists = (PGList **)palloc(nopers * sizeof(PGList *));
    opstrat_lists = (PGList **)palloc(nopers * sizeof(PGList *));
    strats = NULL;
    i = 0;
    foreach (l, opexprs)
    {
        PGBitmapset * this_strats;
        PGListCell * j;

        get_op_btree_interpretation(((PGOpExpr *)lfirst(l))->opno, &opclass_lists[i], &opstrat_lists[i]);

        /*
		 * convert strategy number list to a Bitmapset to make the
		 * intersection calculation easy.
		 */
        this_strats = NULL;
        foreach (j, opstrat_lists[i])
        {
            this_strats = bms_add_member(this_strats, lfirst_int(j));
        }
        if (i == 0)
            strats = this_strats;
        else
            strats = bms_int_members(strats, this_strats);
        i++;
    }

    switch (bms_membership(strats))
    {
        case PG_BMS_EMPTY_SET:
            /* No common interpretation, so fail */
            ereport(
                ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("could not determine interpretation of row comparison operator %s", strVal(llast(opname))),
                 errhint("Row comparison operators must be associated with btree operator classes."),
                 errOmitLocation(true),
                 parser_errposition(pstate, location)));
            rctype = 0; /* keep compiler quiet */
            break;
        case PG_BMS_SINGLETON:
            /* Simple case: just one possible interpretation */
            rctype = bms_singleton_member(strats);
            break;
        case BMS_MULTIPLE:
        default: /* keep compiler quiet */
        {
            /*
				 * Prefer the interpretation with the most default opclasses.
				 */
            int best_defaults = 0;
            bool multiple_best = false;
            int this_rctype;

            rctype = 0; /* keep compiler quiet */
            while ((this_rctype = bms_first_member(strats)) >= 0)
            {
                int ndefaults = 0;

                for (i = 0; i < nopers; i++)
                {
                    forboth(l, opclass_lists[i], r, opstrat_lists[i])
                    {
                        Oid opclass = lfirst_oid(l);
                        int opstrat = lfirst_int(r);

                        if (opstrat == this_rctype && opclass_is_default(opclass))
                            ndefaults++;
                    }
                }
                if (ndefaults > best_defaults)
                {
                    best_defaults = ndefaults;
                    rctype = this_rctype;
                    multiple_best = false;
                }
                else if (ndefaults == best_defaults)
                    multiple_best = true;
            }
            if (best_defaults == 0 || multiple_best)
                ereport(
                    ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg("could not determine interpretation of row comparison operator %s", strVal(llast(opname))),
                     errdetail("There are multiple equally-plausible candidates."),
                     errOmitLocation(true),
                     parser_errposition(pstate, location)));
            break;
        }
    }

    /*
	 * For = and <> cases, we just combine the pairwise operators with AND or
	 * OR respectively.
	 *
	 * Note: this is presently the only place where the parser generates
	 * BoolExpr with more than two arguments.  Should be OK since the rest of
	 * the system thinks BoolExpr is N-argument anyway.
	 */
    if (rctype == PG_ROWCOMPARE_EQ)
        return (PGNode *)makeBoolExpr(PG_AND_EXPR, opexprs, location);
    if (rctype == PG_ROWCOMPARE_NE)
        return (PGNode *)makeBoolExpr(PG_OR_EXPR, opexprs, location);

    /*
	 * Otherwise we need to determine exactly which opclass to associate with
	 * each operator.
	 */
    opclasses = NIL;
    for (i = 0; i < nopers; i++)
    {
        Oid best_opclass = 0;
        int ndefault = 0;
        int nmatch = 0;

        forboth(l, opclass_lists[i], r, opstrat_lists[i])
        {
            Oid opclass = lfirst_oid(l);
            int opstrat = lfirst_int(r);

            if (opstrat == rctype)
            {
                if (ndefault == 0)
                    best_opclass = opclass;
                if (opclass_is_default(opclass))
                    ndefault++;
                else
                    nmatch++;
            }
        }
        if (ndefault == 1 || (ndefault == 0 && nmatch == 1))
            opclasses = lappend_oid(opclasses, best_opclass);
        else
            ereport(
                ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("could not determine interpretation of row comparison operator %s", strVal(llast(opname))),
                 errdetail("There are multiple equally-plausible candidates."),
                 errOmitLocation(true),
                 parser_errposition(pstate, location)));
    }

    /*
	 * Now deconstruct the OpExprs and create a RowCompareExpr.
	 *
	 * Note: can't just reuse the passed largs/rargs lists, because of
	 * possibility that make_op inserted coercion operations.
	 */
    opnos = NIL;
    largs = NIL;
    rargs = NIL;
    foreach (l, opexprs)
    {
        PGOpExpr * cmp = (PGOpExpr *)lfirst(l);

        opnos = lappend_oid(opnos, cmp->opno);
        largs = lappend(largs, linitial(cmp->args));
        rargs = lappend(rargs, lsecond(cmp->args));
    }

    rcexpr = makeNode(PGRowCompareExpr);
    rcexpr->rctype = rctype;
    rcexpr->opnos = opnos;
    rcexpr->opclasses = opclasses;
    rcexpr->largs = largs;
    rcexpr->rargs = rargs;

    return (PGNode *)rcexpr;
};

PGNode * ExprParser::transformAExprOp(PGParseState * pstate,
        PGAExpr * a)
{
    PGNode * lexpr = a->lexpr;
    PGNode * rexpr = a->rexpr;
    PGNode * result;

    /*
	 * Special-case "foo = NULL" and "NULL = foo" for compatibility with
	 * standards-broken products (like Microsoft's).  Turn these into IS NULL
	 * exprs.
	 */
    if (Transform_null_equals && list_length(a->name) == 1 && strcmp(strVal(linitial(a->name)), "=") == 0
        && (exprIsNullConstant(lexpr) || exprIsNullConstant(rexpr)))
    {
        PGNullTest * n = makeNode(PGNullTest);

        n->nulltesttype = IS_NULL;

        if (exprIsNullConstant(lexpr))
            n->arg = (PGExpr *)rexpr;
        else
            n->arg = (PGExpr *)lexpr;

        result = transformExpr(pstate, (PGNode *)n);
    }
    else if (lexpr && IsA(lexpr, PGRowExpr) && rexpr && IsA(rexpr, PGSubLink) && ((PGSubLink *)rexpr)->subLinkType == PG_EXPR_SUBLINK)
    {
        /*
		 * Convert "row op subselect" into a ROWCOMPARE sublink. Formerly the
		 * grammar did this, but now that a row construct is allowed anywhere
		 * in expressions, it's easier to do it here.
		 */
        PGSubLink * s = (PGSubLink *)rexpr;

        s->subLinkType = PG_ROWCOMPARE_SUBLINK;
        s->testexpr = lexpr;
        s->operName = a->name;
        result = transformExpr(pstate, (PGNode *)s);
    }
    else if (lexpr && IsA(lexpr, PGRowExpr) && rexpr && IsA(rexpr, PGRowExpr))
    {
        /* "row op row" */
        lexpr = transformExpr(pstate, lexpr);
        rexpr = transformExpr(pstate, rexpr);
        Assert(IsA(lexpr, PGRowExpr));
        Assert(IsA(rexpr, PGRowExpr));

        /* CDB: Drop a breadcrumb in case of error. */
        pstate->p_breadcrumb.node = (PGNode *)a;

        result = make_row_comparison_op(pstate, a->name, ((PGRowExpr *)lexpr)->args, ((PGRowExpr *)rexpr)->args, a->location);
    }
    else
    {
        /* Ordinary scalar operator */
        lexpr = transformExpr(pstate, lexpr);
        rexpr = transformExpr(pstate, rexpr);

        /* CDB: Drop a breadcrumb in case of error. */
        pstate->p_breadcrumb.node = (PGNode *)a;

        result = (PGNode *)oper_parser.make_op(pstate, a->name, lexpr, rexpr, a->location);
    }

    return result;
};

PGNode * ExprParser::transformAExprAnd(PGParseState * pstate, PGAExpr * a)
{
    PGNode * lexpr = transformExpr(pstate, a->lexpr);
    PGNode * rexpr = transformExpr(pstate, a->rexpr);

    lexpr = coerce_parser.coerce_to_boolean(pstate, lexpr, "AND");
    rexpr = coerce_parser.coerce_to_boolean(pstate, rexpr, "AND");

    return (PGNode *)makeBoolExpr(PG_AND_EXPR, list_make2(lexpr, rexpr), a->location);
};

PGNode * ExprParser::transformAExprOr(PGParseState * pstate, PGAExpr * a)
{
    PGNode * lexpr = transformExpr(pstate, a->lexpr);
    PGNode * rexpr = transformExpr(pstate, a->rexpr);

    lexpr = coerce_parser.coerce_to_boolean(pstate, lexpr, "OR");
    rexpr = coerce_parser.coerce_to_boolean(pstate, rexpr, "OR");

    return (PGNode *)makeBoolExpr(PG_OR_EXPR, list_make2(lexpr, rexpr), a->location);
};

PGNode * ExprParser::transformAExprNot(PGParseState * pstate, PGAExpr * a)
{
    PGNode * rexpr = transformExpr(pstate, a->rexpr);

    rexpr = coerce_parser.coerce_to_boolean(pstate, rexpr, "NOT");

    return (PGNode *)makeBoolExpr(PG_NOT_EXPR, list_make1(rexpr), a->location);
};

PGNode * ExprParser::transformAExprOpAny(PGParseState * pstate, PGAExpr * a)
{
    PGNode * lexpr = transformExpr(pstate, a->lexpr);
    PGNode * rexpr = transformExpr(pstate, a->rexpr);

    return (PGNode *)oper_parser.make_scalar_array_op(pstate, a->name, true, lexpr, rexpr, a->location);
};

PGNode * ExprParser::transformAExprOpAll(PGParseState * pstate, PGAExpr * a)
{
    PGNode * lexpr = transformExpr(pstate, a->lexpr);
    PGNode * rexpr = transformExpr(pstate, a->rexpr);

    return (PGNode *)oper_parser.make_scalar_array_op(pstate, a->name, false, lexpr, rexpr, a->location);
};

PGNode * ExprParser::make_row_distinct_op(PGParseState * pstate, PGList * opname,
        PGRowExpr * lrow, PGRowExpr * rrow, int location)
{
    PGNode * result = NULL;
    PGList * largs = lrow->args;
    PGList * rargs = rrow->args;
    PGListCell *l, *r;

    if (list_length(largs) != list_length(rargs))
        ereport(
            ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR),
             errmsg("unequal number of entries in row expressions"),
             errOmitLocation(true),
             parser_errposition(pstate, location)));

    forboth(l, largs, r, rargs)
    {
        PGNode * larg = (PGNode *)lfirst(l);
        PGNode * rarg = (PGNode *)lfirst(r);
        PGNode * cmp;

        cmp = (PGNode *)make_distinct_op(pstate, opname, larg, rarg, location);
        if (result == NULL)
            result = cmp;
        else
            result = (PGNode *)makeBoolExpr(PG_OR_EXPR, list_make2(result, cmp), location);
    }

    if (result == NULL)
    {
        /* zero-length rows?  Generate constant FALSE */
        result = makeBoolConst(false, false);
    }

    return result;
};

PGExpr * make_distinct_op(PGParseState * pstate, PGList * opname,
        PGNode * ltree, PGNode * rtree, int location)
{
    PGExpr * result;

    result = make_op(pstate, opname, ltree, rtree, location);
    if (((PGOpExpr *)result)->opresulttype != BOOLOID)
        ereport(
            ERROR,
            (errcode(ERRCODE_DATATYPE_MISMATCH),
             errmsg("IS DISTINCT FROM requires = operator to yield boolean"),
             errOmitLocation(true),
             parser_errposition(pstate, location)));

    /*
	 * We rely on DistinctExpr and OpExpr being same struct
	 */
    NodeSetTag(result, T_PGDistinctExpr);

    return result;
};

PGNode * ExprParser::transformAExprDistinct(PGParseState * pstate, PGAExpr * a)
{
    PGNode * lexpr = transformExpr(pstate, a->lexpr);
    PGNode * rexpr = transformExpr(pstate, a->rexpr);

    if (lexpr && IsA(lexpr, PGRowExpr) && rexpr && IsA(rexpr, PGRowExpr))
    {
        /* "row op row" */
        return make_row_distinct_op(pstate, a->name, (PGRowExpr *)lexpr, (PGRowExpr *)rexpr, a->location);
    }
    else
    {
        /* Ordinary scalar operator */
        return (PGNode *)make_distinct_op(pstate, a->name, lexpr, rexpr, a->location);
    }
};

PGNode * ExprParser::transformAExprNullIf(PGParseState * pstate, PGAExpr * a)
{
    PGNode * lexpr = transformExpr(pstate, a->lexpr);
    PGNode * rexpr = transformExpr(pstate, a->rexpr);
    PGNode * result;

    result = (PGNode *)make_op(pstate, a->name, lexpr, rexpr, a->location);
    if (((PGOpExpr *)result)->opresulttype != BOOLOID)
        ereport(
            ERROR,
            (errcode(ERRCODE_DATATYPE_MISMATCH),
             errmsg("NULLIF requires = operator to yield boolean"),
             parser_errposition(pstate, a->location)));

    /*
	 * We rely on NullIfExpr and OpExpr being the same struct
	 */
    NodeSetTag(result, T_PGNullIfExpr);

    return result;
};

PGNode * ExprParser::transformAExprOf(PGParseState * pstate, PGAExpr * a)
{
    /*
	 * Checking an expression for match to a list of type names. Will result
	 * in a boolean constant node.
	 */
    PGNode * lexpr = transformExpr(pstate, a->lexpr);
    PGListCell * telem;
    Oid ltype, rtype;
    bool matched = false;

    ltype = exprType(lexpr);
    foreach (telem, (PGList *)a->rexpr)
    {
        rtype = type_parser.typenameTypeId(pstate, lfirst(telem));
        matched = (rtype == ltype);
        if (matched)
            break;
    }

    /*
	 * We have two forms: equals or not equals. Flip the sense of the result
	 * for not equals.
	 */
    if (strcmp(strVal(linitial(a->name)), "<>") == 0)
        matched = (!matched);

    return makeBoolConst(matched, false);
};

PGNode * ExprParser::transformAExprIn(PGParseState * pstate, PGAExpr * a)
{
    PGNode * lexpr;
    PGList * rexprs;
    PGList * typeids;
    bool useOr;
    bool haveRowExpr;
    PGNode * result;
    PGListCell * l;

    /*
	 * If the operator is <>, combine with AND not OR.
	 */
    if (strcmp(strVal(linitial(a->name)), "<>") == 0)
        useOr = false;
    else
        useOr = true;

    /*
	 * We try to generate a ScalarArrayOpExpr from IN/NOT IN, but this is only
	 * possible if the inputs are all scalars (no RowExprs) and there is a
	 * suitable array type available.  If not, we fall back to a boolean
	 * condition tree with multiple copies of the lefthand expression.
	 *
	 * First step: transform all the inputs, and detect whether any are
	 * RowExprs.
	 */
    lexpr = transformExpr(pstate, a->lexpr);
    haveRowExpr = (lexpr && IsA(lexpr, PGRowExpr));
    typeids = list_make1_oid(exprType(lexpr));
    rexprs = NIL;
    foreach (l, (PGList *)a->rexpr)
    {
        Node * rexpr = transformExpr(pstate, lfirst(l));

        haveRowExpr |= (rexpr && IsA(rexpr, PGRowExpr));
        rexprs = lappend(rexprs, rexpr);
        typeids = lappend_oid(typeids, exprType(rexpr));
    }

    /* CDB: Drop a breadcrumb in case of error. */
    pstate->p_breadcrumb.node = (PGNode *)a;

    /*
	 * If not forced by presence of RowExpr, try to resolve a common scalar
	 * type for all the expressions, and see if it has an array type. (But if
	 * there's only one righthand expression, we may as well just fall through
	 * and generate a simple = comparison.)
	 */
    if (!haveRowExpr && list_length(rexprs) != 1)
    {
        Oid scalar_type;
        Oid array_type;

        /*
		 * Select a common type for the array elements.  Note that since the
		 * LHS' type is first in the list, it will be preferred when there is
		 * doubt (eg, when all the RHS items are unknown literals).
		 */
        scalar_type = coerce_parser.select_common_type(typeids, "IN");

        /* Do we have an array type to use? */
        array_type = get_array_type(scalar_type);
        if (array_type != InvalidOid)
        {
            /*
			 * OK: coerce all the right-hand inputs to the common type and
			 * build an ArrayExpr for them.
			 */
            PGList * aexprs;
            PGArrayExpr * newa;

            aexprs = NIL;
            foreach (l, rexprs)
            {
                PGNode * rexpr = (PGNode *)lfirst(l);

                rexpr = coerce_parser.coerce_to_common_type(pstate, rexpr, scalar_type, "IN");
                aexprs = lappend(aexprs, rexpr);
            }
            newa = makeNode(PGArrayExpr);
            newa->array_typeid = array_type;
            newa->element_typeid = scalar_type;
            newa->elements = aexprs;
            newa->multidims = false;

            return (PGNode *)oper_parser.make_scalar_array_op(pstate, a->name, useOr, lexpr, (PGNode *)newa, a->location);
        }
    }

    /*
	 * Must do it the hard way, ie, with a boolean expression tree.
	 */
    result = NULL;
    foreach (l, rexprs)
    {
        Node * rexpr = (Node *)lfirst(l);
        Node * cmp;

        if (haveRowExpr)
        {
            if (!IsA(lexpr, RowExpr) || !IsA(rexpr, RowExpr))
                ereport(
                    ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                     errmsg("arguments of row IN must all be row expressions"),
                     parser_errposition(pstate, a->location)));
            cmp = make_row_comparison_op(
                pstate, a->name, (List *)copyObject(((RowExpr *)lexpr)->args), ((RowExpr *)rexpr)->args, a->location);
        }
        else
            cmp = (Node *)oper_parser.make_op(pstate, a->name, copyObject(lexpr), rexpr, a->location);

        cmp = coerce_parser.coerce_to_boolean(pstate, cmp, "IN");
        if (result == NULL)
            result = cmp;
        else
            result = (Node *)makeBoolExpr(useOr ? OR_EXPR : AND_EXPR, list_make2(result, cmp), a->location);
    }

    return result;
};

PGNode * ExprParser::transformFuncCall(PGParseState * pstate, PGFuncCall * fn)
{
    PGList * targs;
    PGListCell * args;

    /*
	 * Transform the list of arguments...
	 */
    targs = NIL;
    foreach (args, fn->args)
    {
        targs = lappend(targs, transformExpr(pstate, (PGNode *)lfirst(args)));
    }

    /* CDB: Drop a breadcrumb in case of error. */
    pstate->p_breadcrumb.node = (PGNode *)fn;

    /* ... and hand off to ParseFuncOrColumn */
    return func_parser.ParseFuncOrColumn(
        pstate,
        fn->funcname,
        targs,
        fn->agg_order,
        fn->agg_star,
        fn->agg_distinct,
        fn->func_variadic,
        false,
        (WindowSpec *)fn->over,
        fn->location,
        fn->agg_filter);
};

PGNode * ExprParser::transformSubLink(PGParseState * pstate, PGSubLink * sublink)
{
    PGList * qtrees;
    PGQuery * qtree;
    PGNode * result = (PGNode *)sublink;

    /* If we already transformed this node, do nothing */
    if (IsA(sublink->subselect, PGQuery))
        return result;

    pstate->p_hasSubLinks = true;
    qtrees = select_parser.parse_sub_analyze(sublink->subselect, pstate);

    /*
	 * Check that we got something reasonable.	Many of these conditions are
	 * impossible given restrictions of the grammar, but check 'em anyway.
	 */
    Insist(list_length(qtrees) == 1);
    qtree = (PGQuery *)linitial(qtrees);
    if (!IsA(qtree, PGQuery) || qtree->commandType != PG_CMD_SELECT || qtree->utilityStmt != NULL)
        elog(ERROR, "unexpected non-SELECT command in SubLink");
    if (qtree->intoClause)
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("subquery cannot have SELECT INTO"), errOmitLocation(true)));

    sublink->subselect = (PGNode *)qtree;

    if (sublink->subLinkType == PG_EXISTS_SUBLINK)
    {
        /*
		 * EXISTS needs no test expression or combining operator. These fields
		 * should be null already, but make sure.
		 */
        sublink->testexpr = NULL;
        sublink->operName = NIL;
    }
    else if (sublink->subLinkType == PG_EXPR_SUBLINK || sublink->subLinkType == PG_ARRAY_SUBLINK)
    {
        PGListCell * tlist_item = list_head(qtree->targetList);

        /*
		 * Make sure the subselect delivers a single column (ignoring resjunk
		 * targets).
		 */
        if (tlist_item == NULL || ((PGTargetEntry *)lfirst(tlist_item))->resjunk)
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("subquery must return a column")));
        while ((tlist_item = lnext(tlist_item)) != NULL)
        {
            if (!((PGTargetEntry *)lfirst(tlist_item))->resjunk)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("subquery must return only one column")));
        }

        /*
		 * EXPR and ARRAY need no test expression or combining operator. These
		 * fields should be null already, but make sure.
		 */
        sublink->testexpr = NULL;
        sublink->operName = NIL;
    }
    else
    {
        /* ALL, ANY, or ROWCOMPARE: generate row-comparing expression */
        PGNode * lefthand;
        PGList * left_list;
        PGList * right_list;
        PGListCell * l;

        /*
		 * Transform lefthand expression, and convert to a list
		 */
        lefthand = transformExpr(pstate, sublink->testexpr);
        if (lefthand && IsA(lefthand, PGRowExpr))
            left_list = ((PGRowExpr *)lefthand)->args;
        else
            left_list = list_make1(lefthand);

        /*
		 * Build a list of PARAM_SUBLINK nodes representing the output columns
		 * of the subquery.
		 */
        right_list = NIL;
        foreach (l, qtree->targetList)
        {
            PGTargetEntry * tent = (PGTargetEntry *)lfirst(l);
            PGParam * param;

            if (tent->resjunk)
                continue;

            param = makeNode(PGParam);
            param->paramkind = PG_PARAM_SUBLINK;
            param->paramid = tent->resno;
            param->paramtype = exprType((PGNode *)tent->expr);

            right_list = lappend(right_list, param);
        }

        /* CDB: Drop a breadcrumb in case of error. */
        pstate->p_breadcrumb.node = (PGNode *)sublink;

        /*
		 * We could rely on make_row_comparison_op to complain if the list
		 * lengths differ, but we prefer to generate a more specific error
		 * message.
		 */
        if (list_length(left_list) < list_length(right_list))
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("subquery has too many columns")));
        if (list_length(left_list) > list_length(right_list))
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("subquery has too few columns")));

        /*
		 * Identify the combining operator(s) and generate a suitable
		 * row-comparison expression.
		 */
        sublink->testexpr = make_row_comparison_op(pstate, sublink->operName, left_list, right_list, -1);
    }

    return result;
};

bool ExprParser::isWhenIsNotDistinctFromExpr(PGNode * warg)
{
    if (IsA(warg, PGAExpr))
    {
        PGAExpr * top = (PGAExpr *)warg;
        if (top->kind == AEXPR_NOT && IsA(top->rexpr, PGAExpr))
        {
            PGAExpr * expr = (PGAExpr *)top->rexpr;
            if (expr->kind == PG_AEXPR_DISTINCT && expr->lexpr == NULL)
                return true;
        }
    }
    return false;
};

PGNode * ExprParser::transformCaseExpr(PGParseState * pstate, PGCaseExpr * c)
{
    PGCaseExpr * newc;
    PGNode * arg;
    PGCaseTestExpr * placeholder;
    PGList * newargs;
    PGList * typeids;
    PGListCell * l;
    PGNode * defresult;
    Oid ptype;

    /* If we already transformed this node, do nothing */
    if (OidIsValid(c->casetype))
        return (PGNode *)c;

    newc = makeNode(PGCaseExpr);

    /* transform the test expression, if any */
    arg = transformExpr(pstate, (PGNode *)c->arg);

    /* generate placeholder for test expression */
    if (arg)
    {
        /*
		 * If test expression is an untyped literal, force it to text. We have
		 * to do something now because we won't be able to do this coercion on
		 * the placeholder.  This is not as flexible as what was done in 7.4
		 * and before, but it's good enough to handle the sort of silly coding
		 * commonly seen.
		 */
        if (exprType(arg) == UNKNOWNOID)
            arg = coerce_parser.coerce_to_common_type(pstate, arg, TEXTOID, "CASE");

        placeholder = makeNode(PGCaseTestExpr);
        placeholder->typeId = exprType(arg);
        placeholder->typeMod = exprTypmod(arg);
    }
    else
        placeholder = NULL;

    newc->arg = (PGExpr *)arg;

    /* transform the list of arguments */
    newargs = NIL;
    typeids = NIL;
    foreach (l, c->args)
    {
        PGCaseWhen * w = (PGCaseWhen *)lfirst(l);
        PGCaseWhen * neww = makeNode(PGCaseWhen);
        PGNode * warg;

        Assert(IsA(w, PGCaseWhen));

        warg = (PGNode *)w->expr;
        if (placeholder)
        {
            /* 
			 * CASE placeholder WHEN IS NOT DISTINCT FROM warg:
			 * 		set: warg->rhs->lhs = placeholder
			 */
            if (isWhenIsNotDistinctFromExpr(warg))
            {
                /*
 			 * Make a copy before we change warg.
 			 * In transformation we don't want to change source (CaseExpr* Node).
 			 * Always create new node and do the transformation
 			 */
                warg = copyObject(warg);
                PGAExpr * top = (PGAExpr *)warg;
                PGAExpr * expr = (PGAExpr *)top->rexpr;
                expr->lexpr = (PGNode *)placeholder;
            }
            else
                warg = (PGNode *)makeSimpleA_Expr(PG_AEXPR_OP, "=", (PGNode *)placeholder, warg, -1);
        }
        else
        {
            if (isWhenIsNotDistinctFromExpr(warg))
                ereport(
                    ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                     errmsg("syntax error at or near \"NOT\""),
                     errhint("Missing <operand> for \"CASE <operand> WHEN IS NOT DISTINCT FROM ...\"")));
        }
        neww->expr = (PGExpr *)transformExpr(pstate, warg);

        neww->expr = (PGExpr *)coerce_parser.coerce_to_boolean(pstate, (PGNode *)neww->expr, "CASE/WHEN");

        warg = (PGNode *)w->result;
        neww->result = (PGExpr *)transformExpr(pstate, warg);

        newargs = lappend(newargs, neww);
        typeids = lappend_oid(typeids, exprType((PGNode *)neww->result));
    }

    newc->args = newargs;

    /* transform the default clause */
    defresult = (PGNode *)c->defresult;
    if (defresult == NULL)
    {
        PGAConst * n = makeNode(PGAConst);

        n->val.type = T_PGNull;
        defresult = (PGNode *)n;
    }
    newc->defresult = (PGExpr *)transformExpr(pstate, defresult);

    /*
	 * Note: default result is considered the most significant type in
	 * determining preferred type. This is how the code worked before, but it
	 * seems a little bogus to me --- tgl
	 */
    typeids = lcons_oid(exprType((PGNode *)newc->defresult), typeids);

    /* CDB: Drop a breadcrumb in case of error. */
    pstate->p_breadcrumb.node = (PGNode *)c;

    ptype = coerce_parser.select_common_type(typeids, "CASE");
    Assert(OidIsValid(ptype));
    newc->casetype = ptype;

    /* Convert default result clause, if necessary */
    newc->defresult = (PGExpr *)coerce_parser.coerce_to_common_type(pstate, (PGNode *)newc->defresult, ptype, "CASE/ELSE");

    /* Convert when-clause results, if necessary */
    foreach (l, newc->args)
    {
        PGCaseWhen * w = (PGCaseWhen *)lfirst(l);

        w->result = (PGExpr *)coerce_parser.coerce_to_common_type(pstate, (PGNode *)w->result, ptype, "CASE/WHEN");
    }

    return (PGNode *)newc;
};

PGNode * ExprParser::transformArrayExpr(PGParseState * pstate, PGArrayExpr * a)
{
    PGArrayExpr * newa = makeNode(PGArrayExpr);
    PGList * newelems = NIL;
    PGList * newcoercedelems = NIL;
    PGList * typeids = NIL;
    PGListCell * element;
    Oid array_type;
    Oid element_type;

    /* Transform the element expressions */
    foreach (element, a->elements)
    {
        PGNode * e = (PGNode *)lfirst(element);
        PGNode * newe;

        newe = transformExpr(pstate, e);
        newelems = lappend(newelems, newe);
        typeids = lappend_oid(typeids, exprType(newe));
    }

    /* CDB: Drop a breadcrumb in case of error. */
    pstate->p_breadcrumb.node = (PGNode *)a;

    /* Select a common type for the elements */
    element_type = coerce_parser.select_common_type(typeids, "ARRAY");

    /* Coerce arguments to common type if necessary */
    foreach (element, newelems)
    {
        PGNode * e = (PGNode *)lfirst(element);
        PGNode * newe;

        newe = coerce_parser.coerce_to_common_type(pstate, e, element_type, "ARRAY");
        newcoercedelems = lappend(newcoercedelems, newe);
    }

    /* Do we have an array type to use? */
    array_type = get_array_type(element_type);
    if (array_type != InvalidOid)
    {
        /* Elements are presumably of scalar type */
        newa->multidims = false;
    }
    else
    {
        /* Must be nested array expressions */
        newa->multidims = true;

        array_type = element_type;
        element_type = get_element_type(array_type);
        if (!OidIsValid(element_type))
            ereport(
                ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("could not find array type for data type %s", format_type_be(array_type))));
    }

    newa->array_typeid = array_type;
    newa->element_typeid = element_type;
    newa->elements = newcoercedelems;

    return (PGNode *)newa;
};

PGNode * ExprParser::transformRowExpr(PGParseState * pstate, PGRowExpr * r)
{
    PGRowExpr * newr = makeNode(PGRowExpr);

    /* Transform the field expressions */
    newr->args = target_parser.transformExpressionList(pstate, r->args);

    /* Barring later casting, we consider the type RECORD */
    newr->row_typeid = RECORDOID;
    newr->row_format = PG_COERCE_IMPLICIT_CAST;

    return (PGNode *)newr;
};

PGNode * ExprParser::transformCoalesceExpr(PGParseState * pstate, PGCoalesceExpr * c)
{
    PGCoalesceExpr * newc = makeNode(PGCoalesceExpr);
    PGList * newargs = NIL;
    PGList * newcoercedargs = NIL;
    PGList * typeids = NIL;
    PGListCell * args;

    foreach (args, c->args)
    {
        PGNode * e = (PGNode *)lfirst(args);
        PGNode * newe;

        newe = transformExpr(pstate, e);
        newargs = lappend(newargs, newe);
        typeids = lappend_oid(typeids, exprType(newe));
    }

    newc->coalescetype = coerce_parser.select_common_type(typeids, "COALESCE");

    /* Convert arguments if necessary */
    foreach (args, newargs)
    {
        PGNode * e = (PGNode *)lfirst(args);
        PGNode * newe;

        newe = coerce_parser.coerce_to_common_type(pstate, e, newc->coalescetype, "COALESCE");
        newcoercedargs = lappend(newcoercedargs, newe);
    }

    newc->args = newcoercedargs;
    return (PGNode *)newc;
};

PGNode * ExprParser::transformMinMaxExpr(PGParseState * pstate, PGMinMaxExpr * m)
{
    PGMinMaxExpr * newm = makeNode(PGMinMaxExpr);
    PGList * newargs = NIL;
    PGList * newcoercedargs = NIL;
    PGList * typeids = NIL;
    PGListCell * args;

    newm->op = m->op;
    foreach (args, m->args)
    {
        PGNode * e = (PGNode *)lfirst(args);
        PGNode * newe;

        newe = transformExpr(pstate, e);
        newargs = lappend(newargs, newe);
        typeids = lappend_oid(typeids, exprType(newe));
    }

    newm->minmaxtype = coerce_parser.select_common_type(typeids, "GREATEST/LEAST");

    /* Convert arguments if necessary */
    foreach (args, newargs)
    {
        PGNode * e = (PGNode *)lfirst(args);
        PGNode * newe;

        newe = coerce_parser.coerce_to_common_type(pstate, e, newm->minmaxtype, "GREATEST/LEAST");
        newcoercedargs = lappend(newcoercedargs, newe);
    }

    newm->args = newcoercedargs;
    return (PGNode *)newm;
};

PGNode * ExprParser::transformBooleanTest(PGParseState * pstate, PGBooleanTest * b)
{
    const char * clausename;

    switch (b->booltesttype)
    {
        case PG_IS_TRUE:
            clausename = "IS TRUE";
            break;
        case IS_NOT_TRUE:
            clausename = "IS NOT TRUE";
            break;
        case IS_FALSE:
            clausename = "IS FALSE";
            break;
        case IS_NOT_FALSE:
            clausename = "IS NOT FALSE";
            break;
        case IS_UNKNOWN:
            clausename = "IS UNKNOWN";
            break;
        case IS_NOT_UNKNOWN:
            clausename = "IS NOT UNKNOWN";
            break;
        default:
            elog(ERROR, "unrecognized booltesttype: %d", (int)b->booltesttype);
            clausename = NULL; /* keep compiler quiet */
    }

    b->arg = (PGExpr *)transformExpr(pstate, (PGNode *)b->arg);

    b->arg = (PGExpr *)coerce_parser.coerce_to_boolean(pstate, (PGNode *)b->arg, clausename);

    return (PGNode *)b;
};

PGNode * ExprParser::transformGroupingFunc(PGParseState * pstate, PGGroupingFunc * gf)
{
    PGList * targs = NIL;
    PGListCell * lc;
    PGGroupingFunc * new_gf;

    new_gf = makeNode(PGGroupingFunc);

    /*
	 * Transform the list of arguments.
	 */
    foreach (lc, gf->args)
        targs = lappend(targs, transformExpr(pstate, (PGNode *)lfirst(lc)));

    new_gf->args = targs;

    new_gf->ngrpcols = gf->ngrpcols;

    return (PGNode *)new_gf;
};

PGNode * ExprParser::transformExpr(PGParseState * pstate,
        PGNode * expr)
{
    PGNode * result;
    ParseStateBreadCrumb savebreadcrumb;

    if (expr == NULL)
        return NULL;

    /* Guard against stack overflow due to overly complex expressions */
    check_stack_depth();

    /* CDB: Drop a breadcrumb, then push location stack. Must pop before return! */
    Assert(pstate);
    pstate->p_breadcrumb.node = (PGNode *)expr;
    savebreadcrumb = pstate->p_breadcrumb;
    pstate->p_breadcrumb.pop = &savebreadcrumb;

    result = NULL;
    switch (nodeTag(expr))
    {
        case T_PGColumnRef:
            result = transformColumnRef(pstate, (PGColumnRef *)expr);
            break;

        case T_PGParamRef:
            result = transformParamRef(pstate, (PGParamRef *)expr);
            break;

        case T_PGAConst: {
            PGAConst * con = (PGAConst *)expr;
            PGValue * val = &con->val;

            result = (PGNode *)node_parser.make_const(pstate, val, -1);
            if (con->typname != NULL)
                result = typecast_expression(pstate, result, con->typname);
            break;
        }

        case T_PGAIndirection: {
            PGAIndirection * ind = (PGAIndirection *)expr;

            result = transformExpr(pstate, ind->arg);
            result = transformIndirection(pstate, result, ind->indirection);
            break;
        }

        case T_PGTypeCast: {
            PGTypeCast * tc = (PGTypeCast *)expr;
            PGNode * arg = transformExpr(pstate, tc->arg);

            result = typecast_expression(pstate, arg, tc->typname);
            break;
        }

        case T_PGAExpr: {
            PGAExpr * a = (PGAExpr *)expr;

            switch (a->kind)
            {
                case PG_AEXPR_OP:
                    result = transformAExprOp(pstate, a);
                    break;
                case AEXPR_AND:
                    result = transformAExprAnd(pstate, a);
                    break;
                case AEXPR_OR:
                    result = transformAExprOr(pstate, a);
                    break;
                case AEXPR_NOT:
                    result = transformAExprNot(pstate, a);
                    break;
                case PG_AEXPR_OP_ANY:
                    result = transformAExprOpAny(pstate, a);
                    break;
                case PG_AEXPR_OP_ALL:
                    result = transformAExprOpAll(pstate, a);
                    break;
                case PG_AEXPR_DISTINCT:
                    result = transformAExprDistinct(pstate, a);
                    break;
                case PG_AEXPR_NULLIF:
                    result = transformAExprNullIf(pstate, a);
                    break;
                case PG_AEXPR_OF:
                    result = transformAExprOf(pstate, a);
                    break;
                case PG_AEXPR_IN:
                    result = transformAExprIn(pstate, a);
                    break;
                default:
                    elog(ERROR, "unrecognized A_Expr kind: %d", a->kind);
            }
            break;
        }

        case T_PGFuncCall:
            result = transformFuncCall(pstate, (PGFuncCall *)expr);
            break;


        case T_PGSubLink:
            result = transformSubLink(pstate, (PGSubLink *)expr);
            break;

        case T_PGCaseExpr:
            result = transformCaseExpr(pstate, (PGCaseExpr *)expr);
            break;

        case T_PGArrayExpr:
            result = transformArrayExpr(pstate, (PGArrayExpr *)expr);
            break;

        case T_PGRowExpr:
            result = transformRowExpr(pstate, (PGRowExpr *)expr);
            break;

        // case T_TableValueExpr:
        //     result = transformTableValueExpr(pstate, (TableValueExpr *)expr);
        //     break;

        case T_PGCoalesceExpr:
            result = transformCoalesceExpr(pstate, (PGCoalesceExpr *)expr);
            break;

        case T_PGMinMaxExpr:
            result = transformMinMaxExpr(pstate, (PGMinMaxExpr *)expr);
            break;

        case T_PGNullTest: {
            PGNullTest * n = (PGNullTest *)expr;

            n->arg = (PGExpr *)transformExpr(pstate, (PGNode *)n->arg);
            /* the argument can be any type, so don't coerce it */
            result = expr;
            break;
        }

        case T_PGBooleanTest:
            result = transformBooleanTest(pstate, (PGBooleanTest *)expr);
            break;

        case T_PGCurrentOfExpr: {
            /*
				 * The target RTE must be simply updatable. If not, we error out
				 * early here to avoid having to deal with error cases later:
				 * rewriting/planning against views, for example.
				 */
            Assert(pstate->p_target_rangetblentry != NULL);
            if (!isSimplyUpdatableRelation(pstate->p_target_rangetblentry->relid))
                ereport(
                    ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg("\"%s\" is not simply updatable", pstate->p_target_relation->rd_rel->relname.data)));

            PGCurrentOfExpr * c = (PGCurrentOfExpr *)expr;
            int sublevels_up;
            c->cvarno = relation_parser.RTERangeTablePosn(pstate, pstate->p_target_rangetblentry, &sublevels_up);
            c->target_relid = pstate->p_target_rangetblentry->relid;
            Assert(sublevels_up == 0);
            result = expr;
            break;
        }

        case T_PGGroupingFunc: {
            PGGroupingFunc * gf = (PGGroupingFunc *)expr;
            result = transformGroupingFunc(pstate, gf);
            break;
        }

        // case T_PGPartitionBoundSpec: {
        //     PartitionBoundSpec * in = (PartitionBoundSpec *)expr;
        //     PartitionRangeItem * ri;
        //     PGList * out = NIL;
        //     PGListCell * lc;

        //     if (in->partStart)
        //     {
        //         ri = (PartitionRangeItem *)in->partStart;

        //         /* ALTER TABLE ... ADD PARTITION might feed
		// 			 * "pre-cooked" expressions into the boundspec for
		// 			 * range items (which are Lists) 
		// 			 */
        //         {
        //             Assert(IsA(in->partStart, PartitionRangeItem));

        //             foreach (lc, ri->partRangeVal)
        //             {
        //                 PGNode * n = lfirst(lc);
        //                 out = lappend(out, transformExpr(pstate, n));
        //             }
        //             ri->partRangeVal = out;
        //             out = NIL;
        //         }
        //     }
        //     if (in->partEnd)
        //     {
        //         ri = (PartitionRangeItem *)in->partEnd;

        //         /* ALTER TABLE ... ADD PARTITION might feed
		// 			 * "pre-cooked" expressions into the boundspec for
		// 			 * range items (which are Lists) 
		// 			 */
        //         {
        //             Assert(IsA(in->partEnd, PartitionRangeItem));
        //             foreach (lc, ri->partRangeVal)
        //             {
        //                 PGNode * n = lfirst(lc);
        //                 out = lappend(out, transformExpr(pstate, n));
        //             }
        //             ri->partRangeVal = out;
        //             out = NIL;
        //         }
        //     }
        //     if (in->partEvery)
        //     {
        //         ri = (PartitionRangeItem *)in->partEvery;
        //         Assert(IsA(in->partEvery, PartitionRangeItem));
        //         foreach (lc, ri->partRangeVal)
        //         {
        //             PGNode * n = lfirst(lc);
        //             out = lappend(out, transformExpr(pstate, n));
        //         }
        //         ri->partRangeVal = out;
        //     }

        //     result = (PGNode *)in;
        // }
        //break;

        // case T_PercentileExpr:
        //     result = transformPercentileExpr(pstate, (PercentileExpr *)expr);
        //     break;

            /*********************************************
			 * Quietly accept node types that may be presented when we are
			 * called on an already-transformed tree.
			 *
			 * Do any other node types need to be accepted?  For now we are
			 * taking a conservative approach, and only accepting node
			 * types that are demonstrably necessary to accept.
			 *********************************************/
        case T_PGVar:
        case T_PGConst:
        case T_PGParam:
        case T_PGAggref:
        case T_PGArrayRef:
        case T_PGFuncExpr:
        case T_PGOpExpr:
        case T_PGDistinctExpr:
        case T_PGScalarArrayOpExpr:
        case T_PGNullIfExpr:
        case T_PGBoolExpr:
        case T_PGFieldSelect:
        case T_PGFieldStore:
        case T_PGRelabelType:
        case T_PGConvertRowtypeExpr:
        case T_PGCaseTestExpr:
        case T_PGCoerceToDomain:
        case T_PGCoerceToDomainValue:
        case T_PGSetToDefault:
        // case T_GroupId:
        case T_PGInteger: {
            result = (PGNode *)expr;
            break;
        }

        default:
            /* should not reach here */
            elog(ERROR, "unrecognized node type: %d", (int)nodeTag(expr));
            break;
    }

    /* CDB: Pop error location stack, leaving breadcrumb on our input expr. */
    Assert(pstate->p_breadcrumb.pop == &savebreadcrumb);
    pstate->p_breadcrumb = savebreadcrumb;

    return result;
};

}