#include <Interpreters/orcaopt/CoerceParser.h>

#include <Interpreters/orcaopt/RelationParser.h>
#include <Interpreters/orcaopt/SelectParser.h>
#include <Interpreters/orcaopt/ExprParser.h>
#include <Interpreters/orcaopt/NodeParser.h>
#include <Interpreters/orcaopt/TypeParser.h>
#include <Interpreters/orcaopt/provider/TypeProvider.h>
#include <Interpreters/orcaopt/provider/ProcProvider.h>
#include <Interpreters/orcaopt/provider/CastProvider.h>
#include <Interpreters/orcaopt/provider/RelationProvider.h>

#include <Interpreters/Context.h>

using namespace duckdb_libpgquery;

namespace DB
{

CoerceParser::CoerceParser(ContextPtr& context_) : context(context_)
{
	relation_parser = std::make_shared<RelationParser>(context);
	node_parser = std::make_shared<NodeParser>(context);
	type_parser = std::make_shared<TypeParser>(context);
	type_provider = std::make_shared<TypeProvider>(context);
	proc_provider = std::make_shared<ProcProvider>(context);
	cast_provider = std::make_shared<CastProvider>(context);
	relation_provider = std::make_shared<RelationProvider>(context);
};

PGOid
CoerceParser::select_common_type(PGParseState *pstate, PGList *exprs, const char* context_str,
				   PGNode **which_expr)
{
    PGNode	   *pexpr;
	PGOid			ptype;
	TYPCATEGORY pcategory;
	bool		pispreferred;
	PGListCell   *lc;

	Assert(exprs != NIL)
	pexpr = (PGNode *) linitial(exprs);
	lc = lnext(list_head(exprs));
	ptype = exprType(pexpr);

	/*
	 * If all input types are valid and exactly the same, just pick that type.
	 * This is the only way that we will resolve the result as being a domain
	 * type; otherwise domains are smashed to their base types for comparison.
	 */
	if (ptype != UNKNOWNOID)
	{
		for_each_cell(lc, lc)
		{
			PGNode	   *nexpr = (PGNode *) lfirst(lc);
			PGOid			ntype = exprType(nexpr);

			if (ntype != ptype)
				break;
		}
		if (lc == NULL)			/* got to the end of the list? */
		{
			if (which_expr)
				*which_expr = pexpr;
			return ptype;
		}
	}

	/*
	 * Nope, so set up for the full algorithm.  Note that at this point, lc
	 * points to the first list item with type different from pexpr's; we need
	 * not re-examine any items the previous loop advanced over.
	 */
	ptype = type_provider->getBaseType(ptype);
	type_provider->get_type_category_preferred(ptype, &pcategory, &pispreferred);

	for_each_cell(lc, lc)
	{
		PGNode	   *nexpr = (PGNode *) lfirst(lc);
		PGOid			ntype = type_provider->getBaseType(exprType(nexpr));

		/* move on to next one if no new information... */
		if (ntype != UNKNOWNOID && ntype != ptype)
		{
			TYPCATEGORY ncategory;
			bool		nispreferred;

			type_provider->get_type_category_preferred(ntype, &ncategory, &nispreferred);
			if (ptype == UNKNOWNOID)
			{
				/* so far, only unknowns so take anything... */
				pexpr = nexpr;
				ptype = ntype;
				pcategory = ncategory;
				pispreferred = nispreferred;
			}
			else if (ncategory != pcategory)
			{
				/*
				 * both types in different categories? then not much hope...
				 */
				if (context_str == NULL)
					return InvalidOid;
				parser_errposition(pstate, exprLocation(nexpr));
				ereport(ERROR,
						(errcode(PG_ERRCODE_DATATYPE_MISMATCH),
				/*------
				  translator: first %s is name of a SQL construct, eg CASE */
						 errmsg("%s types %s and %s cannot be matched",
								context_str,
								type_provider->format_type_be(ptype).c_str(),
								type_provider->format_type_be(ntype).c_str())));
			}
			else if (!pispreferred &&
					 can_coerce_type(1, &ptype, &ntype, PG_COERCION_IMPLICIT) &&
					 !can_coerce_type(1, &ntype, &ptype, PG_COERCION_IMPLICIT))
			{
				/*
				 * take new type if can coerce to it implicitly but not the
				 * other way; but if we have a preferred type, stay on it.
				 */
				pexpr = nexpr;
				ptype = ntype;
				pcategory = ncategory;
				pispreferred = nispreferred;
			}
		}
	}

	/*
	 * If all the inputs were UNKNOWN type --- ie, unknown-type literals ---
	 * then resolve as type TEXT.  This situation comes up with constructs
	 * like SELECT (CASE WHEN foo THEN 'bar' ELSE 'baz' END); SELECT 'foo'
	 * UNION SELECT 'bar'; It might seem desirable to leave the construct's
	 * output type as UNKNOWN, but that really doesn't work, because we'd
	 * probably end up needing a runtime coercion from UNKNOWN to something
	 * else, and we usually won't have it.  We need to coerce the unknown
	 * literals while they are still literals, so a decision has to be made
	 * now.
	 */
	if (ptype == UNKNOWNOID)
		ptype = TEXTOID;

	if (which_expr)
		*which_expr = pexpr;
	return ptype;
};

PGNode *
CoerceParser::coerce_record_to_complex(PGParseState *pstate, PGNode *node,
						 PGOid targetTypeId,
						 PGCoercionContext ccontext,
						 PGCoercionForm cformat,
						 int location)
{
    PGRowExpr * rowexpr;
    PGList * args = NIL;
    PGList * newargs;
    int i;
    int ucolno;
    PGListCell * arg;

    if (node && IsA(node, PGRowExpr))
    {
        /*
		 * Since the RowExpr must be of type RECORD, we needn't worry about it
		 * containing any dropped columns.
		 */
        args = ((PGRowExpr *)node)->args;
    }
    else if (node && IsA(node, PGVar) && ((PGVar *)node)->varattno == InvalidAttrNumber)
    {
        int rtindex = ((PGVar *)node)->varno;
        int sublevels_up = ((PGVar *)node)->varlevelsup;
        int vlocation = ((PGVar *)node)->location;
        PGRangeTblEntry * rte;

        rte = relation_parser->GetRTEByRangeTablePosn(pstate, rtindex, sublevels_up);
        relation_parser->expandRTE(rte, rtindex, sublevels_up, vlocation, false, NULL, &args);
    }
    else
        ereport(
            ERROR,
            (errcode(PG_ERRCODE_CANNOT_COERCE),
             errmsg("cannot cast type %s to %s", type_provider->format_type_be(RECORDOID).c_str(), type_provider->format_type_be(targetTypeId).c_str()),
             parser_coercion_errposition(pstate, location, node)));

    PGTupleDescPtr tupdesc = type_provider->lookup_rowtype_tupdesc(targetTypeId, -1);
    newargs = NIL;
    ucolno = 1;
    arg = list_head(args);
    for (i = 0; i < tupdesc->natts; i++)
    {
        PGNode * expr;
        PGNode * cexpr;
        PGOid exprtype;

        /* Fill in NULLs for dropped columns in rowtype */
        if (tupdesc->attrs[i]->attisdropped)
        {
            /*
			 * can't use atttypid here, but it doesn't really matter what type
			 * the Const claims to be.
			 */

            int16_t typLen;
            bool typByVal;
            type_provider->get_typlenbyval(INT4OID, &typLen, &typByVal);
            newargs = lappend(newargs, makeNullConst(typLen, typByVal, INT4OID, -1, InvalidOid));
            continue;
        }

        if (arg == NULL)
            ereport(
                ERROR,
                (errcode(PG_ERRCODE_CANNOT_COERCE),
                 errmsg("cannot cast type %s to %s", type_provider->format_type_be(RECORDOID).c_str(), type_provider->format_type_be(targetTypeId).c_str()),
                 errdetail("Input has too few columns."),
                 parser_coercion_errposition(pstate, location, node)));
        expr = (PGNode *)lfirst(arg);
        exprtype = exprType(expr);

        cexpr = coerce_to_target_type(
            pstate, expr, exprtype, tupdesc->attrs[i]->atttypid, tupdesc->attrs[i]->atttypmod, ccontext, PG_COERCE_IMPLICIT_CAST, -1);
        if (cexpr == NULL)
            ereport(
                ERROR,
                (errcode(PG_ERRCODE_CANNOT_COERCE),
                 errmsg("cannot cast type %s to %s", type_provider->format_type_be(RECORDOID).c_str(), type_provider->format_type_be(targetTypeId).c_str()),
                 errdetail(
                     "Cannot cast type %s to %s in column %d.",
                     type_provider->format_type_be(exprtype).c_str(),
                     type_provider->format_type_be(tupdesc->attrs[i]->atttypid).c_str(),
                     ucolno),
                 parser_coercion_errposition(pstate, location, expr)));
        newargs = lappend(newargs, cexpr);
        ucolno++;
        arg = lnext(arg);
    }
    if (arg != NULL)
        ereport(
            ERROR,
            (errcode(PG_ERRCODE_CANNOT_COERCE),
             errmsg("cannot cast type %s to %s", type_provider->format_type_be(RECORDOID).c_str(), type_provider->format_type_be(targetTypeId).c_str()),
             errdetail("Input has too many columns."),
             parser_coercion_errposition(pstate, location, node)));

    rowexpr = makeNode(PGRowExpr);
    rowexpr->args = newargs;
    rowexpr->row_typeid = targetTypeId;
    rowexpr->row_format = cformat;
    rowexpr->colnames = NIL; /* not needed for named target type */
    rowexpr->location = location;
    return (PGNode *)rowexpr;
};

bool
CoerceParser::IsPreferredType(TYPCATEGORY category, PGOid type)
{
	char		typcategory;
	bool		typispreferred;

	type_provider->get_type_category_preferred(type, &typcategory, &typispreferred);
	if (category == typcategory || category == TYPCATEGORY_INVALID)
		return typispreferred;
	else
		return false;
};

PGNode * CoerceParser::coerce_to_domain(
        PGNode * arg,
        PGOid baseTypeId,
        int32 baseTypeMod,
        PGOid typeId,
        PGCoercionForm cformat,
        int location,
        bool hideInputCoercion,
        bool lengthCoercionDone)
{
	PGCoerceToDomain * result;

    /* Get the base type if it hasn't been supplied */
    if (baseTypeId == InvalidOid)
        baseTypeId = type_provider->getBaseTypeAndTypmod(typeId, &baseTypeMod);

    /* If it isn't a domain, return the node as it was passed in */
    if (baseTypeId == typeId)
        return arg;

    /* Suppress display of nested coercion steps */
    if (hideInputCoercion)
        hide_coercion_node(arg);

    /*
	 * If the domain applies a typmod to its base type, build the appropriate
	 * coercion step.  Mark it implicit for display purposes, because we don't
	 * want it shown separately by ruleutils.c; but the isExplicit flag passed
	 * to the conversion function depends on the manner in which the domain
	 * coercion is invoked, so that the semantics of implicit and explicit
	 * coercion differ.  (Is that really the behavior we want?)
	 *
	 * NOTE: because we apply this as part of the fixed expression structure,
	 * ALTER DOMAIN cannot alter the typtypmod.  But it's unclear that that
	 * would be safe to do anyway, without lots of knowledge about what the
	 * base type thinks the typmod means.
	 */
    if (!lengthCoercionDone)
    {
        if (baseTypeMod >= 0)
            arg = coerce_type_typmod(
                arg, baseTypeId, baseTypeMod, PG_COERCE_IMPLICIT_CAST, location, (cformat != PG_COERCE_IMPLICIT_CAST), false);
    }

    /*
	 * Now build the domain coercion node.  This represents run-time checking
	 * of any constraints currently attached to the domain.  This also ensures
	 * that the expression is properly labeled as to result type.
	 */
    result = makeNode(PGCoerceToDomain);
    result->arg = (PGExpr *)arg;
    result->resulttype = typeId;
    result->resulttypmod = -1; /* currently, always -1 for domains */
    /* resultcollid will be set by parse_collate.c */
    result->coercionformat = cformat;
    result->location = location;

    return (PGNode *)result;
};

bool
CoerceParser::is_complex_array(PGOid typid)
{
	PGOid elemtype = type_provider->get_element_type(typid);

	return (OidIsValid(elemtype) && type_parser->typeOrDomainTypeRelid(elemtype) != InvalidOid);
};

bool CoerceParser::typeIsOfTypedTable(PGOid reltypeId, PGOid reloftypeId)
{
    PGOid relid = type_parser->typeidTypeRelid(reltypeId);
    bool result = false;

    if (relid)
    {
		PGClassPtr tp = relation_provider->getClassByRelOid(relid);
        if (tp == NULL)
            elog(ERROR, "cache lookup failed for relation %u", relid);

        if (tp->reloftype == reloftypeId)
            result = true;
    }

    return result;
};

PGVar * CoerceParser::coerce_unknown_var(
        PGParseState * pstate,
        PGVar * var,
        PGOid targetTypeId,
        int32 targetTypeMod,
        PGCoercionContext ccontext,
        PGCoercionForm cformat,
        int levelsup)
{
    PGRangeTblEntry * rte;
    int netlevelsup = var->varlevelsup + levelsup;

    Assert(IsA(var, PGVar))

    /* 
	 * If the parser isn't set up, we can't do anything here so just return the
	 * Var as is. This can happen if we call back into the parser from the
	 * planner (calls to addRangeTableEntryForJoin()).
	 */
    if (!PointerIsValid(pstate))
        return var;

    rte = relation_parser->GetRTEByRangeTablePosn(pstate, var->varno, netlevelsup);

    switch (rte->rtekind)
    {
        /* Descend thru Join RTEs to a leaf RTE. */
        case PG_RTE_JOIN: {
            PGListCell * cell;
            PGVar * joinvar;

            Assert(var->varattno > 0 && var->varattno <= list_length(rte->joinaliasvars))

            /* Get referenced join result Var */
            cell = list_nth_cell(rte->joinaliasvars, var->varattno - 1);
            joinvar = (PGVar *)lfirst(cell);

            /* If still untyped, try to replace it with a properly typed Var */
            if (joinvar->vartype == UNKNOWNOID && targetTypeId != UNKNOWNOID)
            {
                joinvar = coerce_unknown_var(pstate, joinvar, targetTypeId, targetTypeMod, ccontext, cformat, netlevelsup);

                /* Substitute new Var into join result list. */
                lfirst(cell) = joinvar;
            }

            /* Make a new Var for the caller */
            if (joinvar->vartype != UNKNOWNOID)
            {
                var = (PGVar *)copyObject(var);
                var->vartype = joinvar->vartype;
                var->vartypmod = joinvar->vartypmod;
                var->varcollid = joinvar->varcollid;
            }
            break;
        }

        /* Impose requested type on Const or Param in subquery's targetlist. */
        case PG_RTE_SUBQUERY: {
            PGTargetEntry * ste;
            PGNode * targetexpr;
            PGOid exprtype;

            /* Get referenced subquery result expr */
            ste = relation_parser->get_tle_by_resno(rte->subquery->targetList, var->varattno);
            Assert(ste && !ste->resjunk && ste->expr)
            targetexpr = (PGNode *)ste->expr;

            /* If still untyped, try to coerce it to the requested type. */
            exprtype = exprType(targetexpr);
            if (exprtype == UNKNOWNOID && targetTypeId != UNKNOWNOID)
            {
                PGParseState * subpstate = make_parsestate(pstate);
                subpstate->p_rtable = rte->subquery->rtable;

                targetexpr = coerce_type(subpstate, targetexpr, exprtype, targetTypeId, targetTypeMod, ccontext, cformat, -1);

                free_parsestate(subpstate);
                /* Substitute coerced expr into subquery's targetlist. */
                ste->expr = (PGExpr *)targetexpr;
                exprtype = exprType(targetexpr);
            }

            /* Make a new Var for the caller */
            if (exprtype != UNKNOWNOID)
            {
                var = (PGVar *)copyObject(var);
                var->vartype = exprtype;
                var->vartypmod = exprTypmod(targetexpr);
                var->varcollid = exprCollation(targetexpr);
            }
            break;
        }

        /* Return unchanged Var if leaf RTE is not a subquery. */
        default:
            break;
    }

    return var;
};

PGNode *
CoerceParser::coerce_type(PGParseState *pstate, PGNode *node,
			PGOid inputTypeId, PGOid targetTypeId, int32 targetTypeMod,
			PGCoercionContext ccontext, PGCoercionForm cformat, int location)
{
    PGNode * result;
    PGCoercionPathType pathtype;
    PGOid funcId;

    if (targetTypeId == inputTypeId || node == NULL)
    {
        /* no conversion needed */
        return node;
    }
    if (targetTypeId == ANYOID || targetTypeId == ANYELEMENTOID || targetTypeId == ANYNONARRAYOID)
    {
        /*
		 * Assume can_coerce_type verified that implicit coercion is okay.
		 *
		 * Note: by returning the unmodified node here, we are saying that
		 * it's OK to treat an UNKNOWN constant as a valid input for a
		 * function accepting ANY, ANYELEMENT, or ANYNONARRAY.  This should be
		 * all right, since an UNKNOWN value is still a perfectly valid Datum.
		 *
		 * NB: we do NOT want a RelabelType here: the exposed type of the
		 * function argument must be its actual type, not the polymorphic
		 * pseudotype.
		 */
        return node;
    }
    if (targetTypeId == ANYARRAYOID || targetTypeId == ANYENUMOID || targetTypeId == ANYRANGEOID)
    {
        /*
		 * Assume can_coerce_type verified that implicit coercion is okay.
		 *
		 * These cases are unlike the ones above because the exposed type of
		 * the argument must be an actual array, enum, or range type.  In
		 * particular the argument must *not* be an UNKNOWN constant.  If it
		 * is, we just fall through; below, we'll call anyarray_in,
		 * anyenum_in, or anyrange_in, which will produce an error.  Also, if
		 * what we have is a domain over array, enum, or range, we have to
		 * relabel it to its base type.
		 *
		 * Note: currently, we can't actually see a domain-over-enum here,
		 * since the other functions in this file will not match such a
		 * parameter to ANYENUM.  But that should get changed eventually.
		 */

        /*
		 * GPDB: Special handling for ANYARRAY type. This enables INSERTs into
		 * catalog tables having anyarray columns.
		 *
		 * Restrict the type coercion to INSERT statements as this hack was only
		 * meant to fix INSERTs for dumping/restoring pg_statistic tuples by
		 * external utilities such as gpsd, minirepro, gpbackup/gprestore.
		 */
        if (targetTypeId == ANYARRAYOID && IsA(node, PGConst) && inputTypeId != UNKNOWNOID
            && (pstate != NULL && pstate->p_expr_kind == EXPR_KIND_INSERT_TARGET))
        {
            PGConst * con = (PGConst *)node;
            PGConst * newcon = makeNode(PGConst);
            PGOid elemoid = type_provider->get_element_type(inputTypeId);

            if (elemoid == InvalidOid)
                ereport(ERROR, (errcode(PG_ERRCODE_DATATYPE_MISMATCH), errmsg("Cannot convert non-Array type to ANYARRAY")));

            memcpy(newcon, con, sizeof(PGConst));
            newcon->consttype = ANYARRAYOID;

            return (PGNode *)newcon;
        }

        if (inputTypeId != UNKNOWNOID)
        {
            PGOid baseTypeId = type_provider->getBaseType(inputTypeId);

            if (baseTypeId != inputTypeId)
            {
                PGRelabelType * r = makeRelabelType((PGExpr *)node, baseTypeId, -1, InvalidOid, cformat);

                r->location = location;
                return (PGNode *)r;
            }
            /* Not a domain type, so return it as-is */
            return node;
        }
    }
    if (inputTypeId == UNKNOWNOID && IsA(node, PGConst))
    {
        /*
		 * Input is a string constant with previously undetermined type. Apply
		 * the target type's typinput function to it to produce a constant of
		 * the target type.
		 *
		 * NOTE: this case cannot be folded together with the other
		 * constant-input case, since the typinput function does not
		 * necessarily behave the same as a type conversion function. For
		 * example, int4's typinput function will reject "1.2", whereas
		 * float-to-int type conversion will round to integer.
		 *
		 * XXX if the typinput function is not immutable, we really ought to
		 * postpone evaluation of the function call until runtime. But there
		 * is no way to represent a typinput function call as an expression
		 * tree, because C-string values are not Datums. (XXX This *is*
		 * possible as of 7.3, do we want to do it?)
		 */
        PGConst * con = (PGConst *)node;
        PGConst * newcon = makeNode(PGConst);
        PGOid baseTypeId;
        int32 baseTypeMod;
        int32 inputTypeMod;
        PGTypePtr targetType;
        PGParseCallbackState pcbstate;

        /*
		 * If the target type is a domain, we want to call its base type's
		 * input routine, not domain_in().  This is to avoid premature failure
		 * when the domain applies a typmod: existing input routines follow
		 * implicit-coercion semantics for length checks, which is not always
		 * what we want here.  The needed check will be applied properly
		 * inside coerce_to_domain().
		 */
        baseTypeMod = targetTypeMod;
        baseTypeId = type_provider->getBaseTypeAndTypmod(targetTypeId, &baseTypeMod);

        /*
		 * For most types we pass typmod -1 to the input routine, because
		 * existing input routines follow implicit-coercion semantics for
		 * length checks, which is not always what we want here.  Any length
		 * constraint will be applied later by our caller.  An exception
		 * however is the INTERVAL type, for which we *must* pass the typmod
		 * or it won't be able to obey the bizarre SQL-spec input rules. (Ugly
		 * as sin, but so is this part of the spec...)
		 */
        if (baseTypeId == INTERVALOID)
            inputTypeMod = baseTypeMod;
        else
            inputTypeMod = -1;

        targetType = type_parser->typeidType(baseTypeId);

        newcon->consttype = baseTypeId;
        newcon->consttypmod = inputTypeMod;
        newcon->constcollid = type_parser->typeTypeCollation(targetType);
        newcon->constlen = type_parser->typeLen(targetType);
        newcon->constbyval = type_parser->typeByVal(targetType);
        newcon->constisnull = con->constisnull;

        /*
		 * We use the original literal's location regardless of the position
		 * of the coercion.  This is a change from pre-9.2 behavior, meant to
		 * simplify life for pg_stat_statements.
		 */
        newcon->location = con->location;

        /*
		 * Set up to point at the constant's text if the input routine throws
		 * an error.
		 */
        setup_parser_errposition_callback(&pcbstate, pstate, con->location);

        /*
		 * We assume here that UNKNOWN's internal representation is the same
		 * as CSTRING.
		 */
        if (!con->constisnull)
            newcon->constvalue = type_parser->stringTypeDatum(targetType, DatumGetCString(con->constvalue), inputTypeMod);
        else
            newcon->constvalue = type_parser->stringTypeDatum(targetType, NULL, inputTypeMod);

        cancel_parser_errposition_callback(&pcbstate);

        result = (PGNode *)newcon;

        /* If target is a domain, apply constraints. */
        if (baseTypeId != targetTypeId)
            result = coerce_to_domain(result, baseTypeId, baseTypeMod, targetTypeId, cformat, location, false, false);

        return result;
    }
    // if (IsA(node, PGParam) && pstate != NULL && pstate->p_coerce_param_hook != NULL)
    // {
    //     /*
	// 	 * Allow the CoerceParamHook to decide what happens.  It can return a
	// 	 * transformed node (very possibly the same Param node), or return
	// 	 * NULL to indicate we should proceed with normal coercion.
	// 	 */
    //     result = (*pstate->p_coerce_param_hook)(pstate, (PGParam *)node, targetTypeId, targetTypeMod, location);
    //     if (result)
    //         return result;
    // }
    if (IsA(node, PGCollateExpr))
    {
        /*
		 * If we have a COLLATE clause, we have to push the coercion
		 * underneath the COLLATE.  This is really ugly, but there is little
		 * choice because the above hacks on Consts and Params wouldn't happen
		 * otherwise.  This kluge has consequences in coerce_to_target_type.
		 */
        PGCollateExpr * coll = (PGCollateExpr *)node;
        PGCollateExpr * newcoll = makeNode(PGCollateExpr);

        newcoll->arg
            = (PGExpr *)coerce_type(pstate, (PGNode *)coll->arg, inputTypeId, targetTypeId, targetTypeMod, ccontext, cformat, location);
        newcoll->collOid = coll->collOid;
        newcoll->location = coll->location;
        return (PGNode *)newcoll;
    }

    /*
	 * CDB:  Var of type UNKNOWN probably comes from an untyped Const
	 * or Param in the targetlist of a range subquery.  For a successful
	 * conversion we must coerce the underlying Const or Param to a
	 * proper type and remake the Var.
	 * In postgres, it is handled only for insert statements (in transformInsertStmt)
	 * but not for select queries causing some of the queries to fail.
	 */
    if (pstate != NULL && inputTypeId == UNKNOWNOID && IsA(node, PGVar))
    {
        int32 baseTypeMod = -1;
        PGOid baseTypeId = type_provider->getBaseTypeAndTypmod(targetTypeId, &baseTypeMod);
        PGVar * fixvar = coerce_unknown_var(pstate, (PGVar *)node, baseTypeId, baseTypeMod, ccontext, cformat, 0);
        node = (PGNode *)fixvar;
        inputTypeId = fixvar->vartype;
        if (targetTypeId == inputTypeId)
            return node;
        else
        {
            /* 
			 * That didn't work, so, try and cast the unknown value to
			 * what ever the user wants. If it can't be done, they'll
			 * get an IO error later.
			 */

            PGOid outfunc = InvalidOid;
            bool outtypisvarlena = false;
            PGOid infunc = InvalidOid;
            PGOid intypioparam = InvalidOid;
            PGFuncExpr * fe;
            PGList * args = NIL;

            type_provider->getTypeOutputInfo(UNKNOWNOID, &outfunc, &outtypisvarlena);
            type_provider->getTypeInputInfo(targetTypeId, &infunc, &intypioparam);

			//TODO kindred
            //Insist(OidIsValid(outfunc));
            //Insist(OidIsValid(infunc));
			Assert(OidIsValid(outfunc))
			Assert(OidIsValid(infunc))

            /*
			 * do unknownout(Var)
			 *
			 * always supplying COERCE_IMPLICIT_CAST here, set it as an
			 * implicit cast to hide this Greenplum hack, because the explicit
			 * cast would be dumped but not able to be loaded, for like a cast
			 * unknown::cstring::date
			 */
            fe = makeFuncExpr(outfunc, CSTRINGOID, list_make1(node), InvalidOid, InvalidOid, PG_COERCE_IMPLICIT_CAST);
            fe->location = location;

            if (location >= 0 && (fixvar->location < 0 || location < fixvar->location))
                fixvar->location = location;

            /* 
			 * Now pass the above as an argument to the input function of the
			 * type we're casting to
			 */
            args = list_make3(
                fe,
                makeConst(VOIDOID, -1, InvalidOid, sizeof(PGOid), ObjectIdGetDatum(intypioparam), false, true),
                makeConst(INT4OID, -1, InvalidOid, sizeof(int32), Int32GetDatum(-1), false, true));
            fe = makeFuncExpr(infunc, targetTypeId, args, InvalidOid, InvalidOid, cformat);
            fe->location = location;

            return (PGNode *)fe;
        }
    }

    pathtype = find_coercion_pathway(targetTypeId, inputTypeId, ccontext, &funcId);
    if (pathtype != PG_COERCION_PATH_NONE)
    {
        if (pathtype != PG_COERCION_PATH_RELABELTYPE)
        {
            /*
			 * Generate an expression tree representing run-time application
			 * of the conversion function.  If we are dealing with a domain
			 * target type, the conversion function will yield the base type,
			 * and we need to extract the correct typmod to use from the
			 * domain's typtypmod.
			 */
            PGOid baseTypeId;
            int32 baseTypeMod;

            baseTypeMod = targetTypeMod;
            baseTypeId = type_provider->getBaseTypeAndTypmod(targetTypeId, &baseTypeMod);

            result = build_coercion_expression(
                node, pathtype, funcId, baseTypeId, baseTypeMod, cformat, location, (cformat != PG_COERCE_IMPLICIT_CAST));

            /*
			 * If domain, coerce to the domain type and relabel with domain
			 * type ID.  We can skip the internal length-coercion step if the
			 * selected coercion function was a type-and-length coercion.
			 */
            if (targetTypeId != baseTypeId)
                result = coerce_to_domain(
                    result, baseTypeId, baseTypeMod, targetTypeId, cformat, location, true, exprIsLengthCoercion(result, NULL));
        }
        else
        {
            /*
			 * We don't need to do a physical conversion, but we do need to
			 * attach a RelabelType node so that the expression will be seen
			 * to have the intended type when inspected by higher-level code.
			 *
			 * Also, domains may have value restrictions beyond the base type
			 * that must be accounted for.  If the destination is a domain
			 * then we won't need a RelabelType node.
			 */
            result = coerce_to_domain(node, InvalidOid, -1, targetTypeId, cformat, location, false, false);
            if (result == node)
            {
                /*
				 * XXX could we label result with exprTypmod(node) instead of
				 * default -1 typmod, to save a possible length-coercion
				 * later? Would work if both types have same interpretation of
				 * typmod, which is likely but not certain.
				 */
                PGRelabelType * r = makeRelabelType((PGExpr *)result, targetTypeId, -1, InvalidOid, cformat);

                r->location = location;
                result = (PGNode *)r;
            }
        }
        return result;
    }
    if (inputTypeId == RECORDOID && type_parser->typeidTypeRelid(targetTypeId) != InvalidOid)
    {
        /* Coerce a RECORD to a specific complex type */
        return coerce_record_to_complex(pstate, node, targetTypeId, ccontext, cformat, location);
    }
    if (targetTypeId == RECORDOID && type_parser->typeidTypeRelid(inputTypeId) != InvalidOid)
    {
        /* Coerce a specific complex type to RECORD */
        /* NB: we do NOT want a RelabelType here */
        return node;
    }
#ifdef NOT_USED
    if (inputTypeId == RECORDARRAYOID && is_complex_array(targetTypeId))
    {
        /* Coerce record[] to a specific complex array type */
        /* not implemented yet ... */
    }
#endif
    if (targetTypeId == RECORDARRAYOID && is_complex_array(inputTypeId))
    {
        /* Coerce a specific complex array type to record[] */
        /* NB: we do NOT want a RelabelType here */
        return node;
    }
    if (type_provider->typeInheritsFrom(inputTypeId, targetTypeId) || typeIsOfTypedTable(inputTypeId, targetTypeId))
    {
        /*
		 * Input class type is a subclass of target, so generate an
		 * appropriate runtime conversion (removing unneeded columns and
		 * possibly rearranging the ones that are wanted).
		 */
        PGConvertRowtypeExpr * r = makeNode(PGConvertRowtypeExpr);

        r->arg = (PGExpr *)node;
        r->resulttype = targetTypeId;
        r->convertformat = cformat;
        r->location = location;
        return (PGNode *)r;
    }
    /* If we get here, caller blew it */
    elog(ERROR, "failed to find conversion function from %s to %s", type_provider->format_type_be(inputTypeId).c_str(), type_provider->format_type_be(targetTypeId).c_str());
    return NULL; /* keep compiler quiet */
};

TYPCATEGORY
CoerceParser::TypeCategory(PGOid type)
{
	char		typcategory;
	bool		typispreferred;

	type_provider->get_type_category_preferred(type, &typcategory, &typispreferred);
	Assert(typcategory != TYPCATEGORY_INVALID)
	return (TYPCATEGORY) typcategory;
};

PGCoercionPathType
CoerceParser::find_coercion_pathway(PGOid targetTypeId, PGOid sourceTypeId,
					  PGCoercionContext ccontext,
					  PGOid *funcid)
{
	PGCoercionPathType result = PG_COERCION_PATH_NONE;

	*funcid = InvalidOid;

	/* Perhaps the types are domains; if so, look at their base types */
	if (OidIsValid(sourceTypeId))
		sourceTypeId = type_provider->getBaseType(sourceTypeId);
	if (OidIsValid(targetTypeId))
		targetTypeId = type_provider->getBaseType(targetTypeId);

	/* Domains are always coercible to and from their base type */
	if (sourceTypeId == targetTypeId)
		return PG_COERCION_PATH_RELABELTYPE;

	/* Look in pg_cast */
	PGCastPtr tuple = cast_provider->getCastBySourceTypeAndTargetTypeOid(sourceTypeId, targetTypeId);

	if (tuple != NULL)
	{
		PGCoercionContext castcontext;

		/* convert char value for castcontext to CoercionContext enum */
		switch (tuple->castcontext)
		{
			case PG_COERCION_CODE_IMPLICIT:
				castcontext = PG_COERCION_IMPLICIT;
				break;
			case PG_COERCION_CODE_ASSIGNMENT:
				castcontext = PG_COERCION_ASSIGNMENT;
				break;
			case PG_COERCION_CODE_EXPLICIT:
				castcontext = PG_COERCION_EXPLICIT;
				break;
			default:
				elog(ERROR, "unrecognized castcontext: %d",
					 (int) tuple->castcontext);
				castcontext = PG_COERCION_IMPLICIT;	/* keep compiler quiet */
				break;
		}

		/* Rely on ordering of enum for correct behavior here */
		if (ccontext >= castcontext)
		{
			switch (tuple->castmethod)
			{
				case PG_COERCION_METHOD_FUNCTION:
					result = PG_COERCION_PATH_FUNC;
					*funcid = tuple->castfunc;
					break;
				case PG_COERCION_METHOD_INOUT:
					result = PG_COERCION_PATH_COERCEVIAIO;
					break;
				case PG_COERCION_METHOD_BINARY:
					result = PG_COERCION_PATH_RELABELTYPE;
					break;
				default:
					elog(ERROR, "unrecognized castmethod: %d",
						 (int) tuple->castmethod);
					break;
			}
		}

	}
	else
	{
		/*
		 * If there's no pg_cast entry, perhaps we are dealing with a pair of
		 * array types.  If so, and if their element types have a conversion
		 * pathway, report that we can coerce with an ArrayCoerceExpr.
		 *
		 * Hack: disallow coercions to oidvector and int2vector, which
		 * otherwise tend to capture coercions that should go to "real" array
		 * types.  We want those types to be considered "real" arrays for many
		 * purposes, but not this one.  (Also, ArrayCoerceExpr isn't
		 * guaranteed to produce an output that meets the restrictions of
		 * these datatypes, such as being 1-dimensional.)
		 */
		if (targetTypeId != OIDVECTOROID && targetTypeId != INT2VECTOROID)
		{
			PGOid			targetElem;
			PGOid			sourceElem;

			if ((targetElem = type_provider->get_element_type(targetTypeId)) != InvalidOid &&
				(sourceElem = type_provider->get_element_type(sourceTypeId)) != InvalidOid)
			{
				PGCoercionPathType elempathtype;
				PGOid			elemfuncid;

				elempathtype = find_coercion_pathway(targetElem,
													 sourceElem,
													 ccontext,
													 &elemfuncid);
				if (elempathtype != PG_COERCION_PATH_NONE)
				{
					result = PG_COERCION_PATH_ARRAYCOERCE;
				}
			}
		}

		/*
		 * If we still haven't found a possibility, consider automatic casting
		 * using I/O functions.  We allow assignment casts to string types and
		 * explicit casts from string types to be handled this way. (The
		 * CoerceViaIO mechanism is a lot more general than that, but this is
		 * all we want to allow in the absence of a pg_cast entry.) It would
		 * probably be better to insist on explicit casts in both directions,
		 * but this is a compromise to preserve something of the pre-8.3
		 * behavior that many types had implicit (yipes!) casts to text.
		 */
		if (result == PG_COERCION_PATH_NONE)
		{
			if (ccontext >= PG_COERCION_ASSIGNMENT &&
				TypeCategory(targetTypeId) == TYPCATEGORY_STRING)
				result = PG_COERCION_PATH_COERCEVIAIO;
			else if (ccontext >= PG_COERCION_EXPLICIT &&
					 TypeCategory(sourceTypeId) == TYPCATEGORY_STRING)
				result = PG_COERCION_PATH_COERCEVIAIO;
		}
	}

	return result;
};

bool
CoerceParser::check_generic_type_consistency(const PGOid *actual_arg_types,
							   const PGOid *declared_arg_types,
							   int nargs)
{
	int			j;
	PGOid			elem_typeid = InvalidOid;
	PGOid			array_typeid = InvalidOid;
	PGOid			array_typelem;
	PGOid			range_typeid = InvalidOid;
	PGOid			range_typelem;
	bool		have_anyelement = false;
	bool		have_anynonarray = false;
	bool		have_anyenum = false;

	/*
	 * Loop through the arguments to see if we have any that are polymorphic.
	 * If so, require the actual types to be consistent.
	 */
	for (j = 0; j < nargs; j++)
	{
		PGOid			decl_type = declared_arg_types[j];
		PGOid			actual_type = actual_arg_types[j];

		if (decl_type == ANYELEMENTOID ||
			decl_type == ANYNONARRAYOID ||
			decl_type == ANYENUMOID)
		{
			have_anyelement = true;
			if (decl_type == ANYNONARRAYOID)
				have_anynonarray = true;
			else if (decl_type == ANYENUMOID)
				have_anyenum = true;
			if (actual_type == UNKNOWNOID)
				continue;
			if (OidIsValid(elem_typeid) && actual_type != elem_typeid)
				return false;
			elem_typeid = actual_type;
		}
		else if (decl_type == ANYARRAYOID)
		{
			if (actual_type == UNKNOWNOID)
				continue;
			actual_type = type_provider->getBaseType(actual_type); /* flatten domains */
			if (OidIsValid(array_typeid) && actual_type != array_typeid)
				return false;
			array_typeid = actual_type;
		}
		else if (decl_type == ANYRANGEOID)
		{
			if (actual_type == UNKNOWNOID)
				continue;
			actual_type = type_provider->getBaseType(actual_type); /* flatten domains */
			if (OidIsValid(range_typeid) && actual_type != range_typeid)
				return false;
			range_typeid = actual_type;
		}
	}

	/* Get the element type based on the array type, if we have one */
	if (OidIsValid(array_typeid))
	{
		if (array_typeid == ANYARRAYOID)
		{
			/* Special case for ANYARRAY input: okay iff no ANYELEMENT */
			if (have_anyelement)
				return false;
			return true;
		}

		array_typelem = type_provider->get_element_type(array_typeid);
		if (!OidIsValid(array_typelem))
			return false;		/* should be an array, but isn't */

		if (!OidIsValid(elem_typeid))
		{
			/*
			 * if we don't have an element type yet, use the one we just got
			 */
			elem_typeid = array_typelem;
		}
		else if (array_typelem != elem_typeid)
		{
			/* otherwise, they better match */
			return false;
		}
	}

	/* Get the element type based on the range type, if we have one */
	if (OidIsValid(range_typeid))
	{
		range_typelem = type_provider->get_range_subtype(range_typeid);
		if (!OidIsValid(range_typelem))
			return false;		/* should be a range, but isn't */

		if (!OidIsValid(elem_typeid))
		{
			/*
			 * if we don't have an element type yet, use the one we just got
			 */
			elem_typeid = range_typelem;
		}
		else if (range_typelem != elem_typeid)
		{
			/* otherwise, they better match */
			return false;
		}
	}

	if (have_anynonarray)
	{
		/* require the element type to not be an array or domain over array */
		if (type_provider->get_base_element_type(elem_typeid) != InvalidOid)
			return false;
	}

	if (have_anyenum)
	{
		/* require the element type to be an enum */
		if (!type_provider->type_is_enum(elem_typeid))
			return false;
	}

	/* Looks valid */
	return true;
};

bool
CoerceParser::can_coerce_type(int nargs, const PGOid *input_typeids, const PGOid *target_typeids,
				PGCoercionContext ccontext)
{
	bool		have_generics = false;
	int			i;

	/* run through argument list... */
	for (i = 0; i < nargs; i++)
	{
		PGOid			inputTypeId = input_typeids[i];
		PGOid			targetTypeId = target_typeids[i];
		PGCoercionPathType pathtype;
		PGOid			funcId;

		/* no problem if same type */
		if (inputTypeId == targetTypeId)
			continue;

		/* 
		 * ANYTABLE is a special case that can occur when a function is 
		 * called with a TableValue expression.  A table value expression
		 * can only match a parameter to a function defined as a "anytable".
		 *
		 * Only allow ANYTABLE to match another ANYTABLE, anything else would
		 * be a mismatch of Table domain and Value domain expressions.  
		 *
		 * Validation of ANYTABLE coercion is processed at a higher level
		 * that has more context related to the tupleDesc for the tables
		 * involved.
		 */
		if (targetTypeId == ANYTABLEOID || inputTypeId == ANYTABLEOID)
			return false;

		/* accept if target is ANY */
		if (targetTypeId == ANYOID)
			continue;

		/* accept if target is polymorphic, for now */
		if (IsPolymorphicType(targetTypeId))
		{
			have_generics = true;	/* do more checking later */
			continue;
		}

		/*
		 * If input is an untyped string constant, assume we can convert it to
		 * anything.
		 */
		if (inputTypeId == UNKNOWNOID)
			continue;

		/*
		 * If pg_cast shows that we can coerce, accept.  This test now covers
		 * both binary-compatible and coercion-function cases.
		 */
		pathtype = find_coercion_pathway(targetTypeId, inputTypeId, ccontext,
										 &funcId);
		if (pathtype != PG_COERCION_PATH_NONE)
			continue;

		/*
		 * If input is RECORD and target is a composite type, assume we can
		 * coerce (may need tighter checking here)
		 */
		if (inputTypeId == RECORDOID &&
			type_parser->typeOrDomainTypeRelid(targetTypeId) != InvalidOid)
			continue;

		/*
		 * If input is a composite type and target is RECORD, accept
		 */
		if (targetTypeId == RECORDOID &&
			type_parser->typeOrDomainTypeRelid(inputTypeId) != InvalidOid)
			continue;

#ifdef NOT_USED					/* not implemented yet */

		/*
		 * If input is record[] and target is a composite array type, assume
		 * we can coerce (may need tighter checking here)
		 */
		if (inputTypeId == RECORDARRAYOID &&
			is_complex_array(targetTypeId))
			continue;
#endif

		/*
		 * If input is a composite array type and target is record[], accept
		 */
		if (targetTypeId == RECORDARRAYOID &&
			is_complex_array(inputTypeId))
			continue;

		/*
		 * If input is a class type that inherits from target, accept
		 */
		if (type_provider->typeInheritsFrom(inputTypeId, targetTypeId)
			|| typeIsOfTypedTable(inputTypeId, targetTypeId))
			continue;

		/*
		 * Else, cannot coerce at this argument position
		 */
		return false;
	}

	/* If we found any generic argument types, cross-check them */
	if (have_generics)
	{
		if (!check_generic_type_consistency(input_typeids, target_typeids,
											nargs))
			return false;
	}

	return true;
};

PGCoercionPathType
CoerceParser::find_typmod_coercion_function(PGOid typeId,
							  PGOid *funcid)
{
	PGCoercionPathType result;
	//Type		targetType;
	//Form_pg_type typeForm;
	//HeapTuple	tuple;

	*funcid = InvalidOid;
	result = PG_COERCION_PATH_FUNC;

	PGTypePtr targetType = type_parser->typeidType(typeId);
	//typeForm = (Form_pg_type) GETSTRUCT(targetType);

	/* Check for a varlena array type */
	if (targetType->typelem != InvalidOid && targetType->typlen == -1)
	{
		/* Yes, switch our attention to the element type */
		typeId = targetType->typelem;
		result = PG_COERCION_PATH_ARRAYCOERCE;
	}
	//ReleaseSysCache(targetType);

	/* Look in pg_cast */
	PGCastPtr tuple = cast_provider->getCastBySourceTypeAndTargetTypeOid(typeId, typeId);

	if (tuple != NULL)
	{
		*funcid = tuple->castfunc;
	}

	if (!OidIsValid(*funcid))
		result = PG_COERCION_PATH_NONE;

	return result;
};

void
CoerceParser::hide_coercion_node(PGNode *node)
{
	if (IsA(node, PGFuncExpr))
		((PGFuncExpr *) node)->funcformat = PG_COERCE_IMPLICIT_CAST;
	else if (IsA(node, PGRelabelType))
		((PGRelabelType *) node)->relabelformat = PG_COERCE_IMPLICIT_CAST;
	else if (IsA(node, PGCoerceViaIO))
		((PGCoerceViaIO *) node)->coerceformat = PG_COERCE_IMPLICIT_CAST;
	else if (IsA(node, PGArrayCoerceExpr))
		((PGArrayCoerceExpr *) node)->coerceformat = PG_COERCE_IMPLICIT_CAST;
	else if (IsA(node, PGConvertRowtypeExpr))
		((PGConvertRowtypeExpr *) node)->convertformat = PG_COERCE_IMPLICIT_CAST;
	else if (IsA(node, PGRowExpr))
		((PGRowExpr *) node)->row_format = PG_COERCE_IMPLICIT_CAST;
	else if (IsA(node, PGCoerceToDomain))
		((PGCoerceToDomain *) node)->coercionformat = PG_COERCE_IMPLICIT_CAST;
	else
		elog(ERROR, "unsupported node type: %d", (int) nodeTag(node));
};

PGNode * CoerceParser::coerce_type_typmod(
        PGNode * node, PGOid targetTypeId, int32 targetTypMod,
		PGCoercionForm cformat, int location, bool isExplicit, bool hideInputCoercion)
{
	PGCoercionPathType pathtype;
	PGOid			funcId;

	/*
	 * A negative typmod is assumed to mean that no coercion is wanted. Also,
	 * skip coercion if already done.
	 */
	if (targetTypMod < 0 || targetTypMod == exprTypmod(node))
		return node;

	pathtype = find_typmod_coercion_function(targetTypeId, &funcId);

	if (pathtype != PG_COERCION_PATH_NONE)
	{
		/* Suppress display of nested coercion steps */
		if (hideInputCoercion)
			hide_coercion_node(node);

		node = build_coercion_expression(node, pathtype, funcId,
										 targetTypeId, targetTypMod,
										 cformat, location,
										 isExplicit);
	}

	return node;
};

PGNode *
CoerceParser::coerce_to_target_type(PGParseState *pstate, PGNode *expr, PGOid exprtype,
					  PGOid targettype, int32 targettypmod,
					  PGCoercionContext ccontext,
					  PGCoercionForm cformat,
					  int location)
{
    PGNode * result;
    PGNode * origexpr;

    if (!can_coerce_type(1, &exprtype, &targettype, ccontext))
        return NULL;

    /*
	 * If the input has a CollateExpr at the top, strip it off, perform the
	 * coercion, and put a new one back on.  This is annoying since it
	 * duplicates logic in coerce_type, but if we don't do this then it's too
	 * hard to tell whether coerce_type actually changed anything, and we
	 * *must* know that to avoid possibly calling hide_coercion_node on
	 * something that wasn't generated by coerce_type.  Note that if there are
	 * multiple stacked CollateExprs, we just discard all but the topmost.
	 */
    origexpr = expr;
    while (expr && IsA(expr, PGCollateExpr))
        expr = (PGNode *)((PGCollateExpr *)expr)->arg;

    result = coerce_type(pstate, expr, exprtype, targettype, targettypmod, ccontext, cformat, location);

    /*
	 * If the target is a fixed-length type, it may need a length coercion as
	 * well as a type coercion.  If we find ourselves adding both, force the
	 * inner coercion node to implicit display form.
	 */
    result = coerce_type_typmod(
        result,
        targettype,
        targettypmod,
        cformat,
        location,
        (cformat != PG_COERCE_IMPLICIT_CAST),
        (result != expr && !IsA(result, PGConst) && !IsA(result, PGVar)));

    if (expr != origexpr)
    {
        /* Reinstall top CollateExpr */
        PGCollateExpr * coll = (PGCollateExpr *)origexpr;
        PGCollateExpr * newcoll = makeNode(PGCollateExpr);

        newcoll->arg = (PGExpr *)result;
        newcoll->collOid = coll->collOid;
        newcoll->location = coll->location;
        result = (PGNode *)newcoll;
    }

    return result;
};

PGNode *
CoerceParser::coerce_to_boolean(PGParseState *pstate, PGNode *node,
				  const char *constructName)
{
	PGOid			inputTypeId = exprType(node);

	if (inputTypeId != BOOLOID)
	{
		PGNode	   *newnode;

		newnode = coerce_to_target_type(pstate, node, inputTypeId,
										BOOLOID, -1,
										PG_COERCION_ASSIGNMENT,
										PG_COERCE_IMPLICIT_CAST,
										-1);
		if (newnode == NULL)
		{
			parser_errposition(pstate, exprLocation(node));
			ereport(ERROR,
					(errcode(PG_ERRCODE_DATATYPE_MISMATCH),
			/* translator: first %s is name of a SQL construct, eg WHERE */
					 errmsg("argument of %s must be type %s, not type %s",
							constructName, "boolean",
							type_provider->format_type_be(inputTypeId).c_str())));
		}
		node = newnode;
	}

	if (expression_returns_set(node))
	{
		parser_errposition(pstate, exprLocation(node));
		ereport(ERROR,
				(errcode(PG_ERRCODE_DATATYPE_MISMATCH),
		/* translator: %s is name of a SQL construct, eg WHERE */
				 errmsg("argument of %s must not return a set",
						constructName)));
	}

	return node;
};

PGNode * CoerceParser::build_coercion_expression(
        PGNode * node,
        PGCoercionPathType pathtype,
        PGOid funcId,
        PGOid targetTypeId,
        int32 targetTypMod,
        PGCoercionForm cformat,
        int location,
        bool isExplicit)
{
    int nargs = 0;

    if (OidIsValid(funcId))
    {
        PGProcPtr tp = proc_provider->getProcByOid(funcId);
        if (tp == NULL)
            elog(ERROR, "cache lookup failed for function %u", funcId);

        /*
		 * These Asserts essentially check that function is a legal coercion
		 * function.  We can't make the seemingly obvious tests on prorettype
		 * and proargtypes[0], even in the COERCION_PATH_FUNC case, because of
		 * various binary-compatibility cases.
		 */
        /* Assert(targetTypeId == procstruct->prorettype); */
        Assert(!tp->proretset)
        Assert(!tp->proisagg)
        Assert(!tp->proiswindow)
        nargs = tp->pronargs;
        Assert(nargs >= 1 && nargs <= 3)
        /* Assert(procstruct->proargtypes.values[0] == exprType(node)); */
        Assert(nargs < 2 || tp->proargtypes[1] == INT4OID)
        Assert(nargs < 3 || tp->proargtypes[2] == BOOLOID)
    }

    if (pathtype == PG_COERCION_PATH_FUNC)
    {
        /* We build an ordinary FuncExpr with special arguments */
        PGFuncExpr * fexpr;
        PGList * args;
        PGConst * cons;

        Assert(OidIsValid(funcId))

        args = list_make1(node);

        if (nargs >= 2)
        {
            /* Pass target typmod as an int4 constant */
            cons = makeConst(INT4OID, -1, InvalidOid, sizeof(int32), Int32GetDatum(targetTypMod), false, true);

            args = lappend(args, cons);
        }

        if (nargs == 3)
        {
            /* Pass it a boolean isExplicit parameter, too */
            cons = makeConst(BOOLOID, -1, InvalidOid, sizeof(bool), BoolGetDatum(isExplicit), false, true);

            args = lappend(args, cons);
        }

        fexpr = makeFuncExpr(funcId, targetTypeId, args, InvalidOid, InvalidOid, cformat);
        fexpr->location = location;
        return (PGNode *)fexpr;
    }
    else if (pathtype == PG_COERCION_PATH_ARRAYCOERCE)
    {
        /* We need to build an ArrayCoerceExpr */
        PGArrayCoerceExpr * acoerce = makeNode(PGArrayCoerceExpr);

        acoerce->arg = (PGExpr *)node;
        acoerce->elemfuncid = funcId;
        acoerce->resulttype = targetTypeId;

        /*
		 * Label the output as having a particular typmod only if we are
		 * really invoking a length-coercion function, ie one with more than
		 * one argument.
		 */
        acoerce->resulttypmod = (nargs >= 2) ? targetTypMod : -1;
        /* resultcollid will be set by parse_collate.c */
        acoerce->isExplicit = isExplicit;
        acoerce->coerceformat = cformat;
        acoerce->location = location;

        return (PGNode *)acoerce;
    }
    else if (pathtype == PG_COERCION_PATH_COERCEVIAIO)
    {
        /* We need to build a CoerceViaIO node */
        PGCoerceViaIO * iocoerce = makeNode(PGCoerceViaIO);

        Assert(!OidIsValid(funcId))

        iocoerce->arg = (PGExpr *)node;
        iocoerce->resulttype = targetTypeId;
        /* resultcollid will be set by parse_collate.c */
        iocoerce->coerceformat = cformat;
        iocoerce->location = location;

        return (PGNode *)iocoerce;
    }
    else
    {
        elog(ERROR, "unsupported pathtype %d in build_coercion_expression", (int)pathtype);
        return NULL; /* keep compiler quiet */
    }
};

PGNode *
CoerceParser::coerce_to_common_type(PGParseState *pstate, PGNode *node,
					  PGOid targetTypeId, const char* context_str)
{
	PGOid			inputTypeId = exprType(node);

	if (inputTypeId == targetTypeId)
		return node;			/* no work */
	if (can_coerce_type(1, &inputTypeId, &targetTypeId, PG_COERCION_IMPLICIT))
		node = coerce_type(pstate, node, inputTypeId, targetTypeId, -1,
						   PG_COERCION_IMPLICIT, PG_COERCE_IMPLICIT_CAST, -1);
	else
	{
		parser_errposition(pstate, exprLocation(node));
		ereport(ERROR,
				(errcode(PG_ERRCODE_CANNOT_COERCE),
		/* translator: first %s is name of a SQL construct, eg CASE */
				 errmsg("%s could not convert type %s to %s",
						context_str,
						type_provider->format_type_be(inputTypeId).c_str(),
						type_provider->format_type_be(targetTypeId).c_str())));
	}
	return node;
};

int
CoerceParser::parser_coercion_errposition(PGParseState *pstate,
							int coerce_location,
							PGNode *input_expr)
{
	if (coerce_location >= 0)
		return parser_errposition(pstate, coerce_location);
	else
		return parser_errposition(pstate, exprLocation(input_expr));
};

PGOid
CoerceParser::enforce_generic_type_consistency(const PGOid *actual_arg_types,
								 PGOid *declared_arg_types,
								 int nargs,
								 PGOid rettype,
								 bool allow_poly)
{
	int			j;
	bool		have_generics = false;
	bool		have_unknowns = false;
	PGOid			elem_typeid = InvalidOid;
	PGOid			array_typeid = InvalidOid;
	PGOid			range_typeid = InvalidOid;
	PGOid			array_typelem;
	PGOid			range_typelem;
	bool		have_anyelement = (rettype == ANYELEMENTOID ||
								   rettype == ANYNONARRAYOID ||
								   rettype == ANYENUMOID);
	bool		have_anynonarray = (rettype == ANYNONARRAYOID);
	bool		have_anyenum = (rettype == ANYENUMOID);

	/*
	 * Loop through the arguments to see if we have any that are polymorphic.
	 * If so, require the actual types to be consistent.
	 */
	for (j = 0; j < nargs; j++)
	{
		PGOid			decl_type = declared_arg_types[j];
		PGOid			actual_type = actual_arg_types[j];

		if (decl_type == ANYELEMENTOID ||
			decl_type == ANYNONARRAYOID ||
			decl_type == ANYENUMOID)
		{
			have_generics = have_anyelement = true;
			if (decl_type == ANYNONARRAYOID)
				have_anynonarray = true;
			else if (decl_type == ANYENUMOID)
				have_anyenum = true;
			if (actual_type == UNKNOWNOID)
			{
				have_unknowns = true;
				continue;
			}
			if (allow_poly && decl_type == actual_type)
				continue;		/* no new information here */
			if (OidIsValid(elem_typeid) && actual_type != elem_typeid)
				ereport(ERROR,
						(errcode(PG_ERRCODE_DATATYPE_MISMATCH),
						 errmsg("arguments declared \"anyelement\" are not all alike"),
						 errdetail("%s versus %s",
								   type_provider->format_type_be(elem_typeid).c_str(),
								   type_provider->format_type_be(actual_type).c_str())));
			elem_typeid = actual_type;
		}
		else if (decl_type == ANYARRAYOID)
		{
			have_generics = true;
			if (actual_type == UNKNOWNOID)
			{
				have_unknowns = true;
				continue;
			}
			if (allow_poly && decl_type == actual_type)
				continue;		/* no new information here */
			actual_type = type_provider->getBaseType(actual_type); /* flatten domains */
			if (OidIsValid(array_typeid) && actual_type != array_typeid)
				ereport(ERROR,
						(errcode(PG_ERRCODE_DATATYPE_MISMATCH),
						 errmsg("arguments declared \"anyarray\" are not all alike"),
						 errdetail("%s versus %s",
								   type_provider->format_type_be(array_typeid).c_str(),
								   type_provider->format_type_be(actual_type).c_str())));
			array_typeid = actual_type;
		}
		else if (decl_type == ANYRANGEOID)
		{
			have_generics = true;
			if (actual_type == UNKNOWNOID)
			{
				have_unknowns = true;
				continue;
			}
			if (allow_poly && decl_type == actual_type)
				continue;		/* no new information here */
			actual_type = type_provider->getBaseType(actual_type); /* flatten domains */
			if (OidIsValid(range_typeid) && actual_type != range_typeid)
				ereport(ERROR,
						(errcode(PG_ERRCODE_DATATYPE_MISMATCH),
						 errmsg("arguments declared \"anyrange\" are not all alike"),
						 errdetail("%s versus %s",
								   type_provider->format_type_be(range_typeid).c_str(),
								   type_provider->format_type_be(actual_type).c_str())));
			range_typeid = actual_type;
		}
	}

	/*
	 * Fast Track: if none of the arguments are polymorphic, return the
	 * unmodified rettype.  We assume it can't be polymorphic either.
	 */
	if (!have_generics)
		return rettype;

	/* Get the element type based on the array type, if we have one */
	if (OidIsValid(array_typeid))
	{
		if (array_typeid == ANYARRAYOID && !have_anyelement)
		{
			/* Special case for ANYARRAY input: okay iff no ANYELEMENT */
			array_typelem = ANYELEMENTOID;
		}
		else
		{
			array_typelem = type_provider->get_element_type(array_typeid);
			if (!OidIsValid(array_typelem))
				ereport(ERROR,
						(errcode(PG_ERRCODE_DATATYPE_MISMATCH),
						 errmsg("argument declared %s is not an array but type %s",
								"anyarray", type_provider->format_type_be(array_typeid).c_str())));
		}

		if (!OidIsValid(elem_typeid))
		{
			/*
			 * if we don't have an element type yet, use the one we just got
			 */
			elem_typeid = array_typelem;
		}
		else if (array_typelem != elem_typeid)
		{
			/* otherwise, they better match */
			ereport(ERROR,
					(errcode(PG_ERRCODE_DATATYPE_MISMATCH),
					 errmsg("argument declared %s is not consistent with argument declared %s",
							"anyarray", "anyelement"),
					 errdetail("%s versus %s",
							   type_provider->format_type_be(array_typeid).c_str(),
							   type_provider->format_type_be(elem_typeid).c_str())));
		}
	}

	/* Get the element type based on the range type, if we have one */
	if (OidIsValid(range_typeid))
	{
		if (range_typeid == ANYRANGEOID && !have_anyelement)
		{
			/* Special case for ANYRANGE input: okay iff no ANYELEMENT */
			range_typelem = ANYELEMENTOID;
		}
		else
		{
			range_typelem = type_provider->get_range_subtype(range_typeid);
			if (!OidIsValid(range_typelem))
				ereport(ERROR,
						(errcode(PG_ERRCODE_DATATYPE_MISMATCH),
						 errmsg("argument declared %s is not a range type but type %s",
								"anyrange",
								type_provider->format_type_be(range_typeid).c_str())));
		}

		if (!OidIsValid(elem_typeid))
		{
			/*
			 * if we don't have an element type yet, use the one we just got
			 */
			elem_typeid = range_typelem;
		}
		else if (range_typelem != elem_typeid)
		{
			/* otherwise, they better match */
			ereport(ERROR,
					(errcode(PG_ERRCODE_DATATYPE_MISMATCH),
					 errmsg("argument declared %s is not consistent with argument declared %s",
							"anyrange", "anyelement"),
					 errdetail("%s versus %s",
							   type_provider->format_type_be(range_typeid).c_str(),
							   type_provider->format_type_be(elem_typeid).c_str())));
		}
	}

	if (!OidIsValid(elem_typeid))
	{
		if (allow_poly)
		{
			elem_typeid = ANYELEMENTOID;
			array_typeid = ANYARRAYOID;
			range_typeid = ANYRANGEOID;
		}
		else
		{
			/* Only way to get here is if all the generic args are UNKNOWN */
			ereport(ERROR,
					(errcode(PG_ERRCODE_DATATYPE_MISMATCH),
					 errmsg("could not determine polymorphic type because input has type %s",
							"unknown")));
		}
	}

	if (have_anynonarray && elem_typeid != ANYELEMENTOID)
	{
		/* require the element type to not be an array or domain over array */
		if (type_provider->get_base_element_type(elem_typeid) != InvalidOid)
			ereport(ERROR,
					(errcode(PG_ERRCODE_DATATYPE_MISMATCH),
					 errmsg("type matched to anynonarray is an array type: %s",
							type_provider->format_type_be(elem_typeid).c_str())));
	}

	if (have_anyenum && elem_typeid != ANYELEMENTOID)
	{
		/* require the element type to be an enum */
		if (!type_provider->type_is_enum(elem_typeid))
			ereport(ERROR,
					(errcode(PG_ERRCODE_DATATYPE_MISMATCH),
					 errmsg("type matched to anyenum is not an enum type: %s",
							type_provider->format_type_be(elem_typeid).c_str())));
	}

	/*
	 * If we had any unknown inputs, re-scan to assign correct types
	 */
	if (have_unknowns)
	{
		for (j = 0; j < nargs; j++)
		{
			PGOid			decl_type = declared_arg_types[j];
			PGOid			actual_type = actual_arg_types[j];

			if (actual_type != UNKNOWNOID)
				continue;

			if (decl_type == ANYELEMENTOID ||
				decl_type == ANYNONARRAYOID ||
				decl_type == ANYENUMOID)
				declared_arg_types[j] = elem_typeid;
			else if (decl_type == ANYARRAYOID)
			{
				if (!OidIsValid(array_typeid))
				{
					array_typeid = type_provider->get_array_type(elem_typeid);
					if (!OidIsValid(array_typeid))
						ereport(ERROR,
								(errcode(PG_ERRCODE_UNDEFINED_OBJECT),
								 errmsg("could not find array type for data type %s",
										type_provider->format_type_be(elem_typeid).c_str())));
				}
				declared_arg_types[j] = array_typeid;
			}
			else if (decl_type == ANYRANGEOID)
			{
				if (!OidIsValid(range_typeid))
				{
					ereport(ERROR,
							(errcode(PG_ERRCODE_UNDEFINED_OBJECT),
							 errmsg("could not find range type for data type %s",
									type_provider->format_type_be(elem_typeid).c_str())));
				}
				declared_arg_types[j] = range_typeid;
			}
		}
	}

	/* if we return ANYARRAY use the appropriate argument type */
	if (rettype == ANYARRAYOID)
	{
		if (!OidIsValid(array_typeid))
		{
			array_typeid = type_provider->get_array_type(elem_typeid);
			if (!OidIsValid(array_typeid))
				ereport(ERROR,
						(errcode(PG_ERRCODE_UNDEFINED_OBJECT),
						 errmsg("could not find array type for data type %s",
								type_provider->format_type_be(elem_typeid).c_str())));
		}
		return array_typeid;
	}

	/* if we return ANYRANGE use the appropriate argument type */
	if (rettype == ANYRANGEOID)
	{
		if (!OidIsValid(range_typeid))
		{
			ereport(ERROR,
					(errcode(PG_ERRCODE_UNDEFINED_OBJECT),
					 errmsg("could not find range type for data type %s",
							type_provider->format_type_be(elem_typeid).c_str())));
		}
		return range_typeid;
	}

	/* if we return ANYELEMENT use the appropriate argument type */
	if (rettype == ANYELEMENTOID ||
		rettype == ANYNONARRAYOID ||
		rettype == ANYENUMOID)
		return elem_typeid;

	/* we don't return a generic type; send back the original return type */
	return rettype;
};

PGNode *
CoerceParser::coerce_to_specific_type_typmod(PGParseState *pstate, PGNode *node,
							   PGOid targetTypeId, int32 targetTypmod,
							   const char *constructName)
{
	PGOid			inputTypeId = exprType(node);

	if (inputTypeId != targetTypeId)
	{
		PGNode	   *newnode;

		newnode = coerce_to_target_type(pstate, node, inputTypeId,
										targetTypeId, targetTypmod,
										PG_COERCION_ASSIGNMENT,
										PG_COERCE_IMPLICIT_CAST,
										-1);
		if (newnode == NULL)
		{
			parser_errposition(pstate, exprLocation(node));
			ereport(ERROR,
					(errcode(PG_ERRCODE_DATATYPE_MISMATCH),
			/* translator: first %s is name of a SQL construct, eg LIMIT */
					 errmsg("argument of %s must be type %s, not type %s",
							constructName,
							type_provider->format_type_be(targetTypeId).c_str(),
							type_provider->format_type_be(inputTypeId).c_str())));
		}
		node = newnode;
	}

	if (expression_returns_set(node))
	{
		parser_errposition(pstate, exprLocation(node));
		ereport(ERROR,
				(errcode(PG_ERRCODE_DATATYPE_MISMATCH),
		/* translator: %s is name of a SQL construct, eg LIMIT */
				 errmsg("argument of %s must not return a set",
						constructName)));
	}

	return node;
};

PGNode *
CoerceParser::coerce_to_specific_type(PGParseState *pstate, PGNode *node,
						PGOid targetTypeId,
						const char *constructName)
{
	return coerce_to_specific_type_typmod(pstate, node,
										  targetTypeId, -1,
										  constructName);
};

bool CoerceParser::IsBinaryCoercible(PGOid srctype, PGOid targettype)
{
    bool result;

    /* Fast path if same type */
    if (srctype == targettype)
        return true;

    /* Anything is coercible to ANY or ANYELEMENT */
    if (targettype == ANYOID || targettype == ANYELEMENTOID)
        return true;

    /* If srctype is a domain, reduce to its base type */
    if (OidIsValid(srctype))
        srctype = type_provider->getBaseType(srctype);

    /* Somewhat-fast path for domain -> base type case */
    if (srctype == targettype)
        return true;

    /* Also accept any array type as coercible to ANYARRAY */
    if (targettype == ANYARRAYOID)
        if (type_provider->get_element_type(srctype) != InvalidOid)
            return true;

    /* Also accept any non-array type as coercible to ANYNONARRAY */
    if (targettype == ANYNONARRAYOID)
        if (type_provider->get_element_type(srctype) == InvalidOid)
            return true;

    /* Also accept any enum type as coercible to ANYENUM */
    if (targettype == ANYENUMOID)
        if (type_provider->type_is_enum(srctype))
            return true;

    /* Also accept any range type as coercible to ANYRANGE */
    if (targettype == ANYRANGEOID)
        if (type_provider->type_is_range(srctype))
            return true;

    /* Also accept any composite type as coercible to RECORD */
    if (targettype == RECORDOID)
        if (type_parser->typeidTypeRelid(srctype) != InvalidOid)
            return true;

    /* Also accept any composite array type as coercible to RECORD[] */
    if (targettype == RECORDARRAYOID)
	{
		if (is_complex_array(srctype))
            return true;
	}
        
    /* Else look in pg_cast */
	PGCastPtr tuple = cast_provider->getCastBySourceTypeAndTargetTypeOid(srctype, targettype);
    if (tuple == NULL)
        return false; /* no cast */

    result = (tuple->castmethod == PG_COERCION_METHOD_BINARY && tuple->castcontext == PG_COERCION_CODE_IMPLICIT);

    return result;
};

void CoerceParser::fixup_unknown_vars_in_targetlist(PGParseState * pstate, PGList * targetlist)
{
    PGListCell * cell;

    foreach (cell, targetlist)
    {
        PGTargetEntry * tle = (PGTargetEntry *)lfirst(cell);

        Assert(IsA(tle, PGTargetEntry))

        if (IsA(tle->expr, PGVar) && ((PGVar *)tle->expr)->vartype == UNKNOWNOID)
        {
            tle->expr = (PGExpr *)coerce_unknown_var(pstate, (PGVar *)tle->expr, UNKNOWNOID, -1, PG_COERCION_IMPLICIT, PG_COERCE_EXPLICIT_CALL, 0);
        }
    }
};

}
