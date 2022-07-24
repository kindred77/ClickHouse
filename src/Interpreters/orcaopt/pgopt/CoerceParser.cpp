#include <CoerceParser.h>

using namespace duckdb_libpgquery;

namespace DB
{

bool
CoerceParser::can_coerce_type(int nargs, Oid *input_typeids, Oid *target_typeids,
				PGCoercionContext ccontext)
{
    bool		have_generics = false;
	int			i;

	/* run through argument list... */
	for (i = 0; i < nargs; i++)
	{
		Oid			inputTypeId = input_typeids[i];
		Oid			targetTypeId = target_typeids[i];
		CoercionPathType pathtype;
		Oid			funcId;

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
			have_generics = true;		/* do more checking later */
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
		if (pathtype != COERCION_PATH_NONE)
			continue;

		/*
		 * If input is RECORD and target is a composite type, assume we can
		 * coerce (may need tighter checking here)
		 */
		if (inputTypeId == RECORDOID &&
			ISCOMPLEX(targetTypeId))
			continue;

		/*
		 * If input is a composite type and target is RECORD, accept
		 */
		if (targetTypeId == RECORDOID &&
			ISCOMPLEX(inputTypeId))
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
		if (typeInheritsFrom(inputTypeId, targetTypeId)
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

CoercionPathType
CoerceParser::find_coercion_pathway(Oid targetTypeId, Oid sourceTypeId,
					  PGCoercionContext ccontext,
					  Oid *funcid)
{
    CoercionPathType result = COERCION_PATH_NONE;
	HeapTuple	tuple;

	*funcid = InvalidOid;

	/* Perhaps the types are domains; if so, look at their base types */
	if (OidIsValid(sourceTypeId))
		sourceTypeId = getBaseType(sourceTypeId);
	if (OidIsValid(targetTypeId))
		targetTypeId = getBaseType(targetTypeId);

	/* Domains are always coercible to and from their base type */
	if (sourceTypeId == targetTypeId)
		return COERCION_PATH_RELABELTYPE;

	/* SELECT castcontext from pg_cast */

	/* Look in pg_cast */
	tuple = SearchSysCache2(CASTSOURCETARGET,
							ObjectIdGetDatum(sourceTypeId),
							ObjectIdGetDatum(targetTypeId));

	if (HeapTupleIsValid(tuple))
	{
		Form_pg_cast castForm = (Form_pg_cast) GETSTRUCT(tuple);
		CoercionContext castcontext;

		/* convert char value for castcontext to CoercionContext enum */
		switch (castForm->castcontext)
		{
			case COERCION_CODE_IMPLICIT:
				castcontext = COERCION_IMPLICIT;
				break;
			case COERCION_CODE_ASSIGNMENT:
				castcontext = COERCION_ASSIGNMENT;
				break;
			case COERCION_CODE_EXPLICIT:
				castcontext = COERCION_EXPLICIT;
				break;
			default:
				elog(ERROR, "unrecognized castcontext: %d",
					 (int) castForm->castcontext);
				castcontext = 0;	/* keep compiler quiet */
				break;
		}

		/* Rely on ordering of enum for correct behavior here */
		if (ccontext >= castcontext)
		{
			switch (castForm->castmethod)
			{
				case COERCION_METHOD_FUNCTION:
					result = COERCION_PATH_FUNC;
					*funcid = castForm->castfunc;
					break;
				case COERCION_METHOD_INOUT:
					result = COERCION_PATH_COERCEVIAIO;
					break;
				case COERCION_METHOD_BINARY:
					result = COERCION_PATH_RELABELTYPE;
					break;
				default:
					elog(ERROR, "unrecognized castmethod: %d",
						 (int) castForm->castmethod);
					break;
			}
		}

		ReleaseSysCache(tuple);
	}
	else
	{
		/*
		 * If there's no pg_cast entry, perhaps we are dealing with a pair of
		 * array types.  If so, and if the element types have a suitable cast,
		 * report that we can coerce with an ArrayCoerceExpr.
		 *
		 * Note that the source type can be a domain over array, but not the
		 * target, because ArrayCoerceExpr won't check domain constraints.
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
			Oid			targetElem;
			Oid			sourceElem;

			if ((targetElem = get_element_type(targetTypeId)) != InvalidOid &&
			(sourceElem = get_base_element_type(sourceTypeId)) != InvalidOid)
			{
				CoercionPathType elempathtype;
				Oid			elemfuncid;

				elempathtype = find_coercion_pathway(targetElem,
													 sourceElem,
													 ccontext,
													 &elemfuncid);
				if (elempathtype != COERCION_PATH_NONE &&
					elempathtype != COERCION_PATH_ARRAYCOERCE)
				{
					*funcid = elemfuncid;
					if (elempathtype == COERCION_PATH_COERCEVIAIO)
						result = COERCION_PATH_COERCEVIAIO;
					else
						result = COERCION_PATH_ARRAYCOERCE;
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
		if (result == COERCION_PATH_NONE)
		{
			if (ccontext >= COERCION_ASSIGNMENT &&
				TypeCategory(targetTypeId) == TYPCATEGORY_STRING)
				result = COERCION_PATH_COERCEVIAIO;
			else if (ccontext >= COERCION_EXPLICIT &&
					 TypeCategory(sourceTypeId) == TYPCATEGORY_STRING)
				result = COERCION_PATH_COERCEVIAIO;
		}
	}

	return result;
};

void
CoerceParser::fixup_unknown_vars_in_targetlist(PGParseState *pstate, PGList *targetlist)
{
    ListCell   *cell;

    foreach(cell, targetlist)
    {
        PGTargetEntry    *tle = (PGTargetEntry *)lfirst(cell);

        Assert(IsA(tle, PGTargetEntry));

        if (IsA(tle->expr, PGVar) &&
            ((PGVar *)tle->expr)->vartype == UNKNOWNOID)
        {
            tle->expr = (PGExpr *)coerce_unknown_var(pstate, (PGVar *)tle->expr,
                                                   UNKNOWNOID, -1,
                                                   COERCION_IMPLICIT, COERCE_EXPLICIT_CALL,
                                                   0);
        }
    }
	/* fixup_unknown_vars_in_targetlist */
};

void
CoerceParser::fixup_unknown_vars_in_exprlist(PGParseState *pstate,
	PGList *exprlist)
{
	PGListCell   *cell;

    foreach(cell, exprlist)
    {
        if (IsA(lfirst(cell), PGVar) &&
            ((PGVar *)lfirst(cell))->vartype == UNKNOWNOID)
        {
            lfirst(cell) = coerce_unknown_var(pstate, (PGVar *)lfirst(cell),
                                              UNKNOWNOID, -1,
                                              PG_COERCION_IMPLICIT, PG_COERCE_EXPLICIT_CALL,
                                              0);
        }
    }
};

PGVar *
CoerceParser::coerce_unknown_var(PGParseState *pstate, PGVar *var,
                   Oid targetTypeId, int32 targetTypeMod,
			       PGCoercionContext ccontext,PGCoercionForm cformat,
                   int levelsup)
{
	PGRangeTblEntry  *rte;
    int             netlevelsup = var->varlevelsup + levelsup;

    Assert(IsA(var, PGVar));

	/* 
	 * If the parser isn't set up, we can't do anything here so just return the
	 * Var as is. This can happen if we call back into the parser from the
	 * planner (calls to addRangeTableEntryForJoin()).
	 */
	if (!pstate)
		return var;

    rte = relation_parser.GetRTEByRangeTablePosn(pstate, var->varno, netlevelsup);

    switch (rte->rtekind)
    {
        /* Descend thru Join RTEs to a leaf RTE. */
        case PG_RTE_JOIN:
        {
            PGListCell   *cell;
            PGVar        *joinvar;

            Assert(var->varattno > 0 &&
                   var->varattno <= list_length(rte->joinaliasvars));

            /* Get referenced join result Var */
            cell = list_nth_cell(rte->joinaliasvars, var->varattno - 1);
            joinvar = (PGVar *)lfirst(cell);

            /* If still untyped, try to replace it with a properly typed Var */
            if (joinvar->vartype == UNKNOWNOID &&
                targetTypeId != UNKNOWNOID)
            {
                joinvar = coerce_unknown_var(pstate, joinvar,
                                             targetTypeId, targetTypeMod,
                                             ccontext, cformat,
                                             netlevelsup);

                /* Substitute new Var into join result list. */
                lfirst(cell) = joinvar;
            }

            /* Make a new Var for the caller */
            if (joinvar->vartype != UNKNOWNOID)
            {
                var = (PGVar *) copyObject(var);
                var->vartype = joinvar->vartype;
                var->vartypmod = joinvar->vartypmod;
				var->varcollid = joinvar->varcollid;
            }
            break;
        }

        /* Impose requested type on Const or Param in subquery's targetlist. */
        case PG_RTE_SUBQUERY:
		{
			PGTargetEntry    *ste;
            PGNode           *targetexpr;
            Oid             exprtype;

            /* Get referenced subquery result expr */
            ste = relation_parser.get_tle_by_resno(rte->subquery->targetList, var->varattno);
			Assert(ste && !ste->resjunk && ste->expr);
            targetexpr = (PGNode *)ste->expr;

            /* If still untyped, try to coerce it to the requested type. */
            exprtype = exprType(targetexpr);
            if (exprtype == UNKNOWNOID &&
                targetTypeId != UNKNOWNOID)
            {
				PGParseState	   *subpstate = make_parsestate(pstate);
				subpstate->p_rtable = rte->subquery->rtable;

				targetexpr = coerce_type(subpstate, targetexpr, exprtype,
										 targetTypeId, targetTypeMod,
										 ccontext, cformat, -1);

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
			Oid inputTypeId, Oid targetTypeId, int32 targetTypeMod,
			PGCoercionContext ccontext, PGCoercionForm cformat, int location)
{
	PGNode	   *result;
	CoercionPathType pathtype;
	Oid			funcId;

	if (targetTypeId == inputTypeId ||
		node == NULL)
	{
		/* no conversion needed */
		return node;
	}
	if (targetTypeId == ANYOID ||
		targetTypeId == ANYELEMENTOID ||
		targetTypeId == ANYNONARRAYOID)
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
	if (targetTypeId == ANYARRAYOID ||
		targetTypeId == ANYENUMOID ||
		targetTypeId == ANYRANGEOID)
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
		if(targetTypeId == ANYARRAYOID && IsA(node, PGConst) && inputTypeId != UNKNOWNOID
		   && (pstate != NULL && pstate->p_expr_kind == EXPR_KIND_INSERT_TARGET))
		{
			PGConst	   *con = (PGConst *) node;
			PGConst	   *newcon = makeNode(PGConst);
			Oid elemoid = get_element_type(inputTypeId);

			if(elemoid == InvalidOid)
				ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH), 
					 errmsg("Cannot convert non-Array type to ANYARRAY")));

			memcpy(newcon, con, sizeof(PGConst));
			newcon->consttype = ANYARRAYOID;

			return (PGNode *) newcon;
		}

		if (inputTypeId != UNKNOWNOID)
		{
			Oid			baseTypeId = getBaseType(inputTypeId);

			if (baseTypeId != inputTypeId)
			{
				PGRelabelType *r = makeRelabelType((PGExpr *) node,
												 baseTypeId, -1,
												 InvalidOid,
												 cformat);

				r->location = location;
				return (PGNode *) r;
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
		PGConst	   *con = (PGConst *) node;
		PGConst	   *newcon = makeNode(PGConst);
		Oid			baseTypeId;
		int32		baseTypeMod;
		int32		inputTypeMod;
		Type		targetType;
		ParseCallbackState pcbstate;

		/*
		 * If the target type is a domain, we want to call its base type's
		 * input routine, not domain_in().  This is to avoid premature failure
		 * when the domain applies a typmod: existing input routines follow
		 * implicit-coercion semantics for length checks, which is not always
		 * what we want here.  The needed check will be applied properly
		 * inside coerce_to_domain().
		 */
		baseTypeMod = targetTypeMod;
		baseTypeId = getBaseTypeAndTypmod(targetTypeId, &baseTypeMod);

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

		targetType = typeidType(baseTypeId);

		newcon->consttype = baseTypeId;
		newcon->consttypmod = inputTypeMod;
		newcon->constcollid = typeTypeCollation(targetType);
		newcon->constlen = typeLen(targetType);
		newcon->constbyval = typeByVal(targetType);
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
			newcon->constvalue = stringTypeDatum(targetType,
											DatumGetCString(con->constvalue),
												 inputTypeMod);
		else
			newcon->constvalue = stringTypeDatum(targetType,
												 NULL,
												 inputTypeMod);

		cancel_parser_errposition_callback(&pcbstate);

		result = (PGNode *) newcon;

		/* If target is a domain, apply constraints. */
		if (baseTypeId != targetTypeId)
			result = coerce_to_domain(result,
									  baseTypeId, baseTypeMod,
									  targetTypeId,
									  cformat, location, false, false);

		ReleaseSysCache(targetType);

		return result;
	}
	if (IsA(node, PGParam) &&
		pstate != NULL && pstate->p_coerce_param_hook != NULL)
	{
		/*
		 * Allow the CoerceParamHook to decide what happens.  It can return a
		 * transformed node (very possibly the same Param node), or return
		 * NULL to indicate we should proceed with normal coercion.
		 */
		result = (*pstate->p_coerce_param_hook) (pstate,
												 (PGParam *) node,
												 targetTypeId,
												 targetTypeMod,
												 location);
		if (result)
			return result;
	}
	if (IsA(node, PGCollateExpr))
	{
		/*
		 * If we have a COLLATE clause, we have to push the coercion
		 * underneath the COLLATE.  This is really ugly, but there is little
		 * choice because the above hacks on Consts and Params wouldn't happen
		 * otherwise.  This kluge has consequences in coerce_to_target_type.
		 */
		PGCollateExpr *coll = (PGCollateExpr *) node;
		PGCollateExpr *newcoll = makeNode(PGCollateExpr);

		newcoll->arg = (PGExpr *)
			coerce_type(pstate, (PGNode *) coll->arg,
						inputTypeId, targetTypeId, targetTypeMod,
						ccontext, cformat, location);
		newcoll->collOid = coll->collOid;
		newcoll->location = coll->location;
		return (PGNode *) newcoll;
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
		int32	baseTypeMod = -1;
		Oid		baseTypeId = getBaseTypeAndTypmod(targetTypeId, &baseTypeMod);
        PGVar    *fixvar = coerce_unknown_var(pstate, (PGVar *)node,
                                            baseTypeId, baseTypeMod,
                                            ccontext, cformat,
                                            0);
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

			Oid outfunc = InvalidOid;
			bool outtypisvarlena = false;
			Oid infunc = InvalidOid;
			Oid intypioparam = InvalidOid;
			PGFuncExpr *fe;
			PGList *args = NIL;

			getTypeOutputInfo(UNKNOWNOID, &outfunc, &outtypisvarlena);
			getTypeInputInfo(targetTypeId, &infunc, &intypioparam);

			Insist(OidIsValid(outfunc));
			Insist(OidIsValid(infunc));

			/*
			 * do unknownout(Var)
			 *
			 * always supplying COERCE_IMPLICIT_CAST here, set it as an
			 * implicit cast to hide this Greenplum hack, because the explicit
			 * cast would be dumped but not able to be loaded, for like a cast
			 * unknown::cstring::date
			 */
			fe = makeFuncExpr(outfunc, CSTRINGOID, list_make1(node),
							  InvalidOid, InvalidOid, PG_COERCE_IMPLICIT_CAST);
			fe->location = location;

			if (location >= 0 &&
				(fixvar->location < 0 || location < fixvar->location))
			fixvar->location = location;

			/* 
			 * Now pass the above as an argument to the input function of the
			 * type we're casting to
			 */
			args = list_make3(fe,
							  makeConst(OIDOID, -1, InvalidOid, sizeof(Oid),
										ObjectIdGetDatum(intypioparam),
										false, true),
							  makeConst(INT4OID, -1, InvalidOid, sizeof(int32),
										Int32GetDatum(-1),
										false, true));
			fe = makeFuncExpr(infunc, targetTypeId, args,
							  InvalidOid, InvalidOid, cformat);
			fe->location = location;

			return (PGNode *)fe;
		}
	}

	pathtype = find_coercion_pathway(targetTypeId, inputTypeId, ccontext,
									 &funcId);
	if (pathtype != COERCION_PATH_NONE)
	{
		if (pathtype != COERCION_PATH_RELABELTYPE)
		{
			/*
			 * Generate an expression tree representing run-time application
			 * of the conversion function.  If we are dealing with a domain
			 * target type, the conversion function will yield the base type,
			 * and we need to extract the correct typmod to use from the
			 * domain's typtypmod.
			 */
			Oid			baseTypeId;
			int32		baseTypeMod;

			baseTypeMod = targetTypeMod;
			baseTypeId = getBaseTypeAndTypmod(targetTypeId, &baseTypeMod);

			result = build_coercion_expression(node, pathtype, funcId,
											   baseTypeId, baseTypeMod,
											   cformat, location,
										  (cformat != PG_COERCE_IMPLICIT_CAST));

			/*
			 * If domain, coerce to the domain type and relabel with domain
			 * type ID.  We can skip the internal length-coercion step if the
			 * selected coercion function was a type-and-length coercion.
			 */
			if (targetTypeId != baseTypeId)
				result = coerce_to_domain(result, baseTypeId, baseTypeMod,
										  targetTypeId,
										  cformat, location, true,
										  exprIsLengthCoercion(result,
															   NULL));
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
			result = coerce_to_domain(node, InvalidOid, -1, targetTypeId,
									  cformat, location, false, false);
			if (result == node)
			{
				/*
				 * XXX could we label result with exprTypmod(node) instead of
				 * default -1 typmod, to save a possible length-coercion
				 * later? Would work if both types have same interpretation of
				 * typmod, which is likely but not certain.
				 */
				PGRelabelType *r = makeRelabelType((PGExpr *) result,
												 targetTypeId, -1,
												 InvalidOid,
												 cformat);

				r->location = location;
				result = (PGNode *) r;
			}
		}
		return result;
	}
	if (inputTypeId == RECORDOID &&
		ISCOMPLEX(targetTypeId))
	{
		/* Coerce a RECORD to a specific complex type */
		return coerce_record_to_complex(pstate, node, targetTypeId,
										ccontext, cformat, location);
	}
	if (targetTypeId == RECORDOID &&
		ISCOMPLEX(inputTypeId))
	{
		/* Coerce a specific complex type to RECORD */
		/* NB: we do NOT want a RelabelType here */
		return node;
	}
#ifdef NOT_USED
	if (inputTypeId == RECORDARRAYOID &&
		is_complex_array(targetTypeId))
	{
		/* Coerce record[] to a specific complex array type */
		/* not implemented yet ... */
	}
#endif
	if (targetTypeId == RECORDARRAYOID &&
		is_complex_array(inputTypeId))
	{
		/* Coerce a specific complex array type to record[] */
		/* NB: we do NOT want a RelabelType here */
		return node;
	}
	if (typeInheritsFrom(inputTypeId, targetTypeId)
		|| typeIsOfTypedTable(inputTypeId, targetTypeId))
	{
		/*
		 * Input class type is a subclass of target, so generate an
		 * appropriate runtime conversion (removing unneeded columns and
		 * possibly rearranging the ones that are wanted).
		 */
		PGConvertRowtypeExpr *r = makeNode(PGConvertRowtypeExpr);

		r->arg = (PGExpr *) node;
		r->resulttype = targetTypeId;
		r->convertformat = cformat;
		r->location = location;
		return (PGNode *) r;
	}
	/* If we get here, caller blew it */
	elog(ERROR, "failed to find conversion function from %s to %s",
		 format_type_be(inputTypeId), format_type_be(targetTypeId));
	return NULL;				/* keep compiler quiet */
};

PGNode *
CoerceParser::coerce_to_boolean(PGParseState *pstate, PGNode *node,
				  const char *constructName)
{
	Oid			inputTypeId = exprType(node);

	if (inputTypeId != BOOLOID)
	{
		PGNode	   *newnode;

		newnode = coerce_to_target_type(pstate, node, inputTypeId,
										BOOLOID, -1,
										PG_COERCION_ASSIGNMENT,
										PG_COERCE_IMPLICIT_CAST,
										-1);
		if (newnode == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
			/* translator: first %s is name of a SQL construct, eg WHERE */
				   errmsg("argument of %s must be type boolean, not type %s",
						  constructName, format_type_be(inputTypeId)),
					 parser_errposition(pstate, exprLocation(node))));
		node = newnode;
	}

	if (expression_returns_set(node))
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
		/* translator: %s is name of a SQL construct, eg WHERE */
				 errmsg("argument of %s must not return a set",
						constructName),
				 parser_errposition(pstate, exprLocation(node))));

	return node;
};

Oid
CoerceParser::select_common_type(PGParseState *pstate,
		PGList *exprs, const char *context,
		PGNode **which_expr)
{
	PGNode	   *pexpr;
	Oid			ptype;
	TYPCATEGORY pcategory;
	bool		pispreferred;
	ListCell   *lc;

	Assert(exprs != NIL);
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
			Oid			ntype = exprType(nexpr);

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
	ptype = getBaseType(ptype);
	get_type_category_preferred(ptype, &pcategory, &pispreferred);

	for_each_cell(lc, lc)
	{
		PGNode	   *nexpr = (PGNode *) lfirst(lc);
		Oid			ntype = getBaseType(exprType(nexpr));

		/* move on to next one if no new information... */
		if (ntype != UNKNOWNOID && ntype != ptype)
		{
			TYPCATEGORY ncategory;
			bool		nispreferred;

			get_type_category_preferred(ntype, &ncategory, &nispreferred);
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
				if (context == NULL)
					return InvalidOid;
				ereport(ERROR,
						(errcode(ERRCODE_DATATYPE_MISMATCH),
				/*------
				  translator: first %s is name of a SQL construct, eg CASE */
						 errmsg("%s types %s and %s cannot be matched",
								context,
								format_type_be(ptype),
								format_type_be(ntype)),
						 parser_errposition(pstate, exprLocation(nexpr))));
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

}