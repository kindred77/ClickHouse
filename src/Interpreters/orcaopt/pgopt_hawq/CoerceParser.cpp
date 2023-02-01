#include <Interpreters/orcaopt/pgopt_hawq/CoerceParser.h>

using namespace duckdb_libpgquery;

namespace DB
{

CATEGORY CoerceParser::TypeCategory(Oid inType)
{
    CATEGORY result;

    switch (inType)
    {
        case (BOOLOID):
            result = BOOLEAN_TYPE;
            break;

        case (CHAROID):
        case (NAMEOID):
        case (BPCHAROID):
        case (VARCHAROID):
        case (TEXTOID):
            result = STRING_TYPE;
            break;

        case (BITOID):
        case (VARBITOID):
            result = BITSTRING_TYPE;
            break;

        case (OIDOID):
        case (REGPROCOID):
        case (REGPROCEDUREOID):
        case (REGOPEROID):
        case (REGOPERATOROID):
        case (REGCLASSOID):
        case (REGTYPEOID):
        case (INT2OID):
        case (INT4OID):
        case (INT8OID):
        case (FLOAT4OID):
        case (FLOAT8OID):
        case (NUMERICOID):
        case (CASHOID):
            result = NUMERIC_TYPE;
            break;

        case (DATEOID):
        case (TIMEOID):
        case (TIMETZOID):
        case (ABSTIMEOID):
        case (TIMESTAMPOID):
        case (TIMESTAMPTZOID):
            result = DATETIME_TYPE;
            break;

        case (RELTIMEOID):
        case (TINTERVALOID):
        case (INTERVALOID):
            result = TIMESPAN_TYPE;
            break;

        case (POINTOID):
        case (LSEGOID):
        case (PATHOID):
        case (BOXOID):
        case (POLYGONOID):
        case (LINEOID):
        case (CIRCLEOID):
            result = GEOMETRIC_TYPE;
            break;

        case (INETOID):
        case (CIDROID):
            result = NETWORK_TYPE;
            break;

        case (UNKNOWNOID):
        case (InvalidOid):
            result = UNKNOWN_TYPE;
            break;

        case (RECORDOID):
        case (CSTRINGOID):
        case (ANYOID):
        case (ANYARRAYOID):
        case (VOIDOID):
        case (TRIGGEROID):
        case (LANGUAGE_HANDLEROID):
        case (INTERNALOID):
        case (OPAQUEOID):
        case (ANYELEMENTOID):
            result = GENERIC_TYPE;
            break;

        default:
            result = USER_TYPE;
            break;
    }
    return result;
};

bool CoerceParser::IsPreferredType(CATEGORY category, Oid type)
{
    Oid preftype;

    if (category == INVALID_TYPE)
        category = TypeCategory(type);
    else if (category != TypeCategory(type))
        return false;

    /*
	 * This switch should agree with TypeCategory(), above.  Note that at this
	 * point, category certainly matches the type.
	 */
    switch (category)
    {
        case (UNKNOWN_TYPE):
        case (GENERIC_TYPE):
            preftype = UNKNOWNOID;
            break;

        case (BOOLEAN_TYPE):
            preftype = BOOLOID;
            break;

        case (STRING_TYPE):
            preftype = TEXTOID;
            break;

        case (BITSTRING_TYPE):
            preftype = VARBITOID;
            break;

        case (NUMERIC_TYPE):
            if (type == OIDOID || type == REGPROCOID || type == REGPROCEDUREOID || type == REGOPEROID || type == REGOPERATOROID
                || type == REGCLASSOID || type == REGTYPEOID)
                preftype = OIDOID;
            else
                preftype = FLOAT8OID;
            break;

        case (DATETIME_TYPE):
            if (type == DATEOID)
                preftype = TIMESTAMPOID;
            else
                preftype = TIMESTAMPTZOID;
            break;

        case (TIMESPAN_TYPE):
            preftype = INTERVALOID;
            break;

        case (GEOMETRIC_TYPE):
            preftype = type;
            break;

        case (NETWORK_TYPE):
            preftype = INETOID;
            break;

        case (USER_TYPE):
            preftype = type;
            break;

        default:
            elog(ERROR, "unrecognized type category: %d", (int)category);
            preftype = UNKNOWNOID;
            break;
    }

    return (type == preftype);
};

bool CoerceParser::find_coercion_pathway(Oid targetTypeId, Oid sourceTypeId,
		CoercionContext ccontext, Oid * funcid)
{
    bool result = false;
    HeapTuple tuple;
    cqContext * pcqCtx;

    *funcid = InvalidOid;

    /* Perhaps the types are domains; if so, look at their base types */
    if (OidIsValid(sourceTypeId))
        sourceTypeId = getBaseType(sourceTypeId);
    if (OidIsValid(targetTypeId))
        targetTypeId = getBaseType(targetTypeId);

    /* Domains are always coercible to and from their base type */
    if (sourceTypeId == targetTypeId)
        return true;

    /* SELECT castcontext from pg_cast */

    /* Look in pg_cast */
    pcqCtx = caql_beginscan(
        NULL,
        cql("SELECT * FROM pg_cast "
            " WHERE castsource = :1 "
            " AND casttarget = :2 ",
            ObjectIdGetDatum(sourceTypeId),
            ObjectIdGetDatum(targetTypeId)));

    tuple = caql_getnext(pcqCtx);

    if (HeapTupleIsValid(tuple))
    {
        Form_pg_cast castForm = (Form_pg_cast)GETSTRUCT(tuple);
        CoercionContext castcontext;

        /* convert char value for castcontext to CoercionContext enum */
        switch (castForm->castcontext)
        {
            case COERCION_CODE_IMPLICIT:
                castcontext = PG_COERCION_IMPLICIT;
                break;
            case COERCION_CODE_ASSIGNMENT:
                castcontext = PG_COERCION_ASSIGNMENT;
                break;
            case COERCION_CODE_EXPLICIT:
                castcontext = PG_COERCION_EXPLICIT;
                break;
            default:
                elog(ERROR, "unrecognized castcontext: %d", (int)castForm->castcontext);
                castcontext = 0; /* keep compiler quiet */
                break;
        }

        /* Rely on ordering of enum for correct behavior here */
        if (ccontext >= castcontext)
        {
            *funcid = castForm->castfunc;
            result = true;
        }
    }
    else
    {
        /*
		 * If there's no pg_cast entry, perhaps we are dealing with a pair of
		 * array types.  If so, and if the element types have a suitable cast,
		 * use array_type_coerce() or array_type_length_coerce().
		 *
		 * Hack: disallow coercions to oidvector and int2vector, which
		 * otherwise tend to capture coercions that should go to "real" array
		 * types.  We want those types to be considered "real" arrays for many
		 * purposes, but not this one.	(Also, array_type_coerce isn't
		 * guaranteed to produce an output that meets the restrictions of
		 * these datatypes, such as being 1-dimensional.)
		 */
        Oid targetElemType;
        Oid sourceElemType;
        Oid elemfuncid;

        if (targetTypeId == OIDVECTOROID || targetTypeId == INT2VECTOROID)
            return false;

        if ((targetElemType = get_element_type(targetTypeId)) != InvalidOid
            && (sourceElemType = get_element_type(sourceTypeId)) != InvalidOid)
        {
            if (find_coercion_pathway(targetElemType, sourceElemType, ccontext, &elemfuncid))
            {
                if (!OidIsValid(elemfuncid))
                {
                    /* binary-compatible element type conversion */
                    *funcid = F_ARRAY_TYPE_COERCE;
                }
                else
                {
                    /* does the function take a typmod arg? */
                    if (get_func_nargs(elemfuncid) > 1)
                        *funcid = F_ARRAY_TYPE_LENGTH_COERCE;
                    else
                        *funcid = F_ARRAY_TYPE_COERCE;
                }
                result = true;
            }
        }
    }

    caql_endscan(pcqCtx);
    return result;
};

bool CoerceParser::check_generic_type_consistency(Oid * actual_arg_types, Oid * declared_arg_types, int nargs)
{
    int j;
    Oid elem_typeid = InvalidOid;
    Oid array_typeid = InvalidOid;
    Oid array_typelem;
    bool have_anyelement = false;

    /*
	 * Loop through the arguments to see if we have any that are ANYARRAY or
	 * ANYELEMENT. If so, require the actual types to be self-consistent
	 */
    for (j = 0; j < nargs; j++)
    {
        Oid actual_type = actual_arg_types[j];

        if (declared_arg_types[j] == ANYELEMENTOID)
        {
            have_anyelement = true;
            if (actual_type == UNKNOWNOID)
                continue;
            if (OidIsValid(elem_typeid) && actual_type != elem_typeid)
                return false;
            elem_typeid = actual_type;
        }
        else if (declared_arg_types[j] == ANYARRAYOID)
        {
            if (actual_type == UNKNOWNOID)
                continue;
            if (OidIsValid(array_typeid) && actual_type != array_typeid)
                return false;
            array_typeid = actual_type;
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

        array_typelem = get_element_type(array_typeid);
        if (!OidIsValid(array_typelem))
            return false; /* should be an array, but isn't */

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

    /* Looks valid */
    return true;
};

bool CoerceParser::can_coerce_type(int nargs, Oid *input_typeids, Oid *target_typeids,
		PGCoercionContext ccontext)
{
    bool have_generics = false;
    int i;

    /* run through argument list... */
    for (i = 0; i < nargs; i++)
    {
        Oid inputTypeId = input_typeids[i];
        Oid targetTypeId = target_typeids[i];
        Oid funcId;

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

        /* accept if target is ANYARRAY or ANYELEMENT, for now */
        if (targetTypeId == ANYARRAYOID || targetTypeId == ANYELEMENTOID)
        {
            have_generics = true; /* do more checking later */
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
        if (find_coercion_pathway(targetTypeId, inputTypeId, ccontext, &funcId))
            continue;

        /*
		 * If input is RECORD and target is a composite type, assume we can
		 * coerce (may need tighter checking here)
		 */
        if (inputTypeId == RECORDOID && type_parser.typeidTypeRelid(targetTypeId) != InvalidOid)
            continue;

        /*
		 * If input is a composite type and target is RECORD, accept
		 */
        if (targetTypeId == RECORDOID && type_parser.typeidTypeRelid(inputTypeId) != InvalidOid)
            continue;

        /*
		 * If input is a class type that inherits from target, accept
		 */
        if (func_parser.typeInheritsFrom(inputTypeId, targetTypeId))
            continue;

        /*
		 * Else, cannot coerce at this argument position
		 */
        return false;
    }

    /* If we found any generic argument types, cross-check them */
    if (have_generics)
    {
        if (!check_generic_type_consistency(input_typeids, target_typeids, nargs))
            return false;
    }

    return true;
};

PGNode * CoerceParser::coerce_type(
        PGParseState * pstate,
        PGNode * node,
        Oid inputTypeId,
        Oid targetTypeId,
        int32 targetTypeMod,
        PGCoercionContext ccontext,
        PGCoercionForm cformat,
        int location)
{
    PGNode * result;
    Oid funcId;

    if (targetTypeId == inputTypeId || node == NULL)
    {
        /* no conversion needed */
        return node;
    }
    if (targetTypeId == ANYOID || targetTypeId == ANYELEMENTOID || (targetTypeId == ANYARRAYOID && inputTypeId != UNKNOWNOID))
    {
        /*
		 * Assume can_coerce_type verified that implicit coercion is okay.
		 *
		 * Note: by returning the unmodified node here, we are saying that
		 * it's OK to treat an UNKNOWN constant as a valid input for a
		 * function accepting ANY or ANYELEMENT.  This should be all right,
		 * since an UNKNOWN value is still a perfectly valid Datum.  However
		 * an UNKNOWN value is definitely *not* an array, and so we mustn't
		 * accept it for ANYARRAY.  (Instead, we will call anyarray_in below,
		 * which will produce an error.)
		 *
		 * NB: we do NOT want a RelabelType here.
		 */

        /*
		 * BUG BUG 
		 * JIRA MPP-3786
		 *
		 * Special handling for ANYARRAY type.  
		 */
        if (targetTypeId == ANYARRAYOID && IsA(node, PGConst))
        {
            PGConst * con = (PGConst *)node;
            PGConst * newcon = makeNode(PGConst);
            Oid elemoid = get_element_type(inputTypeId);

            if (elemoid == InvalidOid)
                ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH), errmsg("Cannot convert non-Array type to ANYARRAY")));

            memcpy(newcon, con, sizeof(PGConst));
            newcon->consttype = ANYARRAYOID;

            return (PGNode *)newcon;
        }

        return node;
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
        Oid baseTypeId;
        int32 baseTypeMod;
        int32 inputTypeMod;
        Type targetType;

        /*
		 * If the target type is a domain, we want to call its base type's
		 * input routine, not domain_in().	This is to avoid premature failure
		 * when the domain applies a typmod: existing input routines follow
		 * implicit-coercion semantics for length checks, which is not always
		 * what we want here.  The needed check will be applied properly
		 * inside coerce_to_domain().
		 */
        baseTypeMod = targetTypeMod;
        baseTypeId = getBaseTypeAndTypmod(targetTypeId, &baseTypeMod);

        if (baseTypeId == INTERVALOID)
            inputTypeMod = baseTypeMod;
        else
            inputTypeMod = -1;

        targetType = typeidType(baseTypeId);

        newcon->consttype = baseTypeId;
        newcon->consttypmod = inputTypeMod;
        newcon->constlen = typeLen(targetType);
        newcon->constbyval = typeByVal(targetType);
        newcon->constisnull = con->constisnull;

        /*
		 * We pass typmod -1 to the input routine, primarily because existing
		 * input routines follow implicit-coercion semantics for length
		 * checks, which is not always what we want here. Any length
		 * constraint will be applied later by our caller.
		 *
		 * We assume here that UNKNOWN's internal representation is the same
		 * as CSTRING.
		 */
        if (!con->constisnull)
            newcon->constvalue = stringTypeDatum(targetType, DatumGetCString(con->constvalue), inputTypeMod);
        else
            newcon->constvalue = stringTypeDatum(targetType, NULL, inputTypeMod);

        result = (PGNode *)newcon;

        /* If target is a domain, apply constraints. */
        if (baseTypeId != targetTypeId)
            result = coerce_to_domain(result, baseTypeId, baseTypeMod, targetTypeId, cformat, location, false, false);

        ReleaseType(targetType);

        return result;
    }
    if (inputTypeId == UNKNOWNOID && IsA(node, PGParam) && ((PGParam *)node)->paramkind == PG_PARAM_EXTERN && pstate != NULL
        && pstate->p_variableparams)
    {
        /*
		 * Input is a Param of previously undetermined type, and we want to
		 * update our knowledge of the Param's type.  Find the topmost
		 * ParseState and update the state.
		 */
        PGParam * param = (PGParam *)node;
        int paramno = param->paramid;
        PGParseState * toppstate;

        toppstate = pstate;
        while (toppstate->parentParseState != NULL)
            toppstate = toppstate->parentParseState;

        if (paramno <= 0 || /* shouldn't happen, but... */
            paramno > toppstate->p_numparams)
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_PARAMETER), errmsg("there is no parameter $%d", paramno)));

        if (toppstate->p_paramtypes[paramno - 1] == UNKNOWNOID)
        {
            /* We've successfully resolved the type */
            toppstate->p_paramtypes[paramno - 1] = targetTypeId;
        }
        else if (toppstate->p_paramtypes[paramno - 1] == targetTypeId)
        {
            /* We previously resolved the type, and it matches */
        }
        else
        {
            /* Ooops */
            ereport(
                ERROR,
                (errcode(ERRCODE_AMBIGUOUS_PARAMETER),
                 errmsg("inconsistent types deduced for parameter $%d", paramno),
                 errdetail("%s versus %s", format_type_be(toppstate->p_paramtypes[paramno - 1]), format_type_be(targetTypeId))));
        }

        param->paramtype = targetTypeId;

        return (PGNode *)param;
    }
    if (pstate != NULL && inputTypeId == UNKNOWNOID && IsA(node, PGVar))
    {
        /*
         * CDB:  Var of type UNKNOWN probably comes from an untyped Const
         * or Param in the targetlist of a range subquery.  For a successful
         * conversion we must coerce the underlying Const or Param to a
         * proper type and remake the Var.
         */
        int32 baseTypeMod = -1;
        Oid baseTypeId = getBaseTypeAndTypmod(targetTypeId, &baseTypeMod);
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

            Oid outfunc = InvalidOid;
            bool outtypisvarlena = false;
            Oid infunc = InvalidOid;
            Oid intypioparam = InvalidOid;
            PGFuncExpr * fe;
            PGList * args = NIL;

            getTypeOutputInfo(UNKNOWNOID, &outfunc, &outtypisvarlena);
            getTypeInputInfo(targetTypeId, &infunc, &intypioparam);

            Insist(OidIsValid(outfunc));
            Insist(OidIsValid(infunc));

            /* do unknownout(Var) */
            fe = makeFuncExpr(outfunc, CSTRINGOID, list_make1(node), cformat);

            /* 
			 * Now pass the above as an argument to the input function of the
			 * type we're casting to
			 */
            args = list_make3(
                fe,
                makeConst(OIDOID, -1, sizeof(Oid), ObjectIdGetDatum(intypioparam), false, true),
                makeConst(INT4OID, -1, sizeof(int32), Int32GetDatum(-1), false, true));
            fe = makeFuncExpr(infunc, targetTypeId, args, cformat);
            return (PGNode *)fe;
        }
    }
    if (find_coercion_pathway(targetTypeId, inputTypeId, ccontext, &funcId))
    {
        if (OidIsValid(funcId))
        {
            /*
			 * Generate an expression tree representing run-time application
			 * of the conversion function.	If we are dealing with a domain
			 * target type, the conversion function will yield the base type,
			 * and we need to extract the correct typmod to use from the
			 * domain's typtypmod.
			 */
            Oid baseTypeId;
            int32 baseTypeMod;

            baseTypeMod = targetTypeMod;
            baseTypeId = getBaseTypeAndTypmod(targetTypeId, &baseTypeMod);

            result = build_coercion_expression(node, funcId, baseTypeId, baseTypeMod, cformat, (cformat != COERCE_IMPLICIT_CAST));

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
			 * that must be accounted for.	If the destination is a domain
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
                result = (PGNode *)makeRelabelType((PGExpr *)result, targetTypeId, -1, cformat);
            }
        }
        return result;
    }
    if (inputTypeId == RECORDOID && ISCOMPLEX(targetTypeId))
    {
        /* Coerce a RECORD to a specific complex type */
        return coerce_record_to_complex(pstate, node, targetTypeId, ccontext, cformat);
    }
    if (targetTypeId == RECORDOID && ISCOMPLEX(inputTypeId))
    {
        /* Coerce a specific complex type to RECORD */
        /* NB: we do NOT want a RelabelType here */
        return node;
    }
    if (typeInheritsFrom(inputTypeId, targetTypeId))
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
        return (Node *)r;
    }
    /* If we get here, caller blew it */
    elog(ERROR, "failed to find conversion function from %s to %s", format_type_be(inputTypeId), format_type_be(targetTypeId));
    return NULL; /* keep compiler quiet */
};

Oid CoerceParser::select_common_type(PGList *typeids, const char *context)
{
    Oid ptype;
    CATEGORY pcategory;
    PGListCell * type_item;

    Assert(typeids != NIL);
    ptype = getBaseType(linitial_oid(typeids));
    pcategory = TypeCategory(ptype);

    for_each_cell(type_item, lnext(list_head(typeids)))
    {
        Oid ntype = getBaseType(lfirst_oid(type_item));

        /* move on to next one if no new information... */
        if ((ntype != InvalidOid) && (ntype != UNKNOWNOID) && (ntype != ptype))
        {
            if ((ptype == InvalidOid) || ptype == UNKNOWNOID)
            {
                /* so far, only nulls so take anything... */
                ptype = ntype;
                pcategory = TypeCategory(ptype);
            }
            else if (TypeCategory(ntype) != pcategory)
            {
                /*
				 * both types in different categories? then not much hope...
				 */
                if (context == NULL)
                    return InvalidOid;
                ereport(
                    ERROR,
                    (errcode(ERRCODE_DATATYPE_MISMATCH),

                     /*------
				  translator: first %s is name of a SQL construct, eg CASE */
                     errmsg("%s types %s and %s cannot be matched", context, format_type_be(ptype), format_type_be(ntype))));
            }
            else if (
                !IsPreferredType(pcategory, ptype) && can_coerce_type(1, &ptype, &ntype, PG_COERCION_IMPLICIT)
                && !can_coerce_type(1, &ntype, &ptype, PG_COERCION_IMPLICIT))
            {
                /*
				 * take new type if can coerce to it implicitly but not the
				 * other way; but if we have a preferred type, stay on it.
				 */
                ptype = ntype;
                pcategory = TypeCategory(ptype);
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

    return ptype;
};

PGNode * CoerceParser::coerce_to_target_type(
        PGParseState * pstate,
        PGNode * expr,
        Oid exprtype,
        Oid targettype,
        int32 targettypmod,
        PGCoercionContext ccontext,
        PGCoercionForm cformat,
        int location)
{
    PGNode * result;

    if (!can_coerce_type(1, &exprtype, &targettype, ccontext))
        return NULL;

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
        (cformat != PG_COERCE_IMPLICIT_CAST),
        (result != expr && !IsA(result, PGConst) && !IsA(result, PGVar)));

    return result;
};

PGNode * CoerceParser::coerce_to_bigint(PGParseState * pstate,
      PGNode * node, const char * constructName)
{
    Oid inputTypeId = exprType(node);

    if (inputTypeId != INT8OID)
    {
        node = coerce_to_target_type(pstate, node, inputTypeId, INT8OID, -1, PG_COERCION_ASSIGNMENT, PG_COERCE_IMPLICIT_CAST, -1);
        if (node == NULL)
            ereport(
                ERROR,
                (errcode(ERRCODE_DATATYPE_MISMATCH),
                 /* translator: first %s is name of a SQL construct, eg LIMIT */
                 errmsg("argument of %s must be type bigint, not type %s", constructName, format_type_be(inputTypeId))));
    }

    if (expression_returns_set(node))
        ereport(
            ERROR,
            (errcode(ERRCODE_DATATYPE_MISMATCH),
             /* translator: %s is name of a SQL construct, eg LIMIT */
             errmsg("argument of %s must not return a set", constructName)));

    return node;
};

PGNode * CoerceParser::coerce_to_boolean(PGParseState * pstate,
		PGNode * node, const char * constructName)
{
    Oid inputTypeId = exprType(node);

    if (inputTypeId != BOOLOID)
    {
        node = coerce_to_target_type(pstate, node, inputTypeId, BOOLOID, -1, PG_COERCION_ASSIGNMENT, COERCE_IMPLICIT_CAST, -1);
        if (node == NULL)
            ereport(
                ERROR,
                (errcode(ERRCODE_DATATYPE_MISMATCH),
                 /* translator: first %s is name of a SQL construct, eg WHERE */
                 errmsg("argument of %s must be type boolean, not type %s", constructName, format_type_be(inputTypeId))));
    }

    if (expression_returns_set(node))
        ereport(
            ERROR,
            (errcode(ERRCODE_DATATYPE_MISMATCH),
             /* translator: %s is name of a SQL construct, eg WHERE */
             errmsg("argument of %s must not return a set", constructName)));

    return node;
};

PGNode * CoerceParser::coerce_to_common_type(PGParseState * pstate, PGNode * node,
      Oid targetTypeId, const char * context)
{
    Oid inputTypeId = exprType(node);

    if (inputTypeId == targetTypeId)
        return node; /* no work */
    if (can_coerce_type(1, &inputTypeId, &targetTypeId, PG_COERCION_IMPLICIT))
        node = coerce_type(pstate, node, inputTypeId, targetTypeId, -1, PG_COERCION_IMPLICIT, PG_COERCE_IMPLICIT_CAST, -1);
    else
        ereport(
            ERROR,
            (errcode(ERRCODE_CANNOT_COERCE),
             /* translator: first %s is name of a SQL construct, eg CASE */
             errmsg("%s could not convert type %s to %s", context, format_type_be(inputTypeId), format_type_be(targetTypeId))));
    return node;
};

PGVar * CoerceParser::coerce_unknown_var(
        PGParseState * pstate,
        PGVar * var,
        Oid targetTypeId,
        int32 targetTypeMod,
        PGCoercionContext ccontext,
        PGCoercionForm cformat,
        int levelsup)
{
    PGRangeTblEntry * rte;
    int netlevelsup = var->varlevelsup + levelsup;

    Assert(IsA(var, PGVar));

    /* 
	 * If the parser isn't set up, we can't do anything here so just return the
	 * Var as is. This can happen if we call back into the parser from the
	 * planner (calls to addRangeTableEntryForJoin()).
	 */
    if (!PointerIsValid(pstate))
        return var;

    rte = relation_parser.GetRTEByRangeTablePosn(pstate, var->varno, netlevelsup);

    switch (rte->rtekind)
    {
        /* Descend thru Join RTEs to a leaf RTE. */
        case PG_RTE_JOIN: {
            PGListCell * cell;
            PGVar * joinvar;

            Assert(var->varattno > 0 && var->varattno <= list_length(rte->joinaliasvars));

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
            }
            break;
        }

        /* Impose requested type on Const or Param in subquery's targetlist. */
        case PG_RTE_SUBQUERY: {
            PGTargetEntry * ste;
            PGNode * targetexpr;
            Oid exprtype;

            /* Get referenced subquery result expr */
            ste = relation_parser.get_tle_by_resno(rte->subquery->targetList, var->varattno);
            Assert(ste && !ste->resjunk && ste->expr);
            targetexpr = (PGNode *)ste->expr;

            /* If still untyped, try to coerce it to the requested type. */
            exprtype = exprType(targetexpr);
            if (exprtype == UNKNOWNOID && targetTypeId != UNKNOWNOID)
            {
                PGParseState * subpstate = make_parsestate(pstate);
                subpstate->p_rtable = rte->subquery->rtable;

                targetexpr = coerce_type(subpstate, targetexpr, exprtype, targetTypeId, targetTypeMod, ccontext, cformat, -1);

                free_parsestate(&subpstate);
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
            }
            break;
        }

        /* Return unchanged Var if leaf RTE is not a subquery. */
        default:
            break;
    }

    return var;
};

void CoerceParser::fixup_unknown_vars_in_targetlist(PGParseState * pstate,
		PGList * targetlist)
{
    PGListCell * cell;

    foreach (cell, targetlist)
    {
        PGTargetEntry * tle = (PGTargetEntry *)lfirst(cell);

        Assert(IsA(tle, PGTargetEntry));

        if (IsA(tle->expr, PGVar) && ((PGVar *)tle->expr)->vartype == UNKNOWNOID)
        {
            tle->expr = (PGExpr *)coerce_unknown_var(pstate, (PGVar *)tle->expr, UNKNOWNOID, -1, PG_COERCION_IMPLICIT, COERCE_DONTCARE, 0);
        }
    }
};

Oid CoerceParser::enforce_generic_type_consistency(Oid * actual_arg_types, Oid * declared_arg_types,
      int nargs, Oid rettype)
{
    int j;
    bool have_generics = false;
    bool have_unknowns = false;
    Oid elem_typeid = InvalidOid;
    Oid array_typeid = InvalidOid;
    Oid array_typelem;
    bool have_anyelement = (rettype == ANYELEMENTOID);

    /*
	 * Loop through the arguments to see if we have any that are ANYARRAY or
	 * ANYELEMENT. If so, require the actual types to be self-consistent
	 */
    for (j = 0; j < nargs; j++)
    {
        Oid actual_type = actual_arg_types[j];

        if (declared_arg_types[j] == ANYELEMENTOID)
        {
            have_generics = have_anyelement = true;
            if (actual_type == UNKNOWNOID)
            {
                have_unknowns = true;
                continue;
            }
            if (OidIsValid(elem_typeid) && actual_type != elem_typeid)
                ereport(
                    ERROR,
                    (errcode(ERRCODE_DATATYPE_MISMATCH),
                     errmsg("arguments declared \"anyelement\" are not all alike"),
                     errdetail("%s versus %s", format_type_be(elem_typeid), format_type_be(actual_type))));
            elem_typeid = actual_type;
        }
        else if (declared_arg_types[j] == ANYARRAYOID)
        {
            have_generics = true;
            if (actual_type == UNKNOWNOID)
            {
                have_unknowns = true;
                continue;
            }
            if (OidIsValid(array_typeid) && actual_type != array_typeid)
                ereport(
                    ERROR,
                    (errcode(ERRCODE_DATATYPE_MISMATCH),
                     errmsg("arguments declared \"anyarray\" are not all alike"),
                     errdetail("%s versus %s", format_type_be(array_typeid), format_type_be(actual_type))));
            array_typeid = actual_type;
        }
    }

    /*
	 * Fast Track: if none of the arguments are ANYARRAY or ANYELEMENT, return
	 * the unmodified rettype.
	 */
    if (!have_generics)
        return rettype;

    /* Get the element type based on the array type, if we have one */
    if (OidIsValid(array_typeid))
    {
        if (array_typeid == ANYARRAYOID && !have_anyelement)
        {
            /* Special case for ANYARRAY input: okay iff no ANYELEMENT */
            array_typelem = InvalidOid;
        }
        else
        {
            array_typelem = get_element_type(array_typeid);
            if (!OidIsValid(array_typelem))
                ereport(
                    ERROR,
                    (errcode(ERRCODE_DATATYPE_MISMATCH),
                     errmsg("argument declared \"anyarray\" is not an array but type %s", format_type_be(array_typeid))));
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
            ereport(
                ERROR,
                (errcode(ERRCODE_DATATYPE_MISMATCH),
                 errmsg("argument declared \"anyarray\" is not consistent with argument declared \"anyelement\""),
                 errdetail("%s versus %s", format_type_be(array_typeid), format_type_be(elem_typeid)),
                 errOmitLocation(true)));
        }
    }
    else if (!OidIsValid(elem_typeid))
    {
        /* Only way to get here is if all the generic args are UNKNOWN */
        ereport(
            ERROR,
            (errcode(ERRCODE_DATATYPE_MISMATCH),
             errmsg("could not determine anyarray/anyelement type because input has type \"unknown\""),
             errOmitLocation(true)));
    }

    /*
	 * If we had any unknown inputs, re-scan to assign correct types
	 */
    if (have_unknowns)
    {
        for (j = 0; j < nargs; j++)
        {
            Oid actual_type = actual_arg_types[j];

            if (actual_type != UNKNOWNOID)
                continue;

            if (declared_arg_types[j] == ANYELEMENTOID)
                declared_arg_types[j] = elem_typeid;
            else if (declared_arg_types[j] == ANYARRAYOID)
            {
                if (!OidIsValid(array_typeid))
                {
                    array_typeid = get_array_type(elem_typeid);
                    if (!OidIsValid(array_typeid))
                        ereport(
                            ERROR,
                            (errcode(ERRCODE_UNDEFINED_OBJECT),
                             errmsg("could not find array type for data type %s", format_type_be(elem_typeid))));
                }
                declared_arg_types[j] = array_typeid;
            }
        }
    }

    /* if we return ANYARRAYOID use the appropriate argument type */
    if (rettype == ANYARRAYOID)
    {
        if (!OidIsValid(array_typeid))
        {
            array_typeid = get_array_type(elem_typeid);
            if (!OidIsValid(array_typeid))
                ereport(
                    ERROR,
                    (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("could not find array type for data type %s", format_type_be(elem_typeid))));
        }
        return array_typeid;
    }

    /* if we return ANYELEMENTOID use the appropriate argument type */
    if (rettype == ANYELEMENTOID)
        return elem_typeid;

    /* we don't return a generic type; send back the original return type */
    return rettype;
};

}