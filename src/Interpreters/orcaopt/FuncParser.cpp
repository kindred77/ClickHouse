#include <Interpreters/orcaopt/FuncParser.h>

#include <Interpreters/orcaopt/CoerceParser.h>
#include <Interpreters/orcaopt/AggParser.h>
#include <Interpreters/orcaopt/ClauseParser.h>
#include <Interpreters/orcaopt/NodeParser.h>
#include <Interpreters/orcaopt/TypeParser.h>
#include <Interpreters/orcaopt/RelationParser.h>
#include <Interpreters/orcaopt/TargetParser.h>
#include <Interpreters/orcaopt/ExprParser.h>
#include <Interpreters/orcaopt/provider/TypeProvider.h>
#include <Interpreters/orcaopt/provider/ProcProvider.h>
#include <Interpreters/orcaopt/provider/AggProvider.h>
#include <Interpreters/orcaopt/provider/FunctionProvider.h>

#include <Interpreters/Context.h>

using namespace duckdb_libpgquery;

namespace DB
{

// FuncParser::FuncParser(const ContextPtr& context_) : context(context_)
// {
//     coerce_parser = std::make_shared<CoerceParser>(context);
//     agg_parser = std::make_shared<AggParser>(context);
//     clause_parser = std::make_shared<ClauseParser>(context);
//     node_parser = std::make_shared<NodeParser>(context);
//     type_parser = std::make_shared<TypeParser>(context);
//     relation_parser = std::make_shared<RelationParser>(context);
// 	target_parser = std::make_shared<TargetParser>(context);
//     expr_parser = std::make_shared<ExprParser>(context);
// };

PGNode *
FuncParser::ParseComplexProjection(PGParseState *pstate, const char *funcname, PGNode *first_arg,
					   int location)
{
    PGTupleDescPtr tupdesc = nullptr;
    int i;

    /*
	 * Special case for whole-row Vars so that we can resolve (foo.*).bar even
	 * when foo is a reference to a subselect, join, or RECORD function. A
	 * bonus is that we avoid generating an unnecessary FieldSelect; our
	 * result can omit the whole-row Var and just be a Var for the selected
	 * field.
	 *
	 * This case could be handled by expandRecordVariable, but it's more
	 * efficient to do it this way when possible.
	 */
    if (IsA(first_arg, PGVar) && ((PGVar *)first_arg)->varattno == InvalidAttrNumber)
    {
        PGRangeTblEntry * rte;

        rte = RelationParser::GetRTEByRangeTablePosn(pstate, ((PGVar *)first_arg)->varno, ((PGVar *)first_arg)->varlevelsup);
        /* Return a Var if funcname matches a column, else NULL */
        return RelationParser::scanRTEForColumn(pstate, rte, funcname, location);
    }

    /*
	 * Else do it the hard way with get_expr_result_type().
	 *
	 * If it's a Var of type RECORD, we have to work even harder: we have to
	 * find what the Var refers to, and pass that to get_expr_result_type.
	 * That task is handled by expandRecordVariable().
	 */
    if (IsA(first_arg, PGVar) && ((PGVar *)first_arg)->vartype == RECORDOID)
        tupdesc = TargetParser::expandRecordVariable(pstate, (PGVar *)first_arg, 0);
	//TODO kindred
	else
	{
        ereport(
            ERROR,
            (errcode(PG_ERRCODE_INVALID_COLUMN_REFERENCE),
             errmsg("Complex projection do not supported yet!")));
		return NULL;
    }
    //else if (get_expr_result_type(first_arg, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
    //    return NULL; /* unresolvable RECORD type */
    Assert(tupdesc != nullptr)

    for (i = 0; i < tupdesc->natts; i++)
    {
        PGAttrPtr att = tupdesc->attrs[i];

        if (strcmp(funcname, att->attname.c_str()) == 0 && !att->attisdropped)
        {
            /* Success, so generate a FieldSelect expression */
            PGFieldSelect * fselect = makeNode(PGFieldSelect);

            fselect->arg = (PGExpr *)first_arg;
            fselect->fieldnum = i + 1;
            fselect->resulttype = att->atttypid;
            fselect->resulttypmod = att->atttypmod;
            /* save attribute's collation for parse_collate.c */
            //TODO kindred
            //fselect->resultcollid = att->attcollation;
            return (PGNode *)fselect;
        }
    }

    return NULL; /* funcname does not match any column */
};

int
FuncParser::func_match_argtypes(int nargs,
					PGOid *input_typeids,
					FuncCandidateListPtr raw_candidates,
					FuncCandidateListPtr & candidates)
{
	FuncCandidateListPtr current_candidate;
	FuncCandidateListPtr next_candidate;
	int			ncandidates = 0;

	candidates = nullptr;

	for (current_candidate = raw_candidates;
		 current_candidate != NULL;
		 current_candidate = next_candidate)
	{
		next_candidate = current_candidate->next;
		if (CoerceParser::can_coerce_type(nargs, input_typeids, current_candidate->args,
							PG_COERCION_IMPLICIT))
		{
			current_candidate->next = candidates;
			candidates = current_candidate;
			ncandidates++;
		}
	}

	return ncandidates;
};

FuncCandidateListPtr
FuncParser::func_select_candidate(int nargs,
					  PGOid *input_typeids,
					  FuncCandidateListPtr & candidates)
{
	FuncCandidateListPtr current_candidate,
				first_candidate,
				last_candidate;
	PGOid		   *current_typeids;
	PGOid			current_type;
	int			i;
	int			ncandidates;
	int			nbestMatch,
				nmatch,
				nunknowns;
	PGOid			input_base_typeids[FUNC_MAX_ARGS];
	TYPCATEGORY slot_category[FUNC_MAX_ARGS],
				current_category;
	bool		current_is_preferred;
	bool		slot_has_preferred_type[FUNC_MAX_ARGS];
	bool		resolved_unknowns;

	/* protect local fixed-size arrays */
	if (nargs > FUNC_MAX_ARGS)
		ereport(ERROR,
				(errcode(PG_ERRCODE_TOO_MANY_ARGUMENTS),
				 errmsg("cannot pass more than %d argument to a function",
							   "cannot pass more than %d arguments to a function",
							   FUNC_MAX_ARGS,
							   FUNC_MAX_ARGS)));

	/*
	 * If any input types are domains, reduce them to their base types. This
	 * ensures that we will consider functions on the base type to be "exact
	 * matches" in the exact-match heuristic; it also makes it possible to do
	 * something useful with the type-category heuristics. Note that this
	 * makes it difficult, but not impossible, to use functions declared to
	 * take a domain as an input datatype.  Such a function will be selected
	 * over the base-type function only if it is an exact match at all
	 * argument positions, and so was already chosen by our caller.
	 *
	 * While we're at it, count the number of unknown-type arguments for use
	 * later.
	 */
	nunknowns = 0;
	for (i = 0; i < nargs; i++)
	{
		if (input_typeids[i] != UNKNOWNOID)
			input_base_typeids[i] = TypeProvider::getBaseType(input_typeids[i]);
		else
		{
			/* no need to call getBaseType on UNKNOWNOID */
			input_base_typeids[i] = UNKNOWNOID;
			nunknowns++;
		}
	}

	/*
	 * Run through all candidates and keep those with the most matches on
	 * exact types. Keep all candidates if none match.
	 */
	ncandidates = 0;
	nbestMatch = 0;
	last_candidate = NULL;
	for (current_candidate = candidates;
		 current_candidate != NULL;
		 current_candidate = current_candidate->next)
	{
		current_typeids = current_candidate->args;
		nmatch = 0;
		for (i = 0; i < nargs; i++)
		{
			if (input_base_typeids[i] != UNKNOWNOID &&
				current_typeids[i] == input_base_typeids[i])
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

	if (last_candidate)			/* terminate rebuilt list */
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
		slot_category[i] = CoerceParser::TypeCategory(input_base_typeids[i]);
	ncandidates = 0;
	nbestMatch = 0;
	last_candidate = NULL;
	for (current_candidate = candidates;
		 current_candidate != NULL;
		 current_candidate = current_candidate->next)
	{
		current_typeids = current_candidate->args;
		nmatch = 0;
		for (i = 0; i < nargs; i++)
		{
			if (input_base_typeids[i] != UNKNOWNOID)
			{
				if (current_typeids[i] == input_base_typeids[i] ||
					CoerceParser::IsPreferredType(slot_category[i], current_typeids[i]))
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

	if (last_candidate)			/* terminate rebuilt list */
		last_candidate->next = NULL;

	if (ncandidates == 1)
		return candidates;

	/*
	 * Still too many candidates?  Try assigning types for the unknown inputs.
	 *
	 * If there are no unknown inputs, we have no more heuristics that apply,
	 * and must fail.
	 */
	if (nunknowns == 0)
		return NULL;			/* failed to select a best candidate */

	/*
	 * The next step examines each unknown argument position to see if we can
	 * determine a "type category" for it.  If any candidate has an input
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
	 * wrong category at any unknown position.  Also, if at least one
	 * candidate accepted a preferred type at a position, remove candidates
	 * that accept non-preferred types.  If just one candidate remains, return
	 * that one.  However, if this rule turns out to reject all candidates,
	 * keep them all instead.
	 */
	resolved_unknowns = false;
	for (i = 0; i < nargs; i++)
	{
		bool		have_conflict;

		if (input_base_typeids[i] != UNKNOWNOID)
			continue;
		resolved_unknowns = true;	/* assume we can do it */
		slot_category[i] = TYPCATEGORY_INVALID;
		slot_has_preferred_type[i] = false;
		have_conflict = false;
		for (current_candidate = candidates;
			 current_candidate != NULL;
			 current_candidate = current_candidate->next)
		{
			current_typeids = current_candidate->args;
			current_type = current_typeids[i];
			TypeProvider::get_type_category_preferred(current_type,
										&current_category,
										&current_is_preferred);
			if (slot_category[i] == TYPCATEGORY_INVALID)
			{
				/* first candidate */
				slot_category[i] = current_category;
				slot_has_preferred_type[i] = current_is_preferred;
			}
			else if (current_category == slot_category[i])
			{
				/* more candidates in same category */
				slot_has_preferred_type[i] |= current_is_preferred;
			}
			else
			{
				/* category conflict! */
				if (current_category == TYPCATEGORY_STRING)
				{
					/* STRING always wins if available */
					slot_category[i] = current_category;
					slot_has_preferred_type[i] = current_is_preferred;
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
		if (have_conflict && slot_category[i] != TYPCATEGORY_STRING)
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
		first_candidate = candidates;
		last_candidate = NULL;
		for (current_candidate = candidates;
			 current_candidate != NULL;
			 current_candidate = current_candidate->next)
		{
			bool		keepit = true;

			current_typeids = current_candidate->args;
			for (i = 0; i < nargs; i++)
			{
				if (input_base_typeids[i] != UNKNOWNOID)
					continue;
				current_type = current_typeids[i];
				TypeProvider::get_type_category_preferred(current_type,
											&current_category,
											&current_is_preferred);
				if (current_category != slot_category[i])
				{
					keepit = false;
					break;
				}
				if (slot_has_preferred_type[i] && !current_is_preferred)
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
					first_candidate = current_candidate->next;
			}
		}

		/* if we found any matches, restrict our attention to those */
		if (last_candidate)
		{
			candidates = first_candidate;
			/* terminate rebuilt list */
			last_candidate->next = NULL;
		}

		if (ncandidates == 1)
			return candidates;
	}

	/*
	 * Last gasp: if there are both known- and unknown-type inputs, and all
	 * the known types are the same, assume the unknown inputs are also that
	 * type, and see if that gives us a unique match.  If so, use that match.
	 *
	 * NOTE: for a binary operator with one unknown and one non-unknown input,
	 * we already tried this heuristic in binary_oper_exact().  However, that
	 * code only finds exact matches, whereas here we will handle matches that
	 * involve coercion, polymorphic type resolution, etc.
	 */
	if (nunknowns < nargs)
	{
		PGOid			known_type = UNKNOWNOID;

		for (i = 0; i < nargs; i++)
		{
			if (input_base_typeids[i] == UNKNOWNOID)
				continue;
			if (known_type == UNKNOWNOID)	/* first known arg? */
				known_type = input_base_typeids[i];
			else if (known_type != input_base_typeids[i])
			{
				/* oops, not all match */
				known_type = UNKNOWNOID;
				break;
			}
		}

		if (known_type != UNKNOWNOID)
		{
			/* okay, just one known type, apply the heuristic */
			for (i = 0; i < nargs; i++)
				input_base_typeids[i] = known_type;
			ncandidates = 0;
			last_candidate = NULL;
			for (current_candidate = candidates;
				 current_candidate != NULL;
				 current_candidate = current_candidate->next)
			{
				current_typeids = current_candidate->args;
				if (CoerceParser::can_coerce_type(nargs, input_base_typeids, current_typeids,
									PG_COERCION_IMPLICIT))
				{
					if (++ncandidates > 1)
						break;	/* not unique, give up */
					last_candidate = current_candidate;
				}
			}
			if (ncandidates == 1)
			{
				/* successfully identified a unique match */
				last_candidate->next = NULL;
				return last_candidate;
			}
		}
	}

	return NULL;				/* failed to select a best candidate */
};

PGOid
FuncParser::FuncNameAsType(PGList *funcname)
{
	PGOid			result;

	PGTypePtr typtup = TypeParser::LookupTypeName(NULL, makeTypeNameFromNameList(funcname), NULL, false);
	if (typtup == NULL)
		return InvalidOid;

	if (typtup->typisdefined &&
		!OidIsValid(TypeParser::typeTypeRelid(typtup)))
		result = TypeParser::typeTypeId(typtup);
	else
		result = InvalidOid;

	return result;
};

FuncDetailCode
FuncParser::func_get_detail(PGList *funcname,
				PGList *fargs,
				PGList *fargnames,
				int nargs,
				PGOid *argtypes,
				bool expand_variadic,
				bool expand_defaults,
				PGOid *funcid,	/* return value */
				PGOid *rettype,	/* return value */
				bool *retset,	/* return value */
				int *nvargs,	/* return value */
				PGOid *vatype,	/* return value */
				PGOid **true_typeids, /* return value */
				PGList **argdefaults)
{
    FuncCandidateListPtr raw_candidates;
    FuncCandidateListPtr best_candidate;

    /* initialize output arguments to silence compiler warnings */
    *funcid = InvalidOid;
    *rettype = InvalidOid;
    *retset = false;
    *nvargs = 0;
    *vatype = InvalidOid;
    *true_typeids = NULL;
    if (argdefaults)
        *argdefaults = NIL;

    /* Get list of possible candidates from namespace search */
    raw_candidates = FunctionProvider::FuncnameGetCandidates(funcname, nargs, fargnames, expand_variadic, expand_defaults, false);

    /*
	 * Quickly check if there is an exact match to the input datatypes (there
	 * can be only one)
	 */
    for (best_candidate = raw_candidates; best_candidate != NULL; best_candidate = best_candidate->next)
    {
        if (memcmp(argtypes, best_candidate->args, nargs * sizeof(PGOid)) == 0)
            break;
    }

    if (best_candidate == NULL)
    {
        /*
		 * If we didn't find an exact match, next consider the possibility
		 * that this is really a type-coercion request: a single-argument
		 * function call where the function name is a type name.  If so, and
		 * if the coercion path is RELABELTYPE or COERCEVIAIO, then go ahead
		 * and treat the "function call" as a coercion.
		 *
		 * This interpretation needs to be given higher priority than
		 * interpretations involving a type coercion followed by a function
		 * call, otherwise we can produce surprising results. For example, we
		 * want "text(varchar)" to be interpreted as a simple coercion, not as
		 * "text(name(varchar))" which the code below this point is entirely
		 * capable of selecting.
		 *
		 * We also treat a coercion of a previously-unknown-type literal
		 * constant to a specific type this way.
		 *
		 * The reason we reject COERCION_PATH_FUNC here is that we expect the
		 * cast implementation function to be named after the target type.
		 * Thus the function will be found by normal lookup if appropriate.
		 *
		 * The reason we reject COERCION_PATH_ARRAYCOERCE is mainly that you
		 * can't write "foo[] (something)" as a function call.  In theory
		 * someone might want to invoke it as "_foo (something)" but we have
		 * never supported that historically, so we can insist that people
		 * write it as a normal cast instead.
		 *
		 * We also reject the specific case of COERCEVIAIO for a composite
		 * source type and a string-category target type.  This is a case that
		 * find_coercion_pathway() allows by default, but experience has shown
		 * that it's too commonly invoked by mistake.  So, again, insist that
		 * people use cast syntax if they want to do that.
		 *
		 * NB: it's important that this code does not exceed what coerce_type
		 * can do, because the caller will try to apply coerce_type if we
		 * return FUNCDETAIL_COERCION.  If we return that result for something
		 * coerce_type can't handle, we'll cause infinite recursion between
		 * this module and coerce_type!
		 */
        if (nargs == 1 && fargs != NIL && fargnames == NIL)
        {
            PGOid targetType = FuncNameAsType(funcname);

            if (OidIsValid(targetType))
            {
                PGOid sourceType = argtypes[0];
                PGNode * arg1 = (PGNode *)linitial(fargs);
                bool iscoercion;

                if (sourceType == UNKNOWNOID && IsA(arg1, PGConst))
                {
                    /* always treat typename('literal') as coercion */
                    iscoercion = true;
                }
                else
                {
                    PGCoercionPathType cpathtype;
                    PGOid cfuncid;

                    cpathtype = CoerceParser::find_coercion_pathway(targetType, sourceType, PG_COERCION_EXPLICIT, &cfuncid);
                    switch (cpathtype)
                    {
                        case PG_COERCION_PATH_RELABELTYPE:
                            iscoercion = true;
                            break;
                        case PG_COERCION_PATH_COERCEVIAIO:
                            if ((sourceType == RECORDOID || TypeParser::typeidTypeRelid(sourceType) != InvalidOid) && CoerceParser::TypeCategory(targetType) == TYPCATEGORY_STRING)
                                iscoercion = false;
                            else
                                iscoercion = true;
                            break;
                        default:
                            iscoercion = false;
                            break;
                    }
                }

                if (iscoercion)
                {
                    /* Treat it as a type coercion */
                    *funcid = InvalidOid;
                    *rettype = targetType;
                    *retset = false;
                    *nvargs = 0;
                    *vatype = InvalidOid;
                    *true_typeids = argtypes;
                    return FUNCDETAIL_COERCION;
                }
            }
        }

        /*
		 * didn't find an exact match, so now try to match up candidates...
		 */
        if (raw_candidates != NULL)
        {
            FuncCandidateListPtr current_candidates;
            int ncandidates;

            ncandidates = func_match_argtypes(nargs, argtypes, raw_candidates, current_candidates);

            /* one match only? then run with it... */
            if (ncandidates == 1)
                best_candidate = current_candidates;

            /*
			 * multiple candidates? then better decide or throw an error...
			 */
            else if (ncandidates > 1)
            {
                best_candidate = func_select_candidate(nargs, argtypes, current_candidates);

                /*
				 * If we were able to choose a best candidate, we're done.
				 * Otherwise, ambiguous function call.
				 */
                if (!best_candidate)
                    return FUNCDETAIL_MULTIPLE;
            }
        }
    }

    if (best_candidate)
    {
        //HeapTuple ftup;
        //Form_pg_proc pform;
        FuncDetailCode result;

        /*
		 * If processing named args or expanding variadics or defaults, the
		 * "best candidate" might represent multiple equivalently good
		 * functions; treat this case as ambiguous.
		 */
        if (!OidIsValid(best_candidate->oid))
            return FUNCDETAIL_MULTIPLE;

        /*
		 * We disallow VARIADIC with named arguments unless the last argument
		 * (the one with VARIADIC attached) actually matched the variadic
		 * parameter.  This is mere pedantry, really, but some folks insisted.
		 */
        if (fargnames != NIL && !expand_variadic && nargs > 0 && best_candidate->argnumbers[nargs - 1] != nargs - 1)
            return FUNCDETAIL_NOTFOUND;

        *funcid = best_candidate->oid;
        *nvargs = best_candidate->nvargs;
        *true_typeids = best_candidate->args;

        /*
		 * If processing named args, return actual argument positions into
		 * NamedArgExpr nodes in the fargs list.  This is a bit ugly but not
		 * worth the extra notation needed to do it differently.
		 */
        if (best_candidate->argnumbers != NULL)
        {
            int i = 0;
            PGListCell * lc;

            foreach (lc, fargs)
            {
                PGNamedArgExpr * na = (PGNamedArgExpr *)lfirst(lc);

                if (IsA(na, PGNamedArgExpr))
                    na->argnumber = best_candidate->argnumbers[i];
                i++;
            }
        }

        PGProcPtr ftup = ProcProvider::getProcByOid(best_candidate->oid);
        if (ftup == NULL) /* should not happen */
            elog(ERROR, "cache lookup failed for function %u", best_candidate->oid);
        *rettype = ftup->prorettype;
        *retset = ftup->proretset;
        *vatype = ftup->provariadic;
        /* fetch default args if caller wants 'em */
        if (argdefaults && best_candidate->ndargs > 0)
        {
			elog(ERROR, "Not supported yet!");
			//TODO kindred
            // Datum proargdefaults;
            // bool isnull;
            // char * str;
            // PGList * defaults;

            // /* shouldn't happen, FuncnameGetCandidates messed up */
            // if (best_candidate->ndargs > ftup->pronargdefaults)
            //     elog(ERROR, "not enough default arguments");

            // proargdefaults = SysCacheGetAttr(PROCOID, ftup, Anum_pg_proc_proargdefaults, &isnull);
            // Assert(!isnull)
            // str = TextDatumGetCString(proargdefaults);
            // defaults = (PGList *)stringToNode(str);
            // Assert(IsA(defaults, PGList))
            // pfree(str);

            // /* Delete any unused defaults from the returned list */
            // if (best_candidate->argnumbers != NULL)
            // {
            //     /*
			// 	 * This is a bit tricky in named notation, since the supplied
			// 	 * arguments could replace any subset of the defaults.  We
			// 	 * work by making a bitmapset of the argnumbers of defaulted
			// 	 * arguments, then scanning the defaults list and selecting
			// 	 * the needed items.  (This assumes that defaulted arguments
			// 	 * should be supplied in their positional order.)
			// 	 */
            //     PGBitmapset * defargnumbers;
            //     int * firstdefarg;
            //     PGList * newdefaults;
            //     PGListCell * lc;
            //     int i;

            //     defargnumbers = NULL;
            //     firstdefarg = &best_candidate->argnumbers[best_candidate->nargs - best_candidate->ndargs];
            //     for (i = 0; i < best_candidate->ndargs; i++)
            //         defargnumbers = bms_add_member(defargnumbers, firstdefarg[i]);
            //     newdefaults = NIL;
            //     i = ftup->pronargs - ftup->pronargdefaults;
            //     foreach (lc, defaults)
            //     {
            //         if (bms_is_member(i, defargnumbers))
            //             newdefaults = lappend(newdefaults, lfirst(lc));
            //         i++;
            //     }
            //     Assert(list_length(newdefaults) == best_candidate->ndargs)
            //     bms_free(defargnumbers);
            //     *argdefaults = newdefaults;
            // }
            // else
            // {
            //     /*
			// 	 * Defaults for positional notation are lots easier; just
			// 	 * remove any unwanted ones from the front.
			// 	 */
            //     int ndelete;

            //     ndelete = list_length(defaults) - best_candidate->ndargs;
            //     while (ndelete-- > 0)
            //         defaults = list_delete_first(defaults);
            //     *argdefaults = defaults;
            // }
        }
        if (ftup->proisagg)
            result = FUNCDETAIL_AGGREGATE;
        else if (ftup->proiswindow)
            result = FUNCDETAIL_WINDOWFUNC;
        else
            result = FUNCDETAIL_NORMAL;

        return result;
    }

    return FUNCDETAIL_NOTFOUND;
};


std::string
FuncParser::funcname_signature_string(const char *funcname, int nargs,
						  PGList *argnames, const PGOid *argtypes)
{
	//StringInfoData argbuf;
	int			numposargs;
	PGListCell   *lc;
	int			i;

	std::string result = std::string(funcname) + "(";
	//initStringInfo(&argbuf);

	//appendStringInfo(&argbuf, "%s(", funcname);

	numposargs = nargs - list_length(argnames);
	lc = list_head(argnames);

	for (i = 0; i < nargs; i++)
	{
		if (i)
		{
			//appendStringInfoString(&argbuf, ", ");
			result += ", ";
		}
			
		if (i >= numposargs)
		{
			//appendStringInfo(&argbuf, "%s => ", (char *) lfirst(lc));
			result += std::string((char *) lfirst(lc)) + " => ";
			lc = lnext(lc);
		}
		//appendStringInfoString(&argbuf, format_type_be(argtypes[i]));
		result += TypeProvider::format_type_be(argtypes[i]);
	}

	//appendStringInfoChar(&argbuf, ')');
	result += ")";

	return result;			/* return palloc'd string buffer */
};

std::string
FuncParser::func_signature_string(PGList *funcname, int nargs,
					  PGList *argnames, const PGOid *argtypes)
{
	return funcname_signature_string(PGNameListToString(funcname).c_str(),
									 nargs, argnames, argtypes).c_str();
};

void
FuncParser::unify_hypothetical_args(PGParseState *pstate,
						PGList *fargs,
						int numAggregatedArgs,
						PGOid *actual_arg_types,
						PGOid *declared_arg_types)
{
	PGNode	   *args[FUNC_MAX_ARGS];
	int			numDirectArgs,
				numNonHypotheticalArgs;
	int			i;
	PGListCell   *lc;

	numDirectArgs = list_length(fargs) - numAggregatedArgs;
	numNonHypotheticalArgs = numDirectArgs - numAggregatedArgs;
	/* safety check (should only trigger with a misdeclared agg) */
	if (numNonHypotheticalArgs < 0)
		elog(ERROR, "incorrect number of arguments to hypothetical-set aggregate");

	/* Deconstruct fargs into an array for ease of subscripting */
	i = 0;
	foreach(lc, fargs)
	{
		args[i++] = (PGNode *) lfirst(lc);
	}

	/* Check each hypothetical arg and corresponding aggregated arg */
	for (i = numNonHypotheticalArgs; i < numDirectArgs; i++)
	{
		int			aargpos = numDirectArgs + (i - numNonHypotheticalArgs);
		PGOid			commontype;

		/* A mismatch means AggregateCreate didn't check properly ... */
		if (declared_arg_types[i] != declared_arg_types[aargpos])
			elog(ERROR, "hypothetical-set aggregate has inconsistent declared argument types");

		/* No need to unify if make_fn_arguments will coerce */
		if (declared_arg_types[i] != ANYOID)
			continue;

		/*
		 * Select common type, giving preference to the aggregated argument's
		 * type (we'd rather coerce the direct argument once than coerce all
		 * the aggregated values).
		 */
		commontype = CoerceParser::select_common_type(pstate,
										list_make2(args[aargpos], args[i]),
										"WITHIN GROUP",
										NULL);

		/*
		 * Perform the coercions.  We don't need to worry about NamedArgExprs
		 * here because they aren't supported with aggregates.
		 */
		args[i] = CoerceParser::coerce_type(pstate,
							  args[i],
							  actual_arg_types[i],
							  commontype, -1,
							  PG_COERCION_IMPLICIT,
							  PG_COERCE_IMPLICIT_CAST,
							  -1);
		actual_arg_types[i] = commontype;
		args[aargpos] = CoerceParser::coerce_type(pstate,
									args[aargpos],
									actual_arg_types[aargpos],
									commontype, -1,
									PG_COERCION_IMPLICIT,
									PG_COERCE_IMPLICIT_CAST,
									-1);
		actual_arg_types[aargpos] = commontype;
	}

	/* Reconstruct fargs from array */
	i = 0;
	foreach(lc, fargs)
	{
		lfirst(lc) = args[i++];
	}
};

PGNode * FuncParser::ParseFuncOrColumn(PGParseState * pstate, PGList * funcname, 
		PGList * fargs, PGFuncCall * fn, int location)
{
    bool is_column = (fn == NULL);
    PGList * agg_order = (fn ? fn->agg_order : NIL);
    PGExpr * agg_filter = NULL;
    bool agg_within_group = (fn ? fn->agg_within_group : false);
    bool agg_star = (fn ? fn->agg_star : false);
    bool agg_distinct = (fn ? fn->agg_distinct : false);
    bool func_variadic = (fn ? fn->func_variadic : false);
    PGWindowDef * over = (fn ? fn->over : NULL);
    PGOid rettype;
    PGOid funcid;
    PGListCell * l;
    PGListCell * nextl;
    PGNode * first_arg = NULL;
    int nargs;
    int nargsplusdefs;
    PGOid actual_arg_types[FUNC_MAX_ARGS];
    PGOid * declared_arg_types;
    PGList * argnames;
    PGList * argdefaults;
    PGNode * retval;
    bool retset;
    int nvargs;
    PGOid vatype;
    FuncDetailCode fdresult;
    char aggkind = 0;

    /*
	 * If there's an aggregate filter, transform it using transformWhereClause
	 */
    if (fn && fn->agg_filter != NULL)
        agg_filter = (PGExpr *)ClauseParser::transformWhereClause(pstate, fn->agg_filter, EXPR_KIND_FILTER, "FILTER");

    /*
	 * Most of the rest of the parser just assumes that functions do not have
	 * more than FUNC_MAX_ARGS parameters.  We have to test here to protect
	 * against array overruns, etc.  Of course, this may not be a function,
	 * but the test doesn't hurt.
	 */
    if (list_length(fargs) > FUNC_MAX_ARGS)
        ereport(
            ERROR,
            (errcode(PG_ERRCODE_TOO_MANY_ARGUMENTS),
			//TODO kindred
             /* errmsg_plural(
                 "cannot pass more than %d argument to a function",
                 "cannot pass more than %d arguments to a function",
                 FUNC_MAX_ARGS,
                 FUNC_MAX_ARGS), */
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
        PGNode * arg = (PGNode *)lfirst(l);
        PGOid argtype = exprType(arg);

        nextl = lnext(l);

        if (argtype == VOIDOID && IsA(arg, PGParam) && !is_column && !agg_within_group)
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
    foreach (l, fargs)
    {
        PGNode * arg = (PGNode *)lfirst(l);

        if (IsA(arg, PGNamedArgExpr))
        {
            PGNamedArgExpr * na = (PGNamedArgExpr *)arg;
            PGListCell * lc;

            /* Reject duplicate arg names */
            foreach (lc, argnames)
            {
                if (strcmp(na->name, (char *)lfirst(lc)) == 0)
                    ereport(
                        ERROR,
                        (errcode(PG_ERRCODE_SYNTAX_ERROR),
                         errmsg("argument name \"%s\" used more than once", na->name),
                         parser_errposition(pstate, na->location)));
            }
            argnames = lappend(argnames, na->name);
        }
        else
        {
            if (argnames != NIL)
                ereport(
                    ERROR,
                    (errcode(PG_ERRCODE_SYNTAX_ERROR),
                     errmsg("positional argument cannot follow named argument"),
                     parser_errposition(pstate, exprLocation(arg))));
        }
    }

    if (fargs)
    {
        first_arg = (PGNode *)linitial(fargs);
        Assert(first_arg != NULL)
    }

    /*
	 * Check for column projection: if function has one argument, and that
	 * argument is of complex type, and function name is not qualified, then
	 * the "function call" could be a projection.  We also check that there
	 * wasn't any aggregate or variadic decoration, nor an argument name.
	 */
    if (nargs == 1 && agg_order == NIL && agg_filter == NULL && !agg_star && !agg_distinct && over == NULL && !func_variadic
        && argnames == NIL && list_length(funcname) == 1)
    {
        PGOid argtype = actual_arg_types[0];

        if (argtype == RECORDOID || TypeParser::typeidTypeRelid(argtype) != InvalidOid)
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
	 * the function.
	 *
	 * Note: for a named-notation or variadic function call, the reported
	 * "true" types aren't really what is in pg_proc: the types are reordered
	 * to match the given argument order of named arguments, and a variadic
	 * argument is replaced by a suitable number of copies of its element
	 * type.  We'll fix up the variadic case below.  We may also have to deal
	 * with default arguments.
	 */
    fdresult = func_get_detail(
        funcname,
        fargs,
        argnames,
        nargs,
        actual_arg_types,
        !func_variadic,
        true,
        &funcid,
        &rettype,
        &retset,
        &nvargs,
        &vatype,
        &declared_arg_types,
        &argdefaults);
    if (fdresult == FUNCDETAIL_COERCION)
    {
        /*
		 * We interpreted it as a type coercion. coerce_type can handle these
		 * cases, so why duplicate code...
		 */
        return CoerceParser::coerce_type(pstate, (PGNode *)linitial(fargs), actual_arg_types[0], rettype, -1, PG_COERCION_EXPLICIT, PG_COERCE_EXPLICIT_CALL, location);
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
                (errcode(PG_ERRCODE_WRONG_OBJECT_TYPE),
                 errmsg("%s(*) specified, but %s is not an aggregate function", PGNameListToString(funcname).c_str(), PGNameListToString(funcname).c_str()),
                 parser_errposition(pstate, location)));
        if (agg_distinct)
            ereport(
                ERROR,
                (errcode(PG_ERRCODE_WRONG_OBJECT_TYPE),
                 errmsg("DISTINCT specified, but %s is not an aggregate function", PGNameListToString(funcname).c_str()),
                 parser_errposition(pstate, location)));
        if (agg_within_group)
            ereport(
                ERROR,
                (errcode(PG_ERRCODE_WRONG_OBJECT_TYPE),
                 errmsg("WITHIN GROUP specified, but %s is not an aggregate function", PGNameListToString(funcname).c_str()),
                 parser_errposition(pstate, location)));
        if (agg_within_group)
            ereport(
                ERROR,
                (errcode(PG_ERRCODE_WRONG_OBJECT_TYPE),
                 errmsg("WITHIN GROUP specified, but %s is not an aggregate function", PGNameListToString(funcname).c_str()),
                 parser_errposition(pstate, location)));
        if (agg_order != NIL)
            ereport(
                ERROR,
                (errcode(PG_ERRCODE_WRONG_OBJECT_TYPE),
                 errmsg("ORDER BY specified, but %s is not an aggregate function", PGNameListToString(funcname).c_str()),
                 parser_errposition(pstate, location)));
        if (agg_filter)
            ereport(
                ERROR,
                (errcode(PG_ERRCODE_WRONG_OBJECT_TYPE),
                 errmsg("FILTER specified, but %s is not an aggregate function", PGNameListToString(funcname).c_str()),
                 parser_errposition(pstate, location)));
        if (over)
            ereport(
                ERROR,
                (errcode(PG_ERRCODE_WRONG_OBJECT_TYPE),
                 errmsg("OVER specified, but %s is not a window function nor an aggregate function", PGNameListToString(funcname).c_str()),
                 parser_errposition(pstate, location)));
    }
    else if (fdresult == FUNCDETAIL_AGGREGATE)
    {
        /*
		 * It's an aggregate; fetch needed info from the pg_aggregate entry.
		 */
        //HeapTuple tup;
        //Form_pg_aggregate classForm;
        int catDirectArgs;

        PGAggPtr tup = AggProvider::getAggByFuncOid(funcid);
        if (tup == NULL) /* should not happen */
            elog(ERROR, "cache lookup failed for aggregate %u", funcid);
        //classForm = (Form_pg_aggregate)GETSTRUCT(tup);
        aggkind = tup->aggkind;
        catDirectArgs = tup->aggnumdirectargs;

        /* Now check various disallowed cases. */
        if (AGGKIND_IS_ORDERED_SET(aggkind))
        {
            int numAggregatedArgs;
            int numDirectArgs;

            if (!agg_within_group)
                ereport(
                    ERROR,
                    (errcode(PG_ERRCODE_WRONG_OBJECT_TYPE),
                     errmsg("WITHIN GROUP is required for ordered-set aggregate %s", PGNameListToString(funcname).c_str()),
                     parser_errposition(pstate, location)));
            if (over)
                ereport(
                    ERROR,
                    (errcode(PG_ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg("OVER is not supported for ordered-set aggregate %s", PGNameListToString(funcname).c_str()),
                     parser_errposition(pstate, location)));
            /* gram.y rejects DISTINCT + WITHIN GROUP */
            Assert(!agg_distinct)
            /* gram.y rejects VARIADIC + WITHIN GROUP */
            Assert(!func_variadic)

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
            Assert(numDirectArgs >= 0)

            if (!OidIsValid(vatype))
            {
                /* Test is simple if aggregate isn't variadic */
                if (numDirectArgs != catDirectArgs)
                    ereport(
                        ERROR,
                        (errcode(PG_ERRCODE_UNDEFINED_FUNCTION),
                         errmsg("function %s does not exist", func_signature_string(funcname, nargs, argnames, actual_arg_types).c_str()),
                         errmsg(
                             "There is an ordered-set aggregate %s, but it requires %d direct arguments, not %d.",
                             PGNameListToString(funcname).c_str(),
                             catDirectArgs,
                             numDirectArgs),
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
                int pronargs;

                pronargs = nargs;
                if (nvargs > 1)
                    pronargs -= nvargs - 1;
                if (catDirectArgs < pronargs)
                {
                    /* VARIADIC isn't part of direct args, so still easy */
                    if (numDirectArgs != catDirectArgs)
                        ereport(
                            ERROR,
                            (errcode(PG_ERRCODE_UNDEFINED_FUNCTION),
                             errmsg("function %s does not exist", func_signature_string(funcname, nargs, argnames, actual_arg_types).c_str()),
                             errmsg(
                                 "There is an ordered-set aggregate %s, but it requires %d direct arguments, not %d.",
                                 PGNameListToString(funcname).c_str(),
                                 catDirectArgs,
                                 numDirectArgs),
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
                            ereport(
                                ERROR,
                                (errcode(PG_ERRCODE_UNDEFINED_FUNCTION),
                                 errmsg("function %s does not exist", func_signature_string(funcname, nargs, argnames, actual_arg_types).c_str()),
                                 errmsg(
                                     "To use the hypothetical-set aggregate %s, the number of hypothetical direct arguments (here %d) must "
                                     "match the number of ordering columns (here %d).",
                                     PGNameListToString(funcname).c_str(),
                                     nvargs - numAggregatedArgs,
                                     numAggregatedArgs),
                                 parser_errposition(pstate, location)));
                    }
                    else
                    {
                        if (nvargs <= numAggregatedArgs)
                            ereport(
                                ERROR,
                                (errcode(PG_ERRCODE_UNDEFINED_FUNCTION),
                                 errmsg("function %s does not exist", func_signature_string(funcname, nargs, argnames, actual_arg_types).c_str()),
                                 errmsg(
                                     "There is an ordered-set aggregate %s, but it requires at least %d direct arguments.",
                                     PGNameListToString(funcname).c_str(),
                                     catDirectArgs),
                                 parser_errposition(pstate, location)));
                    }
                }
            }

            /* Check type matching of hypothetical arguments */
            if (aggkind == AGGKIND_HYPOTHETICAL)
                unify_hypothetical_args(pstate, fargs, numAggregatedArgs, actual_arg_types, declared_arg_types);
        }
        else
        {
            /* Normal aggregate, so it can't have WITHIN GROUP */
            if (agg_within_group)
                ereport(
                    ERROR,
                    (errcode(PG_ERRCODE_WRONG_OBJECT_TYPE),
                     errmsg("%s is not an ordered-set aggregate, so it cannot have WITHIN GROUP", PGNameListToString(funcname).c_str()),
                     parser_errposition(pstate, location)));
        }
    }
    else if (fdresult == FUNCDETAIL_WINDOWFUNC)
    {
        /*
		 * True window functions must be called with a window definition.
		 */
        if (!over)
            ereport(
                ERROR,
                (errcode(PG_ERRCODE_WRONG_OBJECT_TYPE),
                 errmsg("window function %s requires an OVER clause", PGNameListToString(funcname).c_str()),
                 parser_errposition(pstate, location)));
        /* And, per spec, WITHIN GROUP isn't allowed */
        if (agg_within_group)
            ereport(
                ERROR,
                (errcode(PG_ERRCODE_WRONG_OBJECT_TYPE),
                 errmsg("window function %s cannot have WITHIN GROUP", PGNameListToString(funcname).c_str()),
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
            ereport(
                ERROR,
                (errcode(PG_ERRCODE_AMBIGUOUS_FUNCTION),
                 errmsg("function %s is not unique", func_signature_string(funcname, nargs, argnames, actual_arg_types).c_str()),
                 errhint("Could not choose a best candidate function. "
                         "You might need to add explicit type casts."),
                 parser_errposition(pstate, location)));
        else if (list_length(agg_order) > 1 && !agg_within_group)
        {
            /* It's agg(x, ORDER BY y,z) ... perhaps misplaced ORDER BY */
            ereport(
                ERROR,
                (errcode(PG_ERRCODE_UNDEFINED_FUNCTION),
                 errmsg("function %s does not exist", func_signature_string(funcname, nargs, argnames, actual_arg_types).c_str()),
                 errhint("No aggregate function matches the given name and argument types. "
                         "Perhaps you misplaced ORDER BY; ORDER BY must appear "
                         "after all regular arguments of the aggregate."),
                 parser_errposition(pstate, location)));
        }
        else
            ereport(
                ERROR,
                (errcode(PG_ERRCODE_UNDEFINED_FUNCTION),
                 errmsg("function %s does not exist", func_signature_string(funcname, nargs, argnames, actual_arg_types).c_str()),
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
    foreach (l, argdefaults)
    {
        PGNode * expr = (PGNode *)lfirst(l);

        /* probably shouldn't happen ... */
        if (nargsplusdefs >= FUNC_MAX_ARGS)
            ereport(
                ERROR,
                (errcode(PG_ERRCODE_TOO_MANY_ARGUMENTS),
                 /* errmsg_plural(
                     "cannot pass more than %d argument to a function",
                     "cannot pass more than %d arguments to a function",
                     FUNC_MAX_ARGS,
                     FUNC_MAX_ARGS), */
                 parser_errposition(pstate, location)));

        actual_arg_types[nargsplusdefs++] = exprType(expr);
    }

    /*
	 * enforce consistency with polymorphic argument and return types,
	 * possibly adjusting return type or declared_arg_types (which will be
	 * used as the cast destination by make_fn_arguments)
	 */
    rettype = CoerceParser::enforce_generic_type_consistency(actual_arg_types, declared_arg_types, nargsplusdefs, rettype, false);

    /* perform the necessary typecasting of arguments */
    make_fn_arguments(pstate, fargs, actual_arg_types, declared_arg_types);

    /*
	 * If the function isn't actually variadic, forget any VARIADIC decoration
	 * on the call.  (Perhaps we should throw an error instead, but
	 * historically we've allowed people to write that.)
	 */
    if (!OidIsValid(vatype))
    {
        Assert(nvargs == 0)
        func_variadic = false;
    }

    /*
	 * If it's a variadic function call, transform the last nvargs arguments
	 * into an array --- unless it's an "any" variadic.
	 */
    if (nvargs > 0 && vatype != ANYOID)
    {
        PGArrayExpr * newa = makeNode(PGArrayExpr);
        int non_var_args = nargs - nvargs;
        PGList * vargs;

        Assert(non_var_args >= 0)
        vargs = list_copy_tail(fargs, non_var_args);
        fargs = list_truncate(fargs, non_var_args);

        newa->elements = vargs;
        /* assume all the variadic arguments were coerced to the same type */
        newa->element_typeid = exprType((PGNode *)linitial(vargs));
        newa->array_typeid = TypeProvider::get_array_type(newa->element_typeid);
        if (!OidIsValid(newa->array_typeid))
            ereport(
                ERROR,
                (errcode(PG_ERRCODE_UNDEFINED_OBJECT),
                 errmsg("could not find array type for data type %s", TypeProvider::format_type_be(newa->element_typeid).c_str()),
                 parser_errposition(pstate, exprLocation((PGNode *)vargs))));
        /* array_collid will be set by parse_collate.c */
        newa->multidims = false;
        newa->location = exprLocation((PGNode *)vargs);

        fargs = lappend(fargs, newa);

        /* We could not have had VARIADIC marking before ... */
        Assert(!func_variadic)
        /* ... but now, it's a VARIADIC call */
        func_variadic = true;
    }

    /*
	 * If an "any" variadic is called with explicit VARIADIC marking, insist
	 * that the variadic parameter be of some array type.
	 */
    if (nargs > 0 && vatype == ANYOID && func_variadic)
    {
        PGOid va_arr_typid = actual_arg_types[nargs - 1];

        if (!OidIsValid(TypeProvider::get_base_element_type(va_arr_typid)))
            ereport(
                ERROR,
                (errcode(PG_ERRCODE_DATATYPE_MISMATCH),
                 errmsg("VARIADIC argument must be an array"),
                 parser_errposition(pstate, exprLocation((PGNode *)llast(fargs)))));
    }

    if (retset)
        check_srf_call_placement(pstate, location);

    /* build the appropriate output structure */
    if (fdresult == FUNCDETAIL_NORMAL)
    {
        PGFuncExpr * funcexpr = makeNode(PGFuncExpr);

        funcexpr->funcid = funcid;
        funcexpr->funcresulttype = rettype;
        funcexpr->funcretset = retset;
        funcexpr->funcvariadic = func_variadic;
        funcexpr->funcformat = PG_COERCE_EXPLICIT_CALL;
        /* funccollid and inputcollid will be set by parse_collate.c */
        funcexpr->args = fargs;
        funcexpr->location = location;

        retval = (PGNode *)funcexpr;
    }
    else if (fdresult == FUNCDETAIL_AGGREGATE && !over)
    {
        /* aggregate function */
        PGAggref * aggref = makeNode(PGAggref);

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
							PGNameListToString(funcname).c_str()),
					 parser_errposition(pstate, location)));
#endif

        if (retset)
            ereport(
                ERROR,
                (errcode(PG_ERRCODE_INVALID_FUNCTION_DEFINITION),
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
            ereport(
                ERROR,
                (errcode(PG_ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("aggregates cannot use named arguments"),
                 parser_errposition(pstate, location)));

        /* parse_agg.c does additional aggregate-specific processing */
        AggParser::transformAggregateCall(pstate, aggref, fargs, agg_order, agg_distinct);

        retval = (PGNode *)aggref;
    }
    else
    {
        /* window function */
        PGWindowFunc * wfunc = makeNode(PGWindowFunc);

        Assert(over) /* lack of this was checked above */
        Assert(!agg_within_group) /* also checked above */

        wfunc->winfnoid = funcid;
        wfunc->wintype = rettype;
        /* wincollid and inputcollid will be set by parse_collate.c */
        wfunc->args = fargs;
        /* winref will be set by transformWindowFuncCall */
        wfunc->winstar = agg_star;
        wfunc->winagg = (fdresult == FUNCDETAIL_AGGREGATE);
        wfunc->aggfilter = agg_filter;
        wfunc->location = location;

		//TODO kindred
        //wfunc->windistinct = agg_distinct;

        /*
		 * agg_star is allowed for aggregate functions but distinct isn't
		 *
		 * GPDB: We have implemented this in GPDB, with some limitations.
		 */
        if (agg_distinct)
        {
            if (fdresult == FUNCDETAIL_WINDOWFUNC)
                ereport(
                    ERROR,
                    (errcode(PG_ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg("DISTINCT is not implemented for window functions"),
                     parser_errposition(pstate, location)));

            if (list_length(fargs) != 1)
                ereport(
                    ERROR,
                    (errcode(PG_ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("DISTINCT is supported only for single-argument window aggregates")));
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
					(errcode(PG_ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("%s(*) must be used to call a parameterless aggregate function",
							PGNameListToString(funcname).c_str()),
					 parser_errposition(pstate, location)));
#endif

        /*
		 * ordered aggs not allowed in windows yet
		 */
        if (agg_order != NIL)
            ereport(
                ERROR,
                (errcode(PG_ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("aggregate ORDER BY is not implemented for window functions"),
                 parser_errposition(pstate, location)));

        /*
		 * FILTER is not yet supported with true window functions
		 */
        if (!wfunc->winagg && agg_filter)
            ereport(
                ERROR,
                (errcode(PG_ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("FILTER is not implemented for non-aggregate window functions"),
                 parser_errposition(pstate, location)));

        if (retset)
            ereport(
                ERROR,
                (errcode(PG_ERRCODE_INVALID_FUNCTION_DEFINITION),
                 errmsg("window functions cannot return sets"),
                 parser_errposition(pstate, location)));

        /* parse_agg.c does additional window-func-specific processing */
        AggParser::transformWindowFuncCall(pstate, wfunc, over);

        retval = (PGNode *)wfunc;
    }

    /*
	 * Mark the context if this is a dynamic typed function, if so we mustn't
	 * allow views to be created from this statement because we cannot 
	 * guarantee that the future return type will be the same as the current
	 * return type.
	 */
	//TODO kindred
    // if (TypeSupportsDescribe(rettype))
    // {
    //     Oid DescribeFuncOid = lookupProcCallback(funcid, PROMETHOD_DESCRIBE);
    //     if (OidIsValid(DescribeFuncOid))
    //     {
    //         PGParseState * state = pstate;

    //         for (state = pstate; state; state = state->parentParseState)
    //             state->p_hasDynamicFunction = true;
    //     }
    // }

    /*
	 * If this function has restrictions on where it can be executed
	 * (EXECUTE ON MASTER or EXECUTE ON ALL SEGMENTS), make note of that,
	 * so that the planner knows to be prepared for it.
	 */
	//TODO kindred
    // if (func_exec_location(funcid) != PROEXECLOCATION_ANY)
    //     pstate->p_hasFuncsWithExecRestrictions = true;

    return retval;
};

void
FuncParser::check_srf_call_placement(PGParseState * pstate, int location)
{
    const char * err;
    bool errkind;

    /*
	 * Check to see if the set-returning function is in an invalid place
	 * within the query.  Basically, we don't allow SRFs anywhere except in
	 * the targetlist (which includes GROUP BY/ORDER BY expressions), VALUES,
	 * and functions in FROM.
	 *
	 * For brevity we support two schemes for reporting an error here: set
	 * "err" to a custom message, or set "errkind" true if the error context
	 * is sufficiently identified by what ParseExprKindName will return, *and*
	 * what it will return is just a SQL keyword.  (Otherwise, use a custom
	 * message to avoid creating translation problems.)
	 */
    err = NULL;
    errkind = false;
    switch (pstate->p_expr_kind)
    {
        case EXPR_KIND_NONE:
            Assert(false) /* can't happen */
            break;
        case EXPR_KIND_OTHER:
            /* Accept SRF here; caller must throw error if wanted */
            break;
        case EXPR_KIND_JOIN_ON:
        case EXPR_KIND_JOIN_USING:
            err = _("set-returning functions are not allowed in JOIN conditions");
            break;
        case EXPR_KIND_FROM_SUBSELECT:
            /* can't get here, but just in case, throw an error */
            errkind = true;
            break;
        case EXPR_KIND_FROM_FUNCTION:
            break;
        case EXPR_KIND_WHERE:
            errkind = true;
            break;
        case EXPR_KIND_HAVING:
            errkind = true;
            break;
        case EXPR_KIND_FILTER:
            errkind = true;
            break;
        case EXPR_KIND_WINDOW_PARTITION:
        case EXPR_KIND_WINDOW_ORDER:
            break;
        case EXPR_KIND_WINDOW_FRAME_RANGE:
        case EXPR_KIND_WINDOW_FRAME_ROWS:
            err = _("set-returning functions are not allowed in window definitions");
            break;
        case EXPR_KIND_SELECT_TARGET:
        case EXPR_KIND_INSERT_TARGET:
            break;
        case EXPR_KIND_UPDATE_SOURCE:
        case EXPR_KIND_UPDATE_TARGET:
            /* disallowed because it would be ambiguous what to do */
            errkind = true;
            break;
        case EXPR_KIND_GROUP_BY:
        case EXPR_KIND_ORDER_BY:
        case EXPR_KIND_DISTINCT_ON:
            break;
        case EXPR_KIND_LIMIT:
        case EXPR_KIND_OFFSET:
            errkind = true;
            break;
        case EXPR_KIND_RETURNING:
            errkind = true;
            break;
        case EXPR_KIND_VALUES:
            break;
        case EXPR_KIND_CHECK_CONSTRAINT:
        case EXPR_KIND_DOMAIN_CHECK:
            err = _("set-returning functions are not allowed in check constraints");
            break;
        case EXPR_KIND_COLUMN_DEFAULT:
        case EXPR_KIND_FUNCTION_DEFAULT:
            err = _("set-returning functions are not allowed in DEFAULT expressions");
            break;
        case EXPR_KIND_INDEX_EXPRESSION:
            err = _("set-returning functions are not allowed in index expressions");
            break;
        case EXPR_KIND_INDEX_PREDICATE:
            err = _("set-returning functions are not allowed in index predicates");
            break;
        case EXPR_KIND_ALTER_COL_TRANSFORM:
            err = _("set-returning functions are not allowed in transform expressions");
            break;
        case EXPR_KIND_EXECUTE_PARAMETER:
            err = _("set-returning functions are not allowed in EXECUTE parameters");
            break;
        case EXPR_KIND_TRIGGER_WHEN:
            err = _("set-returning functions are not allowed in trigger WHEN conditions");
            break;
        case EXPR_KIND_PARTITION_EXPRESSION:
            err = _("set-returning functions are not allowed in partition key expressions");
            break;

        case EXPR_KIND_SCATTER_BY:
            err = _("set-returning functions are not allowed in scatter by expressions");
            break;

            /*
			 * There is intentionally no default: case here, so that the
			 * compiler will warn if we add a new ParseExprKind without
			 * extending this switch.  If we do see an unrecognized value at
			 * runtime, the behavior will be the same as for EXPR_KIND_OTHER,
			 * which is sane anyway.
			 */
    }
    if (err)
        ereport(ERROR, (errcode(PG_ERRCODE_FEATURE_NOT_SUPPORTED), errmsg_internal("%s", err), parser_errposition(pstate, location)));
    if (errkind)
        ereport(
            ERROR,
            (errcode(PG_ERRCODE_FEATURE_NOT_SUPPORTED),
             /* translator: %s is name of a SQL construct, eg GROUP BY */
             errmsg("set-returning functions are not allowed in %s", ExprParser::ParseExprKindName(pstate->p_expr_kind)),
             parser_errposition(pstate, location)));
};

void
FuncParser::make_fn_arguments(PGParseState *pstate,
				  PGList *fargs,
				  PGOid *actual_arg_types,
				  PGOid *declared_arg_types)
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

				node = CoerceParser::coerce_type(pstate,
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
				node = CoerceParser::coerce_type(pstate,
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
