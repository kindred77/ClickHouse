#include <Interpreters/orcaopt/ExprParser.h>

#include <Interpreters/orcaopt/RelationParser.h>
#include <Interpreters/orcaopt/CoerceParser.h>
#include <Interpreters/orcaopt/SelectParser.h>
#include <Interpreters/orcaopt/TargetParser.h>
#include <Interpreters/orcaopt/TypeParser.h>
#include <Interpreters/orcaopt/FuncParser.h>
#include <Interpreters/orcaopt/OperParser.h>
//#include <Interpreters/orcaopt/CollationParser.h>
#include <Interpreters/orcaopt/NodeParser.h>
#include <Interpreters/orcaopt/AggParser.h>
#include <Interpreters/orcaopt/provider/TypeProvider.h>
#include <Interpreters/orcaopt/provider/RelationProvider.h>

#include <Interpreters/Context.h>

using namespace duckdb_libpgquery;

namespace DB
{

ExprParser::ExprParser(const ContextPtr& context_) : context(context_)
{
	relation_parser = std::make_shared<RelationParser>(context);
	coerce_parser = std::make_shared<CoerceParser>(context);
	select_parser = std::make_shared<SelectParser>(context);
	target_parser = std::make_shared<TargetParser>(context);
	type_parser = std::make_shared<TypeParser>(context);
	func_parser = std::make_shared<FuncParser>(context);
	oper_parser = std::make_shared<OperParser>(context);
	node_parser = std::make_shared<NodeParser>(context);
	agg_parser = std::make_shared<AggParser>(context);
	//type_provider = std::make_shared<TypeProvider>(context);
	//relation_provider = std::make_shared<RelationProvider>(context);
};

bool
ExprParser::exprIsNullConstant(PGNode *arg)
{
	if (arg && IsA(arg, PGAConst))
	{
		PGAConst    *con = (PGAConst *) arg;

		if (con->val.type == T_PGNull)
			return true;
	}
	return false;
};

PGNode *
ExprParser::transformExpr(PGParseState *pstate, PGNode *expr, PGParseExprKind exprKind)
{
    PGNode	   *result;
	PGParseExprKind sv_expr_kind;

	/* Save and restore identity of expression type we're parsing */
	Assert(exprKind != EXPR_KIND_NONE)
	sv_expr_kind = pstate->p_expr_kind;
	pstate->p_expr_kind = exprKind;

	result = transformExprRecurse(pstate, expr);

	pstate->p_expr_kind = sv_expr_kind;

	return result;
};

PGNode *
ExprParser::transformWholeRowRef(PGParseState *pstate, PGRangeTblEntry *rte, int location)
{
	PGVar		   *result;
	int			vnum;
	int			sublevels_up;

	/* Find the RTE's rangetable location */
	vnum = relation_parser->RTERangeTablePosn(pstate, rte, &sublevels_up);

	/*
	 * Build the appropriate referencing node.  Note that if the RTE is a
	 * function returning scalar, we create just a plain reference to the
	 * function value, not a composite containing a single column.  This is
	 * pretty inconsistent at first sight, but it's what we've done
	 * historically.  One argument for it is that "rel" and "rel.*" mean the
	 * same thing for composite relations, so why not for scalar functions...
	 */
	PGOid relTypeOid = InvalidOid;
	bool isRowType = false;
	if (rte->rtekind == PG_RTE_RELATION)
	{
		relTypeOid = RelationProvider::get_rel_type_id(rte->relid);
	}
	else if (rte->rtekind == PG_RTE_FUNCTION)
	{
		PGNode* fexpr = ((PGRangeTblFunction *) linitial(rte->functions))->funcexpr;
		PGOid toid = exprType(fexpr);
		isRowType = TypeProvider::type_is_rowtype(toid);
	}
	
	result = makeWholeRowVar(rte, vnum, sublevels_up, true, relTypeOid, isRowType);

	/* location is not filled in by makeWholeRowVar */
	result->location = location;

	/* mark relation as requiring whole-row SELECT access */
	relation_parser->markVarForSelectPriv(pstate, result, rte);

	return (PGNode *) result;
};

PGNode *
ExprParser::transformColumnRef(PGParseState *pstate, PGColumnRef *cref)
{
    PGNode * node = NULL;
    char * nspname = NULL;
    char * relname = NULL;
    char * colname = NULL;
    PGRangeTblEntry * rte;
    int levels_up;
    enum
    {
        CRERR_NO_COLUMN,
        CRERR_NO_RTE,
        CRERR_WRONG_DB,
        CRERR_TOO_MANY
    } crerr
        = CRERR_NO_COLUMN;

    /*
	 * Give the PreParseColumnRefHook, if any, first shot.  If it returns
	 * non-null then that's all, folks.
	 */
    // if (pstate->p_pre_columnref_hook != NULL)
    // {
    //     node = (*pstate->p_pre_columnref_hook)(pstate, cref);
    //     if (node != NULL)
    //         return node;
    // }

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
    switch (list_length(cref->fields))
    {
        case 1: {
            PGNode * field1 = (PGNode *)linitial(cref->fields);

            Assert(IsA(field1, PGString))
            colname = strVal(field1);

            /* Try to identify as an unqualified column */
            node = relation_parser->colNameToVar(pstate, colname, false, cref->location);

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
                if (pstate->p_value_substitute != NULL && strcmp(colname, "value") == 0)
                {
                        node = (PGNode *)copyObject(pstate->p_value_substitute);

                        /*
						 * Try to propagate location knowledge.  This should
						 * be extended if p_value_substitute can ever take on
						 * other node types.
						 */
                        if (IsA(node, PGCoerceToDomainValue))
                            ((PGCoerceToDomainValue *)node)->location = cref->location;
                        break;
                }

                /*
					 * Try to find the name as a relation.  Note that only
					 * relations already entered into the rangetable will be
					 * recognized.
					 *
					 * This is a hack for backwards compatibility with
					 * PostQUEL-inspired syntax.  The preferred form now is
					 * "rel.*".
					 */
                rte = relation_parser->refnameRangeTblEntry(pstate, NULL, colname, cref->location, &levels_up);
                if (rte)
                        node = transformWholeRowRef(pstate, rte, cref->location);
            }
            break;
        }
        case 2: {
            PGNode * field1 = (PGNode *)linitial(cref->fields);
            PGNode * field2 = (PGNode *)lsecond(cref->fields);

            Assert(IsA(field1, PGString))
            relname = strVal(field1);

            /* Locate the referenced RTE */
            rte = relation_parser->refnameRangeTblEntry(pstate, nspname, relname, cref->location, &levels_up);
            if (rte == NULL)
            {
                crerr = CRERR_NO_RTE;
                break;
            }

            /* Whole-row reference? */
            if (IsA(field2, PGAStar))
            {
                node = transformWholeRowRef(pstate, rte, cref->location);
                break;
            }

            Assert(IsA(field2, PGString))
            colname = strVal(field2);

            /* Try to identify as a column of the RTE */
            node = relation_parser->scanRTEForColumn(pstate, rte, colname, cref->location);
            if (node == NULL)
            {
                /* Try it as a function call on the whole row */
                node = transformWholeRowRef(pstate, rte, cref->location);
                node = func_parser->ParseFuncOrColumn(pstate, list_make1(makeString(colname)), list_make1(node), NULL, cref->location);
            }
            break;
        }
        case 3: {
            PGNode * field1 = (PGNode *)linitial(cref->fields);
            PGNode * field2 = (PGNode *)lsecond(cref->fields);
            PGNode * field3 = (PGNode *)lthird(cref->fields);

            Assert(IsA(field1, PGString))
            nspname = strVal(field1);
            Assert(IsA(field2, PGString))
            relname = strVal(field2);

            /* Locate the referenced RTE */
            rte = relation_parser->refnameRangeTblEntry(pstate, nspname, relname, cref->location, &levels_up);
            if (rte == NULL)
            {
                crerr = CRERR_NO_RTE;
                break;
            }

            /* Whole-row reference? */
            if (IsA(field3, PGAStar))
            {
                node = transformWholeRowRef(pstate, rte, cref->location);
                break;
            }

            Assert(IsA(field3, PGString))
            colname = strVal(field3);

            /* Try to identify as a column of the RTE */
            node = relation_parser->scanRTEForColumn(pstate, rte, colname, cref->location);
            if (node == NULL)
            {
                /* Try it as a function call on the whole row */
                node = transformWholeRowRef(pstate, rte, cref->location);
                node = func_parser->ParseFuncOrColumn(pstate, list_make1(makeString(colname)), list_make1(node), NULL, cref->location);
            }
            break;
        }
        case 4: {
            PGNode * field1 = (PGNode *)linitial(cref->fields);
            PGNode * field2 = (PGNode *)lsecond(cref->fields);
            PGNode * field3 = (PGNode *)lthird(cref->fields);
            PGNode * field4 = (PGNode *)lfourth(cref->fields);
            char * catname;

            Assert(IsA(field1, PGString))
            catname = strVal(field1);
            Assert(IsA(field2, PGString))
            nspname = strVal(field2);
            Assert(IsA(field3, PGString))
            relname = strVal(field3);

            /*
				 * We check the catalog name and then ignore it.
				 */
            if (strcmp(catname, RelationProvider::get_database_name(MyDatabaseId).c_str()) != 0)
            {
                crerr = CRERR_WRONG_DB;
                break;
            }

            /* Locate the referenced RTE */
            rte = relation_parser->refnameRangeTblEntry(pstate, nspname, relname, cref->location, &levels_up);
            if (rte == NULL)
            {
                crerr = CRERR_NO_RTE;
                break;
            }

            /* Whole-row reference? */
            if (IsA(field4, PGAStar))
            {
                node = transformWholeRowRef(pstate, rte, cref->location);
                break;
            }

            Assert(IsA(field4, PGString))
            colname = strVal(field4);

            /* Try to identify as a column of the RTE */
            node = relation_parser->scanRTEForColumn(pstate, rte, colname, cref->location);
            if (node == NULL)
            {
                /* Try it as a function call on the whole row */
                node = transformWholeRowRef(pstate, rte, cref->location);
                node = func_parser->ParseFuncOrColumn(pstate, list_make1(makeString(colname)), list_make1(node), NULL, cref->location);
            }
            break;
        }
        default:
            crerr = CRERR_TOO_MANY; /* too many dotted names */
            break;
    }

    /*
	 * Now give the PostParseColumnRefHook, if any, a chance.  We pass the
	 * translation-so-far so that it can throw an error if it wishes in the
	 * case that it has a conflicting interpretation of the ColumnRef. (If it
	 * just translates anyway, we'll throw an error, because we can't undo
	 * whatever effects the preceding steps may have had on the pstate.) If it
	 * returns NULL, use the standard translation, or throw a suitable error
	 * if there is none.
	 */
    // if (pstate->p_post_columnref_hook != NULL)
    // {
    //     PGNode * hookresult;

    //     hookresult = (*pstate->p_post_columnref_hook)(pstate, cref, node);
    //     if (node == NULL)
    //         node = hookresult;
    //     else if (hookresult != NULL)
    //         ereport(
    //             ERROR,
    //             (errcode(ERRCODE_AMBIGUOUS_COLUMN),
    //              errmsg("column reference \"%s\" is ambiguous", NameListToString(cref->fields)),
    //              parser_errposition(pstate, cref->location)));
    // }

    /*
	 * Throw error if no translation found.
	 */
    if (node == NULL)
    {
        switch (crerr)
        {
            case CRERR_NO_COLUMN:
                relation_parser->errorMissingColumn(pstate, relname, colname, cref->location);
                break;
            case CRERR_NO_RTE:
                relation_parser->errorMissingRTE(pstate, makeRangeVar(nspname, relname, cref->location));
                break;
            case CRERR_WRONG_DB:
                ereport(
                    ERROR,
                    (errcode(PG_ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg("cross-database references are not implemented: %s", PGNameListToString(cref->fields).c_str()),
                     parser_errposition(pstate, cref->location)));
                break;
            case CRERR_TOO_MANY:
                ereport(
                    ERROR,
                    (errcode(PG_ERRCODE_SYNTAX_ERROR),
                     errmsg("improper qualified name (too many dotted names): %s", PGNameListToString(cref->fields).c_str()),
                     parser_errposition(pstate, cref->location)));
                break;
        }
    }

    return node;
};

#ifdef __clang__
#pragma clang diagnostic ignored "-Wuninitialized"
#else
#pragma GCC diagnostic ignored "-Wuninitialized"
#endif

PGNode *
ExprParser::transformParamRef(PGParseState *pstate, PGParamRef *pref)
{
	PGNode	   *result;

	/*
	 * The core parser knows nothing about Params.  If a hook is supplied,
	 * call it.  If not, or if the hook returns NULL, throw a generic error.
	 */
	//TODO kindred
	// if (pstate->p_paramref_hook != NULL)
	// 	result = pstate->p_paramref_hook(pstate, pref);
	// else
	// 	result = NULL;

	// if (result == NULL)
	// {
	// 	parser_errposition(pstate, pref->location);
	// 	ereport(ERROR,
	// 			(errcode(ERRCODE_UNDEFINED_PARAMETER),
	// 			 errmsg("there is no parameter $%d", pref->number)));
	// }

	parser_errposition(pstate, pref->location);
		ereport(ERROR,
				(errcode(PG_ERRCODE_UNDEFINED_PARAMETER),
				 errmsg("there is no parameter $%d", pref->number)));

	return result;
};

void
ExprParser::unknown_attribute(PGParseState *pstate, PGNode *relref, const char *attname,
				  int location)
{
	PGRangeTblEntry *rte;

	if (IsA(relref, PGVar) &&
		((PGVar *) relref)->varattno == InvalidAttrNumber)
	{
		/* Reference the RTE by alias not by actual table name */
		rte = relation_parser->GetRTEByRangeTablePosn(pstate,
									 ((PGVar *) relref)->varno,
									 ((PGVar *) relref)->varlevelsup);
		parser_errposition(pstate, location);
		ereport(ERROR,
				(errcode(PG_ERRCODE_UNDEFINED_COLUMN),
				 errmsg("column %s.%s does not exist",
						rte->eref->aliasname, attname)));
	}
	else
	{
		/* Have to do it by reference to the type of the expression */
		PGOid			relTypeId = exprType(relref);

		if (type_parser->typeOrDomainTypeRelid(relTypeId) != InvalidOid)
		{
			parser_errposition(pstate, location);
			ereport(ERROR,
					(errcode(PG_ERRCODE_UNDEFINED_COLUMN),
					 errmsg("column \"%s\" not found in data type %s",
							attname, TypeProvider::format_type_be(relTypeId).c_str())));
		}
		else if (relTypeId == RECORDOID)
		{
			parser_errposition(pstate, location);
			ereport(ERROR,
					(errcode(PG_ERRCODE_UNDEFINED_COLUMN),
					 errmsg("could not identify column \"%s\" in record data type",
							attname)));
		}
		else
		{
			parser_errposition(pstate, location);
			ereport(ERROR,
					(errcode(PG_ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("column notation .%s applied to type %s, "
							"which is not a composite type",
							attname, TypeProvider::format_type_be(relTypeId).c_str())));
		}
	}
};

PGNode *
ExprParser::transformIndirection(PGParseState *pstate, PGNode *basenode, PGList *indirection)
{
    PGNode * result = basenode;
    PGList * subscripts = NIL;
    int location = exprLocation(basenode);
    PGListCell * i;

    /*
	 * We have to split any field-selection operations apart from
	 * subscripting.  Adjacent A_Indices nodes have to be treated as a single
	 * multidimensional subscript operation.
	 */
    foreach (i, indirection)
    {
        PGNode * n = (PGNode *)lfirst(i);

        if (IsA(n, PGAIndices))
            subscripts = lappend(subscripts, n);
        else if (IsA(n, PGAStar))
        {
            ereport(
                ERROR,
                (errcode(PG_ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("row expansion via \"*\" is not supported here"),
                 parser_errposition(pstate, location)));
        }
        else
        {
            PGNode * newresult;

            Assert(IsA(n, PGString))

            /* process subscripts before this field selection */
            if (subscripts)
                result
                    = (PGNode *)node_parser->transformArraySubscripts(pstate, result, exprType(result), InvalidOid, exprTypmod(result), subscripts, NULL);
            subscripts = NIL;

            newresult = func_parser->ParseFuncOrColumn(pstate, list_make1(n), list_make1(result), NULL, location);
            if (newresult == NULL)
                unknown_attribute(pstate, result, strVal(n), location);
            result = newresult;
        }
    }
    /* process trailing subscripts, if any */
    if (subscripts)
        result = (PGNode *)node_parser->transformArraySubscripts(pstate, result, exprType(result), InvalidOid, exprTypmod(result), subscripts, NULL);

    return result;
};

PGNode *
ExprParser::transformArrayExpr(PGParseState *pstate, PGAArrayExpr *a,
				   PGOid array_type, PGOid element_type, int32 typmod)
{
	PGArrayExpr  *newa = makeNode(PGArrayExpr);
	PGList	   *newelems = NIL;
	PGList	   *newcoercedelems = NIL;
	PGListCell   *element;
	PGOid			coerce_type;
	bool		coerce_hard;

	/*
	 * Transform the element expressions
	 *
	 * Assume that the array is one-dimensional unless we find an array-type
	 * element expression.
	 */
	newa->multidims = false;
	foreach(element, a->elements)
	{
		PGNode	   *e = (PGNode *) lfirst(element);
		PGNode	   *newe;

		/* Look through AEXPR_PAREN nodes so they don't affect test below */
		while (e && IsA(e, PGAExpr) &&
			   ((PGAExpr *) e)->kind == AEXPR_PAREN)
			e = ((PGAExpr *) e)->lexpr;

		/*
		 * If an element is itself an A_ArrayExpr, recurse directly so that we
		 * can pass down any target type we were given.
		 */
		if (IsA(e, PGAArrayExpr))
		{
			newe = transformArrayExpr(pstate,
									  (PGAArrayExpr *) e,
									  array_type,
									  element_type,
									  typmod);
			/* we certainly have an array here */
			Assert(array_type == InvalidOid || array_type == exprType(newe))
			newa->multidims = true;
		}
		else
		{
			newe = transformExprRecurse(pstate, e);

			/*
			 * Check for sub-array expressions, if we haven't already found
			 * one.
			 */
			if (!newa->multidims && TypeProvider::get_element_type(exprType(newe)) != InvalidOid)
				newa->multidims = true;
		}

		newelems = lappend(newelems, newe);
	}

	/*
	 * Select a target type for the elements.
	 *
	 * If we haven't been given a target array type, we must try to deduce a
	 * common type based on the types of the individual elements present.
	 */
	if (OidIsValid(array_type))
	{
		/* Caller must ensure array_type matches element_type */
		Assert(OidIsValid(element_type))
		coerce_type = (newa->multidims ? array_type : element_type);
		coerce_hard = true;
	}
	else
	{
		/* Can't handle an empty array without a target type */
		if (newelems == NIL)
		{
			parser_errposition(pstate, a->location);
			ereport(ERROR,
					(errcode(PG_ERRCODE_INDETERMINATE_DATATYPE),
					 errmsg("cannot determine type of empty array"),
					 errhint("Explicitly cast to the desired type, "
							 "for example ARRAY[]::integer[].")));
		}

		/* Select a common type for the elements */
		coerce_type = coerce_parser->select_common_type(pstate, newelems, "ARRAY", NULL);

		if (newa->multidims)
		{
			array_type = coerce_type;
			element_type = TypeProvider::get_element_type(array_type);
			if (!OidIsValid(element_type))
			{
				parser_errposition(pstate, a->location);
				ereport(ERROR,
						(errcode(PG_ERRCODE_UNDEFINED_OBJECT),
						 errmsg("could not find element type for data type %s",
								TypeProvider::format_type_be(array_type).c_str())));
			}
		}
		else
		{
			element_type = coerce_type;
			array_type = TypeProvider::get_array_type(element_type);
			if (!OidIsValid(array_type))
			{
				parser_errposition(pstate, a->location);
				ereport(ERROR,
						(errcode(PG_ERRCODE_UNDEFINED_OBJECT),
						 errmsg("could not find array type for data type %s",
								TypeProvider::format_type_be(element_type).c_str())));
			}
		}
		coerce_hard = false;
	}

	/*
	 * Coerce elements to target type
	 *
	 * If the array has been explicitly cast, then the elements are in turn
	 * explicitly coerced.
	 *
	 * If the array's type was merely derived from the common type of its
	 * elements, then the elements are implicitly coerced to the common type.
	 * This is consistent with other uses of select_common_type().
	 */
	foreach(element, newelems)
	{
		PGNode	   *e = (PGNode *) lfirst(element);
		PGNode	   *newe;

		if (coerce_hard)
		{
			newe = coerce_parser->coerce_to_target_type(pstate, e,
										 exprType(e),
										 coerce_type,
										 typmod,
										 PG_COERCION_EXPLICIT,
										 PG_COERCE_EXPLICIT_CAST,
										 -1);
			if (newe == NULL)
			{
				parser_errposition(pstate, exprLocation(e));
				ereport(ERROR,
						(errcode(PG_ERRCODE_CANNOT_COERCE),
						 errmsg("cannot cast type %s to %s",
								TypeProvider::format_type_be(exprType(e)).c_str(),
								TypeProvider::format_type_be(coerce_type).c_str())));
			}
		}
		else
			newe = coerce_parser->coerce_to_common_type(pstate, e,
										 coerce_type,
										 "ARRAY");
		newcoercedelems = lappend(newcoercedelems, newe);
	}

	newa->array_typeid = array_type;
	/* array_collid will be set by parse_collate.c */
	newa->element_typeid = element_type;
	newa->elements = newcoercedelems;
	newa->location = a->location;

	return (PGNode *) newa;
};

PGNode *
ExprParser::transformTypeCast(PGParseState *pstate, PGTypeCast *tc)
{
	PGNode	   *result;
	PGNode	   *arg = tc->arg;
	PGNode	   *expr;
	PGOid			inputType;
	PGOid			targetType;
	int32		targetTypmod;
	int			location;

	/* Look up the type name first */
	type_parser->typenameTypeIdAndMod(pstate, tc->typeName, &targetType, &targetTypmod);

	/*
	 * Look through any AEXPR_PAREN nodes that may have been inserted thanks
	 * to operator_precedence_warning.  Otherwise, ARRAY[]::foo[] behaves
	 * differently from (ARRAY[])::foo[].
	 */
	while (arg && IsA(arg, PGAExpr) &&
		   ((PGAExpr *) arg)->kind == AEXPR_PAREN)
		arg = ((PGAExpr *) arg)->lexpr;

	/*
	 * If the subject of the typecast is an ARRAY[] construct and the target
	 * type is an array type, we invoke transformArrayExpr() directly so that
	 * we can pass down the type information.  This avoids some cases where
	 * transformArrayExpr() might not infer the correct type.  Otherwise, just
	 * transform the argument normally.
	 */
	if (IsA(arg, PGAArrayExpr))
	{
		PGOid			targetBaseType;
		int32		targetBaseTypmod;
		PGOid			elementType;

		/*
		 * If target is a domain over array, work with the base array type
		 * here.  Below, we'll cast the array type to the domain.  In the
		 * usual case that the target is not a domain, the remaining steps
		 * will be a no-op.
		 */
		targetBaseTypmod = targetTypmod;
		targetBaseType = TypeProvider::getBaseTypeAndTypmod(targetType, &targetBaseTypmod);
		elementType = TypeProvider::get_element_type(targetBaseType);
		if (OidIsValid(elementType))
		{
			expr = transformArrayExpr(pstate,
									  (PGAArrayExpr *) arg,
									  targetBaseType,
									  elementType,
									  targetBaseTypmod);
		}
		else
			expr = transformExprRecurse(pstate, arg);
	}
	else
		expr = transformExprRecurse(pstate, arg);

	inputType = exprType(expr);
	if (inputType == InvalidOid)
		return expr;			/* do nothing if NULL input */

	/*
	 * Location of the coercion is preferentially the location of the :: or
	 * CAST symbol, but if there is none then use the location of the type
	 * name (this can happen in TypeName 'string' syntax, for instance).
	 */
	location = tc->location;
	if (location < 0)
		location = tc->typeName->location;

	result = coerce_parser->coerce_to_target_type(pstate, expr, inputType,
								   targetType, targetTypmod,
								   PG_COERCION_EXPLICIT,
								   PG_COERCE_EXPLICIT_CAST,
								   location);
	if (result == NULL)
	{
		coerce_parser->parser_coercion_errposition(pstate, location, expr);
		ereport(ERROR,
				(errcode(PG_ERRCODE_CANNOT_COERCE),
				 errmsg("cannot cast type %s to %s",
						TypeProvider::format_type_be(inputType).c_str(),
						TypeProvider::format_type_be(targetType).c_str())));
	}

	return result;
};

PGNode *
ExprParser::transformCollateClause(PGParseState *pstate, PGCollateClause *c)
{
	PGCollateExpr *newc;
	PGOid			argtype;

	newc = makeNode(PGCollateExpr);
	newc->arg = (PGExpr *) transformExprRecurse(pstate, c->arg);

	argtype = exprType((PGNode *) newc->arg);

	/*
	 * The unknown type is not collatable, but coerce_type() takes care of it
	 * separately, so we'll let it go here.
	 */
	if (!TypeProvider::type_is_collatable(argtype) && argtype != UNKNOWNOID)
	{
		parser_errposition(pstate, c->location);
		ereport(ERROR,
				(errcode(PG_ERRCODE_DATATYPE_MISMATCH),
				 errmsg("collations are not supported by type %s",
						TypeProvider::format_type_be(argtype).c_str())));
	}

	newc->collOid = type_parser->LookupCollation(pstate, c->collname, c->location);
	newc->location = c->location;

	return (PGNode *) newc;
};

int
ExprParser::operator_precedence_group(PGNode *node, const char **nodename)
{
	int			group = 0;

	*nodename = NULL;
	if (node == NULL)
		return 0;

	if (IsA(node, PGAExpr))
	{
		PGAExpr	   *aexpr = (PGAExpr *) node;

		if (aexpr->kind == PG_AEXPR_OP &&
			aexpr->lexpr != NULL &&
			aexpr->rexpr != NULL)
		{
			/* binary operator */
			if (list_length(aexpr->name) == 1)
			{
				*nodename = strVal(linitial(aexpr->name));
				/* Ignore if op was always higher priority than IS-tests */
				if (strcmp(*nodename, "+") == 0 ||
					strcmp(*nodename, "-") == 0 ||
					strcmp(*nodename, "*") == 0 ||
					strcmp(*nodename, "/") == 0 ||
					strcmp(*nodename, "%") == 0 ||
					strcmp(*nodename, "^") == 0)
					group = 0;
				else if (strcmp(*nodename, "<") == 0 ||
						 strcmp(*nodename, ">") == 0)
					group = PREC_GROUP_LESS;
				else if (strcmp(*nodename, "=") == 0)
					group = PREC_GROUP_EQUAL;
				else if (strcmp(*nodename, "<=") == 0 ||
						 strcmp(*nodename, ">=") == 0 ||
						 strcmp(*nodename, "<>") == 0)
					group = PREC_GROUP_LESS_EQUAL;
				else
					group = PREC_GROUP_INFIX_OP;
			}
			else
			{
				/* schema-qualified operator syntax */
				*nodename = "OPERATOR()";
				group = PREC_GROUP_INFIX_OP;
			}
		}
		else if (aexpr->kind == PG_AEXPR_OP &&
				 aexpr->lexpr == NULL &&
				 aexpr->rexpr != NULL)
		{
			/* prefix operator */
			if (list_length(aexpr->name) == 1)
			{
				*nodename = strVal(linitial(aexpr->name));
				/* Ignore if op was always higher priority than IS-tests */
				if (strcmp(*nodename, "+") == 0 ||
					strcmp(*nodename, "-") == 0)
					group = 0;
				else
					group = PREC_GROUP_PREFIX_OP;
			}
			else
			{
				/* schema-qualified operator syntax */
				*nodename = "OPERATOR()";
				group = PREC_GROUP_PREFIX_OP;
			}
		}
		else if (aexpr->kind == PG_AEXPR_OP &&
				 aexpr->lexpr != NULL &&
				 aexpr->rexpr == NULL)
		{
			/* postfix operator */
			if (list_length(aexpr->name) == 1)
			{
				*nodename = strVal(linitial(aexpr->name));
				group = PREC_GROUP_POSTFIX_OP;
			}
			else
			{
				/* schema-qualified operator syntax */
				*nodename = "OPERATOR()";
				group = PREC_GROUP_POSTFIX_OP;
			}
		}
		else if (aexpr->kind == PG_AEXPR_OP_ANY ||
				 aexpr->kind == PG_AEXPR_OP_ALL)
		{
			*nodename = strVal(llast(aexpr->name));
			group = PREC_GROUP_POSTFIX_OP;
		}
		else if (aexpr->kind == PG_AEXPR_DISTINCT ||
				 aexpr->kind == PG_AEXPR_NOT_DISTINCT)
		{
			*nodename = "IS";
			group = PREC_GROUP_INFIX_IS;
		}
		else if (aexpr->kind == PG_AEXPR_OF)
		{
			*nodename = "IS";
			group = PREC_GROUP_POSTFIX_IS;
		}
		else if (aexpr->kind == PG_AEXPR_IN)
		{
			*nodename = "IN";
			if (strcmp(strVal(linitial(aexpr->name)), "=") == 0)
				group = PREC_GROUP_IN;
			else
				group = PREC_GROUP_NOT_IN;
		}
		else if (aexpr->kind == PG_AEXPR_LIKE)
		{
			*nodename = "LIKE";
			if (strcmp(strVal(linitial(aexpr->name)), "~~") == 0)
				group = PREC_GROUP_LIKE;
			else
				group = PREC_GROUP_NOT_LIKE;
		}
		else if (aexpr->kind == PG_AEXPR_ILIKE)
		{
			*nodename = "ILIKE";
			if (strcmp(strVal(linitial(aexpr->name)), "~~*") == 0)
				group = PREC_GROUP_LIKE;
			else
				group = PREC_GROUP_NOT_LIKE;
		}
		else if (aexpr->kind == PG_AEXPR_SIMILAR)
		{
			*nodename = "SIMILAR";
			if (strcmp(strVal(linitial(aexpr->name)), "~") == 0)
				group = PREC_GROUP_LIKE;
			else
				group = PREC_GROUP_NOT_LIKE;
		}
		else if (aexpr->kind == PG_AEXPR_BETWEEN ||
				 aexpr->kind == PG_AEXPR_BETWEEN_SYM)
		{
			Assert(list_length(aexpr->name) == 1)
			*nodename = strVal(linitial(aexpr->name));
			group = PREC_GROUP_BETWEEN;
		}
		else if (aexpr->kind == PG_AEXPR_NOT_BETWEEN ||
				 aexpr->kind == PG_AEXPR_NOT_BETWEEN_SYM)
		{
			Assert(list_length(aexpr->name) == 1)
			*nodename = strVal(linitial(aexpr->name));
			group = PREC_GROUP_NOT_BETWEEN;
		}
	}
	else if (IsA(node, PGNullTest) ||
			 IsA(node, PGBooleanTest))
	{
		*nodename = "IS";
		group = PREC_GROUP_POSTFIX_IS;
	}
	// else if (IsA(node, PGXmlExpr))
	// {
	// 	PGXmlExpr    *x = (PGXmlExpr *) node;

	// 	if (x->op == PG_IS_DOCUMENT)
	// 	{
	// 		*nodename = "IS";
	// 		group = PREC_GROUP_POSTFIX_IS;
	// 	}
	// }
	else if (IsA(node, PGSubLink))
	{
		PGSubLink    *s = (PGSubLink *) node;

		if (s->subLinkType == PG_ANY_SUBLINK ||
			s->subLinkType == PG_ALL_SUBLINK)
		{
			if (s->operName == NIL)
			{
				*nodename = "IN";
				group = PREC_GROUP_IN;
			}
			else
			{
				*nodename = strVal(llast(s->operName));
				group = PREC_GROUP_POSTFIX_OP;
			}
		}
	}
	else if (IsA(node, PGBoolExpr))
	{
		/*
		 * Must dig into NOTs to see if it's IS NOT DOCUMENT or NOT IN.  This
		 * opens us to possibly misrecognizing, eg, NOT (x IS DOCUMENT) as a
		 * problematic construct.  We can tell the difference by checking
		 * whether the parse locations of the two nodes are identical.
		 *
		 * Note that when we are comparing the child node to its own children,
		 * we will not know that it was a NOT.  Fortunately, that doesn't
		 * matter for these cases.
		 */
		PGBoolExpr   *b = (PGBoolExpr *) node;

		if (b->boolop == PG_NOT_EXPR)
		{
			PGNode	   *child = (PGNode *) linitial(b->args);

			if (IsA(child, PGXmlExpr))
			{
				// PGXmlExpr    *x = (PGXmlExpr *) child;

				// if (x->op == IS_DOCUMENT &&
				// 	x->location == b->location)
				// {
				// 	*nodename = "IS";
				// 	group = PREC_GROUP_POSTFIX_IS;
				// }
			}
			else if (IsA(child, PGSubLink))
			{
				PGSubLink    *s = (PGSubLink *) child;

				if (s->subLinkType == PG_ANY_SUBLINK && s->operName == NIL &&
					s->location == b->location)
				{
					*nodename = "IN";
					group = PREC_GROUP_NOT_IN;
				}
			}
		}
	}
	return group;
};

PGNode *
ExprParser::transformAExprOp(PGParseState *pstate, PGAExpr *a)
{
    PGNode * lexpr = a->lexpr;
    PGNode * rexpr = a->rexpr;
    PGNode * result;

    /*
	 * Special-case "foo = NULL" and "NULL = foo" for compatibility with
	 * standards-broken products (like Microsoft's).  Turn these into IS NULL
	 * exprs. (If either side is a CaseTestExpr, then the expression was
	 * generated internally from a CASE-WHEN expression, and
	 * transform_null_equals does not apply.)
	 */
    if (Transform_null_equals && list_length(a->name) == 1 && strcmp(strVal(linitial(a->name)), "=") == 0
        && (exprIsNullConstant(lexpr) || exprIsNullConstant(rexpr)) && (!IsA(lexpr, PGCaseTestExpr) && !IsA(rexpr, PGCaseTestExpr)))
    {
        PGNullTest * n = makeNode(PGNullTest);

        n->nulltesttype = PG_IS_NULL;

        if (exprIsNullConstant(lexpr))
            n->arg = (PGExpr *)rexpr;
        else
            n->arg = (PGExpr *)lexpr;

        result = transformExprRecurse(pstate, (PGNode *)n);
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
        s->location = a->location;
        result = transformExprRecurse(pstate, (PGNode *)s);
    }
    else if (lexpr && IsA(lexpr, PGRowExpr) && rexpr && IsA(rexpr, PGRowExpr))
    {
		//TODO kindred
        /* ROW() op ROW() is handled specially */
        // lexpr = transformExprRecurse(pstate, lexpr);
        // rexpr = transformExprRecurse(pstate, rexpr);
        // Assert(IsA(lexpr, PGRowExpr))
        // Assert(IsA(rexpr, PGRowExpr))

        // result = make_row_comparison_op(pstate, a->name, ((PGRowExpr *)lexpr)->args, ((PGRowExpr *)rexpr)->args, a->location);

        parser_errposition(pstate, a->location);
        ereport(ERROR, (errmsg("Row comparision do not support yet!")));
    }
    else
    {
        /* Ordinary scalar operator */
        lexpr = transformExprRecurse(pstate, lexpr);
        rexpr = transformExprRecurse(pstate, rexpr);

        result = (PGNode *)oper_parser->make_op(pstate, a->name, lexpr, rexpr, a->location);
    }

    return result;
};

PGNode *
ExprParser::transformAExprOpAny(PGParseState *pstate, PGAExpr *a)
{
	PGNode	   *lexpr = a->lexpr;
	PGNode	   *rexpr = a->rexpr;

	if (operator_precedence_warning)
		emit_precedence_warnings(pstate, PREC_GROUP_POSTFIX_OP,
								 strVal(llast(a->name)),
								 lexpr, NULL,
								 a->location);

	lexpr = transformExprRecurse(pstate, lexpr);
	rexpr = transformExprRecurse(pstate, rexpr);

	return (PGNode *) oper_parser->make_scalar_array_op(pstate,
										 a->name,
										 true,
										 lexpr,
										 rexpr,
										 a->location);
};

void
ExprParser::emit_precedence_warnings(PGParseState *pstate,
						 int opgroup, const char *opname,
						 PGNode *lchild, PGNode *rchild,
						 int location)
{
	int			cgroup;
	const char *copname;

	Assert(opgroup > 0)

	/*
	 * Complain if left child, which should be same or higher precedence
	 * according to current rules, used to be lower precedence.
	 *
	 * Exception to precedence rules: if left child is IN or NOT IN or a
	 * postfix operator, the grouping is syntactically forced regardless of
	 * precedence.
	 */
	cgroup = operator_precedence_group(lchild, &copname);
	if (cgroup > 0)
	{
		if (oldprecedence_l[cgroup] < oldprecedence_r[opgroup] &&
			cgroup != PREC_GROUP_IN &&
			cgroup != PREC_GROUP_NOT_IN &&
			cgroup != PREC_GROUP_POSTFIX_OP &&
			cgroup != PREC_GROUP_POSTFIX_IS)
		{
			parser_errposition(pstate, location);
			ereport(WARNING,
					(errmsg("operator precedence change: %s is now lower precedence than %s",
							opname, copname)));
		}
	}

	/*
	 * Complain if right child, which should be higher precedence according to
	 * current rules, used to be same or lower precedence.
	 *
	 * Exception to precedence rules: if right child is a prefix operator, the
	 * grouping is syntactically forced regardless of precedence.
	 */
	cgroup = operator_precedence_group(rchild, &copname);
	if (cgroup > 0)
	{
		if (oldprecedence_r[cgroup] <= oldprecedence_l[opgroup] &&
			cgroup != PREC_GROUP_PREFIX_OP)
		{
			parser_errposition(pstate, location);
			ereport(WARNING,
					(errmsg("operator precedence change: %s is now lower precedence than %s",
							opname, copname)));
		}
	}
};

PGNode *
ExprParser::transformAExprOpAll(PGParseState *pstate, PGAExpr *a)
{
	PGNode	   *lexpr = a->lexpr;
	PGNode	   *rexpr = a->rexpr;

	if (operator_precedence_warning)
		emit_precedence_warnings(pstate, PREC_GROUP_POSTFIX_OP,
								 strVal(llast(a->name)),
								 lexpr, NULL,
								 a->location);

	lexpr = transformExprRecurse(pstate, lexpr);
	rexpr = transformExprRecurse(pstate, rexpr);

	return (PGNode *) oper_parser->make_scalar_array_op(pstate,
										 a->name,
										 false,
										 lexpr,
										 rexpr,
										 a->location);
};

duckdb_libpgquery::PGNode *
ExprParser::make_nulltest_from_distinct(PGParseState *pstate, PGAExpr *distincta, PGNode *arg)
{
	PGNullTest   *nt = makeNode(PGNullTest);

	nt->arg = (PGExpr *) transformExprRecurse(pstate, arg);
	/* the argument can be any type, so don't coerce it */
	if (distincta->kind == PG_AEXPR_NOT_DISTINCT)
		nt->nulltesttype = PG_IS_NULL;
	else
		nt->nulltesttype = IS_NOT_NULL;
	/* argisrow = false is correct whether or not arg is composite */
	nt->argisrow = false;
	nt->location = distincta->location;
	return (PGNode *) nt;
};

PGExpr *
ExprParser::make_distinct_op(PGParseState *pstate, PGList *opname,
                PGNode *ltree, PGNode *rtree,
				int location)
{
    PGExpr * result;

    result = oper_parser->make_op(pstate, opname, ltree, rtree, location);
    if (((PGOpExpr *)result)->opresulttype != BOOLOID)
        ereport(
            ERROR,
            (errcode(PG_ERRCODE_DATATYPE_MISMATCH),
             errmsg("IS DISTINCT FROM requires = operator to yield boolean"),
             parser_errposition(pstate, location)));

    /*
	 * We rely on DistinctExpr and OpExpr being same struct
	 */
    NodeSetTag(result, T_PGDistinctExpr);

    return result;
};

PGNode *
ExprParser::make_row_distinct_op(PGParseState *pstate, PGList *opname,
					 PGRowExpr *lrow, PGRowExpr *rrow,
					 int location)
{
	PGNode	   *result = NULL;
	PGList	   *largs = lrow->args;
	PGList	   *rargs = rrow->args;
	ListCell   *l,
			   *r;

	if (list_length(largs) != list_length(rargs))
	{
		parser_errposition(pstate, location);
		ereport(ERROR,
				(errcode(PG_ERRCODE_SYNTAX_ERROR),
				 errmsg("unequal number of entries in row expressions")));
	}

	forboth(l, largs, r, rargs)
	{
		PGNode	   *larg = (PGNode *) lfirst(l);
		PGNode	   *rarg = (PGNode *) lfirst(r);
		PGNode	   *cmp;

		cmp = (PGNode *) make_distinct_op(pstate, opname, larg, rarg, location);
		if (result == NULL)
			result = cmp;
		else
			result = (PGNode *) makeBoolExpr(PG_OR_EXPR,
										   list_make2(result, cmp),
										   location);
	}

	if (result == NULL)
	{
		/* zero-length rows?  Generate constant FALSE */
		result = makeBoolConst(false, false);
	}

	return result;
};

PGNode *
ExprParser::transformAExprDistinct(PGParseState *pstate, PGAExpr *a)
{
	PGNode	   *lexpr = a->lexpr;
	PGNode	   *rexpr = a->rexpr;
	PGNode	   *result;

	if (operator_precedence_warning)
		emit_precedence_warnings(pstate, PREC_GROUP_INFIX_IS, "IS",
								 lexpr, rexpr,
								 a->location);

	/*
	 * If either input is an undecorated NULL literal, transform to a NullTest
	 * on the other input. That's simpler to process than a full DistinctExpr,
	 * and it avoids needing to require that the datatype have an = operator.
	 */
	if (exprIsNullConstant(rexpr))
		return make_nulltest_from_distinct(pstate, a, lexpr);
	if (exprIsNullConstant(lexpr))
		return make_nulltest_from_distinct(pstate, a, rexpr);

	lexpr = transformExprRecurse(pstate, lexpr);
	rexpr = transformExprRecurse(pstate, rexpr);

	if (lexpr && IsA(lexpr, PGRowExpr) &&
		rexpr && IsA(rexpr, PGRowExpr))
	{
		/* ROW() op ROW() is handled specially */
		result = make_row_distinct_op(pstate, a->name,
									  (PGRowExpr *) lexpr,
									  (PGRowExpr *) rexpr,
									  a->location);
	}
	else
	{
		/* Ordinary scalar operator */
		result = (PGNode *) make_distinct_op(pstate,
										   a->name,
										   lexpr,
										   rexpr,
										   a->location);
	}

	/*
	 * If it's NOT DISTINCT, we first build a DistinctExpr and then stick a
	 * NOT on top.
	 */
	if (a->kind == PG_AEXPR_NOT_DISTINCT)
		result = (PGNode *) makeBoolExpr(PG_NOT_EXPR,
									   list_make1(result),
									   a->location);

	return result;
};

PGNode *
ExprParser::transformAExprNullIf(PGParseState *pstate, PGAExpr *a)
{
    PGNode * lexpr = transformExprRecurse(pstate, a->lexpr);
    PGNode * rexpr = transformExprRecurse(pstate, a->rexpr);
    PGOpExpr * result;

    result = (PGOpExpr *)oper_parser->make_op(pstate, a->name, lexpr, rexpr, a->location);

    /*
	 * The comparison operator itself should yield boolean ...
	 */
    if (result->opresulttype != BOOLOID)
        ereport(
            ERROR,
            (errcode(PG_ERRCODE_DATATYPE_MISMATCH),
             errmsg("NULLIF requires = operator to yield boolean"),
             parser_errposition(pstate, a->location)));

    /*
	 * ... but the NullIfExpr will yield the first operand's type.
	 */
    result->opresulttype = exprType((PGNode *)linitial(result->args));

    /*
	 * We rely on NullIfExpr and OpExpr being the same struct
	 */
    NodeSetTag(result, T_PGNullIfExpr);

    return (PGNode *)result;
};

PGNode *
ExprParser::transformAExprOf(PGParseState *pstate, PGAExpr *a)
{
	PGNode	   *lexpr = a->lexpr;
	PGConst	   *result;
	PGListCell   *telem;
	PGOid			ltype,
				rtype;
	bool		matched = false;

	if (operator_precedence_warning)
		emit_precedence_warnings(pstate, PREC_GROUP_POSTFIX_IS, "IS",
								 lexpr, NULL,
								 a->location);

	lexpr = transformExprRecurse(pstate, lexpr);

	ltype = exprType(lexpr);
	foreach(telem, (PGList *) a->rexpr)
	{
		rtype = type_parser->typenameTypeId(pstate, (PGTypeName *)lfirst(telem));
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

	result = (PGConst *) makeBoolConst(matched, false);

	/* Make the result have the original input's parse location */
	result->location = exprLocation((PGNode *) a);

	return (PGNode *) result;
};

PGNode *
ExprParser::transformAExprIn(PGParseState *pstate, PGAExpr *a)
{
    PGNode * result = NULL;
    PGNode * lexpr;
    PGList * rexprs;
    PGList * rvars;
    PGList * rnonvars;
    bool useOr;
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
	 * possible if there is a suitable array type available.  If not, we fall
	 * back to a boolean condition tree with multiple copies of the lefthand
	 * expression.  Also, any IN-list items that contain Vars are handled as
	 * separate boolean conditions, because that gives the planner more scope
	 * for optimization on such clauses.
	 *
	 * First step: transform all the inputs, and detect whether any contain
	 * Vars.
	 */
    lexpr = transformExprRecurse(pstate, a->lexpr);
    rexprs = rvars = rnonvars = NIL;
    foreach (l, (PGList *)a->rexpr)
    {
        PGNode * rexpr = transformExprRecurse(pstate, (PGNode *)lfirst(l));

        rexprs = lappend(rexprs, rexpr);
        if (contain_vars_of_level(rexpr, 0))
            rvars = lappend(rvars, rexpr);
        else
            rnonvars = lappend(rnonvars, rexpr);
    }

    /*
	 * ScalarArrayOpExpr is only going to be useful if there's more than one
	 * non-Var righthand item.
	 */
    if (list_length(rnonvars) > 1)
    {
        PGList * allexprs;
        PGOid scalar_type;
        PGOid array_type;

        /*
		 * Try to select a common type for the array elements.  Note that
		 * since the LHS' type is first in the list, it will be preferred when
		 * there is doubt (eg, when all the RHS items are unknown literals).
		 *
		 * Note: use list_concat here not lcons, to avoid damaging rnonvars.
		 */
        allexprs = list_concat(list_make1(lexpr), rnonvars);
        scalar_type = coerce_parser->select_common_type(pstate, allexprs, NULL, NULL);

        /*
		 * Do we have an array type to use?  Aside from the case where there
		 * isn't one, we don't risk using ScalarArrayOpExpr when the common
		 * type is RECORD, because the RowExpr comparison logic below can cope
		 * with some cases of non-identical row types.
		 */
        if (OidIsValid(scalar_type) && scalar_type != RECORDOID)
            array_type = TypeProvider::get_array_type(scalar_type);
        else
            array_type = InvalidOid;
        if (array_type != InvalidOid)
        {
            /*
			 * OK: coerce all the right-hand non-Var inputs to the common type
			 * and build an ArrayExpr for them.
			 */
            PGList * aexprs;
            PGArrayExpr * newa;

            aexprs = NIL;
            foreach (l, rnonvars)
            {
                PGNode * rexpr = (PGNode *)lfirst(l);

                rexpr = coerce_parser->coerce_to_common_type(pstate, rexpr, scalar_type, "IN");
                aexprs = lappend(aexprs, rexpr);
            }
            newa = makeNode(PGArrayExpr);
            newa->array_typeid = array_type;
            /* array_collid will be set by parse_collate.c */
            newa->element_typeid = scalar_type;
            newa->elements = aexprs;
            newa->multidims = false;
            newa->location = -1;

            result = (PGNode *)oper_parser->make_scalar_array_op(pstate, a->name, useOr, lexpr, (PGNode *)newa, a->location);

            /* Consider only the Vars (if any) in the loop below */
            rexprs = rvars;
        }
    }

    /*
	 * Must do it the hard way, ie, with a boolean expression tree.
	 */
    foreach (l, rexprs)
    {
        PGNode * rexpr = (PGNode *)lfirst(l);
        PGNode * cmp;

        if (IsA(lexpr, PGRowExpr) && IsA(rexpr, PGRowExpr))
        {
            /* ROW() op ROW() is handled specially */
			//TODO kindred
            // cmp = make_row_comparison_op(
            //     pstate, a->name, (PGList *)copyObject(((PGRowExpr *)lexpr)->args), ((PGRowExpr *)rexpr)->args, a->location);

			parser_errposition(pstate, a->location);
			ereport(ERROR,
					(errmsg("Row comparision do not support yet!")));
		}
        else
        {
            /* Ordinary scalar operator */
            cmp = (PGNode *)oper_parser->make_op(pstate, a->name, (PGNode *)copyObject(lexpr), rexpr, a->location);
        }

        cmp = coerce_parser->coerce_to_boolean(pstate, cmp, "IN");
        if (result == NULL)
            result = cmp;
        else
            result = (PGNode *)makeBoolExpr(useOr ? PG_OR_EXPR : PG_AND_EXPR, list_make2(result, cmp), a->location);
    }

    return result;
};

PGNode *
ExprParser::transformAExprBetween(PGParseState *pstate, PGAExpr *a)
{
	PGNode	   *aexpr;
	PGNode	   *bexpr;
	PGNode	   *cexpr;
	PGNode	   *result;
	PGNode	   *sub1;
	PGNode	   *sub2;
	PGList	   *args;

	/* Deconstruct A_Expr into three subexprs */
	aexpr = a->lexpr;
	args = castNode(PGList, a->rexpr);
	Assert(list_length(args) == 2)
	bexpr = (PGNode *) linitial(args);
	cexpr = (PGNode *) lsecond(args);

	if (operator_precedence_warning)
	{
		int			opgroup;
		const char *opname;

		opgroup = operator_precedence_group((PGNode *) a, &opname);
		emit_precedence_warnings(pstate, opgroup, opname,
								 aexpr, cexpr,
								 a->location);
		/* We can ignore bexpr thanks to syntactic restrictions */
		/* Wrap subexpressions to prevent extra warnings */
		aexpr = (PGNode *) makeAExpr(AEXPR_PAREN, NIL, aexpr, NULL, -1);
		bexpr = (PGNode *) makeAExpr(AEXPR_PAREN, NIL, bexpr, NULL, -1);
		cexpr = (PGNode *) makeAExpr(AEXPR_PAREN, NIL, cexpr, NULL, -1);
	}

	/*
	 * Build the equivalent comparison expression.  Make copies of
	 * multiply-referenced subexpressions for safety.  (XXX this is really
	 * wrong since it results in multiple runtime evaluations of what may be
	 * volatile expressions ...)
	 *
	 * Ideally we would not use hard-wired operators here but instead use
	 * opclasses.  However, mixed data types and other issues make this
	 * difficult:
	 * http://archives.postgresql.org/pgsql-hackers/2008-08/msg01142.php
	 */
	switch (a->kind)
	{
		case PG_AEXPR_BETWEEN:
			args = list_make2(makeSimpleAExpr(PG_AEXPR_OP, ">=",
											   aexpr, bexpr,
											   a->location),
							  makeSimpleAExpr(PG_AEXPR_OP, "<=",
											   (PGNode *)copyObject(aexpr), cexpr,
											   a->location));
			result = (PGNode *) makeBoolExpr(PG_AND_EXPR, args, a->location);
			break;
		case PG_AEXPR_NOT_BETWEEN:
			args = list_make2(makeSimpleAExpr(PG_AEXPR_OP, "<",
											   aexpr, bexpr,
											   a->location),
							  makeSimpleAExpr(PG_AEXPR_OP, ">",
											   (PGNode *)copyObject(aexpr), cexpr,
											   a->location));
			result = (PGNode *) makeBoolExpr(PG_OR_EXPR, args, a->location);
			break;
		case PG_AEXPR_BETWEEN_SYM:
			args = list_make2(makeSimpleAExpr(PG_AEXPR_OP, ">=",
											   aexpr, bexpr,
											   a->location),
							  makeSimpleAExpr(PG_AEXPR_OP, "<=",
											   (PGNode *)copyObject(aexpr), cexpr,
											   a->location));
			sub1 = (PGNode *) makeBoolExpr(PG_AND_EXPR, args, a->location);
			args = list_make2(makeSimpleAExpr(PG_AEXPR_OP, ">=",
											   (PGNode *)copyObject(aexpr), (PGNode *)copyObject(cexpr),
											   a->location),
							  makeSimpleAExpr(PG_AEXPR_OP, "<=",
											   (PGNode *)copyObject(aexpr), (PGNode *)copyObject(bexpr),
											   a->location));
			sub2 = (PGNode *) makeBoolExpr(PG_AND_EXPR, args, a->location);
			args = list_make2(sub1, sub2);
			result = (PGNode *) makeBoolExpr(PG_OR_EXPR, args, a->location);
			break;
		case PG_AEXPR_NOT_BETWEEN_SYM:
			args = list_make2(makeSimpleAExpr(PG_AEXPR_OP, "<",
											   aexpr, bexpr,
											   a->location),
							  makeSimpleAExpr(PG_AEXPR_OP, ">",
											   (PGNode *)copyObject(aexpr), cexpr,
											   a->location));
			sub1 = (PGNode *) makeBoolExpr(PG_OR_EXPR, args, a->location);
			args = list_make2(makeSimpleAExpr(PG_AEXPR_OP, "<",
											   (PGNode *)copyObject(aexpr), (PGNode *)copyObject(cexpr),
											   a->location),
							  makeSimpleAExpr(PG_AEXPR_OP, ">",
											   (PGNode *)copyObject(aexpr), (PGNode *)copyObject(bexpr),
											   a->location));
			sub2 = (PGNode *) makeBoolExpr(PG_OR_EXPR, args, a->location);
			args = list_make2(sub1, sub2);
			result = (PGNode *) makeBoolExpr(PG_AND_EXPR, args, a->location);
			break;
		default:
			elog(ERROR, "unrecognized A_Expr kind: %d", a->kind);
			result = NULL;		/* keep compiler quiet */
			break;
	}

	return transformExprRecurse(pstate, result);
};

PGNode *
ExprParser::transformBoolExpr(PGParseState *pstate, PGBoolExpr *a)
{
	PGList	   *args = NIL;
	const char *opname;
	PGListCell   *lc;

	switch (a->boolop)
	{
		case PG_AND_EXPR:
			opname = "AND";
			break;
		case PG_OR_EXPR:
			opname = "OR";
			break;
		case PG_NOT_EXPR:
			opname = "NOT";
			break;
		default:
			elog(ERROR, "unrecognized boolop: %d", (int) a->boolop);
			opname = NULL;		/* keep compiler quiet */
			break;
	}

	foreach(lc, a->args)
	{
		PGNode	   *arg = (PGNode *) lfirst(lc);

		arg = transformExprRecurse(pstate, arg);
		arg = coerce_parser->coerce_to_boolean(pstate, arg, opname);
		args = lappend(args, arg);
	}

	return (PGNode *) makeBoolExpr(a->boolop, args, a->location);
};

PGNode *
ExprParser::transformFuncCall(PGParseState *pstate, PGFuncCall *fn)
{
    PGList * targs;
    PGListCell * args;

    /* Transform the list of arguments ... */
    targs = NIL;
    foreach (args, fn->args)
    {
        targs = lappend(targs, transformExprRecurse(pstate, (PGNode *)lfirst(args)));
    }

    /*
	 * When WITHIN GROUP is used, we treat its ORDER BY expressions as
	 * additional arguments to the function, for purposes of function lookup
	 * and argument type coercion.  So, transform each such expression and add
	 * them to the targs list.  We don't explicitly mark where each argument
	 * came from, but ParseFuncOrColumn can tell what's what by reference to
	 * list_length(fn->agg_order).
	 */
    if (fn->agg_within_group)
    {
        Assert(fn->agg_order != NIL)
        foreach (args, fn->agg_order)
        {
            PGSortBy * arg = (PGSortBy *)lfirst(args);

            targs = lappend(targs, transformExpr(pstate, arg->node, EXPR_KIND_ORDER_BY));
        }
    }

    /* ... and hand off to ParseFuncOrColumn */
    return func_parser->ParseFuncOrColumn(pstate, fn->funcname, targs, fn, fn->location);
};

PGNode *
ExprParser::transformRowExpr(PGParseState *pstate, PGRowExpr *r)
{
    PGRowExpr * newr;
    char fname[16];
    int fnum;
    PGListCell * lc;

    /* If we already transformed this node, do nothing */
    if (OidIsValid(r->row_typeid))
        return (PGNode *)r;

    newr = makeNode(PGRowExpr);

    /* Transform the field expressions */
    newr->args = target_parser->transformExpressionList(pstate, r->args, pstate->p_expr_kind);

    /* Barring later casting, we consider the type RECORD */
    newr->row_typeid = RECORDOID;
    newr->row_format = PG_COERCE_IMPLICIT_CAST;

    /* ROW() has anonymous columns, so invent some field names */
    newr->colnames = NIL;
    fnum = 1;
    foreach (lc, newr->args)
    {
        snprintf(fname, sizeof(fname), "f%d", fnum++);
        newr->colnames = lappend(newr->colnames, makeString(pstrdup(fname)));
    }

    newr->location = r->location;

    return (PGNode *)newr;
};

PGNode *
ExprParser::transformSubLink(PGParseState *pstate, PGSubLink *sublink)
{
	PGNode	   *result = (PGNode *) sublink;
	PGQuery	   *qtree;
	const char *err;

	/* If we already transformed this node, do nothing */
	if (IsA(sublink->subselect, PGQuery))
		return result;

	/*
	 * Check to see if the sublink is in an invalid place within the query. We
	 * allow sublinks everywhere in SELECT/INSERT/UPDATE/DELETE, but generally
	 * not in utility statements.
	 */
	err = NULL;
	switch (pstate->p_expr_kind)
	{
		case EXPR_KIND_NONE:
			Assert(false)		/* can't happen */
			break;
		case EXPR_KIND_OTHER:
			/* Accept sublink here; caller must throw error if wanted */
			break;
		case EXPR_KIND_JOIN_ON:
		case EXPR_KIND_JOIN_USING:
		case EXPR_KIND_FROM_SUBSELECT:
		case EXPR_KIND_FROM_FUNCTION:
		case EXPR_KIND_WHERE:
		case EXPR_KIND_HAVING:
		case EXPR_KIND_FILTER:
		case EXPR_KIND_WINDOW_PARTITION:
		case EXPR_KIND_WINDOW_ORDER:
		case EXPR_KIND_WINDOW_FRAME_RANGE:
		case EXPR_KIND_WINDOW_FRAME_ROWS:
		case EXPR_KIND_SELECT_TARGET:
		case EXPR_KIND_INSERT_TARGET:
		case EXPR_KIND_UPDATE_SOURCE:
		case EXPR_KIND_UPDATE_TARGET:
		case EXPR_KIND_GROUP_BY:
		case EXPR_KIND_ORDER_BY:
		case EXPR_KIND_DISTINCT_ON:
		case EXPR_KIND_LIMIT:
		case EXPR_KIND_OFFSET:
		case EXPR_KIND_RETURNING:
		case EXPR_KIND_VALUES:
			/* okay */
			break;
		case EXPR_KIND_CHECK_CONSTRAINT:
		case EXPR_KIND_DOMAIN_CHECK:
			err = _("cannot use subquery in check constraint");
			break;
		case EXPR_KIND_COLUMN_DEFAULT:
		case EXPR_KIND_FUNCTION_DEFAULT:
			err = _("cannot use subquery in DEFAULT expression");
			break;
		case EXPR_KIND_INDEX_EXPRESSION:
			err = _("cannot use subquery in index expression");
			break;
		case EXPR_KIND_INDEX_PREDICATE:
			err = _("cannot use subquery in index predicate");
			break;
		case EXPR_KIND_ALTER_COL_TRANSFORM:
			err = _("cannot use subquery in transform expression");
			break;
		case EXPR_KIND_EXECUTE_PARAMETER:
			err = _("cannot use subquery in EXECUTE parameter");
			break;
		case EXPR_KIND_TRIGGER_WHEN:
			err = _("cannot use subquery in trigger WHEN condition");
			break;
		case EXPR_KIND_PARTITION_EXPRESSION:
			err = _("cannot use subquery in partition key expression");
			break;

		case EXPR_KIND_SCATTER_BY:
			/* okay */
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
		ereport(ERROR,
				(errcode(PG_ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg_internal("%s", err),
				 parser_errposition(pstate, sublink->location)));

	pstate->p_hasSubLinks = true;

	/*
	 * OK, let's transform the sub-SELECT.
	 */
	qtree = select_parser->parse_sub_analyze(sublink->subselect, pstate, NULL, NULL);

	/*
	 * Check that we got something reasonable.  Many of these conditions are
	 * impossible given restrictions of the grammar, but check 'em anyway.
	 */
	if (!IsA(qtree, PGQuery) ||
		qtree->commandType != PG_CMD_SELECT ||
		qtree->utilityStmt != NULL)
		elog(ERROR, "unexpected non-SELECT command in SubLink");

	sublink->subselect = (PGNode *) qtree;

	if (sublink->subLinkType == PG_EXISTS_SUBLINK)
	{
		/*
		 * EXISTS needs no test expression or combining operator. These fields
		 * should be null already, but make sure.
		 */
		sublink->testexpr = NULL;
		sublink->operName = NIL;
	}
	else if (sublink->subLinkType == PG_EXPR_SUBLINK ||
			 sublink->subLinkType == PG_ARRAY_SUBLINK)
	{
		ListCell   *tlist_item = list_head(qtree->targetList);

		/*
		 * Make sure the subselect delivers a single column (ignoring resjunk
		 * targets).
		 */
		if (tlist_item == NULL ||
			((PGTargetEntry *) lfirst(tlist_item))->resjunk)
			ereport(ERROR,
					(errcode(PG_ERRCODE_SYNTAX_ERROR),
					 errmsg("subquery must return a column"),
					 parser_errposition(pstate, sublink->location)));
		while ((tlist_item = lnext(tlist_item)) != NULL)
		{
			if (!((PGTargetEntry *) lfirst(tlist_item))->resjunk)
				ereport(ERROR,
						(errcode(PG_ERRCODE_SYNTAX_ERROR),
						 errmsg("subquery must return only one column"),
						 parser_errposition(pstate, sublink->location)));
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
		PGNode	   *lefthand;
		PGList	   *left_list;
		PGList	   *right_list;
		PGListCell   *l;

		/*
		 * Transform lefthand expression, and convert to a list
		 */
		lefthand = transformExprRecurse(pstate, sublink->testexpr);
		if (lefthand && IsA(lefthand, PGRowExpr))
			left_list = ((PGRowExpr *) lefthand)->args;
		else
			left_list = list_make1(lefthand);

		/*
		 * Build a list of PARAM_SUBLINK nodes representing the output columns
		 * of the subquery.
		 */
		right_list = NIL;
		foreach(l, qtree->targetList)
		{
			PGTargetEntry *tent = (PGTargetEntry *) lfirst(l);
			PGParam	   *param;

			if (tent->resjunk)
				continue;

			param = makeNode(PGParam);
			param->paramkind = PG_PARAM_SUBLINK;
			param->paramid = tent->resno;
			param->paramtype = exprType((PGNode *) tent->expr);
			param->paramtypmod = exprTypmod((PGNode *) tent->expr);
			param->paramcollid = exprCollation((PGNode *) tent->expr);
			param->location = -1;

			right_list = lappend(right_list, param);
		}

		/*
		 * We could rely on make_row_comparison_op to complain if the list
		 * lengths differ, but we prefer to generate a more specific error
		 * message.
		 */
		if (list_length(left_list) < list_length(right_list))
			ereport(ERROR,
					(errcode(PG_ERRCODE_SYNTAX_ERROR),
					 errmsg("subquery has too many columns"),
					 parser_errposition(pstate, sublink->location)));
		if (list_length(left_list) > list_length(right_list))
			ereport(ERROR,
					(errcode(PG_ERRCODE_SYNTAX_ERROR),
					 errmsg("subquery has too few columns"),
					 parser_errposition(pstate, sublink->location)));

		/*
		 * Identify the combining operator(s) and generate a suitable
		 * row-comparison expression.
		 */
		//TODO kindred
		// sublink->testexpr = make_row_comparison_op(pstate,
		// 										   sublink->operName,
		// 										   left_list,
		// 										   right_list,
		// 										   sublink->location);

		parser_errposition(pstate, sublink->location);
			ereport(ERROR,
					(errmsg("Row comparision do not support yet!")));
	}

	return result;
};

bool
ExprParser::isWhenIsNotDistinctFromExpr(PGNode *warg)
{
	if (IsA(warg, PGBoolExpr))
	{
		PGBoolExpr *bexpr = (PGBoolExpr *) warg;
		PGNode *arg = (PGNode *)linitial(bexpr->args);
		if (bexpr->boolop == PG_NOT_EXPR && IsA(arg, PGAExpr))
		{
			PGAExpr *expr = (PGAExpr *) arg;
			if (expr->kind == PG_AEXPR_DISTINCT && expr->lexpr == NULL)
				return true;
		}
	}
	return false;
};

PGNode *
ExprParser::transformCaseExpr(PGParseState *pstate, PGCaseExpr *c)
{
	PGCaseExpr   *newc = makeNode(PGCaseExpr);
	PGNode	   *last_srf = pstate->p_last_srf;
	PGNode	   *arg;
	PGCaseTestExpr *placeholder;
	PGList	   *newargs;
	PGList	   *resultexprs;
	PGListCell   *l;
	PGNode	   *defresult;
	PGOid			ptype;

	/* transform the test expression, if any */
	arg = transformExprRecurse(pstate, (PGNode *) c->arg);

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
			arg = coerce_parser->coerce_to_common_type(pstate, arg, TEXTOID, "CASE");

		/*
		 * Run collation assignment on the test expression so that we know
		 * what collation to mark the placeholder with.  In principle we could
		 * leave it to parse_collate.c to do that later, but propagating the
		 * result to the CaseTestExpr would be unnecessarily complicated.
		 */
		//TODO kindred
		//collation_parser->assign_expr_collations(pstate, arg);

		placeholder = makeNode(PGCaseTestExpr);
		placeholder->typeId = exprType(arg);
		placeholder->typeMod = exprTypmod(arg);
		placeholder->collation = exprCollation(arg);
	}
	else
		placeholder = NULL;

	newc->arg = (PGExpr *) arg;

	/* transform the list of arguments */
	newargs = NIL;
	resultexprs = NIL;
	foreach(l, c->args)
	{
		PGCaseWhen   *w = lfirst_node(PGCaseWhen, l);
		PGCaseWhen   *neww = makeNode(PGCaseWhen);
		PGNode	   *warg;

		warg = (PGNode *) w->expr;
		if (placeholder)
		{
			/* 
			 * CASE placeholder WHEN IS NOT DISTINCT FROM warg:
			 * 	set the first list element: expr->lexpr = placeholder
			 */
			if (isWhenIsNotDistinctFromExpr(warg))
			{
				/*
				 * Make a copy before we change warg.
				 * In transformation we don't want to change source (BoolExpr* Node).
				 * Always create new node and do the transformation
				 */
				warg = (PGNode *)copyObject(warg);
				PGAExpr *expr = (PGAExpr *) linitial(((PGBoolExpr *) warg)->args);
				expr->lexpr = (PGNode *) placeholder;
			}
			else
				warg = (PGNode *) makeSimpleAExpr(PG_AEXPR_OP, "=",
													(PGNode *) placeholder,
													 warg,
													 w->location);
		}
		else
		{
			if (isWhenIsNotDistinctFromExpr(warg))
			{
				parser_errposition(pstate, exprLocation((PGNode *) warg));
				ereport(ERROR,
						(errcode(PG_ERRCODE_SYNTAX_ERROR),
						 errmsg("syntax error at or near \"NOT\""),
						 errhint("Missing <operand> for \"CASE <operand> WHEN IS NOT DISTINCT FROM ...\"")));
			}
		}
		neww->expr = (PGExpr *) transformExprRecurse(pstate, warg);

		neww->expr = (PGExpr *) coerce_parser->coerce_to_boolean(pstate,
												(PGNode *) neww->expr,
												"CASE/WHEN");

		warg = (PGNode *) w->result;
		neww->result = (PGExpr *) transformExprRecurse(pstate, warg);
		neww->location = w->location;

		newargs = lappend(newargs, neww);
		resultexprs = lappend(resultexprs, neww->result);
	}

	newc->args = newargs;

	/* transform the default clause */
	defresult = (PGNode *) c->defresult;
	if (defresult == NULL)
	{
		PGAConst    *n = makeNode(PGAConst);

		n->val.type = T_PGNull;
		n->location = -1;
		defresult = (PGNode *) n;
	}
	newc->defresult = (PGExpr *) transformExprRecurse(pstate, defresult);

	/*
	 * Note: default result is considered the most significant type in
	 * determining preferred type. This is how the code worked before, but it
	 * seems a little bogus to me --- tgl
	 */
	resultexprs = lcons(newc->defresult, resultexprs);

	ptype = coerce_parser->select_common_type(pstate, resultexprs, "CASE", NULL);
	Assert(OidIsValid(ptype))
	newc->casetype = ptype;
	/* casecollid will be set by parse_collate.c */

	/* Convert default result clause, if necessary */
	newc->defresult = (PGExpr *)
		coerce_parser->coerce_to_common_type(pstate,
							  (PGNode *) newc->defresult,
							  ptype,
							  "CASE/ELSE");

	/* Convert when-clause results, if necessary */
	foreach(l, newc->args)
	{
		PGCaseWhen   *w = (PGCaseWhen *) lfirst(l);

		w->result = (PGExpr *)
			coerce_parser->coerce_to_common_type(pstate,
								  (PGNode *) w->result,
								  ptype,
								  "CASE/WHEN");
	}

	/* if any subexpression contained a SRF, complain */
	if (pstate->p_last_srf != last_srf)
	{
		parser_errposition(pstate,
									exprLocation(pstate->p_last_srf));
		ereport(ERROR,
				(errcode(PG_ERRCODE_FEATURE_NOT_SUPPORTED),
		/* translator: %s is name of a SQL construct, eg GROUP BY */
				 errmsg("set-returning functions are not allowed in %s",
						"CASE"),
				 errhint("You might be able to move the set-returning function into a LATERAL FROM item.")));
	}

	newc->location = c->location;

	return (PGNode *) newc;
};

PGNode *
ExprParser::transformCoalesceExpr(PGParseState *pstate, PGCoalesceExpr *c)
{
	PGCoalesceExpr *newc = makeNode(PGCoalesceExpr);
	PGNode	   *last_srf = pstate->p_last_srf;
	PGList	   *newargs = NIL;
	PGList	   *newcoercedargs = NIL;
	ListCell   *args;

	foreach(args, c->args)
	{
		PGNode	   *e = (PGNode *) lfirst(args);
		PGNode	   *newe;

		newe = transformExprRecurse(pstate, e);
		newargs = lappend(newargs, newe);
	}

	newc->coalescetype = coerce_parser->select_common_type(pstate, newargs, "COALESCE", NULL);
	/* coalescecollid will be set by parse_collate.c */

	/* Convert arguments if necessary */
	foreach(args, newargs)
	{
		PGNode	   *e = (PGNode *) lfirst(args);
		PGNode	   *newe;

		newe = coerce_parser->coerce_to_common_type(pstate, e,
									 newc->coalescetype,
									 "COALESCE");
		newcoercedargs = lappend(newcoercedargs, newe);
	}

	/* if any subexpression contained a SRF, complain */
	if (pstate->p_last_srf != last_srf)
	{
		parser_errposition(pstate,
									exprLocation(pstate->p_last_srf));
		ereport(ERROR,
				(errcode(PG_ERRCODE_FEATURE_NOT_SUPPORTED),
		/* translator: %s is name of a SQL construct, eg GROUP BY */
				 errmsg("set-returning functions are not allowed in %s",
						"COALESCE"),
				 errhint("You might be able to move the set-returning function into a LATERAL FROM item.")));
	}

	newc->args = newcoercedargs;
	newc->location = c->location;
	return (PGNode *) newc;
};

PGNode *
ExprParser::transformMinMaxExpr(PGParseState *pstate, PGMinMaxExpr *m)
{
	PGMinMaxExpr *newm = makeNode(PGMinMaxExpr);
	PGList	   *newargs = NIL;
	PGList	   *newcoercedargs = NIL;
	const char *funcname = (m->op == PG_IS_GREATEST) ? "GREATEST" : "LEAST";
	ListCell   *args;

	newm->op = m->op;
	foreach(args, m->args)
	{
		PGNode	   *e = (PGNode *) lfirst(args);
		PGNode	   *newe;

		newe = transformExprRecurse(pstate, e);
		newargs = lappend(newargs, newe);
	}

	newm->minmaxtype = coerce_parser->select_common_type(pstate, newargs, funcname, NULL);
	/* minmaxcollid and inputcollid will be set by parse_collate.c */

	/* Convert arguments if necessary */
	foreach(args, newargs)
	{
		PGNode	   *e = (PGNode *) lfirst(args);
		PGNode	   *newe;

		newe = coerce_parser->coerce_to_common_type(pstate, e,
									 newm->minmaxtype,
									 funcname);
		newcoercedargs = lappend(newcoercedargs, newe);
	}

	newm->args = newcoercedargs;
	newm->location = m->location;
	return (PGNode *) newm;
};

PGNode *
ExprParser::transformBooleanTest(PGParseState *pstate, PGBooleanTest *b)
{
	const char *clausename;

	if (operator_precedence_warning)
		emit_precedence_warnings(pstate, PREC_GROUP_POSTFIX_IS, "IS",
								 (PGNode *) b->arg, NULL,
								 b->location);

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
			elog(ERROR, "unrecognized booltesttype: %d",
				 (int) b->booltesttype);
			clausename = NULL;	/* keep compiler quiet */
	}

	b->arg = (PGExpr *) transformExprRecurse(pstate, (PGNode *) b->arg);

	b->arg = (PGExpr *) coerce_parser->coerce_to_boolean(pstate,
										(PGNode *) b->arg,
										clausename);

	return (PGNode *) b;
};

PGNode *
ExprParser::transformCurrentOfExpr(PGParseState *pstate, PGCurrentOfExpr *cexpr)
{
	int			sublevels_up;

	/*
	 * The target RTE must be simply updatable. If not, we error out
	 * early here to avoid having to deal with error cases later:
	 * rewriting/planning against views, for example.
	 */
	Assert(pstate->p_target_rangetblentry != NULL)
	(void) relation_parser->isSimplyUpdatableRelation(pstate->p_target_rangetblentry->relid, false);

	/* CURRENT OF can only appear at top level of UPDATE/DELETE */
	Assert(pstate->p_target_rangetblentry != NULL)
	cexpr->cvarno = relation_parser->RTERangeTablePosn(pstate,
									  pstate->p_target_rangetblentry,
									  &sublevels_up);
	Assert(sublevels_up == 0)

	//cexpr->target_relid = pstate->p_target_rangetblentry->relid;

	/*
	 * Check to see if the cursor name matches a parameter of type REFCURSOR.
	 * If so, replace the raw name reference with a parameter reference. (This
	 * is a hack for the convenience of plpgsql.)
	 */
	if (cexpr->cursor_name != NULL) /* in case already transformed */
	{
		PGColumnRef  *cref = makeNode(PGColumnRef);
		PGNode	   *node = NULL;

		/* Build an unqualified ColumnRef with the given name */
		cref->fields = list_make1(makeString(cexpr->cursor_name));
		cref->location = -1;

		/* See if there is a translation available from a parser hook */
		// if (pstate->p_pre_columnref_hook != NULL)
		// 	node = pstate->p_pre_columnref_hook(pstate, cref);
		// if (node == NULL && pstate->p_post_columnref_hook != NULL)
		// 	node = pstate->p_post_columnref_hook(pstate, cref, NULL);

		/*
		 * XXX Should we throw an error if we get a translation that isn't a
		 * refcursor Param?  For now it seems best to silently ignore false
		 * matches.
		 */
		if (node != NULL && IsA(node, PGParam))
		{
			//TODO kindred
			// PGParam	   *p = (PGParam *) node;

			// if (p->paramkind == PG_PARAM_EXTERN &&
			// 	p->paramtype == REFCURSOROID)
			// {
			// 	/* Matches, so convert CURRENT OF to a param reference */
			// 	cexpr->cursor_name = NULL;
			// 	cexpr->cursor_param = p->paramid;
			// }
			ereport(
                ERROR,
                (errcode(PG_ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("Do not supported yet!")));
		}
	}

	return (PGNode *) cexpr;
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
        targs = lappend(targs, transformExpr(pstate, (PGNode *)lfirst(lc), EXPR_KIND_GROUP_BY));

    new_gf->args = targs;

    //new_gf->ngrpcols = gf->ngrpcols;

    return (PGNode *)new_gf;
};

PGNode *
ExprParser::transformExprRecurse(PGParseState *pstate, PGNode *expr)
{
    PGNode * result;

    if (expr == NULL)
        return NULL;

    /* Guard against stack overflow due to overly complex expressions */
	//TODO kindred
    //check_stack_depth();

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

            result = (PGNode *) node_parser->make_const(pstate, val, con->location);
            break;
        }

        case T_PGAIndirection: {
            PGAIndirection * ind = (PGAIndirection *)expr;

            result = transformExprRecurse(pstate, ind->arg);
            result = transformIndirection(pstate, result, ind->indirection);
            break;
        }

        case T_PGAArrayExpr:
            result = transformArrayExpr(pstate, (PGAArrayExpr *)expr, InvalidOid, InvalidOid, -1);
            break;

        case T_PGTypeCast: {
            PGTypeCast * tc = (PGTypeCast *)expr;

            /*
				 * If the subject of the typecast is an ARRAY[] construct and
				 * the target type is an array type, we invoke
				 * transformArrayExpr() directly so that we can pass down the
				 * type information.  This avoids some cases where
				 * transformArrayExpr() might not infer the correct type.
				 */
            if (IsA(tc->arg, PGAArrayExpr))
            {
                    PGOid targetType;
                    PGOid elementType;
                    int32 targetTypmod;

                    type_parser->typenameTypeIdAndMod(pstate, tc->typeName, &targetType, &targetTypmod);

                    /*
					 * If target is a domain over array, work with the base
					 * array type here.  transformTypeCast below will cast the
					 * array type to the domain.  In the usual case that the
					 * target is not a domain, transformTypeCast is a no-op.
					 */
                    targetType = TypeProvider::getBaseTypeAndTypmod(targetType, &targetTypmod);
                    elementType = TypeProvider::get_element_type(targetType);
                    if (OidIsValid(elementType))
                    {
                        tc = (PGTypeCast *)copyObject(tc);
                        tc->arg = transformArrayExpr(pstate, (PGAArrayExpr *)tc->arg, targetType, elementType, targetTypmod);
                    }
            }

            result = transformTypeCast(pstate, tc);
            break;
        }

        case T_PGCollateClause:
            result = transformCollateClause(pstate, (PGCollateClause *)expr);
            break;

        case T_PGAExpr: {
            PGAExpr * a = (PGAExpr *)expr;

            switch (a->kind)
            {
                    case PG_AEXPR_OP:
                        result = transformAExprOp(pstate, a);
                        break;
					//TODO kindred
                    // case AEXPR_AND:
                    //     result = transformAExprAnd(pstate, a);
                    //     break;
                    // case AEXPR_OR:
                    //     result = transformAExprOr(pstate, a);
                    //     break;
                    // case PG_AEXPR_NOT:
                    //     result = transformAExprNot(pstate, a);
                    //     break;
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
                        result = NULL; /* keep compiler quiet */
                        break;
            }
            break;
        }

        case T_PGFuncCall:
            result = transformFuncCall(pstate, (PGFuncCall *)expr);
            break;

        case T_PGNamedArgExpr: {
            PGNamedArgExpr * na = (PGNamedArgExpr *)expr;

            na->arg = (PGExpr *)transformExprRecurse(pstate, (PGNode *)na->arg);
            result = expr;
            break;
        }

        case T_PGSubLink:
            result = transformSubLink(pstate, (PGSubLink *)expr);
            break;

        case T_PGCaseExpr:
            result = transformCaseExpr(pstate, (PGCaseExpr *)expr);
            break;

        case T_PGRowExpr:
            result = transformRowExpr(pstate, (PGRowExpr *)expr);
            break;
		//TODO kindred
        // case T_TableValueExpr:
        //     result = transformTableValueExpr(pstate, (TableValueExpr *)expr);
        //     break;

        case T_PGCoalesceExpr:
            result = transformCoalesceExpr(pstate, (PGCoalesceExpr *)expr);
            break;

        case T_PGMinMaxExpr:
            result = transformMinMaxExpr(pstate, (PGMinMaxExpr *)expr);
            break;
		//TODO kindred
        // case T_XmlExpr:
        //     result = transformXmlExpr(pstate, (XmlExpr *)expr);
        //     break;

        // case T_XmlSerialize:
        //     result = transformXmlSerialize(pstate, (XmlSerialize *)expr);
        //     break;

        case T_PGNullTest: {
            PGNullTest * n = (PGNullTest *)expr;

            n->arg = (PGExpr *)transformExprRecurse(pstate, (PGNode *)n->arg);
            /* the argument can be any type, so don't coerce it */
            n->argisrow = TypeProvider::type_is_rowtype(exprType((PGNode *)n->arg));
            result = expr;
            break;
        }

        case T_PGBooleanTest:
            result = transformBooleanTest(pstate, (PGBooleanTest *)expr);
            break;

        case T_PGCurrentOfExpr:
            result = transformCurrentOfExpr(pstate, (PGCurrentOfExpr *)expr);
            break;

        case T_PGGroupingFunc: {
            PGGroupingFunc * gf = (PGGroupingFunc *)expr;
            result = transformGroupingFunc(pstate, gf);
            break;
        }

		//TODO kindred
        // case T_PartitionBoundSpec: {
        //     PartitionBoundSpec * in = (PartitionBoundSpec *)expr;
        //     PartitionRangeItem * ri;
        //     List * out = NIL;
        //     ListCell * lc;

        //     if (in->partStart)
        //     {
        //             ri = (PartitionRangeItem *)in->partStart;

        //             /* ALTER TABLE ... ADD PARTITION might feed
		// 			 * "pre-cooked" expressions into the boundspec for
		// 			 * range items (which are Lists) 
		// 			 */
        //             {
        //                 Assert(IsA(in->partStart, PartitionRangeItem));

        //                 foreach (lc, ri->partRangeVal)
        //                 {
        //                     Node * n = lfirst(lc);
        //                     out = lappend(out, transformExpr(pstate, n, EXPR_KIND_PARTITION_EXPRESSION));
        //                 }
        //                 ri->partRangeVal = out;
        //                 out = NIL;
        //             }
        //     }
        //     if (in->partEnd)
        //     {
        //             ri = (PartitionRangeItem *)in->partEnd;

        //             /* ALTER TABLE ... ADD PARTITION might feed
		// 			 * "pre-cooked" expressions into the boundspec for
		// 			 * range items (which are Lists) 
		// 			 */
        //             {
        //                 Assert(IsA(in->partEnd, PartitionRangeItem));
        //                 foreach (lc, ri->partRangeVal)
        //                 {
        //                     Node * n = lfirst(lc);
        //                     out = lappend(out, transformExpr(pstate, n, EXPR_KIND_PARTITION_EXPRESSION));
        //                 }
        //                 ri->partRangeVal = out;
        //                 out = NIL;
        //             }
        //     }
        //     if (in->partEvery)
        //     {
        //             ri = (PartitionRangeItem *)in->partEvery;
        //             Assert(IsA(in->partEvery, PartitionRangeItem));
        //             foreach (lc, ri->partRangeVal)
        //             {
        //                 Node * n = lfirst(lc);
        //                 out = lappend(out, transformExpr(pstate, n, EXPR_KIND_PARTITION_EXPRESSION));
        //             }
        //             ri->partRangeVal = out;
        //     }

        //     result = (Node *)in;
        // }
        // break;

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
        case T_PGNullIfExpr:
        case T_PGScalarArrayOpExpr:
        case T_PGBoolExpr:
        case T_PGFieldSelect:
        case T_PGFieldStore:
        case T_PGRelabelType:
        case T_PGCoerceViaIO:
        case T_PGArrayCoerceExpr:
        case T_PGConvertRowtypeExpr:
        case T_PGCollateExpr:
        case T_PGCaseTestExpr:
        case T_PGArrayExpr:
        case T_PGCoerceToDomain:
        case T_PGCoerceToDomainValue:
        case T_PGSetToDefault:
        // case T_PGGroupId:
        case T_PGInteger: {
            result = (PGNode *)expr;
            break;
        }

        default:
            /* should not reach here */
            elog(ERROR, "unrecognized node type: %d", (int)nodeTag(expr));
            result = NULL; /* keep compiler quiet */
            break;
    }

    return result;
};

const char *
ExprParser::ParseExprKindName(PGParseExprKind exprKind)
{
	switch (exprKind)
	{
		case EXPR_KIND_NONE:
			return "invalid expression context";
		case EXPR_KIND_OTHER:
			return "extension expression";
		case EXPR_KIND_JOIN_ON:
			return "JOIN/ON";
		case EXPR_KIND_JOIN_USING:
			return "JOIN/USING";
		case EXPR_KIND_FROM_SUBSELECT:
			return "sub-SELECT in FROM";
		case EXPR_KIND_FROM_FUNCTION:
			return "function in FROM";
		case EXPR_KIND_WHERE:
			return "WHERE";
		case EXPR_KIND_POLICY:
			return "POLICY";
		case EXPR_KIND_HAVING:
			return "HAVING";
		case EXPR_KIND_FILTER:
			return "FILTER";
		case EXPR_KIND_WINDOW_PARTITION:
			return "window PARTITION BY";
		case EXPR_KIND_WINDOW_ORDER:
			return "window ORDER BY";
		case EXPR_KIND_WINDOW_FRAME_RANGE:
			return "window RANGE";
		case EXPR_KIND_WINDOW_FRAME_ROWS:
			return "window ROWS";
		case EXPR_KIND_WINDOW_FRAME_GROUPS:
			return "window GROUPS";
		case EXPR_KIND_SELECT_TARGET:
			return "SELECT";
		case EXPR_KIND_INSERT_TARGET:
			return "INSERT";
		case EXPR_KIND_UPDATE_SOURCE:
		case EXPR_KIND_UPDATE_TARGET:
			return "UPDATE";
		case EXPR_KIND_GROUP_BY:
			return "GROUP BY";
		case EXPR_KIND_ORDER_BY:
			return "ORDER BY";
		case EXPR_KIND_DISTINCT_ON:
			return "DISTINCT ON";
		case EXPR_KIND_LIMIT:
			return "LIMIT";
		case EXPR_KIND_OFFSET:
			return "OFFSET";
		case EXPR_KIND_RETURNING:
			return "RETURNING";
		case EXPR_KIND_VALUES:
		case EXPR_KIND_VALUES_SINGLE:
			return "VALUES";
		case EXPR_KIND_CHECK_CONSTRAINT:
		case EXPR_KIND_DOMAIN_CHECK:
			return "CHECK";
		case EXPR_KIND_COLUMN_DEFAULT:
		case EXPR_KIND_FUNCTION_DEFAULT:
			return "DEFAULT";
		case EXPR_KIND_INDEX_EXPRESSION:
			return "index expression";
		case EXPR_KIND_INDEX_PREDICATE:
			return "index predicate";
		case EXPR_KIND_ALTER_COL_TRANSFORM:
			return "USING";
		case EXPR_KIND_EXECUTE_PARAMETER:
			return "EXECUTE";
		case EXPR_KIND_TRIGGER_WHEN:
			return "WHEN";
		case EXPR_KIND_PARTITION_BOUND:
			return "partition bound";
		case EXPR_KIND_PARTITION_EXPRESSION:
			return "PARTITION BY";
		case EXPR_KIND_CALL_ARGUMENT:
			return "CALL";
		case EXPR_KIND_COPY_WHERE:
			return "WHERE";
		case EXPR_KIND_GENERATED_COLUMN:
			return "GENERATED AS";
		case EXPR_KIND_SCATTER_BY:
			return "SCATTER BY";

			/*
			 * There is intentionally no default: case here, so that the
			 * compiler will warn if we add a new ParseExprKind without
			 * extending this switch.  If we do see an unrecognized value at
			 * runtime, we'll fall through to the "unrecognized" return.
			 */
	}
	return "unrecognized expression kind";
};

}
