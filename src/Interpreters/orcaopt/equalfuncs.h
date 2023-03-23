#pragma once

#ifdef __clang__
#pragma clang diagnostic ignored "-Wunused-parameter"
#else
#pragma GCC diagnostic ignored "-Wunused-parameter"
#endif

#include <Interpreters/orcaopt/parser_common.h>
#include <Interpreters/orcaopt/datum.h>

extern bool pg_equal(const void * a, const void * b);

/*
 * Macros to simplify comparison of different kinds of fields.  Use these
 * wherever possible to reduce the chance for silly typos.  Note that these
 * hard-wire the convention that the local variables in an Equal routine are
 * named 'a' and 'b'.
 */

/* Compare a simple scalar field (int, float, bool, enum, etc) */
#define COMPARE_SCALAR_FIELD(fldname) \
    do \
    { \
        if (a->fldname != b->fldname) \
            return false; \
    } while (0)

/* Compare a field that is a pointer to some kind of Node or Node tree */
#define COMPARE_NODE_FIELD(fldname) \
    do \
    { \
        if (!pg_equal(a->fldname, b->fldname)) \
            return false; \
    } while (0)

/* Compare a field that is a pointer to a Bitmapset */
#define COMPARE_BITMAPSET_FIELD(fldname) \
    do \
    { \
        if (!bms_equal(a->fldname, b->fldname)) \
            return false; \
    } while (0)

/* Compare a field that is a pointer to a C string, or perhaps NULL */
#define COMPARE_STRING_FIELD(fldname) \
    do \
    { \
        if (!equalstr(a->fldname, b->fldname)) \
            return false; \
    } while (0)

/* Macro for comparing string fields that might be NULL */
#define equalstr(a, b) (((a) != NULL && (b) != NULL) ? (strcmp(a, b) == 0) : (a) == (b))

/* Compare a field that is a pointer to a simple palloc'd object of size sz */
#define COMPARE_POINTER_FIELD(fldname, sz) \
    do \
    { \
        if (memcmp(a->fldname, b->fldname, (sz)) != 0) \
            return false; \
    } while (0)

/*
 * Compare a field that is a varlena datum to the other.
 * Note the result will be false if one is toasted and the other is untoasted.
 * It depends on the context if we can say those are equal or not.
 */
#define COMPARE_VARLENA_FIELD(fldname, len) \
    do \
    { \
        if (a->fldname != b->fldname) \
        { \
            if (a->fldname == NULL || b->fldname == NULL) \
                return false; \
            if (!datumIsEqual(PointerGetDatum(a->fldname), PointerGetDatum(b->fldname), false, len)) \
                return false; \
        } \
    } while (0)

/* Compare a parse location field (this is a no-op, per note above) */
#define COMPARE_LOCATION_FIELD(fldname) ((void)0)

/* Compare a CoercionForm field (also a no-op, per comment in primnodes.h) */
#define COMPARE_COERCIONFORM_FIELD(fldname) ((void)0)


/*
 *	Stuff from primnodes.h
 */

static bool _equalAlias(const void * a_arg, const void * b_arg)
{
    const duckdb_libpgquery::PGAlias * a = (const duckdb_libpgquery::PGAlias *)a_arg;
    const duckdb_libpgquery::PGAlias * b = (const duckdb_libpgquery::PGAlias *)b_arg;
    COMPARE_STRING_FIELD(aliasname);
    COMPARE_NODE_FIELD(colnames);

    return true;
}

static bool _equalRangeVar(const void * a_arg, const void * b_arg)
{
    const duckdb_libpgquery::PGRangeVar * a = (const duckdb_libpgquery::PGRangeVar *)a_arg;
    const duckdb_libpgquery::PGRangeVar * b = (const duckdb_libpgquery::PGRangeVar *)b_arg;

    COMPARE_STRING_FIELD(catalogname);
    COMPARE_STRING_FIELD(schemaname);
    COMPARE_STRING_FIELD(relname);
    COMPARE_SCALAR_FIELD(inh);
    COMPARE_SCALAR_FIELD(relpersistence);
    COMPARE_NODE_FIELD(alias);
    COMPARE_LOCATION_FIELD(location);

    return true;
}

/*
 * Records information about the target of a CTAS (SELECT ... INTO).
 */
static bool _equalIntoClause(const void * a_arg, const void * b_arg)
{
    const duckdb_libpgquery::PGIntoClause * a = (const duckdb_libpgquery::PGIntoClause *)a_arg;
    const duckdb_libpgquery::PGIntoClause * b = (const duckdb_libpgquery::PGIntoClause *)b_arg;

    COMPARE_NODE_FIELD(rel);
    COMPARE_NODE_FIELD(colNames);
    COMPARE_NODE_FIELD(options);
    COMPARE_SCALAR_FIELD(onCommit);
    COMPARE_STRING_FIELD(tableSpaceName);
    COMPARE_NODE_FIELD(viewQuery);
    COMPARE_SCALAR_FIELD(skipData);
    //COMPARE_NODE_FIELD(distributedBy);

    return true;
}

/*
 * We don't need an _equalExpr because Expr is an abstract supertype which
 * should never actually get instantiated.  Also, since it has no common
 * fields except NodeTag, there's no need for a helper routine to factor
 * out comparing the common fields...
 */

static bool _equalVar(const void * a_arg, const void * b_arg)
{
    const duckdb_libpgquery::PGVar * a = (const duckdb_libpgquery::PGVar *)a_arg;
    const duckdb_libpgquery::PGVar * b = (const duckdb_libpgquery::PGVar *)b_arg;

    COMPARE_SCALAR_FIELD(varno);
    COMPARE_SCALAR_FIELD(varattno);
    COMPARE_SCALAR_FIELD(vartype);
    COMPARE_SCALAR_FIELD(vartypmod);
    COMPARE_SCALAR_FIELD(varcollid);
    COMPARE_SCALAR_FIELD(varlevelsup);
    COMPARE_SCALAR_FIELD(varnoold);
    COMPARE_SCALAR_FIELD(varoattno);
    COMPARE_LOCATION_FIELD(location);

    return true;
}

static bool _equalConst(const void * a_arg, const void * b_arg)
{
    const duckdb_libpgquery::PGConst * a = (const duckdb_libpgquery::PGConst *)a_arg;
    const duckdb_libpgquery::PGConst * b = (const duckdb_libpgquery::PGConst *)b_arg;

    COMPARE_SCALAR_FIELD(consttype);
    COMPARE_SCALAR_FIELD(consttypmod);
    COMPARE_SCALAR_FIELD(constcollid);
    COMPARE_SCALAR_FIELD(constlen);
    COMPARE_SCALAR_FIELD(constisnull);
    COMPARE_SCALAR_FIELD(constbyval);
    COMPARE_LOCATION_FIELD(location);

    /*
	 * We treat all NULL constants of the same type as equal. Someday this
	 * might need to change?  But datumIsEqual doesn't work on nulls, so...
	 */
    if (a->constisnull)
        return true;
    return datumIsEqual(a->constvalue, b->constvalue, a->constbyval, a->constlen);
}

static bool _equalParam(const void * a_arg, const void * b_arg)
{
    const duckdb_libpgquery::PGParam * a = (const duckdb_libpgquery::PGParam *)a_arg;
    const duckdb_libpgquery::PGParam * b = (const duckdb_libpgquery::PGParam *)b_arg;

    COMPARE_SCALAR_FIELD(paramkind);
    COMPARE_SCALAR_FIELD(paramid);
    COMPARE_SCALAR_FIELD(paramtype);
    COMPARE_SCALAR_FIELD(paramtypmod);
    COMPARE_SCALAR_FIELD(paramcollid);
    COMPARE_LOCATION_FIELD(location);

    return true;
}

static bool _equalAggref(const void * a_arg, const void * b_arg)
{
    const duckdb_libpgquery::PGAggref * a = (const duckdb_libpgquery::PGAggref *)a_arg;
    const duckdb_libpgquery::PGAggref * b = (const duckdb_libpgquery::PGAggref *)b_arg;

    COMPARE_SCALAR_FIELD(aggfnoid);
    COMPARE_SCALAR_FIELD(aggtype);
    COMPARE_SCALAR_FIELD(aggcollid);
    COMPARE_SCALAR_FIELD(inputcollid);
    COMPARE_NODE_FIELD(aggdirectargs);
    COMPARE_NODE_FIELD(args);
    COMPARE_NODE_FIELD(aggorder);
    COMPARE_NODE_FIELD(aggdistinct);
    COMPARE_NODE_FIELD(aggfilter);
    COMPARE_SCALAR_FIELD(aggstar);
    COMPARE_SCALAR_FIELD(aggvariadic);
    COMPARE_SCALAR_FIELD(aggkind);
    COMPARE_SCALAR_FIELD(agglevelsup);
    COMPARE_SCALAR_FIELD(aggsplit);
    COMPARE_LOCATION_FIELD(location);

    return true;
}

static bool _equalWindowFunc(const void * a_arg, const void * b_arg)
{
    const duckdb_libpgquery::PGWindowFunc * a = (const duckdb_libpgquery::PGWindowFunc *)a_arg;
    const duckdb_libpgquery::PGWindowFunc * b = (const duckdb_libpgquery::PGWindowFunc *)b_arg;

    COMPARE_SCALAR_FIELD(winfnoid);
    COMPARE_SCALAR_FIELD(wintype);
    COMPARE_SCALAR_FIELD(wincollid);
    COMPARE_SCALAR_FIELD(inputcollid);
    COMPARE_NODE_FIELD(args);
    COMPARE_NODE_FIELD(aggfilter);
    COMPARE_SCALAR_FIELD(winref);
    COMPARE_SCALAR_FIELD(winstar);
    COMPARE_SCALAR_FIELD(winagg);
    //COMPARE_SCALAR_FIELD(windistinct);
    COMPARE_LOCATION_FIELD(location);

    return true;
}

static bool _equalArrayRef(const void * a_arg, const void * b_arg)
{
    const duckdb_libpgquery::PGArrayRef * a = (const duckdb_libpgquery::PGArrayRef *)a_arg;
    const duckdb_libpgquery::PGArrayRef * b = (const duckdb_libpgquery::PGArrayRef *)b_arg;

    COMPARE_SCALAR_FIELD(refarraytype);
    COMPARE_SCALAR_FIELD(refelemtype);
    COMPARE_SCALAR_FIELD(reftypmod);
    COMPARE_SCALAR_FIELD(refcollid);
    COMPARE_NODE_FIELD(refupperindexpr);
    COMPARE_NODE_FIELD(reflowerindexpr);
    COMPARE_NODE_FIELD(refexpr);
    COMPARE_NODE_FIELD(refassgnexpr);

    return true;
}

static bool _equalFuncExpr(const void * a_arg, const void * b_arg)
{
    const duckdb_libpgquery::PGFuncExpr * a = (const duckdb_libpgquery::PGFuncExpr *)a_arg;
    const duckdb_libpgquery::PGFuncExpr * b = (const duckdb_libpgquery::PGFuncExpr *)b_arg;

    COMPARE_SCALAR_FIELD(funcid);
    COMPARE_SCALAR_FIELD(funcresulttype);
    COMPARE_SCALAR_FIELD(funcretset);
    COMPARE_SCALAR_FIELD(funcvariadic);
    COMPARE_COERCIONFORM_FIELD(funcformat);
    COMPARE_SCALAR_FIELD(funccollid);
    COMPARE_SCALAR_FIELD(inputcollid);
    COMPARE_NODE_FIELD(args);
    COMPARE_LOCATION_FIELD(location);

    return true;
}

static bool _equalNamedArgExpr(const void * a_arg, const void * b_arg)
{
    const duckdb_libpgquery::PGNamedArgExpr * a = (const duckdb_libpgquery::PGNamedArgExpr *)a_arg;
    const duckdb_libpgquery::PGNamedArgExpr * b = (const duckdb_libpgquery::PGNamedArgExpr *)b_arg;

    COMPARE_NODE_FIELD(arg);
    COMPARE_STRING_FIELD(name);
    COMPARE_SCALAR_FIELD(argnumber);
    COMPARE_LOCATION_FIELD(location);

    return true;
}

static bool _equalOpExpr(const void * a_arg, const void * b_arg)
{
    const duckdb_libpgquery::PGOpExpr * a = (const duckdb_libpgquery::PGOpExpr *)a_arg;
    const duckdb_libpgquery::PGOpExpr * b = (const duckdb_libpgquery::PGOpExpr *)b_arg;

    COMPARE_SCALAR_FIELD(opno);

    /*
	 * Special-case opfuncid: it is allowable for it to differ if one node
	 * contains zero and the other doesn't.  This just means that the one node
	 * isn't as far along in the parse/plan pipeline and hasn't had the
	 * opfuncid cache filled yet.
	 */
    if (a->opfuncid != b->opfuncid && a->opfuncid != 0 && b->opfuncid != 0)
        return false;

    COMPARE_SCALAR_FIELD(opresulttype);
    COMPARE_SCALAR_FIELD(opretset);
    COMPARE_SCALAR_FIELD(opcollid);
    COMPARE_SCALAR_FIELD(inputcollid);
    COMPARE_NODE_FIELD(args);
    COMPARE_LOCATION_FIELD(location);

    return true;
}

static bool _equalDistinctExpr(const void * a_arg, const void * b_arg)
{
    const duckdb_libpgquery::DistinctExpr * a = (const duckdb_libpgquery::DistinctExpr *)a_arg;
    const duckdb_libpgquery::DistinctExpr * b = (const duckdb_libpgquery::DistinctExpr *)b_arg;

    COMPARE_SCALAR_FIELD(opno);

    /*
	 * Special-case opfuncid: it is allowable for it to differ if one node
	 * contains zero and the other doesn't.  This just means that the one node
	 * isn't as far along in the parse/plan pipeline and hasn't had the
	 * opfuncid cache filled yet.
	 */
    if (a->opfuncid != b->opfuncid && a->opfuncid != 0 && b->opfuncid != 0)
        return false;

    COMPARE_SCALAR_FIELD(opresulttype);
    COMPARE_SCALAR_FIELD(opretset);
    COMPARE_SCALAR_FIELD(opcollid);
    COMPARE_SCALAR_FIELD(inputcollid);
    COMPARE_NODE_FIELD(args);
    COMPARE_LOCATION_FIELD(location);

    return true;
}

static bool _equalNullIfExpr(const void * a_arg, const void * b_arg)
{
    const duckdb_libpgquery::NullIfExpr * a = (const duckdb_libpgquery::NullIfExpr *)a_arg;
    const duckdb_libpgquery::NullIfExpr * b = (const duckdb_libpgquery::NullIfExpr *)b_arg;

    COMPARE_SCALAR_FIELD(opno);

    /*
	 * Special-case opfuncid: it is allowable for it to differ if one node
	 * contains zero and the other doesn't.  This just means that the one node
	 * isn't as far along in the parse/plan pipeline and hasn't had the
	 * opfuncid cache filled yet.
	 */
    if (a->opfuncid != b->opfuncid && a->opfuncid != 0 && b->opfuncid != 0)
        return false;

    COMPARE_SCALAR_FIELD(opresulttype);
    COMPARE_SCALAR_FIELD(opretset);
    COMPARE_SCALAR_FIELD(opcollid);
    COMPARE_SCALAR_FIELD(inputcollid);
    COMPARE_NODE_FIELD(args);
    COMPARE_LOCATION_FIELD(location);

    return true;
}

static bool _equalScalarArrayOpExpr(const void * a_arg, const void * b_arg)
{
    const duckdb_libpgquery::PGScalarArrayOpExpr * a = (const duckdb_libpgquery::PGScalarArrayOpExpr *)a_arg;
    const duckdb_libpgquery::PGScalarArrayOpExpr * b = (const duckdb_libpgquery::PGScalarArrayOpExpr *)b_arg;

    COMPARE_SCALAR_FIELD(opno);

    /*
	 * Special-case opfuncid: it is allowable for it to differ if one node
	 * contains zero and the other doesn't.  This just means that the one node
	 * isn't as far along in the parse/plan pipeline and hasn't had the
	 * opfuncid cache filled yet.
	 */
    if (a->opfuncid != b->opfuncid && a->opfuncid != 0 && b->opfuncid != 0)
        return false;

    COMPARE_SCALAR_FIELD(useOr);
    COMPARE_SCALAR_FIELD(inputcollid);
    COMPARE_NODE_FIELD(args);
    COMPARE_LOCATION_FIELD(location);

    return true;
}

static bool _equalBoolExpr(const void * a_arg, const void * b_arg)
{
    const duckdb_libpgquery::PGBoolExpr * a = (const duckdb_libpgquery::PGBoolExpr *)a_arg;
    const duckdb_libpgquery::PGBoolExpr * b = (const duckdb_libpgquery::PGBoolExpr *)b_arg;

    COMPARE_SCALAR_FIELD(boolop);
    COMPARE_NODE_FIELD(args);
    COMPARE_LOCATION_FIELD(location);

    return true;
}

static bool _equalSubLink(const void * a_arg, const void * b_arg)
{
    const duckdb_libpgquery::PGSubLink * a = (const duckdb_libpgquery::PGSubLink *)a_arg;
    const duckdb_libpgquery::PGSubLink * b = (const duckdb_libpgquery::PGSubLink *)b_arg;

    COMPARE_SCALAR_FIELD(subLinkType);
    COMPARE_NODE_FIELD(testexpr);
    COMPARE_NODE_FIELD(operName);
    COMPARE_NODE_FIELD(subselect);
    COMPARE_LOCATION_FIELD(location);

    return true;
}

static bool _equalSubPlan(const void * a_arg, const void * b_arg)
{
    const duckdb_libpgquery::PGSubPlan * a = (const duckdb_libpgquery::PGSubPlan *)a_arg;
    const duckdb_libpgquery::PGSubPlan * b = (const duckdb_libpgquery::PGSubPlan *)b_arg;

    COMPARE_SCALAR_FIELD(subLinkType);
    /* CDB: Ignore value of qDispSliceId. */
    COMPARE_NODE_FIELD(testexpr);
    COMPARE_NODE_FIELD(paramIds);
    COMPARE_SCALAR_FIELD(plan_id);
    COMPARE_STRING_FIELD(plan_name);
    COMPARE_SCALAR_FIELD(firstColType);
    COMPARE_SCALAR_FIELD(firstColTypmod);
    COMPARE_SCALAR_FIELD(firstColCollation);
    COMPARE_SCALAR_FIELD(useHashTable);
    COMPARE_SCALAR_FIELD(unknownEqFalse);
    /* CDB: Ignore value of is_initplan */
    //COMPARE_SCALAR_FIELD(is_multirow); /*CDB*/
    COMPARE_NODE_FIELD(setParam);
    COMPARE_NODE_FIELD(parParam);
    COMPARE_NODE_FIELD(args);
    //COMPARE_NODE_FIELD(extParam);
    COMPARE_SCALAR_FIELD(startup_cost);
    COMPARE_SCALAR_FIELD(per_call_cost);

    return true;
}

// static bool _equalAlternativeSubPlan(const void * a_arg, const void * b_arg)
// {
//     const duckdb_libpgquery::PGAlternativeSubPlan * a = (const duckdb_libpgquery::PGAlternativeSubPlan *)a_arg;
//     const duckdb_libpgquery::PGAlternativeSubPlan * b = (const duckdb_libpgquery::PGAlternativeSubPlan *)b_arg;

//     COMPARE_NODE_FIELD(subplans);

//     return true;
// }

static bool _equalFieldSelect(const void * a_arg, const void * b_arg)
{
    const duckdb_libpgquery::PGFieldSelect * a = (const duckdb_libpgquery::PGFieldSelect *)a_arg;
    const duckdb_libpgquery::PGFieldSelect * b = (const duckdb_libpgquery::PGFieldSelect *)b_arg;

    COMPARE_NODE_FIELD(arg);
    COMPARE_SCALAR_FIELD(fieldnum);
    COMPARE_SCALAR_FIELD(resulttype);
    COMPARE_SCALAR_FIELD(resulttypmod);
    COMPARE_SCALAR_FIELD(resultcollid);

    return true;
}

// static bool _equalFieldStore(const void * a_arg, const void * b_arg)
// {
//     const duckdb_libpgquery::PGFieldStore * a = (const duckdb_libpgquery::PGFieldStore *)a_arg;
//     const duckdb_libpgquery::PGFieldStore * b = (const duckdb_libpgquery::PGFieldStore *)b_arg;

//     COMPARE_NODE_FIELD(arg);
//     COMPARE_NODE_FIELD(newvals);
//     COMPARE_NODE_FIELD(fieldnums);
//     COMPARE_SCALAR_FIELD(resulttype);

//     return true;
// }

// static bool _equalRelabelType(const duckdb_libpgquery::PGRelabelType * a, const duckdb_libpgquery::PGRelabelType * b)
// {
//     COMPARE_NODE_FIELD(arg);
//     COMPARE_SCALAR_FIELD(resulttype);
//     COMPARE_SCALAR_FIELD(resulttypmod);
//     COMPARE_SCALAR_FIELD(resultcollid);
//     COMPARE_COERCIONFORM_FIELD(relabelformat);
//     COMPARE_LOCATION_FIELD(location);

//     return true;
// }

// static bool _equalCoerceViaIO(const duckdb_libpgquery::PGCoerceViaIO * a, const duckdb_libpgquery::PGCoerceViaIO * b)
// {
//     COMPARE_NODE_FIELD(arg);
//     COMPARE_SCALAR_FIELD(resulttype);
//     COMPARE_SCALAR_FIELD(resultcollid);
//     COMPARE_COERCIONFORM_FIELD(coerceformat);
//     COMPARE_LOCATION_FIELD(location);

//     return true;
// }

// static bool _equalArrayCoerceExpr(const duckdb_libpgquery::PGArrayCoerceExpr * a, const duckdb_libpgquery::PGArrayCoerceExpr * b)
// {
//     COMPARE_NODE_FIELD(arg);
//     COMPARE_SCALAR_FIELD(elemfuncid);
//     COMPARE_SCALAR_FIELD(resulttype);
//     COMPARE_SCALAR_FIELD(resulttypmod);
//     COMPARE_SCALAR_FIELD(resultcollid);
//     COMPARE_SCALAR_FIELD(isExplicit);
//     COMPARE_COERCIONFORM_FIELD(coerceformat);
//     COMPARE_LOCATION_FIELD(location);

//     return true;
// }

// static bool _equalConvertRowtypeExpr(const duckdb_libpgquery::PGConvertRowtypeExpr * a, const duckdb_libpgquery::PGConvertRowtypeExpr * b)
// {
//     COMPARE_NODE_FIELD(arg);
//     COMPARE_SCALAR_FIELD(resulttype);
//     COMPARE_COERCIONFORM_FIELD(convertformat);
//     COMPARE_LOCATION_FIELD(location);

//     return true;
// }

// static bool _equalCollateExpr(const duckdb_libpgquery::PGCollateExpr * a, const duckdb_libpgquery::PGCollateExpr * b)
// {
//     COMPARE_NODE_FIELD(arg);
//     COMPARE_SCALAR_FIELD(collOid);
//     COMPARE_LOCATION_FIELD(location);

//     return true;
// }

static bool _equalCaseExpr(const void * a_arg, const void * b_arg)
{
    const duckdb_libpgquery::PGCaseExpr * a = (const duckdb_libpgquery::PGCaseExpr *)a_arg;
    const duckdb_libpgquery::PGCaseExpr * b = (const duckdb_libpgquery::PGCaseExpr *)b_arg;

    COMPARE_SCALAR_FIELD(casetype);
    COMPARE_SCALAR_FIELD(casecollid);
    COMPARE_NODE_FIELD(arg);
    COMPARE_NODE_FIELD(args);
    COMPARE_NODE_FIELD(defresult);
    COMPARE_LOCATION_FIELD(location);

    return true;
}

static bool _equalCaseWhen(const void * a_arg, const void * b_arg)
{
    const duckdb_libpgquery::PGCaseWhen * a = (const duckdb_libpgquery::PGCaseWhen *)a_arg;
    const duckdb_libpgquery::PGCaseWhen * b = (const duckdb_libpgquery::PGCaseWhen *)b_arg;

    COMPARE_NODE_FIELD(expr);
    COMPARE_NODE_FIELD(result);
    COMPARE_LOCATION_FIELD(location);

    return true;
}

static bool _equalCaseTestExpr(const void * a_arg, const void * b_arg)
{
    const duckdb_libpgquery::PGCaseTestExpr * a = (const duckdb_libpgquery::PGCaseTestExpr *)a_arg;
    const duckdb_libpgquery::PGCaseTestExpr * b = (const duckdb_libpgquery::PGCaseTestExpr *)b_arg;

    COMPARE_SCALAR_FIELD(typeId);
    COMPARE_SCALAR_FIELD(typeMod);
    COMPARE_SCALAR_FIELD(collation);

    return true;
}

static bool _equalArrayExpr(const void * a_arg, const void * b_arg)
{
    const duckdb_libpgquery::PGArrayExpr * a = (const duckdb_libpgquery::PGArrayExpr *)a_arg;
    const duckdb_libpgquery::PGArrayExpr * b = (const duckdb_libpgquery::PGArrayExpr *)b_arg;

    COMPARE_SCALAR_FIELD(array_typeid);
    COMPARE_SCALAR_FIELD(array_collid);
    COMPARE_SCALAR_FIELD(element_typeid);
    COMPARE_NODE_FIELD(elements);
    COMPARE_SCALAR_FIELD(multidims);
    COMPARE_LOCATION_FIELD(location);

    return true;
}

static bool _equalRowExpr(const void * a_arg, const void * b_arg)
{
    const duckdb_libpgquery::PGRowExpr * a = (const duckdb_libpgquery::PGRowExpr *)a_arg;
    const duckdb_libpgquery::PGRowExpr * b = (const duckdb_libpgquery::PGRowExpr *)b_arg;

    COMPARE_NODE_FIELD(args);
    COMPARE_SCALAR_FIELD(row_typeid);
    COMPARE_COERCIONFORM_FIELD(row_format);
    COMPARE_NODE_FIELD(colnames);
    COMPARE_LOCATION_FIELD(location);

    return true;
}

static bool _equalRowCompareExpr(const void * a_arg, const void * b_arg)
{
    const duckdb_libpgquery::PGRowCompareExpr * a = (const duckdb_libpgquery::PGRowCompareExpr *)a_arg;
    const duckdb_libpgquery::PGRowCompareExpr * b = (const duckdb_libpgquery::PGRowCompareExpr *)b_arg;

    COMPARE_SCALAR_FIELD(rctype);
    COMPARE_NODE_FIELD(opnos);
    COMPARE_NODE_FIELD(opfamilies);
    COMPARE_NODE_FIELD(inputcollids);
    COMPARE_NODE_FIELD(largs);
    COMPARE_NODE_FIELD(rargs);

    return true;
}

static bool _equalCoalesceExpr(const void * a_arg, const void * b_arg)
{
    const duckdb_libpgquery::PGCoalesceExpr * a = (const duckdb_libpgquery::PGCoalesceExpr *)a_arg;
    const duckdb_libpgquery::PGCoalesceExpr * b = (const duckdb_libpgquery::PGCoalesceExpr *)b_arg;

    COMPARE_SCALAR_FIELD(coalescetype);
    COMPARE_SCALAR_FIELD(coalescecollid);
    COMPARE_NODE_FIELD(args);
    COMPARE_LOCATION_FIELD(location);

    return true;
}

static bool _equalMinMaxExpr(const void * a_arg, const void * b_arg)
{
    const duckdb_libpgquery::PGMinMaxExpr * a = (const duckdb_libpgquery::PGMinMaxExpr *)a_arg;
    const duckdb_libpgquery::PGMinMaxExpr * b = (const duckdb_libpgquery::PGMinMaxExpr *)b_arg;

    COMPARE_SCALAR_FIELD(minmaxtype);
    COMPARE_SCALAR_FIELD(minmaxcollid);
    COMPARE_SCALAR_FIELD(inputcollid);
    COMPARE_SCALAR_FIELD(op);
    COMPARE_NODE_FIELD(args);
    COMPARE_LOCATION_FIELD(location);

    return true;
}

// static bool _equalXmlExpr(const PGXmlExpr * a, const PGXmlExpr * b)
// {
//     COMPARE_SCALAR_FIELD(op);
//     COMPARE_STRING_FIELD(name);
//     COMPARE_NODE_FIELD(named_args);
//     COMPARE_NODE_FIELD(arg_names);
//     COMPARE_NODE_FIELD(args);
//     COMPARE_SCALAR_FIELD(xmloption);
//     COMPARE_SCALAR_FIELD(type);
//     COMPARE_SCALAR_FIELD(typmod);
//     COMPARE_LOCATION_FIELD(location);

//     return true;
// }

static bool _equalNullTest(const void * a_arg, const void * b_arg)
{
    const duckdb_libpgquery::PGNullTest * a = (const duckdb_libpgquery::PGNullTest *)a_arg;
    const duckdb_libpgquery::PGNullTest * b = (const duckdb_libpgquery::PGNullTest *)b_arg;

    COMPARE_NODE_FIELD(arg);
    COMPARE_SCALAR_FIELD(nulltesttype);
    COMPARE_SCALAR_FIELD(argisrow);

    return true;
}

static bool _equalBooleanTest(const void * a_arg, const void * b_arg)
{
    const duckdb_libpgquery::PGBooleanTest * a = (const duckdb_libpgquery::PGBooleanTest *)a_arg;
    const duckdb_libpgquery::PGBooleanTest * b = (const duckdb_libpgquery::PGBooleanTest *)b_arg;

    COMPARE_NODE_FIELD(arg);
    COMPARE_SCALAR_FIELD(booltesttype);

    return true;
}

static bool _equalCoerceToDomain(const void * a_arg, const void * b_arg)
{
    const duckdb_libpgquery::PGCoerceToDomain * a = (const duckdb_libpgquery::PGCoerceToDomain *)a_arg;
    const duckdb_libpgquery::PGCoerceToDomain * b = (const duckdb_libpgquery::PGCoerceToDomain *)b_arg;

    COMPARE_NODE_FIELD(arg);
    COMPARE_SCALAR_FIELD(resulttype);
    COMPARE_SCALAR_FIELD(resulttypmod);
    COMPARE_SCALAR_FIELD(resultcollid);
    COMPARE_COERCIONFORM_FIELD(coercionformat);
    COMPARE_LOCATION_FIELD(location);

    return true;
}

static bool _equalCoerceToDomainValue(const void * a_arg, const void * b_arg)
{
    const duckdb_libpgquery::PGCoerceToDomainValue * a = (const duckdb_libpgquery::PGCoerceToDomainValue *)a_arg;
    const duckdb_libpgquery::PGCoerceToDomainValue * b = (const duckdb_libpgquery::PGCoerceToDomainValue *)b_arg;

    COMPARE_SCALAR_FIELD(typeId);
    COMPARE_SCALAR_FIELD(typeMod);
    COMPARE_SCALAR_FIELD(collation);
    COMPARE_LOCATION_FIELD(location);

    return true;
}

// static bool _equalSetToDefault(const duckdb_libpgquery::PGSetToDefault * a, const duckdb_libpgquery::PGSetToDefault * b)
// {
//     COMPARE_SCALAR_FIELD(typeId);
//     COMPARE_SCALAR_FIELD(typeMod);
//     COMPARE_SCALAR_FIELD(collation);
//     COMPARE_LOCATION_FIELD(location);

//     return true;
// }

static bool _equalCurrentOfExpr(const void * a_arg, const void * b_arg)
{
    const duckdb_libpgquery::PGCurrentOfExpr * a = (const duckdb_libpgquery::PGCurrentOfExpr *)a_arg;
    const duckdb_libpgquery::PGCurrentOfExpr * b = (const duckdb_libpgquery::PGCurrentOfExpr *)b_arg;

    COMPARE_SCALAR_FIELD(cvarno);
    COMPARE_STRING_FIELD(cursor_name);
    COMPARE_SCALAR_FIELD(cursor_param);
    //COMPARE_SCALAR_FIELD(target_relid);

    /* some attributes omitted as they're bound only just before executor dispatch */

    return true;
}

static bool _equalTargetEntry(const void * a_arg, const void * b_arg)
{
    const duckdb_libpgquery::PGTargetEntry * a = (const duckdb_libpgquery::PGTargetEntry *)a_arg;
    const duckdb_libpgquery::PGTargetEntry * b = (const duckdb_libpgquery::PGTargetEntry *)b_arg;

    COMPARE_NODE_FIELD(expr);
    COMPARE_SCALAR_FIELD(resno);
    COMPARE_STRING_FIELD(resname);
    COMPARE_SCALAR_FIELD(ressortgroupref);
    COMPARE_SCALAR_FIELD(resorigtbl);
    COMPARE_SCALAR_FIELD(resorigcol);
    COMPARE_SCALAR_FIELD(resjunk);

    return true;
}

static bool _equalRangeTblRef(const void * a_arg, const void * b_arg)
{
    const duckdb_libpgquery::PGRangeTblRef * a = (const duckdb_libpgquery::PGRangeTblRef *)a_arg;
    const duckdb_libpgquery::PGRangeTblRef * b = (const duckdb_libpgquery::PGRangeTblRef *)b_arg;

    COMPARE_SCALAR_FIELD(rtindex);

    return true;
}

static bool _equalJoinExpr(const void * a_arg, const void * b_arg)
{
    const duckdb_libpgquery::PGJoinExpr * a = (const duckdb_libpgquery::PGJoinExpr *)a_arg;
    const duckdb_libpgquery::PGJoinExpr * b = (const duckdb_libpgquery::PGJoinExpr *)b_arg;

    COMPARE_SCALAR_FIELD(jointype);
    COMPARE_SCALAR_FIELD(isNatural);
    COMPARE_NODE_FIELD(larg);
    COMPARE_NODE_FIELD(rarg);
    COMPARE_NODE_FIELD(usingClause);
    COMPARE_NODE_FIELD(quals);
    COMPARE_NODE_FIELD(alias);
    COMPARE_SCALAR_FIELD(rtindex);

    return true;
}

static bool _equalFromExpr(const void * a_arg, const void * b_arg)
{
    const duckdb_libpgquery::PGFromExpr * a = (const duckdb_libpgquery::PGFromExpr *)a_arg;
    const duckdb_libpgquery::PGFromExpr * b = (const duckdb_libpgquery::PGFromExpr *)b_arg;

    COMPARE_NODE_FIELD(fromlist);
    COMPARE_NODE_FIELD(quals);

    return true;
}

// static bool _equalFlow(const PGFlow * a, const PGFlow * b)
// {
//     COMPARE_SCALAR_FIELD(flotype);
//     COMPARE_SCALAR_FIELD(req_move);
//     COMPARE_SCALAR_FIELD(locustype);
//     COMPARE_SCALAR_FIELD(segindex);
//     COMPARE_SCALAR_FIELD(numsegments);
//     COMPARE_NODE_FIELD(hashExprs);
//     COMPARE_NODE_FIELD(hashOpfamilies);

//     return true;
// }


// /*
//  * Stuff from relation.h
//  */

// static bool _equalPathKey(const PGPathKey * a, const PGPathKey * b)
// {
//     /* We assume pointer equality is sufficient to compare the eclasses */
//     COMPARE_SCALAR_FIELD(pk_eclass);
//     COMPARE_SCALAR_FIELD(pk_opfamily);
//     COMPARE_SCALAR_FIELD(pk_strategy);
//     COMPARE_SCALAR_FIELD(pk_nulls_first);

//     return true;
// }

// static bool _equalRestrictInfo(const PGRestrictInfo * a, const PGRestrictInfo * b)
// {
//     COMPARE_NODE_FIELD(clause);
//     COMPARE_SCALAR_FIELD(is_pushed_down);
//     COMPARE_SCALAR_FIELD(outerjoin_delayed);
//     COMPARE_BITMAPSET_FIELD(required_relids);
//     COMPARE_BITMAPSET_FIELD(outer_relids);
//     COMPARE_BITMAPSET_FIELD(nullable_relids);

//     /*
// 	 * We ignore all the remaining fields, since they may not be set yet, and
// 	 * should be derivable from the clause anyway.
// 	 */

//     return true;
// }

// static bool _equalPlaceHolderVar(const PGPlaceHolderVar * a, const PGPlaceHolderVar * b)
// {
//     /*
// 	 * We intentionally do not compare phexpr.  Two PlaceHolderVars with the
// 	 * same ID and levelsup should be considered equal even if the contained
// 	 * expressions have managed to mutate to different states.  This will
// 	 * happen during final plan construction when there are nested PHVs, since
// 	 * the inner PHV will get replaced by a Param in some copies of the outer
// 	 * PHV.  Another way in which it can happen is that initplan sublinks
// 	 * could get replaced by differently-numbered Params when sublink folding
// 	 * is done.  (The end result of such a situation would be some
// 	 * unreferenced initplans, which is annoying but not really a problem.) On
// 	 * the same reasoning, there is no need to examine phrels.
// 	 *
// 	 * COMPARE_NODE_FIELD(phexpr);
// 	 *
// 	 * COMPARE_BITMAPSET_FIELD(phrels);
// 	 */
//     COMPARE_SCALAR_FIELD(phid);
//     COMPARE_SCALAR_FIELD(phlevelsup);

//     return true;
// }

// static bool _equalSpecialJoinInfo(const SpecialJoinInfo * a, const SpecialJoinInfo * b)
// {
//     COMPARE_BITMAPSET_FIELD(min_lefthand);
//     COMPARE_BITMAPSET_FIELD(min_righthand);
//     COMPARE_BITMAPSET_FIELD(syn_lefthand);
//     COMPARE_BITMAPSET_FIELD(syn_righthand);
//     COMPARE_SCALAR_FIELD(jointype);
//     COMPARE_SCALAR_FIELD(lhs_strict);
//     COMPARE_SCALAR_FIELD(delay_upper_joins);
//     COMPARE_NODE_FIELD(join_quals);

//     return true;
// }

// static bool _equalLateralJoinInfo(const LateralJoinInfo * a, const LateralJoinInfo * b)
// {
//     COMPARE_BITMAPSET_FIELD(lateral_lhs);
//     COMPARE_BITMAPSET_FIELD(lateral_rhs);

//     return true;
// }

// static bool _equalAppendRelInfo(const AppendRelInfo * a, const AppendRelInfo * b)
// {
//     COMPARE_SCALAR_FIELD(parent_relid);
//     COMPARE_SCALAR_FIELD(child_relid);
//     COMPARE_SCALAR_FIELD(parent_reltype);
//     COMPARE_SCALAR_FIELD(child_reltype);
//     COMPARE_NODE_FIELD(translated_vars);
//     COMPARE_SCALAR_FIELD(parent_reloid);

//     return true;
// }

// static bool _equalPlaceHolderInfo(const PGPlaceHolderInfo * a, const PGPlaceHolderInfo * b)
// {
//     COMPARE_SCALAR_FIELD(phid);
//     COMPARE_NODE_FIELD(ph_var); /* should be redundant */
//     COMPARE_BITMAPSET_FIELD(ph_eval_at);
//     COMPARE_BITMAPSET_FIELD(ph_lateral);
//     COMPARE_BITMAPSET_FIELD(ph_needed);
//     COMPARE_SCALAR_FIELD(ph_width);

//     return true;
// }


/*
 * Stuff from parsenodes.h
 */

static bool _equalQuery(const void * a_arg, const void * b_arg)
{
    const duckdb_libpgquery::PGQuery * a = (const duckdb_libpgquery::PGQuery *)a_arg;
    const duckdb_libpgquery::PGQuery * b = (const duckdb_libpgquery::PGQuery *)b_arg;

    COMPARE_SCALAR_FIELD(commandType);
    COMPARE_SCALAR_FIELD(querySource);
    /* we intentionally ignore queryId, since it might not be set */
    COMPARE_SCALAR_FIELD(canSetTag);
    COMPARE_NODE_FIELD(utilityStmt);
    COMPARE_SCALAR_FIELD(resultRelation);
    COMPARE_SCALAR_FIELD(hasAggs);
    COMPARE_SCALAR_FIELD(hasWindowFuncs);
    COMPARE_SCALAR_FIELD(hasSubLinks);
    //COMPARE_SCALAR_FIELD(hasDynamicFunctions);
    //COMPARE_SCALAR_FIELD(hasFuncsWithExecRestrictions);
    COMPARE_SCALAR_FIELD(hasDistinctOn);
    COMPARE_SCALAR_FIELD(hasRecursive);
    COMPARE_SCALAR_FIELD(hasModifyingCTE);
    COMPARE_SCALAR_FIELD(hasForUpdate);
    COMPARE_NODE_FIELD(cteList);
    COMPARE_NODE_FIELD(rtable);
    COMPARE_NODE_FIELD(jointree);
    COMPARE_NODE_FIELD(targetList);
    COMPARE_NODE_FIELD(withCheckOptions);
    COMPARE_NODE_FIELD(returningList);
    COMPARE_NODE_FIELD(groupClause);
    COMPARE_NODE_FIELD(havingQual);
    COMPARE_NODE_FIELD(windowClause);
    COMPARE_NODE_FIELD(distinctClause);
    COMPARE_NODE_FIELD(sortClause);
    //COMPARE_NODE_FIELD(scatterClause);
    //COMPARE_SCALAR_FIELD(isTableValueSelect);
    COMPARE_NODE_FIELD(limitOffset);
    COMPARE_NODE_FIELD(limitCount);
    COMPARE_NODE_FIELD(rowMarks);
    COMPARE_NODE_FIELD(setOperations);
    COMPARE_NODE_FIELD(constraintDeps);

    /* Prior to 3.4 this test was
	 *     COMPARE_SCALAR_FIELD(intoPolicy);
	 * Maybe GpPolicy should be a Node?
	 */
    //if (!GpPolicyEqual(a->intoPolicy, b->intoPolicy))
        //return false;

    //COMPARE_SCALAR_FIELD(parentStmtType);

    return true;
}

static bool _equalInsertStmt(const void * a_arg, const void * b_arg)
{
    const duckdb_libpgquery::PGInsertStmt * a = (const duckdb_libpgquery::PGInsertStmt *)a_arg;
    const duckdb_libpgquery::PGInsertStmt * b = (const duckdb_libpgquery::PGInsertStmt *)b_arg;

    COMPARE_NODE_FIELD(relation);
    COMPARE_NODE_FIELD(cols);
    COMPARE_NODE_FIELD(selectStmt);
    COMPARE_NODE_FIELD(returningList);
    COMPARE_NODE_FIELD(withClause);

    return true;
}

static bool _equalDeleteStmt(const void * a_arg, const void * b_arg)
{
    const duckdb_libpgquery::PGDeleteStmt * a = (const duckdb_libpgquery::PGDeleteStmt *)a_arg;
    const duckdb_libpgquery::PGDeleteStmt * b = (const duckdb_libpgquery::PGDeleteStmt *)b_arg;

    COMPARE_NODE_FIELD(relation);
    COMPARE_NODE_FIELD(usingClause);
    COMPARE_NODE_FIELD(whereClause);
    COMPARE_NODE_FIELD(returningList);
    COMPARE_NODE_FIELD(withClause);

    return true;
}

static bool _equalUpdateStmt(const void * a_arg, const void * b_arg)
{
    const duckdb_libpgquery::PGUpdateStmt * a = (const duckdb_libpgquery::PGUpdateStmt *)a_arg;
    const duckdb_libpgquery::PGUpdateStmt * b = (const duckdb_libpgquery::PGUpdateStmt *)b_arg;

    COMPARE_NODE_FIELD(relation);
    COMPARE_NODE_FIELD(targetList);
    COMPARE_NODE_FIELD(whereClause);
    COMPARE_NODE_FIELD(fromClause);
    COMPARE_NODE_FIELD(returningList);
    COMPARE_NODE_FIELD(withClause);

    return true;
}

static bool _equalSelectStmt(const void * a_arg, const void * b_arg)
{
    const duckdb_libpgquery::PGSelectStmt * a = (const duckdb_libpgquery::PGSelectStmt *)a_arg;
    const duckdb_libpgquery::PGSelectStmt * b = (const duckdb_libpgquery::PGSelectStmt *)b_arg;

    COMPARE_NODE_FIELD(distinctClause);
    COMPARE_NODE_FIELD(intoClause);
    COMPARE_NODE_FIELD(targetList);
    COMPARE_NODE_FIELD(fromClause);
    COMPARE_NODE_FIELD(whereClause);
    COMPARE_NODE_FIELD(groupClause);
    COMPARE_NODE_FIELD(havingClause);
    COMPARE_NODE_FIELD(windowClause);
    COMPARE_NODE_FIELD(valuesLists);
    COMPARE_NODE_FIELD(sortClause);
    //COMPARE_NODE_FIELD(scatterClause);
    COMPARE_NODE_FIELD(limitOffset);
    COMPARE_NODE_FIELD(limitCount);
    COMPARE_NODE_FIELD(lockingClause);
    COMPARE_NODE_FIELD(withClause);
    COMPARE_SCALAR_FIELD(op);
    COMPARE_SCALAR_FIELD(all);
    COMPARE_NODE_FIELD(larg);
    COMPARE_NODE_FIELD(rarg);

    return true;
}

// static bool _equalSetOperationStmt(const PGSetOperationStmt * a, const PGSetOperationStmt * b)
// {
//     COMPARE_SCALAR_FIELD(op);
//     COMPARE_SCALAR_FIELD(all);
//     COMPARE_NODE_FIELD(larg);
//     COMPARE_NODE_FIELD(rarg);
//     COMPARE_NODE_FIELD(colTypes);
//     COMPARE_NODE_FIELD(colTypmods);
//     COMPARE_NODE_FIELD(colCollations);
//     COMPARE_NODE_FIELD(groupClauses);

//     return true;
// }

// static bool _equalAlterTableStmt(const PGAlterTableStmt * a, const PGAlterTableStmt * b)
// {
//     COMPARE_NODE_FIELD(relation);
//     COMPARE_NODE_FIELD(cmds);
//     COMPARE_SCALAR_FIELD(relkind);
//     COMPARE_SCALAR_FIELD(missing_ok);

//     /* No need to compare AT workspace fields.  */

//     return true;
// }

// static bool _equalAlterTableCmd(const PGAlterTableCmd * a, const PGAlterTableCmd * b)
// {
//     COMPARE_SCALAR_FIELD(subtype);
//     COMPARE_STRING_FIELD(name);
//     COMPARE_NODE_FIELD(def);
//     COMPARE_SCALAR_FIELD(behavior);
//     COMPARE_SCALAR_FIELD(part_expanded);

//     /* No need to compare AT workspace field, partoids.  */
//     COMPARE_SCALAR_FIELD(missing_ok);

//     return true;
// }

// static bool _equalSetDistributionCmd(const SetDistributionCmd * a, const SetDistributionCmd * b)
// {
//     COMPARE_SCALAR_FIELD(backendId);
//     COMPARE_NODE_FIELD(relids);

//     return true;
// }

// static bool _equalInheritPartitionCmd(const InheritPartitionCmd * a, const InheritPartitionCmd * b)
// {
//     COMPARE_NODE_FIELD(parent);

//     return true;
// }

// static bool _equalAlterPartitionCmd(const AlterPartitionCmd * a, const AlterPartitionCmd * b)
// {
//     COMPARE_NODE_FIELD(partid);
//     COMPARE_NODE_FIELD(arg1);
//     COMPARE_NODE_FIELD(arg2);

//     return true;
// }

// static bool _equalAlterPartitionId(const AlterPartitionId * a, const AlterPartitionId * b)
// {
//     COMPARE_SCALAR_FIELD(idtype);
//     COMPARE_NODE_FIELD(partiddef);

//     return true;
// }

// static bool _equalAlterDomainStmt(const AlterDomainStmt * a, const AlterDomainStmt * b)
// {
//     COMPARE_SCALAR_FIELD(subtype);
//     COMPARE_NODE_FIELD(typeName);
//     COMPARE_STRING_FIELD(name);
//     COMPARE_NODE_FIELD(def);
//     COMPARE_SCALAR_FIELD(behavior);
//     COMPARE_SCALAR_FIELD(missing_ok);

//     return true;
// }

// static bool _equalGrantStmt(const GrantStmt * a, const GrantStmt * b)
// {
//     COMPARE_SCALAR_FIELD(is_grant);
//     COMPARE_SCALAR_FIELD(targtype);
//     COMPARE_SCALAR_FIELD(objtype);
//     COMPARE_NODE_FIELD(objects);
//     COMPARE_NODE_FIELD(privileges);
//     COMPARE_NODE_FIELD(grantees);
//     COMPARE_SCALAR_FIELD(grant_option);
//     COMPARE_SCALAR_FIELD(behavior);

//     return true;
// }

// static bool _equalPrivGrantee(const PrivGrantee * a, const PrivGrantee * b)
// {
//     COMPARE_STRING_FIELD(rolname);

//     return true;
// }

// static bool _equalFuncWithArgs(const FuncWithArgs * a, const FuncWithArgs * b)
// {
//     COMPARE_NODE_FIELD(funcname);
//     COMPARE_NODE_FIELD(funcargs);

//     return true;
// }

// static bool _equalAccessPriv(const AccessPriv * a, const AccessPriv * b)
// {
//     COMPARE_STRING_FIELD(priv_name);
//     COMPARE_NODE_FIELD(cols);

//     return true;
// }

// static bool _equalGrantRoleStmt(const GrantRoleStmt * a, const GrantRoleStmt * b)
// {
//     COMPARE_NODE_FIELD(granted_roles);
//     COMPARE_NODE_FIELD(grantee_roles);
//     COMPARE_SCALAR_FIELD(is_grant);
//     COMPARE_SCALAR_FIELD(admin_opt);
//     COMPARE_STRING_FIELD(grantor);
//     COMPARE_SCALAR_FIELD(behavior);

//     return true;
// }

// static bool _equalAlterDefaultPrivilegesStmt(const AlterDefaultPrivilegesStmt * a, const AlterDefaultPrivilegesStmt * b)
// {
//     COMPARE_NODE_FIELD(options);
//     COMPARE_NODE_FIELD(action);

//     return true;
// }

// static bool _equalDeclareCursorStmt(const DeclareCursorStmt * a, const DeclareCursorStmt * b)
// {
//     COMPARE_STRING_FIELD(portalname);
//     COMPARE_SCALAR_FIELD(options);
//     COMPARE_NODE_FIELD(query);

//     return true;
// }

// static bool _equalClosePortalStmt(const ClosePortalStmt * a, const ClosePortalStmt * b)
// {
//     COMPARE_STRING_FIELD(portalname);

//     return true;
// }

// static bool _equalClusterStmt(const ClusterStmt * a, const ClusterStmt * b)
// {
//     COMPARE_NODE_FIELD(relation);
//     COMPARE_STRING_FIELD(indexname);
//     COMPARE_SCALAR_FIELD(verbose);

//     return true;
// }

// static bool _equalSingleRowErrorDesc(const SingleRowErrorDesc * a, const SingleRowErrorDesc * b)
// {
//     COMPARE_SCALAR_FIELD(rejectlimit);
//     COMPARE_SCALAR_FIELD(is_limit_in_rows);
//     COMPARE_SCALAR_FIELD(into_file);
//     COMPARE_SCALAR_FIELD(log_errors_type);

//     return true;
// }

// static bool _equalCopyStmt(const CopyStmt * a, const CopyStmt * b)
// {
//     COMPARE_NODE_FIELD(relation);
//     COMPARE_NODE_FIELD(query);
//     COMPARE_NODE_FIELD(attlist);
//     COMPARE_SCALAR_FIELD(is_from);
//     COMPARE_SCALAR_FIELD(is_program);
//     COMPARE_SCALAR_FIELD(skip_ext_partition);
//     COMPARE_STRING_FIELD(filename);
//     COMPARE_NODE_FIELD(options);
//     COMPARE_NODE_FIELD(sreh);

//     return true;
// }

// static bool _equalCreateStmt(const void * a_arg, const void * b_arg)
// {
//     const duckdb_libpgquery::PGCreateStmt * a = (const duckdb_libpgquery::PGCreateStmt *)a_arg;
//     const duckdb_libpgquery::PGCreateStmt * b = (const duckdb_libpgquery::PGCreateStmt *)b_arg;

//     COMPARE_NODE_FIELD(relation);
//     COMPARE_NODE_FIELD(tableElts);
//     COMPARE_NODE_FIELD(inhRelations);
//     //COMPARE_NODE_FIELD(inhOids);
//     //COMPARE_SCALAR_FIELD(parentOidCount);
//     COMPARE_NODE_FIELD(ofTypename);
//     COMPARE_NODE_FIELD(constraints);
//     COMPARE_NODE_FIELD(options);
//     COMPARE_SCALAR_FIELD(oncommit);
//     COMPARE_STRING_FIELD(tablespacename);
//     //COMPARE_SCALAR_FIELD(if_not_exists);

//     //COMPARE_NODE_FIELD(distributedBy);
//     //COMPARE_SCALAR_FIELD(relKind);
//     //COMPARE_SCALAR_FIELD(relStorage);
//     /* deferredStmts omitted */
//     //COMPARE_SCALAR_FIELD(is_part_child);
//     //COMPARE_SCALAR_FIELD(is_add_part);
//     //COMPARE_SCALAR_FIELD(is_split_part);
//     //COMPARE_SCALAR_FIELD(ownerid);
//     //COMPARE_SCALAR_FIELD(buildAoBlkdir);
//     //COMPARE_NODE_FIELD(attr_encodings);
//     //COMPARE_SCALAR_FIELD(isCtas);

//     return true;
// }

// static bool _equalColumnReferenceStorageDirective(const ColumnReferenceStorageDirective * a, const ColumnReferenceStorageDirective * b)
// {
//     COMPARE_STRING_FIELD(column);
//     COMPARE_SCALAR_FIELD(deflt);
//     COMPARE_NODE_FIELD(encoding);

//     return true;
// }

// static bool _equalPartitionRangeItem(const PartitionRangeItem * a, const PartitionRangeItem * b)
// {
//     COMPARE_NODE_FIELD(partRangeVal);
//     COMPARE_SCALAR_FIELD(partedge);
//     COMPARE_SCALAR_FIELD(everycount);

//     return true;
// }

// static bool _equalExtTableTypeDesc(const ExtTableTypeDesc * a, const ExtTableTypeDesc * b)
// {
//     COMPARE_SCALAR_FIELD(exttabletype);
//     COMPARE_NODE_FIELD(location_list);
//     COMPARE_NODE_FIELD(on_clause);
//     COMPARE_STRING_FIELD(command_string);

//     return true;
// }

// static bool _equalCreateExternalStmt(const CreateExternalStmt * a, const CreateExternalStmt * b)
// {
//     COMPARE_NODE_FIELD(relation);
//     COMPARE_NODE_FIELD(tableElts);
//     COMPARE_NODE_FIELD(exttypedesc);
//     COMPARE_STRING_FIELD(format);
//     COMPARE_NODE_FIELD(formatOpts);
//     COMPARE_SCALAR_FIELD(isweb);
//     COMPARE_SCALAR_FIELD(iswritable);
//     COMPARE_NODE_FIELD(sreh);
//     COMPARE_NODE_FIELD(extOptions);
//     COMPARE_NODE_FIELD(encoding);
//     COMPARE_NODE_FIELD(distributedBy);

//     return true;
// }

// static bool _equalTableLikeClause(const TableLikeClause * a, const TableLikeClause * b)
// {
//     COMPARE_NODE_FIELD(relation);
//     COMPARE_SCALAR_FIELD(options);

//     return true;
// }

// static bool _equalDefineStmt(const DefineStmt * a, const DefineStmt * b)
// {
//     COMPARE_SCALAR_FIELD(kind);
//     COMPARE_SCALAR_FIELD(oldstyle);
//     COMPARE_NODE_FIELD(defnames);
//     COMPARE_NODE_FIELD(args);
//     COMPARE_NODE_FIELD(definition);
//     COMPARE_SCALAR_FIELD(trusted); /* CDB */

//     return true;
// }

// static bool _equalDropStmt(const DropStmt * a, const DropStmt * b)
// {
//     COMPARE_NODE_FIELD(objects);
//     COMPARE_NODE_FIELD(arguments);
//     COMPARE_SCALAR_FIELD(removeType);
//     COMPARE_SCALAR_FIELD(behavior);
//     COMPARE_SCALAR_FIELD(missing_ok);
//     COMPARE_SCALAR_FIELD(concurrent);

//     return true;
// }

// static bool _equalTruncateStmt(const TruncateStmt * a, const TruncateStmt * b)
// {
//     COMPARE_NODE_FIELD(relations);
//     COMPARE_SCALAR_FIELD(restart_seqs);
//     COMPARE_SCALAR_FIELD(behavior);

//     return true;
// }

// static bool _equalCommentStmt(const CommentStmt * a, const CommentStmt * b)
// {
//     COMPARE_SCALAR_FIELD(objtype);
//     COMPARE_NODE_FIELD(objname);
//     COMPARE_NODE_FIELD(objargs);
//     COMPARE_STRING_FIELD(comment);

//     return true;
// }

// static bool _equalSecLabelStmt(const SecLabelStmt * a, const SecLabelStmt * b)
// {
//     COMPARE_SCALAR_FIELD(objtype);
//     COMPARE_NODE_FIELD(objname);
//     COMPARE_NODE_FIELD(objargs);
//     COMPARE_STRING_FIELD(provider);
//     COMPARE_STRING_FIELD(label);

//     return true;
// }

// static bool _equalFetchStmt(const FetchStmt * a, const FetchStmt * b)
// {
//     COMPARE_SCALAR_FIELD(direction);
//     COMPARE_SCALAR_FIELD(howMany);
//     COMPARE_STRING_FIELD(portalname);
//     COMPARE_SCALAR_FIELD(ismove);

//     return true;
// }

// static bool _equalRetrieveStmt(const RetrieveStmt * a, const RetrieveStmt * b)
// {
//     COMPARE_STRING_FIELD(endpoint_name);
//     COMPARE_SCALAR_FIELD(count);
//     COMPARE_SCALAR_FIELD(is_all);

//     return true;
// }

// static bool _equalIndexStmt(const IndexStmt * a, const IndexStmt * b)
// {
//     COMPARE_STRING_FIELD(idxname);
//     COMPARE_NODE_FIELD(relation);
//     COMPARE_STRING_FIELD(accessMethod);
//     COMPARE_STRING_FIELD(tableSpace);
//     COMPARE_NODE_FIELD(indexParams);
//     COMPARE_NODE_FIELD(options);
//     COMPARE_NODE_FIELD(whereClause);
//     COMPARE_NODE_FIELD(excludeOpNames);
//     COMPARE_STRING_FIELD(idxcomment);
//     COMPARE_SCALAR_FIELD(indexOid);
//     COMPARE_SCALAR_FIELD(oldNode);
//     COMPARE_SCALAR_FIELD(is_part_child);
//     COMPARE_SCALAR_FIELD(unique);
//     COMPARE_SCALAR_FIELD(primary);
//     COMPARE_SCALAR_FIELD(isconstraint);
//     COMPARE_SCALAR_FIELD(deferrable);
//     COMPARE_SCALAR_FIELD(initdeferred);
//     COMPARE_SCALAR_FIELD(concurrent);
//     COMPARE_SCALAR_FIELD(is_split_part);
//     COMPARE_SCALAR_FIELD(parentIndexId);
//     COMPARE_SCALAR_FIELD(parentConstraintId);

//     return true;
// }

// static bool _equalCreateFunctionStmt(const CreateFunctionStmt * a, const CreateFunctionStmt * b)
// {
//     COMPARE_SCALAR_FIELD(replace);
//     COMPARE_NODE_FIELD(funcname);
//     COMPARE_NODE_FIELD(parameters);
//     COMPARE_NODE_FIELD(returnType);
//     COMPARE_NODE_FIELD(options);
//     COMPARE_NODE_FIELD(withClause);

//     return true;
// }

// static bool _equalFunctionParameter(const FunctionParameter * a, const FunctionParameter * b)
// {
//     COMPARE_STRING_FIELD(name);
//     COMPARE_NODE_FIELD(argType);
//     COMPARE_SCALAR_FIELD(mode);
//     COMPARE_NODE_FIELD(defexpr);

//     return true;
// }

// static bool _equalAlterFunctionStmt(const AlterFunctionStmt * a, const AlterFunctionStmt * b)
// {
//     COMPARE_NODE_FIELD(func);
//     COMPARE_NODE_FIELD(actions);

//     return true;
// }

// static bool _equalDoStmt(const DoStmt * a, const DoStmt * b)
// {
//     COMPARE_NODE_FIELD(args);

//     return true;
// }

// static bool _equalRenameStmt(const RenameStmt * a, const RenameStmt * b)
// {
//     COMPARE_SCALAR_FIELD(renameType);
//     COMPARE_SCALAR_FIELD(relationType);
//     COMPARE_NODE_FIELD(relation);
//     COMPARE_SCALAR_FIELD(objid);
//     COMPARE_NODE_FIELD(object);
//     COMPARE_NODE_FIELD(objarg);
//     COMPARE_STRING_FIELD(subname);
//     COMPARE_STRING_FIELD(newname);
//     COMPARE_SCALAR_FIELD(behavior);
//     COMPARE_SCALAR_FIELD(missing_ok);

//     return true;
// }

// static bool _equalAlterObjectSchemaStmt(const AlterObjectSchemaStmt * a, const AlterObjectSchemaStmt * b)
// {
//     COMPARE_SCALAR_FIELD(objectType);
//     COMPARE_NODE_FIELD(relation);
//     COMPARE_NODE_FIELD(object);
//     COMPARE_NODE_FIELD(objarg);
//     COMPARE_STRING_FIELD(newschema);
//     COMPARE_SCALAR_FIELD(missing_ok);

//     return true;
// }

// static bool _equalAlterOwnerStmt(const AlterOwnerStmt * a, const AlterOwnerStmt * b)
// {
//     COMPARE_SCALAR_FIELD(objectType);
//     COMPARE_NODE_FIELD(relation);
//     COMPARE_NODE_FIELD(object);
//     COMPARE_NODE_FIELD(objarg);
//     COMPARE_STRING_FIELD(newowner);

//     return true;
// }

// static bool _equalRuleStmt(const RuleStmt * a, const RuleStmt * b)
// {
//     COMPARE_NODE_FIELD(relation);
//     COMPARE_STRING_FIELD(rulename);
//     COMPARE_NODE_FIELD(whereClause);
//     COMPARE_SCALAR_FIELD(event);
//     COMPARE_SCALAR_FIELD(instead);
//     COMPARE_NODE_FIELD(actions);
//     COMPARE_SCALAR_FIELD(replace);

//     return true;
// }

// static bool _equalNotifyStmt(const NotifyStmt * a, const NotifyStmt * b)
// {
//     COMPARE_STRING_FIELD(conditionname);
//     COMPARE_STRING_FIELD(payload);

//     return true;
// }

// static bool _equalListenStmt(const ListenStmt * a, const ListenStmt * b)
// {
//     COMPARE_STRING_FIELD(conditionname);

//     return true;
// }

// static bool _equalUnlistenStmt(const UnlistenStmt * a, const UnlistenStmt * b)
// {
//     COMPARE_STRING_FIELD(conditionname);

//     return true;
// }

// static bool _equalTransactionStmt(const TransactionStmt * a, const TransactionStmt * b)
// {
//     COMPARE_SCALAR_FIELD(kind);
//     COMPARE_NODE_FIELD(options);
//     COMPARE_STRING_FIELD(gid);

//     return true;
// }

// static bool _equalCompositeTypeStmt(const CompositeTypeStmt * a, const CompositeTypeStmt * b)
// {
//     COMPARE_NODE_FIELD(typevar);
//     COMPARE_NODE_FIELD(coldeflist);

//     return true;
// }

// static bool _equalCreateEnumStmt(const CreateEnumStmt * a, const CreateEnumStmt * b)
// {
//     COMPARE_NODE_FIELD(typeName);
//     COMPARE_NODE_FIELD(vals);

//     return true;
// }

// static bool _equalCreateRangeStmt(const CreateRangeStmt * a, const CreateRangeStmt * b)
// {
//     COMPARE_NODE_FIELD(typeName);
//     COMPARE_NODE_FIELD(params);

//     return true;
// }

// static bool _equalAlterEnumStmt(const AlterEnumStmt * a, const AlterEnumStmt * b)
// {
//     COMPARE_NODE_FIELD(typeName);
//     COMPARE_STRING_FIELD(newVal);
//     COMPARE_STRING_FIELD(newValNeighbor);
//     COMPARE_SCALAR_FIELD(newValIsAfter);
//     COMPARE_SCALAR_FIELD(skipIfExists);

//     return true;
// }

// static bool _equalViewStmt(const ViewStmt * a, const ViewStmt * b)
// {
//     COMPARE_NODE_FIELD(view);
//     COMPARE_NODE_FIELD(aliases);
//     COMPARE_NODE_FIELD(query);
//     COMPARE_SCALAR_FIELD(replace);
//     COMPARE_NODE_FIELD(options);
//     COMPARE_SCALAR_FIELD(withCheckOption);

//     return true;
// }

// static bool _equalLoadStmt(const LoadStmt * a, const LoadStmt * b)
// {
//     COMPARE_STRING_FIELD(filename);

//     return true;
// }

// static bool _equalCreateDomainStmt(const CreateDomainStmt * a, const CreateDomainStmt * b)
// {
//     COMPARE_NODE_FIELD(domainname);
//     COMPARE_NODE_FIELD(typeName);
//     COMPARE_NODE_FIELD(collClause);
//     COMPARE_NODE_FIELD(constraints);

//     return true;
// }

// static bool _equalCreateOpClassStmt(const CreateOpClassStmt * a, const CreateOpClassStmt * b)
// {
//     COMPARE_NODE_FIELD(opclassname);
//     COMPARE_NODE_FIELD(opfamilyname);
//     COMPARE_STRING_FIELD(amname);
//     COMPARE_NODE_FIELD(datatype);
//     COMPARE_NODE_FIELD(items);
//     COMPARE_SCALAR_FIELD(isDefault);

//     return true;
// }

// static bool _equalCreateOpClassItem(const CreateOpClassItem * a, const CreateOpClassItem * b)
// {
//     COMPARE_SCALAR_FIELD(itemtype);
//     COMPARE_NODE_FIELD(name);
//     COMPARE_NODE_FIELD(args);
//     COMPARE_SCALAR_FIELD(number);
//     COMPARE_NODE_FIELD(order_family);
//     COMPARE_NODE_FIELD(class_args);
//     COMPARE_NODE_FIELD(storedtype);

//     return true;
// }

// static bool _equalCreateOpFamilyStmt(const CreateOpFamilyStmt * a, const CreateOpFamilyStmt * b)
// {
//     COMPARE_NODE_FIELD(opfamilyname);
//     COMPARE_STRING_FIELD(amname);

//     return true;
// }

// static bool _equalAlterOpFamilyStmt(const AlterOpFamilyStmt * a, const AlterOpFamilyStmt * b)
// {
//     COMPARE_NODE_FIELD(opfamilyname);
//     COMPARE_STRING_FIELD(amname);
//     COMPARE_SCALAR_FIELD(isDrop);
//     COMPARE_NODE_FIELD(items);

//     return true;
// }

// static bool _equalCreatedbStmt(const CreatedbStmt * a, const CreatedbStmt * b)
// {
//     COMPARE_STRING_FIELD(dbname);
//     COMPARE_NODE_FIELD(options);
//     return true;
// }

// static bool _equalAlterDatabaseStmt(const AlterDatabaseStmt * a, const AlterDatabaseStmt * b)
// {
//     COMPARE_STRING_FIELD(dbname);
//     COMPARE_NODE_FIELD(options);

//     return true;
// }


// static bool _equalAlterDatabaseSetStmt(const AlterDatabaseSetStmt * a, const AlterDatabaseSetStmt * b)
// {
//     COMPARE_STRING_FIELD(dbname);
//     COMPARE_NODE_FIELD(setstmt);

//     return true;
// }

// static bool _equalDropdbStmt(const DropdbStmt * a, const DropdbStmt * b)
// {
//     COMPARE_STRING_FIELD(dbname);
//     COMPARE_SCALAR_FIELD(missing_ok);

//     return true;
// }

// static bool _equalVacuumStmt(const VacuumStmt * a, const VacuumStmt * b)
// {
//     COMPARE_SCALAR_FIELD(options);
//     COMPARE_SCALAR_FIELD(freeze_min_age);
//     COMPARE_SCALAR_FIELD(freeze_table_age);
//     COMPARE_SCALAR_FIELD(multixact_freeze_min_age);
//     COMPARE_SCALAR_FIELD(multixact_freeze_table_age);
//     COMPARE_NODE_FIELD(relation);
//     COMPARE_NODE_FIELD(va_cols);
//     COMPARE_NODE_FIELD(expanded_relids);

//     return true;
// }

// static bool _equalExplainStmt(const ExplainStmt * a, const ExplainStmt * b)
// {
//     COMPARE_NODE_FIELD(query);
//     COMPARE_NODE_FIELD(options);

//     return true;
// }

// static bool _equalCreateTableAsStmt(const CreateTableAsStmt * a, const CreateTableAsStmt * b)
// {
//     COMPARE_NODE_FIELD(query);
//     COMPARE_NODE_FIELD(into);
//     COMPARE_SCALAR_FIELD(relkind);
//     COMPARE_SCALAR_FIELD(is_select_into);

//     return true;
// }

// static bool _equalRefreshMatViewStmt(const RefreshMatViewStmt * a, const RefreshMatViewStmt * b)
// {
//     COMPARE_SCALAR_FIELD(concurrent);
//     COMPARE_SCALAR_FIELD(skipData);
//     COMPARE_NODE_FIELD(relation);

//     return true;
// }

// static bool _equalReplicaIdentityStmt(const ReplicaIdentityStmt * a, const ReplicaIdentityStmt * b)
// {
//     COMPARE_SCALAR_FIELD(identity_type);
//     COMPARE_STRING_FIELD(name);

//     return true;
// }

// static bool _equalAlterSystemStmt(const AlterSystemStmt * a, const AlterSystemStmt * b)
// {
//     COMPARE_NODE_FIELD(setstmt);

//     return true;
// }


// static bool _equalCreateSeqStmt(const CreateSeqStmt * a, const CreateSeqStmt * b)
// {
//     COMPARE_NODE_FIELD(sequence);
//     COMPARE_NODE_FIELD(options);
//     COMPARE_SCALAR_FIELD(ownerId);

//     return true;
// }

// static bool _equalAlterSeqStmt(const AlterSeqStmt * a, const AlterSeqStmt * b)
// {
//     COMPARE_NODE_FIELD(sequence);
//     COMPARE_NODE_FIELD(options);
//     COMPARE_SCALAR_FIELD(missing_ok);

//     return true;
// }

// static bool _equalVariableSetStmt(const VariableSetStmt * a, const VariableSetStmt * b)
// {
//     COMPARE_SCALAR_FIELD(kind);
//     COMPARE_STRING_FIELD(name);
//     COMPARE_NODE_FIELD(args);
//     COMPARE_SCALAR_FIELD(is_local);

//     return true;
// }

// static bool _equalVariableShowStmt(const VariableShowStmt * a, const VariableShowStmt * b)
// {
//     COMPARE_STRING_FIELD(name);

//     return true;
// }

// static bool _equalDiscardStmt(const DiscardStmt * a, const DiscardStmt * b)
// {
//     COMPARE_SCALAR_FIELD(target);

//     return true;
// }

// static bool _equalCreateTableSpaceStmt(const CreateTableSpaceStmt * a, const CreateTableSpaceStmt * b)
// {
//     COMPARE_STRING_FIELD(tablespacename);
//     COMPARE_STRING_FIELD(owner);
//     COMPARE_STRING_FIELD(location);
//     COMPARE_NODE_FIELD(options);

//     return true;
// }

// static bool _equalDropTableSpaceStmt(const DropTableSpaceStmt * a, const DropTableSpaceStmt * b)
// {
//     COMPARE_STRING_FIELD(tablespacename);
//     COMPARE_SCALAR_FIELD(missing_ok);

//     return true;
// }

// static bool _equalAlterTableSpaceOptionsStmt(const AlterTableSpaceOptionsStmt * a, const AlterTableSpaceOptionsStmt * b)
// {
//     COMPARE_STRING_FIELD(tablespacename);
//     COMPARE_NODE_FIELD(options);
//     COMPARE_SCALAR_FIELD(isReset);

//     return true;
// }

// static bool _equalAlterTableMoveAllStmt(const AlterTableMoveAllStmt * a, const AlterTableMoveAllStmt * b)
// {
//     COMPARE_STRING_FIELD(orig_tablespacename);
//     COMPARE_SCALAR_FIELD(objtype);
//     COMPARE_NODE_FIELD(roles);
//     COMPARE_STRING_FIELD(new_tablespacename);
//     COMPARE_SCALAR_FIELD(nowait);

//     return true;
// }

// static bool _equalCreateExtensionStmt(const CreateExtensionStmt * a, const CreateExtensionStmt * b)
// {
//     COMPARE_STRING_FIELD(extname);
//     COMPARE_SCALAR_FIELD(if_not_exists);
//     COMPARE_NODE_FIELD(options);
//     COMPARE_SCALAR_FIELD(create_ext_state);

//     return true;
// }

// static bool _equalAlterExtensionStmt(const AlterExtensionStmt * a, const AlterExtensionStmt * b)
// {
//     COMPARE_STRING_FIELD(extname);
//     COMPARE_NODE_FIELD(options);
//     COMPARE_SCALAR_FIELD(update_ext_state);

//     return true;
// }

// static bool _equalAlterExtensionContentsStmt(const AlterExtensionContentsStmt * a, const AlterExtensionContentsStmt * b)
// {
//     COMPARE_STRING_FIELD(extname);
//     COMPARE_SCALAR_FIELD(action);
//     COMPARE_SCALAR_FIELD(objtype);
//     COMPARE_NODE_FIELD(objname);
//     COMPARE_NODE_FIELD(objargs);

//     return true;
// }

// static bool _equalCreateFdwStmt(const CreateFdwStmt * a, const CreateFdwStmt * b)
// {
//     COMPARE_STRING_FIELD(fdwname);
//     COMPARE_NODE_FIELD(func_options);
//     COMPARE_NODE_FIELD(options);

//     return true;
// }

// static bool _equalAlterFdwStmt(const AlterFdwStmt * a, const AlterFdwStmt * b)
// {
//     COMPARE_STRING_FIELD(fdwname);
//     COMPARE_NODE_FIELD(func_options);
//     COMPARE_NODE_FIELD(options);

//     return true;
// }

// static bool _equalCreateForeignServerStmt(const CreateForeignServerStmt * a, const CreateForeignServerStmt * b)
// {
//     COMPARE_STRING_FIELD(servername);
//     COMPARE_STRING_FIELD(servertype);
//     COMPARE_STRING_FIELD(version);
//     COMPARE_STRING_FIELD(fdwname);
//     COMPARE_NODE_FIELD(options);

//     return true;
// }

// static bool _equalAlterForeignServerStmt(const AlterForeignServerStmt * a, const AlterForeignServerStmt * b)
// {
//     COMPARE_STRING_FIELD(servername);
//     COMPARE_STRING_FIELD(version);
//     COMPARE_NODE_FIELD(options);
//     COMPARE_SCALAR_FIELD(has_version);

//     return true;
// }

// static bool _equalCreateUserMappingStmt(const CreateUserMappingStmt * a, const CreateUserMappingStmt * b)
// {
//     COMPARE_STRING_FIELD(username);
//     COMPARE_STRING_FIELD(servername);
//     COMPARE_NODE_FIELD(options);

//     return true;
// }

// static bool _equalAlterUserMappingStmt(const AlterUserMappingStmt * a, const AlterUserMappingStmt * b)
// {
//     COMPARE_STRING_FIELD(username);
//     COMPARE_STRING_FIELD(servername);
//     COMPARE_NODE_FIELD(options);

//     return true;
// }

// static bool _equalDropUserMappingStmt(const DropUserMappingStmt * a, const DropUserMappingStmt * b)
// {
//     COMPARE_STRING_FIELD(username);
//     COMPARE_STRING_FIELD(servername);
//     COMPARE_SCALAR_FIELD(missing_ok);

//     return true;
// }

// static bool _equalCreateForeignTableStmt(const CreateForeignTableStmt * a, const CreateForeignTableStmt * b)
// {
//     if (!_equalCreateStmt(&a->base, &b->base))
//         return false;

//     COMPARE_STRING_FIELD(servername);
//     COMPARE_NODE_FIELD(options);

//     return true;
// }

// static bool _equalCreateTrigStmt(const CreateTrigStmt * a, const CreateTrigStmt * b)
// {
//     COMPARE_STRING_FIELD(trigname);
//     COMPARE_NODE_FIELD(relation);
//     COMPARE_NODE_FIELD(funcname);
//     COMPARE_NODE_FIELD(args);
//     COMPARE_SCALAR_FIELD(row);
//     COMPARE_SCALAR_FIELD(timing);
//     COMPARE_SCALAR_FIELD(events);
//     COMPARE_NODE_FIELD(columns);
//     COMPARE_NODE_FIELD(whenClause);
//     COMPARE_SCALAR_FIELD(isconstraint);
//     COMPARE_SCALAR_FIELD(deferrable);
//     COMPARE_SCALAR_FIELD(initdeferred);
//     COMPARE_NODE_FIELD(constrrel);

//     return true;
// }

// static bool _equalCreateEventTrigStmt(const CreateEventTrigStmt * a, const CreateEventTrigStmt * b)
// {
//     COMPARE_STRING_FIELD(trigname);
//     COMPARE_STRING_FIELD(eventname);
//     COMPARE_NODE_FIELD(funcname);
//     COMPARE_NODE_FIELD(whenclause);

//     return true;
// }

// static bool _equalAlterEventTrigStmt(const AlterEventTrigStmt * a, const AlterEventTrigStmt * b)
// {
//     COMPARE_STRING_FIELD(trigname);
//     COMPARE_SCALAR_FIELD(tgenabled);

//     return true;
// }

// static bool _equalCreatePLangStmt(const CreatePLangStmt * a, const CreatePLangStmt * b)
// {
//     COMPARE_SCALAR_FIELD(replace);
//     COMPARE_STRING_FIELD(plname);
//     COMPARE_NODE_FIELD(plhandler);
//     COMPARE_NODE_FIELD(plinline);
//     COMPARE_NODE_FIELD(plvalidator);
//     COMPARE_SCALAR_FIELD(pltrusted);

//     return true;
// }

// static bool _equalCreateRoleStmt(const CreateRoleStmt * a, const CreateRoleStmt * b)
// {
//     COMPARE_SCALAR_FIELD(stmt_type);
//     COMPARE_STRING_FIELD(role);
//     COMPARE_NODE_FIELD(options);

//     return true;
// }

// static bool _equalDenyLoginInterval(const DenyLoginInterval * a, const DenyLoginInterval * b)
// {
//     COMPARE_NODE_FIELD(start);
//     COMPARE_NODE_FIELD(end);

//     return true;
// }

// static bool _equalDenyLoginPoint(const DenyLoginPoint * a, const DenyLoginPoint * b)
// {
//     COMPARE_NODE_FIELD(day);
//     COMPARE_NODE_FIELD(time);

//     return true;
// }

// static bool _equalAlterRoleStmt(const AlterRoleStmt * a, const AlterRoleStmt * b)
// {
//     COMPARE_STRING_FIELD(role);
//     COMPARE_NODE_FIELD(options);
//     COMPARE_SCALAR_FIELD(action);

//     return true;
// }

// static bool _equalAlterRoleSetStmt(const AlterRoleSetStmt * a, const AlterRoleSetStmt * b)
// {
//     COMPARE_STRING_FIELD(role);
//     COMPARE_STRING_FIELD(database);
//     COMPARE_NODE_FIELD(setstmt);

//     return true;
// }

// static bool _equalDropRoleStmt(const DropRoleStmt * a, const DropRoleStmt * b)
// {
//     COMPARE_NODE_FIELD(roles);
//     COMPARE_SCALAR_FIELD(missing_ok);

//     return true;
// }

// static bool _equalLockStmt(const LockStmt * a, const LockStmt * b)
// {
//     COMPARE_NODE_FIELD(relations);
//     COMPARE_SCALAR_FIELD(mode);
//     COMPARE_SCALAR_FIELD(nowait);
//     COMPARE_SCALAR_FIELD(masteronly);

//     return true;
// }

// static bool _equalConstraintsSetStmt(const ConstraintsSetStmt * a, const ConstraintsSetStmt * b)
// {
//     COMPARE_NODE_FIELD(constraints);
//     COMPARE_SCALAR_FIELD(deferred);

//     return true;
// }

// static bool _equalReindexStmt(const ReindexStmt * a, const ReindexStmt * b)
// {
//     COMPARE_SCALAR_FIELD(kind);
//     COMPARE_NODE_FIELD(relation);
//     COMPARE_STRING_FIELD(name);
//     COMPARE_SCALAR_FIELD(do_system);
//     COMPARE_SCALAR_FIELD(do_user);
//     COMPARE_SCALAR_FIELD(relid);

//     return true;
// }

// static bool _equalCreateSchemaStmt(const CreateSchemaStmt * a, const CreateSchemaStmt * b)
// {
//     COMPARE_STRING_FIELD(schemaname);
//     COMPARE_STRING_FIELD(authid);
//     COMPARE_NODE_FIELD(schemaElts);
//     COMPARE_SCALAR_FIELD(if_not_exists);
//     COMPARE_SCALAR_FIELD(istemp);

//     return true;
// }

// static bool _equalCreateConversionStmt(const CreateConversionStmt * a, const CreateConversionStmt * b)
// {
//     COMPARE_NODE_FIELD(conversion_name);
//     COMPARE_STRING_FIELD(for_encoding_name);
//     COMPARE_STRING_FIELD(to_encoding_name);
//     COMPARE_NODE_FIELD(func_name);
//     COMPARE_SCALAR_FIELD(def);

//     return true;
// }

// static bool _equalCreateCastStmt(const CreateCastStmt * a, const CreateCastStmt * b)
// {
//     COMPARE_NODE_FIELD(sourcetype);
//     COMPARE_NODE_FIELD(targettype);
//     COMPARE_NODE_FIELD(func);
//     COMPARE_SCALAR_FIELD(context);
//     COMPARE_SCALAR_FIELD(inout);

//     return true;
// }

// static bool _equalPrepareStmt(const PrepareStmt * a, const PrepareStmt * b)
// {
//     COMPARE_STRING_FIELD(name);
//     COMPARE_NODE_FIELD(argtypes);
//     COMPARE_NODE_FIELD(query);

//     return true;
// }

// static bool _equalExecuteStmt(const ExecuteStmt * a, const ExecuteStmt * b)
// {
//     COMPARE_STRING_FIELD(name);
//     COMPARE_NODE_FIELD(params);

//     return true;
// }

// static bool _equalDeallocateStmt(const DeallocateStmt * a, const DeallocateStmt * b)
// {
//     COMPARE_STRING_FIELD(name);

//     return true;
// }

// static bool _equalDropOwnedStmt(const DropOwnedStmt * a, const DropOwnedStmt * b)
// {
//     COMPARE_NODE_FIELD(roles);
//     COMPARE_SCALAR_FIELD(behavior);

//     return true;
// }


// static bool _equalCreateQueueStmt(const CreateQueueStmt * a, const CreateQueueStmt * b)
// {
//     COMPARE_STRING_FIELD(queue);
//     COMPARE_NODE_FIELD(options);
//     return true;
// }

// static bool _equalAlterQueueStmt(const AlterQueueStmt * a, const AlterQueueStmt * b)
// {
//     COMPARE_STRING_FIELD(queue);
//     COMPARE_NODE_FIELD(options);
//     return true;
// }

// static bool _equalDropQueueStmt(const DropQueueStmt * a, const DropQueueStmt * b)
// {
//     COMPARE_STRING_FIELD(queue);
//     return true;
// }

// static bool _equalCreateResourceGroupStmt(const CreateResourceGroupStmt * a, const CreateResourceGroupStmt * b)
// {
//     COMPARE_STRING_FIELD(name);
//     COMPARE_NODE_FIELD(options);
//     return true;
// }

// static bool _equalDropResourceGroupStmt(const DropResourceGroupStmt * a, const DropResourceGroupStmt * b)
// {
//     COMPARE_STRING_FIELD(name);
//     return true;
// }

// static bool _equalAlterResourceGroupStmt(const AlterResourceGroupStmt * a, const AlterResourceGroupStmt * b)
// {
//     COMPARE_STRING_FIELD(name);
//     COMPARE_NODE_FIELD(options);
//     return true;
// }

// /*
//  * stuff from parsenodes.h
//  */

// static bool _equalReassignOwnedStmt(const ReassignOwnedStmt * a, const ReassignOwnedStmt * b)
// {
//     COMPARE_NODE_FIELD(roles);
//     COMPARE_STRING_FIELD(newrole);

//     return true;
// }

// static bool _equalAlterTSDictionaryStmt(const AlterTSDictionaryStmt * a, const AlterTSDictionaryStmt * b)
// {
//     COMPARE_NODE_FIELD(dictname);
//     COMPARE_NODE_FIELD(options);

//     return true;
// }

// static bool _equalAlterTSConfigurationStmt(const AlterTSConfigurationStmt * a, const AlterTSConfigurationStmt * b)
// {
//     COMPARE_NODE_FIELD(cfgname);
//     COMPARE_NODE_FIELD(tokentype);
//     COMPARE_NODE_FIELD(dicts);
//     COMPARE_SCALAR_FIELD(override);
//     COMPARE_SCALAR_FIELD(replace);
//     COMPARE_SCALAR_FIELD(missing_ok);

//     return true;
// }

static bool _equalAExpr(const void * a_arg, const void * b_arg)
{
    const duckdb_libpgquery::PGAExpr * a = (const duckdb_libpgquery::PGAExpr *)a_arg;
    const duckdb_libpgquery::PGAExpr * b = (const duckdb_libpgquery::PGAExpr *)b_arg;

    COMPARE_SCALAR_FIELD(kind);
    COMPARE_NODE_FIELD(name);
    COMPARE_NODE_FIELD(lexpr);
    COMPARE_NODE_FIELD(rexpr);
    COMPARE_LOCATION_FIELD(location);

    return true;
}

static bool _equalColumnRef(const void * a_arg, const void * b_arg)
{
    const duckdb_libpgquery::PGColumnRef * a = (const duckdb_libpgquery::PGColumnRef *)a_arg;
    const duckdb_libpgquery::PGColumnRef * b = (const duckdb_libpgquery::PGColumnRef *)b_arg;

    COMPARE_NODE_FIELD(fields);
    COMPARE_LOCATION_FIELD(location);

    return true;
}

static bool _equalParamRef(const void * a_arg, const void * b_arg)
{
    const duckdb_libpgquery::PGParamRef * a = (const duckdb_libpgquery::PGParamRef *)a_arg;
    const duckdb_libpgquery::PGParamRef * b = (const duckdb_libpgquery::PGParamRef *)b_arg;

    COMPARE_SCALAR_FIELD(number);
    COMPARE_LOCATION_FIELD(location);

    return true;
}

static bool _equalAConst(const void * a_arg, const void * b_arg)
{
    const duckdb_libpgquery::PGAConst * a = (const duckdb_libpgquery::PGAConst *)a_arg;
    const duckdb_libpgquery::PGAConst * b = (const duckdb_libpgquery::PGAConst *)b_arg;

    if (!pg_equal(&a->val, &b->val)) /* hack for in-line Value field */
        return false;
    COMPARE_LOCATION_FIELD(location);

    return true;
}

static bool _equalFuncCall(const void * a_arg, const void * b_arg)
{
    const duckdb_libpgquery::PGFuncCall * a = (const duckdb_libpgquery::PGFuncCall *)a_arg;
    const duckdb_libpgquery::PGFuncCall * b = (const duckdb_libpgquery::PGFuncCall *)b_arg;

    COMPARE_NODE_FIELD(funcname);
    COMPARE_NODE_FIELD(args);
    COMPARE_NODE_FIELD(agg_order);
    COMPARE_NODE_FIELD(agg_filter);
    COMPARE_SCALAR_FIELD(agg_within_group);
    COMPARE_SCALAR_FIELD(agg_star);
    COMPARE_SCALAR_FIELD(agg_distinct);
    COMPARE_SCALAR_FIELD(func_variadic);
    COMPARE_NODE_FIELD(over);
    COMPARE_LOCATION_FIELD(location);

    return true;
}

static bool _equalAStar(const void * a_arg, const void * b_arg)
{
    
    return true;
}

static bool _equalAIndices(const void * a_arg, const void * b_arg)
{
    const duckdb_libpgquery::PGAIndices * a = (const duckdb_libpgquery::PGAIndices *)a_arg;
    const duckdb_libpgquery::PGAIndices * b = (const duckdb_libpgquery::PGAIndices *)b_arg;

    COMPARE_NODE_FIELD(lidx);
    COMPARE_NODE_FIELD(uidx);

    return true;
}

static bool _equalA_Indirection(const void * a_arg, const void * b_arg)
{
    const duckdb_libpgquery::PGAIndirection * a = (const duckdb_libpgquery::PGAIndirection *)a_arg;
    const duckdb_libpgquery::PGAIndirection * b = (const duckdb_libpgquery::PGAIndirection *)b_arg;

    COMPARE_NODE_FIELD(arg);
    COMPARE_NODE_FIELD(indirection);

    return true;
}

static bool _equalA_ArrayExpr(const void * a_arg, const void * b_arg)
{
    const duckdb_libpgquery::PGAArrayExpr * a = (const duckdb_libpgquery::PGAArrayExpr *)a_arg;
    const duckdb_libpgquery::PGAArrayExpr * b = (const duckdb_libpgquery::PGAArrayExpr *)b_arg;

    COMPARE_NODE_FIELD(elements);
    COMPARE_LOCATION_FIELD(location);

    return true;
}

static bool _equalResTarget(const void * a_arg, const void * b_arg)
{
    const duckdb_libpgquery::PGResTarget * a = (const duckdb_libpgquery::PGResTarget *)a_arg;
    const duckdb_libpgquery::PGResTarget * b = (const duckdb_libpgquery::PGResTarget *)b_arg;

    COMPARE_STRING_FIELD(name);
    COMPARE_NODE_FIELD(indirection);
    COMPARE_NODE_FIELD(val);
    COMPARE_LOCATION_FIELD(location);

    return true;
}

static bool _equalTypeName(const void * a_arg, const void * b_arg)
{
    const duckdb_libpgquery::PGTypeName * a = (const duckdb_libpgquery::PGTypeName *)a_arg;
    const duckdb_libpgquery::PGTypeName * b = (const duckdb_libpgquery::PGTypeName *)b_arg;

    COMPARE_NODE_FIELD(names);
    COMPARE_SCALAR_FIELD(typeOid);
    COMPARE_SCALAR_FIELD(setof);
    COMPARE_SCALAR_FIELD(pct_type);
    COMPARE_NODE_FIELD(typmods);
    COMPARE_SCALAR_FIELD(typemod);
    COMPARE_NODE_FIELD(arrayBounds);
    COMPARE_LOCATION_FIELD(location);

    return true;
}

static bool _equalTypeCast(const void * a_arg, const void * b_arg)
{
    const duckdb_libpgquery::PGTypeCast * a = (const duckdb_libpgquery::PGTypeCast *)a_arg;
    const duckdb_libpgquery::PGTypeCast * b = (const duckdb_libpgquery::PGTypeCast *)b_arg;

    COMPARE_NODE_FIELD(arg);
    COMPARE_NODE_FIELD(typeName);
    COMPARE_LOCATION_FIELD(location);

    return true;
}

// static bool _equalCollateClause(const duckdb_libpgquery::PGCollateClause * a, const duckdb_libpgquery::PGCollateClause * b)
// {
//     COMPARE_NODE_FIELD(arg);
//     COMPARE_NODE_FIELD(collname);
//     COMPARE_LOCATION_FIELD(location);

//     return true;
// }

static bool _equalSortBy(const void * a_arg, const void * b_arg)
{
    const duckdb_libpgquery::PGSortBy * a = (const duckdb_libpgquery::PGSortBy *)a_arg;
    const duckdb_libpgquery::PGSortBy * b = (const duckdb_libpgquery::PGSortBy *)b_arg;

    COMPARE_NODE_FIELD(node);
    COMPARE_SCALAR_FIELD(sortby_dir);
    COMPARE_SCALAR_FIELD(sortby_nulls);
    COMPARE_NODE_FIELD(useOp);
    COMPARE_LOCATION_FIELD(location);

    return true;
}

static bool _equalWindowDef(const void * a_arg, const void * b_arg)
{
    const duckdb_libpgquery::PGWindowDef * a = (const duckdb_libpgquery::PGWindowDef *)a_arg;
    const duckdb_libpgquery::PGWindowDef * b = (const duckdb_libpgquery::PGWindowDef *)b_arg;

    COMPARE_STRING_FIELD(name);
    COMPARE_STRING_FIELD(refname);
    COMPARE_NODE_FIELD(partitionClause);
    COMPARE_NODE_FIELD(orderClause);
    COMPARE_SCALAR_FIELD(frameOptions);
    COMPARE_NODE_FIELD(startOffset);
    COMPARE_NODE_FIELD(endOffset);
    COMPARE_LOCATION_FIELD(location);

    return true;
}

static bool _equalRangeSubselect(const void * a_arg, const void * b_arg)
{
    const duckdb_libpgquery::PGRangeSubselect * a = (const duckdb_libpgquery::PGRangeSubselect *)a_arg;
    const duckdb_libpgquery::PGRangeSubselect * b = (const duckdb_libpgquery::PGRangeSubselect *)b_arg;

    COMPARE_SCALAR_FIELD(lateral);
    COMPARE_NODE_FIELD(subquery);
    COMPARE_NODE_FIELD(alias);

    return true;
}

static bool _equalRangeFunction(const void * a_arg, const void * b_arg)
{
    const duckdb_libpgquery::PGRangeFunction * a = (const duckdb_libpgquery::PGRangeFunction *)a_arg;
    const duckdb_libpgquery::PGRangeFunction * b = (const duckdb_libpgquery::PGRangeFunction *)b_arg;

    COMPARE_SCALAR_FIELD(lateral);
    COMPARE_SCALAR_FIELD(ordinality);
    COMPARE_SCALAR_FIELD(is_rowsfrom);
    COMPARE_NODE_FIELD(functions);
    COMPARE_NODE_FIELD(alias);
    COMPARE_NODE_FIELD(coldeflist);

    return true;
}

// static bool _equalIndexElem(const duckdb_libpgquery::PGIndexElem * a, const duckdb_libpgquery::PGIndexElem * b)
// {
//     COMPARE_STRING_FIELD(name);
//     COMPARE_NODE_FIELD(expr);
//     COMPARE_STRING_FIELD(indexcolname);
//     COMPARE_NODE_FIELD(collation);
//     COMPARE_NODE_FIELD(opclass);
//     COMPARE_SCALAR_FIELD(ordering);
//     COMPARE_SCALAR_FIELD(nulls_ordering);

//     return true;
// }

static bool _equalColumnDef(const void * a_arg, const void * b_arg)
{
    const duckdb_libpgquery::PGColumnDef * a = (const duckdb_libpgquery::PGColumnDef *)a_arg;
    const duckdb_libpgquery::PGColumnDef * b = (const duckdb_libpgquery::PGColumnDef *)b_arg;

    COMPARE_STRING_FIELD(colname);
    COMPARE_NODE_FIELD(typeName);
    COMPARE_SCALAR_FIELD(inhcount);
    COMPARE_SCALAR_FIELD(is_local);
    COMPARE_SCALAR_FIELD(is_not_null);
    COMPARE_SCALAR_FIELD(is_from_type);
    //COMPARE_SCALAR_FIELD(attnum);
    COMPARE_SCALAR_FIELD(storage);
    COMPARE_NODE_FIELD(raw_default);
    COMPARE_NODE_FIELD(cooked_default);
    COMPARE_NODE_FIELD(collClause);
    COMPARE_SCALAR_FIELD(collOid);
    COMPARE_NODE_FIELD(constraints);
    /* GPDB_90_MERGE_FIXME: should we be comparing encoding? */
    COMPARE_NODE_FIELD(fdwoptions);
    COMPARE_LOCATION_FIELD(location);

    return true;
}

// static bool _equalConstraint(const Constraint * a, const Constraint * b)
// {
//     COMPARE_SCALAR_FIELD(contype);
//     COMPARE_STRING_FIELD(conname);
//     COMPARE_SCALAR_FIELD(deferrable);
//     COMPARE_SCALAR_FIELD(initdeferred);
//     COMPARE_LOCATION_FIELD(location);
//     COMPARE_SCALAR_FIELD(is_no_inherit);
//     COMPARE_NODE_FIELD(raw_expr);
//     COMPARE_STRING_FIELD(cooked_expr);
//     COMPARE_NODE_FIELD(keys);
//     COMPARE_NODE_FIELD(exclusions);
//     COMPARE_NODE_FIELD(options);
//     COMPARE_STRING_FIELD(indexname);
//     COMPARE_STRING_FIELD(indexspace);
//     COMPARE_STRING_FIELD(access_method);
//     COMPARE_NODE_FIELD(where_clause);
//     COMPARE_NODE_FIELD(pktable);
//     COMPARE_NODE_FIELD(fk_attrs);
//     COMPARE_NODE_FIELD(pk_attrs);
//     COMPARE_SCALAR_FIELD(fk_matchtype);
//     COMPARE_SCALAR_FIELD(fk_upd_action);
//     COMPARE_SCALAR_FIELD(fk_del_action);
//     COMPARE_NODE_FIELD(old_conpfeqop);
//     COMPARE_SCALAR_FIELD(old_pktable_oid);
//     COMPARE_SCALAR_FIELD(skip_validation);
//     COMPARE_SCALAR_FIELD(initially_valid);

//     COMPARE_SCALAR_FIELD(trig1Oid);
//     COMPARE_SCALAR_FIELD(trig2Oid);
//     COMPARE_SCALAR_FIELD(trig3Oid);
//     COMPARE_SCALAR_FIELD(trig4Oid);

//     return true;
// }

// static bool _equalDefElem(const DefElem * a, const DefElem * b)
// {
//     COMPARE_STRING_FIELD(defnamespace);
//     COMPARE_STRING_FIELD(defname);
//     COMPARE_NODE_FIELD(arg);
//     COMPARE_SCALAR_FIELD(defaction);

//     return true;
// }

static bool _equalLockingClause(const void * a_arg, const void * b_arg)
{
    const duckdb_libpgquery::PGLockingClause * a = (const duckdb_libpgquery::PGLockingClause *)a_arg;
    const duckdb_libpgquery::PGLockingClause * b = (const duckdb_libpgquery::PGLockingClause *)b_arg;

    COMPARE_NODE_FIELD(lockedRels);
    COMPARE_SCALAR_FIELD(strength);
    COMPARE_SCALAR_FIELD(waitPolicy);

    return true;
}

static bool _equalRangeTblEntry(const void * a_arg, const void * b_arg)
{
    const duckdb_libpgquery::PGRangeTblEntry * a = (const duckdb_libpgquery::PGRangeTblEntry *)a_arg;
    const duckdb_libpgquery::PGRangeTblEntry * b = (const duckdb_libpgquery::PGRangeTblEntry *)b_arg;

    COMPARE_SCALAR_FIELD(rtekind);
    COMPARE_SCALAR_FIELD(relid);
    COMPARE_SCALAR_FIELD(relkind);
    COMPARE_NODE_FIELD(subquery);
    //COMPARE_SCALAR_FIELD(security_barrier);
    COMPARE_SCALAR_FIELD(jointype);
    COMPARE_NODE_FIELD(joinaliasvars);
    COMPARE_NODE_FIELD(functions);
    COMPARE_SCALAR_FIELD(funcordinality);
    COMPARE_NODE_FIELD(values_lists);
    //COMPARE_NODE_FIELD(values_collations);
    COMPARE_STRING_FIELD(ctename);
    COMPARE_SCALAR_FIELD(ctelevelsup);
    COMPARE_SCALAR_FIELD(self_reference);
    //COMPARE_NODE_FIELD(ctecoltypes);
    //COMPARE_NODE_FIELD(ctecoltypmods);
    //COMPARE_NODE_FIELD(ctecolcollations);
    COMPARE_NODE_FIELD(alias);
    COMPARE_NODE_FIELD(eref);
    COMPARE_SCALAR_FIELD(lateral);
    COMPARE_SCALAR_FIELD(inh);
    COMPARE_SCALAR_FIELD(inFromCl);
    //COMPARE_SCALAR_FIELD(requiredPerms);
    //COMPARE_SCALAR_FIELD(checkAsUser);
    //COMPARE_BITMAPSET_FIELD(selectedCols);
    //COMPARE_BITMAPSET_FIELD(modifiedCols);
    //COMPARE_NODE_FIELD(securityQuals);

    return true;
}

static bool _equalRangeTblFunction(const void * a_arg, const void * b_arg)
{
    const duckdb_libpgquery::PGRangeTblFunction * a = (const duckdb_libpgquery::PGRangeTblFunction *)a_arg;
    const duckdb_libpgquery::PGRangeTblFunction * b = (const duckdb_libpgquery::PGRangeTblFunction *)b_arg;

    COMPARE_NODE_FIELD(funcexpr);
    COMPARE_SCALAR_FIELD(funccolcount);
    COMPARE_NODE_FIELD(funccolnames);
    COMPARE_NODE_FIELD(funccoltypes);
    COMPARE_NODE_FIELD(funccoltypmods);
    COMPARE_NODE_FIELD(funccolcollations);
    //COMPARE_VARLENA_FIELD(funcuserdata, -1);
    COMPARE_BITMAPSET_FIELD(funcparams);

    return true;
}

// static bool _equalWithCheckOption(const WithCheckOption * a, const WithCheckOption * b)
// {
//     COMPARE_STRING_FIELD(viewname);
//     COMPARE_NODE_FIELD(qual);
//     COMPARE_SCALAR_FIELD(cascaded);

//     return true;
// }

static bool _equalSortGroupClause(const void * a_arg, const void * b_arg)
{
    const duckdb_libpgquery::PGSortGroupClause * a = (const duckdb_libpgquery::PGSortGroupClause *)a_arg;
    const duckdb_libpgquery::PGSortGroupClause * b = (const duckdb_libpgquery::PGSortGroupClause *)b_arg;

    COMPARE_SCALAR_FIELD(tleSortGroupRef);
    COMPARE_SCALAR_FIELD(eqop);
    COMPARE_SCALAR_FIELD(sortop);
    COMPARE_SCALAR_FIELD(nulls_first);
    COMPARE_SCALAR_FIELD(hashable);

    return true;
}

// static bool _equalGroupingClause(const PGGroupingClause * a, const PGGroupingClause * b)
// {
//     COMPARE_SCALAR_FIELD(groupType);
//     COMPARE_NODE_FIELD(groupsets);

//     return true;
// }

static bool _equalGroupingFunc(const void * a_arg, const void * b_arg)
{
    const duckdb_libpgquery::PGGroupingFunc * a = (const duckdb_libpgquery::PGGroupingFunc *)a_arg;
    const duckdb_libpgquery::PGGroupingFunc * b = (const duckdb_libpgquery::PGGroupingFunc *)b_arg;

    COMPARE_NODE_FIELD(args);
    //COMPARE_SCALAR_FIELD(ngrpcols);

    return true;
}

// static bool _equalGrouping(const Grouping * a __attribute__((unused)), const Grouping * b __attribute__((unused)))

// {
//     return true;
// }

// static bool _equalGroupId(const GroupId * a __attribute__((unused)), const GroupId * b __attribute__((unused)))
// {
//     return true;
// }

static bool _equalWindowClause(const void * a_arg, const void * b_arg)
{
    const duckdb_libpgquery::PGWindowClause * a = (const duckdb_libpgquery::PGWindowClause *)a_arg;
    const duckdb_libpgquery::PGWindowClause * b = (const duckdb_libpgquery::PGWindowClause *)b_arg;

    COMPARE_STRING_FIELD(name);
    COMPARE_STRING_FIELD(refname);
    COMPARE_NODE_FIELD(partitionClause);
    COMPARE_NODE_FIELD(orderClause);
    COMPARE_SCALAR_FIELD(frameOptions);
    COMPARE_NODE_FIELD(startOffset);
    COMPARE_NODE_FIELD(endOffset);
    COMPARE_SCALAR_FIELD(winref);
    COMPARE_SCALAR_FIELD(copiedOrder);

    return true;
}

// static bool _equalRowMarkClause(const RowMarkClause * a, const RowMarkClause * b)
// {
//     COMPARE_SCALAR_FIELD(rti);
//     COMPARE_SCALAR_FIELD(strength);
//     COMPARE_SCALAR_FIELD(noWait);
//     COMPARE_SCALAR_FIELD(pushedDown);

//     return true;
// }

static bool _equalWithClause(const void * a_arg, const void * b_arg)
{
    const duckdb_libpgquery::PGWithClause * a = (const duckdb_libpgquery::PGWithClause *)a_arg;
    const duckdb_libpgquery::PGWithClause * b = (const duckdb_libpgquery::PGWithClause *)b_arg;

    COMPARE_NODE_FIELD(ctes);
    COMPARE_SCALAR_FIELD(recursive);
    COMPARE_LOCATION_FIELD(location);

    return true;
}

static bool _equalCommonTableExpr(const void * a_arg, const void * b_arg)
{
    const duckdb_libpgquery::PGCommonTableExpr * a = (const duckdb_libpgquery::PGCommonTableExpr *)a_arg;
    const duckdb_libpgquery::PGCommonTableExpr * b = (const duckdb_libpgquery::PGCommonTableExpr *)b_arg;

    COMPARE_STRING_FIELD(ctename);
    COMPARE_NODE_FIELD(aliascolnames);
    COMPARE_NODE_FIELD(ctequery);
    COMPARE_LOCATION_FIELD(location);
    COMPARE_SCALAR_FIELD(cterecursive);
    COMPARE_SCALAR_FIELD(cterefcount);
    COMPARE_NODE_FIELD(ctecolnames);
    COMPARE_NODE_FIELD(ctecoltypes);
    COMPARE_NODE_FIELD(ctecoltypmods);
    COMPARE_NODE_FIELD(ctecolcollations);

    return true;
}

// static bool _equalTableValueExpr(const TableValueExpr * a, const TableValueExpr * b)
// {
//     COMPARE_NODE_FIELD(subquery);

//     return true;
// }

// static bool _equalAlterTypeStmt(const AlterTypeStmt * a, const AlterTypeStmt * b)
// {
//     COMPARE_NODE_FIELD(typeName);
//     COMPARE_NODE_FIELD(encoding);

//     return true;
// }

// static bool _equalDistributedBy(const DistributedBy * a, const DistributedBy * b)
// {
//     COMPARE_SCALAR_FIELD(ptype);
//     COMPARE_SCALAR_FIELD(numsegments);
//     COMPARE_NODE_FIELD(keyCols);

//     return true;
// }

// static bool _equalPartitionRule(const PartitionRule * a, const PartitionRule * b)
// {
//     COMPARE_SCALAR_FIELD(parchildrelid);

//     return true;
// }

// static bool _equalXmlSerialize(const XmlSerialize * a, const XmlSerialize * b)
// {
//     COMPARE_SCALAR_FIELD(xmloption);
//     COMPARE_NODE_FIELD(expr);
//     COMPARE_NODE_FIELD(typeName);
//     COMPARE_LOCATION_FIELD(location);

//     return true;
// }

/*
 * Stuff from pg_list.h
 */

static bool _equalList(const void * a_arg, const void * b_arg)
{
    const duckdb_libpgquery::PGList * a = (const duckdb_libpgquery::PGList *)a_arg;
    const duckdb_libpgquery::PGList * b = (const duckdb_libpgquery::PGList *)b_arg;

    using duckdb_libpgquery::elog;
    using duckdb_libpgquery::PGListCell;
    using duckdb_libpgquery::T_PGList;
    using duckdb_libpgquery::T_PGIntList;
    using duckdb_libpgquery::T_PGOidList;

    const PGListCell * item_a;
    const PGListCell * item_b;

    /*
	 * Try to reject by simple scalar checks before grovelling through all the
	 * list elements...
	 */
    COMPARE_SCALAR_FIELD(type);
    COMPARE_SCALAR_FIELD(length);

    /*
	 * We place the switch outside the loop for the sake of efficiency; this
	 * may not be worth doing...
	 */
    switch (a->type)
    {
        case T_PGList:
            forboth(item_a, a, item_b, b)
            {
                if (!pg_equal(lfirst(item_a), lfirst(item_b)))
                    return false;
            }
            break;
        case T_PGIntList:
            forboth(item_a, a, item_b, b)
            {
                if (lfirst_int(item_a) != lfirst_int(item_b))
                    return false;
            }
            break;
        case T_PGOidList:
            forboth(item_a, a, item_b, b)
            {
                if (lfirst_oid(item_a) != lfirst_oid(item_b))
                    return false;
            }
            break;
        default:
            elog(ERROR, "unrecognized list node type: %d", (int)a->type);
            return false; /* keep compiler quiet */
    }

    /*
	 * If we got here, we should have run out of elements of both lists
	 */
    Assert(item_a == NULL)
    Assert(item_b == NULL)

    return true;
}

/*
 * Stuff from value.h
 */

static bool _equalValue(const void * a_arg, const void * b_arg)
{
    const duckdb_libpgquery::PGValue * a = (const duckdb_libpgquery::PGValue *)a_arg;
    const duckdb_libpgquery::PGValue * b = (const duckdb_libpgquery::PGValue *)b_arg;

    using duckdb_libpgquery::elog;
    using duckdb_libpgquery::T_PGInteger;
    using duckdb_libpgquery::T_PGFloat;
    using duckdb_libpgquery::T_PGString;
    using duckdb_libpgquery::T_PGBitString;
    using duckdb_libpgquery::T_PGNull;

    COMPARE_SCALAR_FIELD(type);

    switch (a->type)
    {
        case T_PGInteger:
            COMPARE_SCALAR_FIELD(val.ival);
            break;
        case T_PGFloat:
        case T_PGString:
        case T_PGBitString:
            COMPARE_STRING_FIELD(val.str);
            break;
        case T_PGNull:
            /* nothing to do */
            break;
        default:
            elog(ERROR, "unrecognized node type: %d", (int)a->type);
            break;
    }

    return true;
}

/*
 * equal
 *	  returns whether two nodes are equal
 */
bool pg_equal(const void * a, const void * b)
{
    using duckdb_libpgquery::elog;

    using duckdb_libpgquery::PGNode;

    using duckdb_libpgquery::T_PGList;
    using duckdb_libpgquery::T_PGInteger;
    using duckdb_libpgquery::T_PGFloat;
    using duckdb_libpgquery::T_PGString;
    using duckdb_libpgquery::T_PGBitString;
    using duckdb_libpgquery::T_PGNull;
    using duckdb_libpgquery::T_PGIntList;
    using duckdb_libpgquery::T_PGOidList;

    using duckdb_libpgquery::T_PGAExpr;
    using duckdb_libpgquery::T_PGColumnRef;
    using duckdb_libpgquery::T_PGParamRef;
    using duckdb_libpgquery::T_PGAConst;
    using duckdb_libpgquery::T_PGFuncCall;
    using duckdb_libpgquery::T_PGAStar;
    using duckdb_libpgquery::T_PGAIndices;
    using duckdb_libpgquery::T_PGAIndirection;
    using duckdb_libpgquery::T_PGAArrayExpr;
    using duckdb_libpgquery::T_PGResTarget;
    using duckdb_libpgquery::T_PGTypeCast;
    using duckdb_libpgquery::T_PGCollateClause;
    using duckdb_libpgquery::T_PGSortBy;
    using duckdb_libpgquery::T_PGWindowDef;
    using duckdb_libpgquery::T_PGRangeSubselect;
    using duckdb_libpgquery::T_PGRangeFunction;
    using duckdb_libpgquery::T_PGTypeName;
    using duckdb_libpgquery::T_PGIndexElem;
    using duckdb_libpgquery::T_PGColumnDef;
    using duckdb_libpgquery::T_PGLockingClause;
    using duckdb_libpgquery::T_PGRangeTblEntry;
    using duckdb_libpgquery::T_PGRangeTblFunction;
    using duckdb_libpgquery::T_PGSortGroupClause;
    //using duckdb_libpgquery::T_PGGroupingClause;
    using duckdb_libpgquery::T_PGGroupingFunc;
    //using duckdb_libpgquery::T_PGGrouping;
    using duckdb_libpgquery::T_PGWindowClause;
    using duckdb_libpgquery::T_PGWithClause;
    using duckdb_libpgquery::T_PGCommonTableExpr;

    using duckdb_libpgquery::T_PGAlias;
    using duckdb_libpgquery::T_PGRangeVar;
    using duckdb_libpgquery::T_PGIntoClause;
    using duckdb_libpgquery::T_PGVar;
    using duckdb_libpgquery::T_PGConst;
    using duckdb_libpgquery::T_PGParam;
    using duckdb_libpgquery::T_PGAggref;
    using duckdb_libpgquery::T_PGWindowFunc;
    using duckdb_libpgquery::T_PGArrayRef;
    using duckdb_libpgquery::T_PGFuncExpr;
    using duckdb_libpgquery::T_PGNamedArgExpr;
    using duckdb_libpgquery::T_PGOpExpr;
    using duckdb_libpgquery::T_PGDistinctExpr;
    using duckdb_libpgquery::T_PGNullIfExpr;
    using duckdb_libpgquery::T_PGScalarArrayOpExpr;
    using duckdb_libpgquery::T_PGBoolExpr;
    using duckdb_libpgquery::T_PGSubLink;
    using duckdb_libpgquery::T_PGSubPlan;
    using duckdb_libpgquery::T_PGFieldSelect;
    using duckdb_libpgquery::T_PGConvertRowtypeExpr;
    using duckdb_libpgquery::T_PGCaseExpr;
    using duckdb_libpgquery::T_PGCaseWhen;
    using duckdb_libpgquery::T_PGCaseTestExpr;
    using duckdb_libpgquery::T_PGArrayExpr;
    using duckdb_libpgquery::T_PGRowExpr;
    using duckdb_libpgquery::T_PGRowCompareExpr;
    using duckdb_libpgquery::T_PGCoalesceExpr;
    using duckdb_libpgquery::T_PGMinMaxExpr;
    using duckdb_libpgquery::T_PGXmlExpr;
    using duckdb_libpgquery::T_PGNullTest;
    using duckdb_libpgquery::T_PGBooleanTest;
    using duckdb_libpgquery::T_PGCoerceToDomain;
    using duckdb_libpgquery::T_PGCoerceToDomainValue;
    using duckdb_libpgquery::T_PGCurrentOfExpr;
    using duckdb_libpgquery::T_PGTargetEntry;
    using duckdb_libpgquery::T_PGRangeTblRef;
    using duckdb_libpgquery::T_PGFromExpr;
    using duckdb_libpgquery::T_PGJoinExpr;

    using duckdb_libpgquery::T_PGQuery;
    using duckdb_libpgquery::T_PGInsertStmt;
    using duckdb_libpgquery::T_PGDeleteStmt;
    using duckdb_libpgquery::T_PGUpdateStmt;
    using duckdb_libpgquery::T_PGSelectStmt;
    using duckdb_libpgquery::T_PGSetOperationStmt;

    bool retval;

    if (a == b)
        return true;

    /*
	 * note that a!=b, so only one of them can be NULL
	 */
    if (a == NULL || b == NULL)
        return false;

    /*
	 * are they the same type of nodes?
	 */
    if (nodeTag(a) != nodeTag(b))
        return false;

    /* Guard against stack overflow due to overly complex expressions */
    //TODO kindred
    //check_stack_depth();

    switch (nodeTag(a))
    {
            /*
			 * PRIMITIVE NODES
			 */
        case T_PGAlias:
            retval = _equalAlias(a, b);
            break;
        case T_PGRangeVar:
            retval = _equalRangeVar(a, b);
            break;
        case T_PGIntoClause:
            retval = _equalIntoClause(a, b);
            break;
        case T_PGVar:
            retval = _equalVar(a, b);
            break;
        case T_PGConst:
            retval = _equalConst(a, b);
            break;
        case T_PGParam:
            retval = _equalParam(a, b);
            break;
        case T_PGAggref:
            retval = _equalAggref(a, b);
            break;
        case T_PGWindowFunc:
            retval = _equalWindowFunc(a, b);
            break;
        case T_PGArrayRef:
            retval = _equalArrayRef(a, b);
            break;
        case T_PGFuncExpr:
            retval = _equalFuncExpr(a, b);
            break;
        case T_PGNamedArgExpr:
            retval = _equalNamedArgExpr(a, b);
            break;
        case T_PGOpExpr:
            retval = _equalOpExpr(a, b);
            break;
        case T_PGDistinctExpr:
            retval = _equalDistinctExpr(a, b);
            break;
        case T_PGNullIfExpr:
            retval = _equalNullIfExpr(a, b);
            break;
        case T_PGScalarArrayOpExpr:
            retval = _equalScalarArrayOpExpr(a, b);
            break;
        case T_PGBoolExpr:
            retval = _equalBoolExpr(a, b);
            break;
        case T_PGSubLink:
            retval = _equalSubLink(a, b);
            break;
        case T_PGSubPlan:
            retval = _equalSubPlan(a, b);
            break;
        // case T_PGAlternativeSubPlan:
        //     retval = _equalAlternativeSubPlan(a, b);
        //     break;
        case T_PGFieldSelect:
            retval = _equalFieldSelect(a, b);
            break;
        // case T_PGFieldStore:
        //     retval = _equalFieldStore(a, b);
        //     break;
        // case T_PGRelabelType:
        //     retval = _equalRelabelType(a, b);
        //     break;
        // case T_PGCoerceViaIO:
        //     retval = _equalCoerceViaIO(a, b);
        //     break;
        // case T_PGArrayCoerceExpr:
        //     retval = _equalArrayCoerceExpr(a, b);
        //     break;
        // case T_PGConvertRowtypeExpr:
        //     retval = _equalConvertRowtypeExpr(a, b);
        //     break;
        // case T_PGCollateExpr:
        //     retval = _equalCollateExpr(a, b);
        //     break;
        case T_PGCaseExpr:
            retval = _equalCaseExpr(a, b);
            break;
        case T_PGCaseWhen:
            retval = _equalCaseWhen(a, b);
            break;
        case T_PGCaseTestExpr:
            retval = _equalCaseTestExpr(a, b);
            break;
        case T_PGArrayExpr:
            retval = _equalArrayExpr(a, b);
            break;
        case T_PGRowExpr:
            retval = _equalRowExpr(a, b);
            break;
        case T_PGRowCompareExpr:
            retval = _equalRowCompareExpr(a, b);
            break;
        case T_PGCoalesceExpr:
            retval = _equalCoalesceExpr(a, b);
            break;
        case T_PGMinMaxExpr:
            retval = _equalMinMaxExpr(a, b);
            break;
        // case T_PGXmlExpr:
        //     retval = _equalXmlExpr(a, b);
        //     break;
        case T_PGNullTest:
            retval = _equalNullTest(a, b);
            break;
        case T_PGBooleanTest:
            retval = _equalBooleanTest(a, b);
            break;
        case T_PGCoerceToDomain:
            retval = _equalCoerceToDomain(a, b);
            break;
        case T_PGCoerceToDomainValue:
            retval = _equalCoerceToDomainValue(a, b);
            break;
        // case T_PGSetToDefault:
        //     retval = _equalSetToDefault(a, b);
        //     break;
        case T_PGCurrentOfExpr:
            retval = _equalCurrentOfExpr(a, b);
            break;
        case T_PGTargetEntry:
            retval = _equalTargetEntry(a, b);
            break;
        case T_PGRangeTblRef:
            retval = _equalRangeTblRef(a, b);
            break;
        case T_PGFromExpr:
            retval = _equalFromExpr(a, b);
            break;
        // case T_PGFlow:
        //     retval = _equalFlow(a, b);
        //     break;
        case T_PGJoinExpr:
            retval = _equalJoinExpr(a, b);
            break;

            /*
			 * RELATION NODES
			 */
        // case T_PGPathKey:
        //     retval = _equalPathKey(a, b);
        //     break;
        // case T_PGRestrictInfo:
        //     retval = _equalRestrictInfo(a, b);
        //     break;
        // case T_PGPlaceHolderVar:
        //     retval = _equalPlaceHolderVar(a, b);
        //     break;
        // case T_PGSpecialJoinInfo:
        //     retval = _equalSpecialJoinInfo(a, b);
        //     break;
        // case T_LateralJoinInfo:
        //     retval = _equalLateralJoinInfo(a, b);
        //     break;
        // case T_AppendRelInfo:
        //     retval = _equalAppendRelInfo(a, b);
        //     break;
        // case T_PlaceHolderInfo:
        //     retval = _equalPlaceHolderInfo(a, b);
        //     break;

        case T_PGList:
        case T_PGIntList:
        case T_PGOidList:
            retval = _equalList(a, b);
            break;

        case T_PGInteger:
        case T_PGFloat:
        case T_PGString:
        case T_PGBitString:
        case T_PGNull:
            retval = _equalValue(a, b);
            break;

            /*
			 * PARSE NODES
			 */
        case T_PGQuery:
            retval = _equalQuery(a, b);
            break;
        case T_PGInsertStmt:
            retval = _equalInsertStmt(a, b);
            break;
        case T_PGDeleteStmt:
            retval = _equalDeleteStmt(a, b);
            break;
        case T_PGUpdateStmt:
            retval = _equalUpdateStmt(a, b);
            break;
        case T_PGSelectStmt:
            retval = _equalSelectStmt(a, b);
            break;
        // case T_PGSetOperationStmt:
        //     retval = _equalSetOperationStmt(a, b);
        //     break;
        // case T_PGAlterTableStmt:
        //     retval = _equalAlterTableStmt(a, b);
        //     break;
        // case T_PGAlterTableCmd:
        //     retval = _equalAlterTableCmd(a, b);
        //     break;
        // case T_SetDistributionCmd:
        //     retval = _equalSetDistributionCmd(a, b);
        //     break;
        // case T_InheritPartitionCmd:
        //     retval = _equalInheritPartitionCmd(a, b);
        //     break;
        // case T_AlterPartitionCmd:
        //     retval = _equalAlterPartitionCmd(a, b);
        //     break;
        // case T_AlterPartitionId:
        //     retval = _equalAlterPartitionId(a, b);
        //     break;
        // case T_AlterDomainStmt:
        //     retval = _equalAlterDomainStmt(a, b);
        //     break;
        // case T_GrantStmt:
        //     retval = _equalGrantStmt(a, b);
        //     break;
        // case T_GrantRoleStmt:
        //     retval = _equalGrantRoleStmt(a, b);
        //     break;
        // case T_AlterDefaultPrivilegesStmt:
        //     retval = _equalAlterDefaultPrivilegesStmt(a, b);
        //     break;
        // case T_DeclareCursorStmt:
        //     retval = _equalDeclareCursorStmt(a, b);
        //     break;
        // case T_ClosePortalStmt:
        //     retval = _equalClosePortalStmt(a, b);
        //     break;
        // case T_ClusterStmt:
        //     retval = _equalClusterStmt(a, b);
        //     break;
        // case T_SingleRowErrorDesc:
        //     retval = _equalSingleRowErrorDesc(a, b);
        //     break;
        // case T_CopyStmt:
        //     retval = _equalCopyStmt(a, b);
        //     break;
        // case T_CreateStmt:
        //     retval = _equalCreateStmt(a, b);
        //     break;
        // case T_ColumnReferenceStorageDirective:
        //     retval = _equalColumnReferenceStorageDirective(a, b);
        //     break;
        // case T_PartitionRangeItem:
        //     retval = _equalPartitionRangeItem(a, b);
        //     break;
        // case T_ExtTableTypeDesc:
        //     retval = _equalExtTableTypeDesc(a, b);
        //     break;
        // case T_CreateExternalStmt:
        //     retval = _equalCreateExternalStmt(a, b);
        //     break;
        // case T_TableLikeClause:
        //     retval = _equalTableLikeClause(a, b);
        //     break;
        // case T_DefineStmt:
        //     retval = _equalDefineStmt(a, b);
        //     break;
        // case T_DropStmt:
        //     retval = _equalDropStmt(a, b);
        //     break;
        // case T_TruncateStmt:
        //     retval = _equalTruncateStmt(a, b);
        //     break;
        // case T_CommentStmt:
        //     retval = _equalCommentStmt(a, b);
        //     break;
        // case T_SecLabelStmt:
        //     retval = _equalSecLabelStmt(a, b);
        //     break;
        // case T_FetchStmt:
        //     retval = _equalFetchStmt(a, b);
        //     break;
        // case T_RetrieveStmt:
        //     retval = _equalRetrieveStmt(a, b);
        //     break;
        // case T_IndexStmt:
        //     retval = _equalIndexStmt(a, b);
        //     break;
        // case T_CreateFunctionStmt:
        //     retval = _equalCreateFunctionStmt(a, b);
        //     break;
        // case T_FunctionParameter:
        //     retval = _equalFunctionParameter(a, b);
        //     break;
        // case T_AlterFunctionStmt:
        //     retval = _equalAlterFunctionStmt(a, b);
        //     break;
        // case T_DoStmt:
        //     retval = _equalDoStmt(a, b);
        //     break;
        // case T_RenameStmt:
        //     retval = _equalRenameStmt(a, b);
        //     break;
        // case T_AlterObjectSchemaStmt:
        //     retval = _equalAlterObjectSchemaStmt(a, b);
        //     break;
        // case T_AlterOwnerStmt:
        //     retval = _equalAlterOwnerStmt(a, b);
        //     break;
        // case T_RuleStmt:
        //     retval = _equalRuleStmt(a, b);
        //     break;
        // case T_NotifyStmt:
        //     retval = _equalNotifyStmt(a, b);
        //     break;
        // case T_ListenStmt:
        //     retval = _equalListenStmt(a, b);
        //     break;
        // case T_UnlistenStmt:
        //     retval = _equalUnlistenStmt(a, b);
        //     break;
        // case T_TransactionStmt:
        //     retval = _equalTransactionStmt(a, b);
        //     break;
        // case T_CompositeTypeStmt:
        //     retval = _equalCompositeTypeStmt(a, b);
        //     break;
        // case T_CreateEnumStmt:
        //     retval = _equalCreateEnumStmt(a, b);
        //     break;
        // case T_CreateRangeStmt:
        //     retval = _equalCreateRangeStmt(a, b);
        //     break;
        // case T_AlterEnumStmt:
        //     retval = _equalAlterEnumStmt(a, b);
        //     break;
        // case T_ViewStmt:
        //     retval = _equalViewStmt(a, b);
        //     break;
        // case T_LoadStmt:
        //     retval = _equalLoadStmt(a, b);
        //     break;
        // case T_CreateDomainStmt:
        //     retval = _equalCreateDomainStmt(a, b);
        //     break;
        // case T_CreateOpClassStmt:
        //     retval = _equalCreateOpClassStmt(a, b);
        //     break;
        // case T_CreateOpClassItem:
        //     retval = _equalCreateOpClassItem(a, b);
        //     break;
        // case T_CreateOpFamilyStmt:
        //     retval = _equalCreateOpFamilyStmt(a, b);
        //     break;
        // case T_AlterOpFamilyStmt:
        //     retval = _equalAlterOpFamilyStmt(a, b);
        //     break;
        // case T_CreatedbStmt:
        //     retval = _equalCreatedbStmt(a, b);
        //     break;
        // case T_AlterDatabaseStmt:
        //     retval = _equalAlterDatabaseStmt(a, b);
        //     break;
        // case T_AlterDatabaseSetStmt:
        //     retval = _equalAlterDatabaseSetStmt(a, b);
        //     break;
        // case T_DropdbStmt:
        //     retval = _equalDropdbStmt(a, b);
        //     break;
        // case T_VacuumStmt:
        //     retval = _equalVacuumStmt(a, b);
        //     break;
        // case T_ExplainStmt:
        //     retval = _equalExplainStmt(a, b);
        //     break;
        // case T_CreateTableAsStmt:
        //     retval = _equalCreateTableAsStmt(a, b);
        //     break;
        // case T_RefreshMatViewStmt:
        //     retval = _equalRefreshMatViewStmt(a, b);
        //     break;
        // case T_ReplicaIdentityStmt:
        //     retval = _equalReplicaIdentityStmt(a, b);
        //     break;
        // case T_AlterSystemStmt:
        //     retval = _equalAlterSystemStmt(a, b);
        //     break;
        // case T_CreateSeqStmt:
        //     retval = _equalCreateSeqStmt(a, b);
        //     break;
        // case T_AlterSeqStmt:
        //     retval = _equalAlterSeqStmt(a, b);
        //     break;
        // case T_VariableSetStmt:
        //     retval = _equalVariableSetStmt(a, b);
        //     break;
        // case T_VariableShowStmt:
        //     retval = _equalVariableShowStmt(a, b);
        //     break;
        // case T_DiscardStmt:
        //     retval = _equalDiscardStmt(a, b);
        //     break;
        // case T_CreateTableSpaceStmt:
        //     retval = _equalCreateTableSpaceStmt(a, b);
        //     break;
        // case T_DropTableSpaceStmt:
        //     retval = _equalDropTableSpaceStmt(a, b);
        //     break;
        // case T_AlterTableSpaceOptionsStmt:
        //     retval = _equalAlterTableSpaceOptionsStmt(a, b);
        //     break;
        // case T_AlterTableMoveAllStmt:
        //     retval = _equalAlterTableMoveAllStmt(a, b);
        //     break;
        // case T_CreateExtensionStmt:
        //     retval = _equalCreateExtensionStmt(a, b);
        //     break;
        // case T_AlterExtensionStmt:
        //     retval = _equalAlterExtensionStmt(a, b);
        //     break;
        // case T_AlterExtensionContentsStmt:
        //     retval = _equalAlterExtensionContentsStmt(a, b);
        //     break;
        // case T_CreateFdwStmt:
        //     retval = _equalCreateFdwStmt(a, b);
        //     break;
        // case T_AlterFdwStmt:
        //     retval = _equalAlterFdwStmt(a, b);
        //     break;
        // case T_CreateForeignServerStmt:
        //     retval = _equalCreateForeignServerStmt(a, b);
        //     break;
        // case T_AlterForeignServerStmt:
        //     retval = _equalAlterForeignServerStmt(a, b);
        //     break;
        // case T_CreateUserMappingStmt:
        //     retval = _equalCreateUserMappingStmt(a, b);
        //     break;
        // case T_AlterUserMappingStmt:
        //     retval = _equalAlterUserMappingStmt(a, b);
        //     break;
        // case T_DropUserMappingStmt:
        //     retval = _equalDropUserMappingStmt(a, b);
        //     break;
        // case T_CreateForeignTableStmt:
        //     retval = _equalCreateForeignTableStmt(a, b);
        //     break;
        // case T_CreateTrigStmt:
        //     retval = _equalCreateTrigStmt(a, b);
        //     break;
        // case T_CreateEventTrigStmt:
        //     retval = _equalCreateEventTrigStmt(a, b);
        //     break;
        // case T_AlterEventTrigStmt:
        //     retval = _equalAlterEventTrigStmt(a, b);
        //     break;
        // case T_CreatePLangStmt:
        //     retval = _equalCreatePLangStmt(a, b);
        //     break;
        // case T_CreateRoleStmt:
        //     retval = _equalCreateRoleStmt(a, b);
        //     break;
        // case T_AlterRoleStmt:
        //     retval = _equalAlterRoleStmt(a, b);
        //     break;
        // case T_AlterRoleSetStmt:
        //     retval = _equalAlterRoleSetStmt(a, b);
        //     break;
        // case T_DropRoleStmt:
        //     retval = _equalDropRoleStmt(a, b);
        //     break;
        // case T_LockStmt:
        //     retval = _equalLockStmt(a, b);
        //     break;
        // case T_ConstraintsSetStmt:
        //     retval = _equalConstraintsSetStmt(a, b);
        //     break;
        // case T_ReindexStmt:
        //     retval = _equalReindexStmt(a, b);
        //     break;
        // case T_CheckPointStmt:
        //     retval = true;
        //     break;
        // case T_CreateSchemaStmt:
        //     retval = _equalCreateSchemaStmt(a, b);
        //     break;
        // case T_CreateConversionStmt:
        //     retval = _equalCreateConversionStmt(a, b);
        //     break;
        // case T_CreateCastStmt:
        //     retval = _equalCreateCastStmt(a, b);
        //     break;
        // case T_PrepareStmt:
        //     retval = _equalPrepareStmt(a, b);
        //     break;
        // case T_ExecuteStmt:
        //     retval = _equalExecuteStmt(a, b);
        //     break;
        // case T_DeallocateStmt:
        //     retval = _equalDeallocateStmt(a, b);
        //     break;
        // case T_DropOwnedStmt:
        //     retval = _equalDropOwnedStmt(a, b);
        //     break;
        // case T_ReassignOwnedStmt:
        //     retval = _equalReassignOwnedStmt(a, b);
        //     break;
        // case T_AlterTSDictionaryStmt:
        //     retval = _equalAlterTSDictionaryStmt(a, b);
        //     break;
        // case T_AlterTSConfigurationStmt:
        //     retval = _equalAlterTSConfigurationStmt(a, b);
        //     break;

        // case T_CreateQueueStmt:
        //     retval = _equalCreateQueueStmt(a, b);
        //     break;
        // case T_AlterQueueStmt:
        //     retval = _equalAlterQueueStmt(a, b);
        //     break;
        // case T_DropQueueStmt:
        //     retval = _equalDropQueueStmt(a, b);
        //     break;

        // case T_CreateResourceGroupStmt:
        //     retval = _equalCreateResourceGroupStmt(a, b);
        //     break;
        // case T_DropResourceGroupStmt:
        //     retval = _equalDropResourceGroupStmt(a, b);
        //     break;
        // case T_AlterResourceGroupStmt:
        //     retval = _equalAlterResourceGroupStmt(a, b);
        //     break;

        case T_PGAExpr:
            retval = _equalAExpr(a, b);
            break;
        case T_PGColumnRef:
            retval = _equalColumnRef(a, b);
            break;
        case T_PGParamRef:
            retval = _equalParamRef(a, b);
            break;
        case T_PGAConst:
            retval = _equalAConst(a, b);
            break;
        case T_PGFuncCall:
            retval = _equalFuncCall(a, b);
            break;
        case T_PGAStar:
            retval = _equalAStar(a, b);
            break;
        case T_PGAIndices:
            retval = _equalAIndices(a, b);
            break;
        case T_PGAIndirection:
            retval = _equalA_Indirection(a, b);
            break;
        case T_PGAArrayExpr:
            retval = _equalA_ArrayExpr(a, b);
            break;
        case T_PGResTarget:
            retval = _equalResTarget(a, b);
            break;
        case T_PGTypeCast:
            retval = _equalTypeCast(a, b);
            break;
        // case T_PGCollateClause:
        //     retval = _equalCollateClause(a, b);
        //     break;
        case T_PGSortBy:
            retval = _equalSortBy(a, b);
            break;
        case T_PGWindowDef:
            retval = _equalWindowDef(a, b);
            break;
        case T_PGRangeSubselect:
            retval = _equalRangeSubselect(a, b);
            break;
        case T_PGRangeFunction:
            retval = _equalRangeFunction(a, b);
            break;
        case T_PGTypeName:
            retval = _equalTypeName(a, b);
            break;
        // case T_PGIndexElem:
        //     retval = _equalIndexElem(a, b);
        //     break;
        case T_PGColumnDef:
            retval = _equalColumnDef(a, b);
            break;
        // case T_PGConstraint:
        //     retval = _equalConstraint(a, b);
        //     break;
        // case T_PGDefElem:
        //     retval = _equalDefElem(a, b);
        //     break;
        case T_PGLockingClause:
            retval = _equalLockingClause(a, b);
            break;
        case T_PGRangeTblEntry:
            retval = _equalRangeTblEntry(a, b);
            break;
        case T_PGRangeTblFunction:
            retval = _equalRangeTblFunction(a, b);
            break;
        // case T_WithCheckOption:
        //     retval = _equalWithCheckOption(a, b);
        //     break;
        case T_PGSortGroupClause:
            retval = _equalSortGroupClause(a, b);
            break;
        // case T_PGGroupingClause:
        //     retval = _equalGroupingClause(a, b);
        //     break;
        case T_PGGroupingFunc:
            retval = _equalGroupingFunc(a, b);
            break;
        // case T_PGGrouping:
        //     retval = _equalGrouping(a, b);
        //     break;
        // case T_GroupId:
        //     retval = _equalGroupId(a, b);
        //     break;
        case T_PGWindowClause:
            retval = _equalWindowClause(a, b);
            break;
        // case T_RowMarkClause:
        //     retval = _equalRowMarkClause(a, b);
        //     break;
        case T_PGWithClause:
            retval = _equalWithClause(a, b);
            break;
        case T_PGCommonTableExpr:
            retval = _equalCommonTableExpr(a, b);
            break;
        // case T_PrivGrantee:
        //     retval = _equalPrivGrantee(a, b);
        //     break;
        // case T_FuncWithArgs:
        //     retval = _equalFuncWithArgs(a, b);
        //     break;
        // case T_AccessPriv:
        //     retval = _equalAccessPriv(a, b);
        //     break;
        // case T_XmlSerialize:
        //     retval = _equalXmlSerialize(a, b);
        //     break;
        // case T_TableValueExpr:
        //     retval = _equalTableValueExpr(a, b);
        //     break;
        // case T_DenyLoginInterval:
        //     retval = _equalDenyLoginInterval(a, b);
        //     break;
        // case T_DenyLoginPoint:
        //     retval = _equalDenyLoginPoint(a, b);
        //     break;
        // case T_AlterTypeStmt:
        //     retval = _equalAlterTypeStmt(a, b);
        //     break;
        // case T_DistributedBy:
        //     retval = _equalDistributedBy(a, b);
        //     break;
        // case T_PartitionRule:
        //     retval = _equalPartitionRule(a, b);
        //     break;
        default:
            elog(ERROR, "unrecognized node type: %d", (int)nodeTag(a));
            retval = false; /* keep compiler quiet */
            break;
    }

    return retval;
};

/*
 * Compare two TupleDesc structures for logical equality
 *
 * Note: we deliberately do not check the attrelid and tdtypmod fields.
 * This allows typcache.c to use this routine to see if a cached record type
 * matches a requested type, and is harmless for relcache.c's uses.
 * We don't compare tdrefcount, either.
 */
bool equalTupleDescs(PGTupleDescPtr tupdesc1, PGTupleDescPtr tupdesc2, bool strict)
{
    int i, j, n;

    if (tupdesc1->natts != tupdesc2->natts)
        return false;
    if (strict && tupdesc1->tdtypeid != tupdesc2->tdtypeid)
        return false;
    if (tupdesc1->tdhasoid != tupdesc2->tdhasoid)
        return false;

    for (i = 0; i < tupdesc1->natts; i++)
    {
        PGAttrPtr attr1 = tupdesc1->attrs[i];
        PGAttrPtr attr2 = tupdesc2->attrs[i];

        /*
		 * We do not need to check every single field here: we can disregard
		 * attrelid and attnum (which were used to place the row in the attrs
		 * array in the first place).  It might look like we could dispense
		 * with checking attlen/attbyval/attalign, since these are derived
		 * from atttypid; but in the case of dropped columns we must check
		 * them (since atttypid will be zero for all dropped columns) and in
		 * general it seems safer to check them always.
		 *
		 * attcacheoff must NOT be checked since it's possibly not set in both
		 * copies.
		 */
        if (strcmp(attr1->attname.c_str(), attr2->attname.c_str()) != 0)
            return false;
        if (attr1->atttypid != attr2->atttypid)
            return false;
        if (attr1->attstattarget != attr2->attstattarget)
            return false;
        if (attr1->attlen != attr2->attlen)
            return false;
        if (attr1->attndims != attr2->attndims)
            return false;
        if (attr1->atttypmod != attr2->atttypmod)
            return false;
        if (attr1->attbyval != attr2->attbyval)
            return false;
        if (attr1->attstorage != attr2->attstorage)
            return false;
        if (attr1->attalign != attr2->attalign)
            return false;

        if (strict)
        {
            if (attr1->attnotnull != attr2->attnotnull)
                return false;
            if (attr1->atthasdef != attr2->atthasdef)
                return false;
            if (attr1->attisdropped != attr2->attisdropped)
                return false;
            if (attr1->attislocal != attr2->attislocal)
                return false;
            if (attr1->attinhcount != attr2->attinhcount)
                return false;
            if (attr1->attcollation != attr2->attcollation)
                return false;
            /* attacl and attoptions are not even present... */
        }
    }

    if (!strict)
        return true;

    if (tupdesc1->constr != nullptr)
    {
        PGTupleConstrPtr constr1 = tupdesc1->constr;
        PGTupleConstrPtr constr2 = tupdesc2->constr;

        if (constr2 == NULL)
            return false;
        if (constr1->has_not_null != constr2->has_not_null)
            return false;
        n = constr1->num_defval;
        if (n != (int)constr2->num_defval)
            return false;
        for (i = 0; i < n; i++)
        {
            PGAttrDefaultPtr defval1 = constr1->defval[i];
            PGAttrDefaultPtr defval2 = nullptr;

            /*
			 * We can't assume that the items are always read from the system
			 * catalogs in the same order; so use the adnum field to identify
			 * the matching item to compare.
			 */
            for (j = 0; j < n; j++)
            {
                defval2 = constr2->defval[j];
                if (defval1->adnum == defval2->adnum)
                    break;
            }
            if (j >= n)
                return false;
            if (defval1->adbin != defval2->adbin)
                return false;
        }
        n = constr1->num_check;
        if (n != (int)constr2->num_check)
            return false;
        for (i = 0; i < n; i++)
        {
            PGConstrCheckPtr check1 = constr1->check[i];
            PGConstrCheckPtr check2 = nullptr;

            /*
			 * Similarly, don't assume that the checks are always read in the
			 * same order; match them up by name and contents. (The name
			 * *should* be unique, but...)
			 */
            for (j = 0; j < n; j++)
            {
                check2 = constr2->check[j];
                if (check1->ccname == check2->ccname && check1->ccbin == check2->ccbin
                    && check1->ccvalid == check2->ccvalid && check1->ccnoinherit == check2->ccnoinherit)
                    break;
            }
            if (j >= n)
                return false;
        }
    }
    else if (tupdesc2->constr != NULL)
        return false;
    return true;
};
