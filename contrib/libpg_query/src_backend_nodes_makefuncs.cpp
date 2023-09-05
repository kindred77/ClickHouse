/*--------------------------------------------------------------------
 * Symbols referenced in this file:
 * - makeDefElem
 * - makeTypeNameFromNameList
 * - makeDefElemExtended
 * - makeAlias
 * - makeSimpleAExpr
 * - makeGroupingSet
 * - makeTypeName
 * - makeFuncCall
 * - makeAExpr
 * - makeRangeVar
 * - makeBoolExpr
 *--------------------------------------------------------------------
 */

/*-------------------------------------------------------------------------
 *
 * makefuncs.c
 *	  creator functions for primitive nodes. The functions here are for
 *	  the most frequently created nodes.
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development PGGroup
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/nodes/makefuncs.c
 *
 *-------------------------------------------------------------------------
 */
#include "pg_functions.hpp"
#include "fmgr.hpp"
#include "nodes/makefuncs.hpp"
#include "nodes/nodeFuncs.hpp"
#include "common/common_datum.hpp"
#include "common/common_macro.hpp"

namespace duckdb_libpgquery {

/*
 * makeAExpr -
 *		makes an PGAExpr node
 */
PGAExpr *makeAExpr(PGAExpr_Kind kind, PGList *name, PGNode *lexpr, PGNode *rexpr, int location) {
	PGAExpr *a = makeNode(PGAExpr);

	a->kind = kind;
	a->name = name;
	a->lexpr = lexpr;
	a->rexpr = rexpr;
	a->location = location;
	return a;
}

/*
 * makeSimpleAExpr -
 *		As above, given a simple (unqualified) operator name
 */
PGAExpr *makeSimpleAExpr(PGAExpr_Kind kind, const char *name, PGNode *lexpr, PGNode *rexpr, int location) {
	PGAExpr *a = makeNode(PGAExpr);

	a->kind = kind;
	a->name = list_make1(makeString((char *)name));
	a->lexpr = lexpr;
	a->rexpr = rexpr;
	a->location = location;
	return a;
}

/*
 * makeVar -
 *	  creates a PGVar node
 */
PGVar *makeVar(PGIndex varno, PGAttrNumber varattno, PGOid vartype, int32_t vartypmod, PGOid varcollid,
               PGIndex varlevelsup)
{
	PGVar *var = makeNode(PGVar);

	var->varno = varno;
	var->varattno = varattno;
	var->vartype = vartype;
	var->vartypmod = vartypmod;
	var->varcollid = varcollid;
	var->varlevelsup = varlevelsup;

	/*
	 * Since few if any routines ever create Var nodes with varnoold/varoattno
	 * different from varno/varattno, we don't provide separate arguments for
	 * them, but just initialize them to the given varno/varattno. This
	 * reduces code clutter and chance of error for most callers.
	 */
	var->varnoold = varno;
	var->varoattno = varattno;

	/* Likewise, we just set location to "unknown" here */
	var->location = -1;

	return var;
}

/*
 * makeVarFromTargetEntry -
 *		convenience function to create a same-level PGVar node from a
 *		PGTargetEntry
 */

/*
 * makeWholeRowVar -
 *	  creates a PGVar node representing a whole row of the specified RTE
 *
 * A whole-row reference is a PGVar with varno set to the correct range
 * table entry, and varattno == 0 to signal that it references the whole
 * tuple.  (Use of zero here is unclean, since it could easily be confused
 * with error cases, but it's not worth changing now.)  The vartype indicates
 * a rowtype; either a named composite type, or RECORD.  This function
 * encapsulates the logic for determining the correct rowtype OID to use.
 *
 * If allowScalar is true, then for the case where the RTE is a single function
 * returning a non-composite result type, we produce a normal PGVar referencing
 * the function's result directly, instead of the single-column composite
 * value that the whole-row notation might otherwise suggest.
 */

PGVar *makeWholeRowVar(PGRangeTblEntry *rte, PGIndex varno, PGIndex varlevelsup, bool allowScalar, PGOid relTypeOid, bool isRowType)
{
	PGVar		   *result;
	PGOid			toid;
	PGNode	   *fexpr;

	switch (rte->rtekind)
	{
		case PG_RTE_RELATION:
			/* relation: the rowtype is a named composite type */
			//toid = get_rel_type_id(rte->relid);
			toid = relTypeOid;
			if (!OidIsValid(toid))
				elog(ERROR, "could not find type OID for relation %u",
					 rte->relid);
			result = makeVar(varno,
							 InvalidAttrNumber,
							 toid,
							 -1,
							 InvalidOid,
							 varlevelsup);
			break;
		//case PG_RTE_TABLEFUNCTION:
		case PG_RTE_FUNCTION:

			/*
			 * If there's more than one function, or ordinality is requested,
			 * force a RECORD result, since there's certainly more than one
			 * column involved and it can't be a known named type.
			 */
			if (rte->funcordinality || list_length(rte->functions) != 1)
			{
				/* always produces an anonymous RECORD result */
				result = makeVar(varno,
								 InvalidAttrNumber,
								 RECORDOID,
								 -1,
								 InvalidOid,
								 varlevelsup);
				break;
			}

			fexpr = ((PGRangeTblFunction *) linitial(rte->functions))->funcexpr;
			toid = exprType(fexpr);
			//if (type_is_rowtype(toid))
			if (isRowType)
			{
				/* func returns composite; same as relation case */
				result = makeVar(varno,
								 InvalidAttrNumber,
								 toid,
								 -1,
								 InvalidOid,
								 varlevelsup);
			}
			else if (allowScalar)
			{
				/* func returns scalar; just return its output as-is */
				result = makeVar(varno,
								 1,
								 toid,
								 -1,
								 exprCollation(fexpr),
								 varlevelsup);
			}
			else
			{
				/* func returns scalar, but we want a composite result */
				result = makeVar(varno,
								 InvalidAttrNumber,
								 RECORDOID,
								 -1,
								 InvalidOid,
								 varlevelsup);
			}
			break;

		default:

			/*
			 * RTE is a join, subselect, or VALUES.  We represent this as a
			 * whole-row Var of RECORD type. (Note that in most cases the Var
			 * will be expanded to a RowExpr during planning, but that is not
			 * our concern here.)
			 */
			result = makeVar(varno,
							 InvalidAttrNumber,
							 RECORDOID,
							 -1,
							 InvalidOid,
							 varlevelsup);
			break;
	}

	return result;
}

/*
 * makeTargetEntry -
 *	  creates a PGTargetEntry node
 */
PGTargetEntry *makeTargetEntry(PGExpr *expr, PGAttrNumber resno, char *resname, bool resjunk)
{
	PGTargetEntry *tle = makeNode(PGTargetEntry);
	
	tle->expr = expr;
	tle->resno = resno;
	tle->resname = resname;

	/*
	 * We always set these fields to 0. If the caller wants to change them he
	 * must do so explicitly.  Few callers do that, so omitting these
	 * arguments reduces the chance of error.
	 */
	tle->ressortgroupref = 0;
	tle->resorigtbl = InvalidOid;
	tle->resorigcol = 0;

	tle->resjunk = resjunk;

	return tle;
}

/*
 * flatCopyTargetEntry -
 *	  duplicate a PGTargetEntry, but don't copy substructure
 *
 * This is commonly used when we just want to modify the resno or substitute
 * a new expression.
 */

/*
 * makeFromExpr -
 *	  creates a FromExpr node
 */
PGFromExpr *
makeFromExpr(PGList *fromlist, PGNode *quals)
{
	PGFromExpr   *f = makeNode(PGFromExpr);

	f->fromlist = fromlist;
	f->quals = quals;
	return f;
}

/*
 * makeConst -
 *	  creates a PGConst node
 */
PGConst *makeConst(PGOid consttype, int32_t consttypmod, PGOid constcollid, int constlen, PGDatum constvalue,
                   bool constisnull, bool constbyval)
{
	PGConst	   *cnst = makeNode(PGConst);

	cnst->consttype = consttype;
	cnst->consttypmod = consttypmod;
	cnst->constcollid = constcollid;
	cnst->constlen = constlen;
	cnst->constvalue = constvalue;
	cnst->constisnull = constisnull;
	cnst->constbyval = constbyval;
	cnst->location = -1;		/* "unknown" */

	return cnst;
}

/*
 * makeNullConst -
 *	  creates a Const node representing a NULL of the specified type/typmod
 *
 * This is a convenience routine that just saves a lookup of the type's
 * storage properties.
 */
PGConst *makeNullConst(int16_t typLen, bool typByVal, PGOid consttype, int32_t consttypmod, PGOid constcollid)
{
	// int16_t		typLen;
	// bool		typByVal;

	// get_typlenbyval(consttype, &typLen, &typByVal);
	return makeConst(consttype,
					 consttypmod,
					 constcollid,
					 (int) typLen,
					 (PGDatum) 0,
					 true,
					 typByVal);
}

/*
 * makeBoolConst -
 *	  creates a PGConst node representing a boolean value (can be NULL too)
 */

PGNode *makeBoolConst(bool value, bool isnull)
{
	/* note that pg_type.h hardwires size of bool as 1 ... duplicate it */
	return (PGNode *) makeConst(BOOLOID, -1, InvalidOid, 1,
							  BoolGetDatum(value), isnull, true);
}

/*
 * makeBoolExpr -
 *	  creates a PGBoolExpr node
 */
PGExpr *makeBoolExpr(PGBoolExprType boolop, PGList *args, int location) {
	PGBoolExpr *b = makeNode(PGBoolExpr);

	b->boolop = boolop;
	b->args = args;
	b->location = location;

	return (PGExpr *)b;
}

/*
 * makeAlias -
 *	  creates an PGAlias node
 *
 * NOTE: the given name is copied, but the colnames list (if any) isn't.
 */
PGAlias *makeAlias(const char *aliasname, PGList *colnames) {
	PGAlias *a = makeNode(PGAlias);

	a->aliasname = pstrdup(aliasname);
	a->colnames = colnames;

	return a;
}

/*
 * makeRelabelType -
 *	  creates a RelabelType node
 */
PGRelabelType *makeRelabelType(PGExpr *arg, PGOid rtype, int32_t rtypmod, PGOid rcollid, PGCoercionForm rformat)
{
	PGRelabelType *r = makeNode(PGRelabelType);

	r->arg = arg;
	r->resulttype = rtype;
	r->resulttypmod = rtypmod;
	r->resultcollid = rcollid;
	r->relabelformat = rformat;
	r->location = -1;

	return r;
}

/*
 * makeRangeVar -
 *	  creates a PGRangeVar node (rather oversimplified case)
 */
PGRangeVar *makeRangeVar(char *schemaname, char *relname, int location) {
	PGRangeVar *r = makeNode(PGRangeVar);

	r->catalogname = NULL;
	r->schemaname = schemaname;
	r->relname = relname;
	r->inh = true;
	r->relpersistence = RELPERSISTENCE_PERMANENT;
	r->alias = NULL;
	r->location = location;
	r->sample = NULL;

	return r;
}

/*
 * makeTypeName -
 *	build a PGTypeName node for an unqualified name.
 *
 * typmod is defaulted, but can be changed later by caller.
 */
PGTypeName *makeTypeName(char *typnam) {
	return makeTypeNameFromNameList(list_make1(makeString(typnam)));
}

/*
 * makeTypeNameFromNameList -
 *	build a PGTypeName node for a String list representing a qualified name.
 *
 * typmod is defaulted, but can be changed later by caller.
 */
PGTypeName *makeTypeNameFromNameList(PGList *names) {
	PGTypeName *n = makeNode(PGTypeName);

	n->names = names;
	n->typmods = NIL;
	n->typemod = -1;
	n->location = -1;
	return n;
}

/*
 * makeTypeNameFromOid -
 *	build a PGTypeName node to represent a type already known by OID/typmod.
 */

/*
 * makeColumnDef -
 *	build a PGColumnDef node to represent a simple column definition.
 *
 * Type and collation are specified by OID.
 * Other properties are all basic to start with.
 */

/*
 * makeFuncExpr -
 *	build an expression tree representing a function call.
 *
 * The argument expressions must have been transformed already.
 */
PGFuncExpr *makeFuncExpr(PGOid funcid, PGOid rettype, PGList *args, PGOid funccollid, PGOid inputcollid,
                         PGCoercionForm fformat)
{
	PGFuncExpr   *funcexpr;

	funcexpr = makeNode(PGFuncExpr);
	funcexpr->funcid = funcid;
	funcexpr->funcresulttype = rettype;
	funcexpr->funcretset = false;		/* only allowed case here */
	funcexpr->funcvariadic = false;		/* only allowed case here */
	funcexpr->funcformat = fformat;
	funcexpr->funccollid = funccollid;
	funcexpr->inputcollid = inputcollid;
	funcexpr->args = args;
	funcexpr->location = -1;

	return funcexpr;
}

/*
 * makeDefElem -
 *	build a PGDefElem node
 *
 * This is sufficient for the "typical" case with an unqualified option name
 * and no special action.
 */
PGDefElem *makeDefElem(const char *name, PGNode *arg, int location) {
	PGDefElem *res = makeNode(PGDefElem);

	res->defnamespace = NULL;
	res->defname = (char *)name;
	res->arg = arg;
	res->defaction = PG_DEFELEM_UNSPEC;
	res->location = location;

	return res;
}

/*
 * makeDefElemExtended -
 *	build a PGDefElem node with all fields available to be specified
 */
PGDefElem *makeDefElemExtended(const char *nameSpace, const char *name, PGNode *arg, PGDefElemAction defaction,
                               int location) {
	PGDefElem *res = makeNode(PGDefElem);

	res->defnamespace = (char *)nameSpace;
	res->defname = (char *)name;
	res->arg = arg;
	res->defaction = defaction;
	res->location = location;

	return res;
}

/*
 * makeFuncCall -
 *
 * Initialize a PGFuncCall struct with the information every caller must
 * supply.  Any non-default parameters have to be inserted by the caller.
 */
PGFuncCall *makeFuncCall(PGList *name, PGList *args, int location) {
	PGFuncCall *n = makeNode(PGFuncCall);

	n->funcname = name;
	n->args = args;
	n->agg_order = NIL;
	n->agg_filter = NULL;
	n->agg_within_group = false;
	n->agg_star = false;
	n->agg_distinct = false;
	n->func_variadic = false;
	n->over = NULL;
	n->location = location;
	return n;
}

/*
 * makeGroupingSet
 *
 */
PGGroupingSet *makeGroupingSet(GroupingSetKind kind, PGList *content, int location) {
	PGGroupingSet *n = makeNode(PGGroupingSet);

	n->kind = kind;
	n->content = content;
	n->location = location;
	return n;
}
}