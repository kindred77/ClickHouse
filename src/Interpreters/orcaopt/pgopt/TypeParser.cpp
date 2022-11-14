#include <Interpreters/orcaopt/pgopt/TypeParser.h>

namespace DB
{
using namespace duckdb_libpgquery;

void
TypeParser::typenameTypeIdAndMod(PGParseState *pstate, const PGTypeName *typeName,
					 Oid *typeid_p, int32 *typmod_p)
{
    HeapTuple		tup;

	tup = typenameType(pstate, typeName, typmod_p);
	*typeid_p = HeapTupleGetOid(tup);
	ReleaseSysCache(tup);
};

int32
TypeParser::typenameTypeMod(PGParseState *pstate, const PGTypeName *typeName, HeapTuple typ)
{
    int32		result;
	Oid			typmodin;
	PGDatum	   *datums;
	int			n;
	PGListCell   *l;
	ArrayType  *arrtypmod;
	ParseCallbackState pcbstate;

	/* Return prespecified typmod if no typmod expressions */
	if (typeName->typmods == NIL)
		return typeName->typemod;

	/*
	 * Else, type had better accept typmods.  We give a special error message
	 * for the shell-type case, since a shell couldn't possibly have a
	 * typmodin function.
	 */
	if (!((Form_pg_type) GETSTRUCT(typ))->typisdefined)
		ereport(ERROR,
				(errcode(PG_ERRCODE_SYNTAX_ERROR),
			errmsg("type modifier cannot be specified for shell type \"%s\"",
				   TypeNameToString(typeName)),
				 node_parser.parser_errposition(pstate, typeName->location)));

	typmodin = ((Form_pg_type) GETSTRUCT(typ))->typmodin;

	if (typmodin == InvalidOid)
		ereport(ERROR,
				(errcode(PG_ERRCODE_SYNTAX_ERROR),
				 errmsg("type modifier is not allowed for type \"%s\"",
						TypeNameToString(typeName)),
				 node_parser.parser_errposition(pstate, typeName->location)));

	/*
	 * Convert the list of raw-grammar-output expressions to a cstring array.
	 * Currently, we allow simple numeric constants, string literals, and
	 * identifiers; possibly this list could be extended.
	 */
	datums = (PGDatum *) palloc(list_length(typeName->typmods) * sizeof(PGDatum));
	n = 0;
	foreach(l, typeName->typmods)
	{
		PGNode	   *tm = (PGNode *) lfirst(l);
		char	   *cstr = NULL;

		if (IsA(tm, PGAConst))
		{
			PGAConst    *ac = (PGAConst *) tm;

			if (IsA(&ac->val, PGInteger))
			{
				cstr = psprintf("%ld", (long) ac->val.val.ival);
			}
			else if (IsA(&ac->val, PGFloat) ||
					 IsA(&ac->val, PGString))
			{
				/* we can just use the str field directly. */
				cstr = ac->val.val.str;
			}
		}
		else if (IsA(tm, PGColumnRef))
		{
			PGColumnRef  *cr = (PGColumnRef *) tm;

			if (list_length(cr->fields) == 1 &&
				IsA(linitial(cr->fields), PGString))
				cstr = strVal(linitial(cr->fields));
		}
		if (!cstr)
			ereport(ERROR,
					(errcode(PG_ERRCODE_SYNTAX_ERROR),
			errmsg("type modifiers must be simple constants or identifiers"),
					 node_parser.parser_errposition(pstate, typeName->location)));
		datums[n++] = CStringGetDatum(cstr);
	}

	/* hardwired knowledge about cstring's representation details here */
	arrtypmod = construct_array(datums, n, CSTRINGOID,
								-2, false, 'c');

	/* arrange to report location if type's typmodin function fails */
	setup_parser_errposition_callback(&pcbstate, pstate, typeName->location);

	result = DatumGetInt32(OidFunctionCall1(typmodin,
											PointerGetDatum(arrtypmod)));

	cancel_parser_errposition_callback(&pcbstate);

	pfree(datums);
	pfree(arrtypmod);

	return result;
};

Oid
TypeParser::typeidTypeRelid(Oid type_id)
{
	HeapTuple	typeTuple;
	Form_pg_type type;
	Oid			result;

	typeTuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(type_id));
	if (!HeapTupleIsValid(typeTuple))
		elog(ERROR, "cache lookup failed for type %u", type_id);

	type = (Form_pg_type) GETSTRUCT(typeTuple);
	result = type->typrelid;
	ReleaseSysCache(typeTuple);
	return result;
};

Oid
TypeParser::LookupCollation(PGParseState *pstate, PGList *collnames, int location)
{
	Oid			colloid;
	ParseCallbackState pcbstate;

	if (pstate)
		setup_parser_errposition_callback(&pcbstate, pstate, location);

	colloid = get_collation_oid(collnames, false);

	if (pstate)
		cancel_parser_errposition_callback(&pcbstate);

	return colloid;
};

Type
TypeParser::LookupTypeNameExtended(PGParseState *pstate,
					   const PGTypeName *typeName, int32 *typmod_p,
					   bool temp_ok, bool missing_ok)
{
	Oid			typoid;
	HeapTuple	tup;
	int32		typmod;

	if (typeName->names == NIL)
	{
		/* We have the OID already if it's an internally generated TypeName */
		typoid = typeName->typeOid;
	}
	else if (typeName->pct_type)
	{
		/* Handle %TYPE reference to type of an existing field */
		PGRangeVar   *rel = makeRangeVar(NULL, NULL, typeName->location);
		char	   *field = NULL;
		Oid			relid;
		PGAttrNumber	attnum;

		/* deconstruct the name list */
		switch (list_length(typeName->names))
		{
			case 1:
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
				errmsg("improper %%TYPE reference (too few dotted names): %s",
					   NameListToString(typeName->names)),
						 node_parser.parser_errposition(pstate, typeName->location)));
				break;
			case 2:
				rel->relname = strVal(linitial(typeName->names));
				field = strVal(lsecond(typeName->names));
				break;
			case 3:
				rel->schemaname = strVal(linitial(typeName->names));
				rel->relname = strVal(lsecond(typeName->names));
				field = strVal(lthird(typeName->names));
				break;
			case 4:
				rel->catalogname = strVal(linitial(typeName->names));
				rel->schemaname = strVal(lsecond(typeName->names));
				rel->relname = strVal(lthird(typeName->names));
				field = strVal(lfourth(typeName->names));
				break;
			default:
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("improper %%TYPE reference (too many dotted names): %s",
								NameListToString(typeName->names)),
						 node_parser.parser_errposition(pstate, typeName->location)));
				break;
		}

		/*
		 * Look up the field.
		 *
		 * XXX: As no lock is taken here, this might fail in the presence of
		 * concurrent DDL.  But taking a lock would carry a performance
		 * penalty and would also require a permissions check.
		 */
		relid = RangeVarGetRelid(rel, NoLock, missing_ok);
		attnum = get_attnum(relid, field);
		if (attnum == InvalidAttrNumber)
		{
			if (missing_ok)
				typoid = InvalidOid;
			else
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_COLUMN),
					errmsg("column \"%s\" of relation \"%s\" does not exist",
						   field, rel->relname),
						 node_parser.parser_errposition(pstate, typeName->location)));
		}
		else
		{
			typoid = get_atttype(relid, attnum);

			/* this construct should never have an array indicator */
			Assert(typeName->arrayBounds == NIL);

			/* emit nuisance notice (intentionally not errposition'd) */
			ereport(NOTICE,
					(errmsg("type reference %s converted to %s",
							TypeNameToString(typeName),
							format_type_be(typoid))));
		}
	}
	else
	{
		/* Normal reference to a type name */
		char	   *schemaname;
		char	   *typname;

		/* deconstruct the name list */
		DeconstructQualifiedName(typeName->names, &schemaname, &typname);

		if (schemaname)
		{
			/* Look in specific schema only */
			Oid			namespaceId;

			namespaceId = LookupExplicitNamespace(schemaname, missing_ok);
			if (OidIsValid(namespaceId))
				typoid = GetSysCacheOid2(TYPENAMENSP,
										 PointerGetDatum(typname),
										 ObjectIdGetDatum(namespaceId));
			else
				typoid = InvalidOid;
		}
		else
		{
			/* Unqualified type name, so search the search path */
			typoid = TypenameGetTypidExtended(typname, temp_ok);
		}

		/* If an array reference, return the array type instead */
		if (typeName->arrayBounds != NIL)
			typoid = get_array_type(typoid);
	}

	if (!OidIsValid(typoid))
	{
		if (typmod_p)
			*typmod_p = -1;
		return NULL;
	}

	tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typoid));
	if (!HeapTupleIsValid(tup)) /* should not happen */
		elog(ERROR, "cache lookup failed for type %u", typoid);

	typmod = typenameTypeMod(pstate, typeName, (Type) tup);

	if (typmod_p)
		*typmod_p = typmod;

	return (Type) tup;
};

}