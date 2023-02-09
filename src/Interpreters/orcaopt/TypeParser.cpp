#include <Interpreters/orcaopt/TypeParser.h>

#include <Interpreters/orcaopt/RelationParser.h>
#include <Interpreters/orcaopt/NodeParser.h>

namespace DB
{
using namespace duckdb_libpgquery;

void
TypeParser::typenameTypeIdAndMod(PGParseState *pstate, const PGTypeName *typeName,
					 Oid *typeid_p, int32 *typmod_p)
{
    Type		tup;

	tup = typenameType(pstate, typeName, typmod_p);
	*typeid_p = ((Form_pg_type) GETSTRUCT(tup))->oid;
	ReleaseSysCache(tup);
};

char *
TypeParser::TypeNameToString(const PGTypeName *typeName)
{
    StringInfoData string;

	initStringInfo(&string);
	appendTypeNameToBuffer(typeName, &string);
	return string.data;
};

Type
TypeParser::LookupTypeName(PGParseState *pstate, const PGTypeName *typeName,
			   int32 *typmod_p, bool missing_ok)
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
				node_parser.parser_errposition(pstate, typeName->location);
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("improper %%TYPE reference (too few dotted names): %s",
								NameListToString(typeName->names))));
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
				node_parser.parser_errposition(pstate, typeName->location);
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("improper %%TYPE reference (too many dotted names): %s",
								NameListToString(typeName->names))));
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
			{
				node_parser.parser_errposition(pstate, typeName->location);
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_COLUMN),
						 errmsg("column \"%s\" of relation \"%s\" does not exist",
								field, rel->relname)));
			}
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
			PGParseCallbackState pcbstate;

			node_parser.setup_parser_errposition_callback(&pcbstate, pstate, typeName->location);

			namespaceId = LookupExplicitNamespace(schemaname, missing_ok);
			if (OidIsValid(namespaceId))
				typoid = GetSysCacheOid2(TYPENAMENSP, Anum_pg_type_oid,
										 PointerGetDatum(typname),
										 ObjectIdGetDatum(namespaceId));
			else
				typoid = InvalidOid;

			node_parser.cancel_parser_errposition_callback(&pcbstate);
		}
		else
		{
			/* Unqualified type name, so search the search path */
			typoid = TypenameGetTypid(typname);
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

int32
TypeParser::typenameTypeMod(PGParseState *pstate, const PGTypeName *typeName, Type typ)
{
    int32		result;
	Oid			typmodin;
	Datum	   *datums;
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
	{
		node_parser.parser_errposition(pstate, typeName->location);
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("type modifier cannot be specified for shell type \"%s\"",
						TypeNameToString(typeName))));
	}

	typmodin = ((Form_pg_type) GETSTRUCT(typ))->typmodin;

	if (typmodin == InvalidOid)
	{
		node_parser.parser_errposition(pstate, typeName->location);
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("type modifier is not allowed for type \"%s\"",
						TypeNameToString(typeName))));
	}

	/*
	 * Convert the list of raw-grammar-output expressions to a cstring array.
	 * Currently, we allow simple numeric constants, string literals, and
	 * identifiers; possibly this list could be extended.
	 */
	datums = (Datum *) palloc(list_length(typeName->typmods) * sizeof(Datum));
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
		{
			node_parser.parser_errposition(pstate, typeName->location);
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("type modifiers must be simple constants or identifiers")));
		}
		datums[n++] = CStringGetDatum(cstr);
	}

	/* hardwired knowledge about cstring's representation details here */
	arrtypmod = construct_array(datums, n, CSTRINGOID,
								-2, false, 'c');

	/* arrange to report location if type's typmodin function fails */
	node_parser.setup_parser_errposition_callback(&pcbstate, pstate, typeName->location);

	result = DatumGetInt32(OidFunctionCall1(typmodin,
											PointerGetDatum(arrtypmod)));

	node_parser.cancel_parser_errposition_callback(&pcbstate);

	pfree(datums);
	pfree(arrtypmod);

	return result;
};

Type
TypeParser::typenameType(PGParseState *pstate, const PGTypeName *typeName, int32 *typmod_p)
{
    Type		tup;

	tup = LookupTypeName(pstate, typeName, typmod_p, false);
	if (tup == NULL)
	{
		node_parser.parser_errposition(pstate, typeName->location);
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("type \"%s\" does not exist",
						TypeNameToString(typeName))));
	}
	if (!((Form_pg_type) GETSTRUCT(tup))->typisdefined)
	{
		node_parser.parser_errposition(pstate, typeName->location);
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("type \"%s\" is only a shell",
						TypeNameToString(typeName))));
	}
	return tup;
};

Type
TypeParser::typeidType(Oid id)
{
	HeapTuple	tup;

	tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(id));
	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for type %u", id);
	return (Type) tup;
};

Oid
TypeParser::typeTypeCollation(Type typ)
{
	Form_pg_type typtup;

	typtup = (Form_pg_type) GETSTRUCT(typ);
	return typtup->typcollation;
};

int16
TypeParser::typeLen(Type t)
{
	Form_pg_type typ;

	typ = (Form_pg_type) GETSTRUCT(t);
	return typ->typlen;
};

bool
TypeParser::typeByVal(Type t)
{
	Form_pg_type typ;

	typ = (Form_pg_type) GETSTRUCT(t);
	return typ->typbyval;
};

Datum
TypeParser::stringTypeDatum(Type tp, char *string, int32 atttypmod)
{
	Form_pg_type typform = (Form_pg_type) GETSTRUCT(tp);
	Oid			typinput = typform->typinput;
	Oid			typioparam = getTypeIOParam(tp);

	return OidInputFunctionCall(typinput, string, typioparam, atttypmod);
};

Oid
TypeParser::typeOrDomainTypeRelid(Oid type_id)
{
	HeapTuple	typeTuple;
	Form_pg_type type;
	Oid			result;

	for (;;)
	{
		typeTuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(type_id));
		if (!HeapTupleIsValid(typeTuple))
			elog(ERROR, "cache lookup failed for type %u", type_id);
		type = (Form_pg_type) GETSTRUCT(typeTuple);
		if (type->typtype != TYPTYPE_DOMAIN)
		{
			/* Not a domain, so done looking through domains */
			break;
		}
		/* It is a domain, so examine the base type instead */
		type_id = type->typbasetype;
		ReleaseSysCache(typeTuple);
	}
	result = type->typrelid;
	ReleaseSysCache(typeTuple);
	return result;
};

Oid
TypeParser::typeTypeRelid(Type typ)
{
	Form_pg_type typtup;

	typtup = (Form_pg_type) GETSTRUCT(typ);
	return typtup->typrelid;
};

Oid
TypeParser::typeTypeId(Type tp)
{
	if (tp == NULL)				/* probably useless */
		elog(ERROR, "typeTypeId() called with NULL type struct");
	return ((Form_pg_type) GETSTRUCT(tp))->oid;
};

Oid
TypeParser::LookupCollation(PGParseState *pstate, duckdb_libpgquery::PGList *collnames, int location)
{
	Oid			colloid;
	PGParseCallbackState pcbstate;

	if (pstate)
		node_parser.setup_parser_errposition_callback(&pcbstate, pstate, location);

	colloid = get_collation_oid(collnames, false);

	if (pstate)
		node_parser.cancel_parser_errposition_callback(&pcbstate);

	return colloid;
};

}
