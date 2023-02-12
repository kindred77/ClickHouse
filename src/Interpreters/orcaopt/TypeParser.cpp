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
    PGTypePtr		tup;

	tup = typenameType(pstate, typeName, typmod_p);
	*typeid_p = tup->oid;
};

char *
TypeParser::TypeNameToString(const PGTypeName *typeName)
{
    StringInfoData string;

	initStringInfo(&string);
	appendTypeNameToBuffer(typeName, &string);
	return string.data;
};

PGTypePtr
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
				parser_errposition(pstate, typeName->location);
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
				parser_errposition(pstate, typeName->location);
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
				parser_errposition(pstate, typeName->location);
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
			Assert(typeName->arrayBounds == NIL)

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

			setup_parser_errposition_callback(&pcbstate, pstate, typeName->location);

			namespaceId = LookupExplicitNamespace(schemaname, missing_ok);
			if (OidIsValid(namespaceId))
				typoid = GetSysCacheOid2(TYPENAMENSP, Anum_pg_type_oid,
										 PointerGetDatum(typname),
										 ObjectIdGetDatum(namespaceId));
			else
				typoid = InvalidOid;

			cancel_parser_errposition_callback(&pcbstate);
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
		parser_errposition(pstate, typeName->location);
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("type modifier cannot be specified for shell type \"%s\"",
						TypeNameToString(typeName))));
	}

	typmodin = ((Form_pg_type) GETSTRUCT(typ))->typmodin;

	if (typmodin == InvalidOid)
	{
		parser_errposition(pstate, typeName->location);
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
			parser_errposition(pstate, typeName->location);
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
	setup_parser_errposition_callback(&pcbstate, pstate, typeName->location);

	result = DatumGetInt32(OidFunctionCall1(typmodin,
											PointerGetDatum(arrtypmod)));

	cancel_parser_errposition_callback(&pcbstate);

	pfree(datums);
	pfree(arrtypmod);

	return result;
};

PGTypePtr
TypeParser::typenameType(PGParseState *pstate, const PGTypeName *typeName, int32 *typmod_p)
{
    Type		tup;

	tup = LookupTypeName(pstate, typeName, typmod_p, false);
	if (tup == NULL)
	{
		parser_errposition(pstate, typeName->location);
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("type \"%s\" does not exist",
						TypeNameToString(typeName))));
	}
	if (!((Form_pg_type) GETSTRUCT(tup))->typisdefined)
	{
		parser_errposition(pstate, typeName->location);
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("type \"%s\" is only a shell",
						TypeNameToString(typeName))));
	}
	return tup;
};

PGTypePtr
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
TypeParser::stringTypeDatum(Type tp, const char *string, int32 atttypmod)
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
		setup_parser_errposition_callback(&pcbstate, pstate, location);

	colloid = get_collation_oid(collnames, false);

	if (pstate)
		cancel_parser_errposition_callback(&pcbstate);

	return colloid;
};

char * TypeParser::format_type_with_typemod(Oid type_oid, int32 typemod)
{
	return format_type_internal(type_oid, typemod, true, false, false);
};

char * TypeParser::format_type_internal(Oid type_oid, int32 typemod, bool typemod_given, bool allow_invalid, bool force_qualify)
{
    bool with_typemod = typemod_given && (typemod >= 0);
    HeapTuple tuple;
    Form_pg_type typeform;
    Oid array_base_type;
    bool is_array;
    char * buf;

    if (type_oid == InvalidOid && allow_invalid)
        return pstrdup("-");

    tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(type_oid));
    if (!HeapTupleIsValid(tuple))
    {
        if (allow_invalid)
            return pstrdup("???");
        else
            elog(ERROR, "cache lookup failed for type %u", type_oid);
    }
    typeform = (Form_pg_type)GETSTRUCT(tuple);

    /*
	 * Check if it's a regular (variable length) array type.  Fixed-length
	 * array types such as "name" shouldn't get deconstructed.  As of Postgres
	 * 8.1, rather than checking typlen we check the toast property, and don't
	 * deconstruct "plain storage" array types --- this is because we don't
	 * want to show oidvector as oid[].
	 */
    array_base_type = typeform->typelem;

    if (array_base_type != InvalidOid && typeform->typstorage != 'p')
    {
        /* Switch our attention to the array element type */
        ReleaseSysCache(tuple);
        tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(array_base_type));
        if (!HeapTupleIsValid(tuple))
        {
            if (allow_invalid)
                return pstrdup("???[]");
            else
                elog(ERROR, "cache lookup failed for type %u", type_oid);
        }
        typeform = (Form_pg_type)GETSTRUCT(tuple);
        type_oid = array_base_type;
        is_array = true;
    }
    else
        is_array = false;

    /*
	 * See if we want to special-case the output for certain built-in types.
	 * Note that these special cases should all correspond to special
	 * productions in gram.y, to ensure that the type name will be taken as a
	 * system type, not a user type of the same name.
	 *
	 * If we do not provide a special-case output here, the type name will be
	 * handled the same way as a user type name --- in particular, it will be
	 * double-quoted if it matches any lexer keyword.  This behavior is
	 * essential for some cases, such as types "bit" and "char".
	 */
    buf = NULL; /* flag for no special case */

    switch (type_oid)
    {
        case BITOID:
            if (with_typemod)
                buf = printTypmod("bit", typemod, typeform->typmodout);
            else if (typemod_given)
            {
                /*
				 * bit with typmod -1 is not the same as BIT, which means
				 * BIT(1) per SQL spec.  Report it as the quoted typename so
				 * that parser will not assign a bogus typmod.
				 */
            }
            else
                buf = pstrdup("bit");
            break;

        case BOOLOID:
            buf = pstrdup("boolean");
            break;

        case BPCHAROID:
            if (with_typemod)
                buf = printTypmod("character", typemod, typeform->typmodout);
            else if (typemod_given)
            {
                /*
				 * bpchar with typmod -1 is not the same as CHARACTER, which
				 * means CHARACTER(1) per SQL spec.  Report it as bpchar so
				 * that parser will not assign a bogus typmod.
				 */
            }
            else
                buf = pstrdup("character");
            break;

        case FLOAT4OID:
            buf = pstrdup("real");
            break;

        case FLOAT8OID:
            buf = pstrdup("double precision");
            break;

        case INT2OID:
            buf = pstrdup("smallint");
            break;

        case INT4OID:
            buf = pstrdup("integer");
            break;

        case INT8OID:
            buf = pstrdup("bigint");
            break;

        case NUMERICOID:
            if (with_typemod)
                buf = printTypmod("numeric", typemod, typeform->typmodout);
            else
                buf = pstrdup("numeric");
            break;

        case INTERVALOID:
            if (with_typemod)
                buf = printTypmod("interval", typemod, typeform->typmodout);
            else
                buf = pstrdup("interval");
            break;

        case TIMEOID:
            if (with_typemod)
                buf = printTypmod("time", typemod, typeform->typmodout);
            else
                buf = pstrdup("time without time zone");
            break;

        case TIMETZOID:
            if (with_typemod)
                buf = printTypmod("time", typemod, typeform->typmodout);
            else
                buf = pstrdup("time with time zone");
            break;

        case TIMESTAMPOID:
            if (with_typemod)
                buf = printTypmod("timestamp", typemod, typeform->typmodout);
            else
                buf = pstrdup("timestamp without time zone");
            break;

        case TIMESTAMPTZOID:
            if (with_typemod)
                buf = printTypmod("timestamp", typemod, typeform->typmodout);
            else
                buf = pstrdup("timestamp with time zone");
            break;

        case VARBITOID:
            if (with_typemod)
                buf = printTypmod("bit varying", typemod, typeform->typmodout);
            else
                buf = pstrdup("bit varying");
            break;

        case VARCHAROID:
            if (with_typemod)
                buf = printTypmod("character varying", typemod, typeform->typmodout);
            else
                buf = pstrdup("character varying");
            break;
    }

    if (buf == NULL)
    {
        /*
		 * Default handling: report the name as it appears in the catalog.
		 * Here, we must qualify the name if it is not visible in the search
		 * path, and we must double-quote it if it's not a standard identifier
		 * or if it matches any keyword.
		 */
        char * nspname;
        char * typname;

        if (!force_qualify && TypeIsVisible(type_oid))
            nspname = NULL;
        else
            nspname = get_namespace_name(typeform->typnamespace);

        typname = NameStr(typeform->typname);

        buf = quote_qualified_identifier(nspname, typname);

        if (with_typemod)
            buf = printTypmod(buf, typemod, typeform->typmodout);
    }

    if (is_array)
        buf = psprintf("%s[]", buf);

    ReleaseSysCache(tuple);

    return buf;
};

}
