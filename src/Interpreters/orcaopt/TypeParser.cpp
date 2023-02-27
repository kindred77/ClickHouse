#include <Interpreters/orcaopt/TypeParser.h>

#include <Interpreters/orcaopt/RelationParser.h>
#include <Interpreters/orcaopt/NodeParser.h>
#include <Interpreters/orcaopt/provider/TypeProvider.h>
#include <Interpreters/orcaopt/provider/RelationProvider.h>
#include <Interpreters/orcaopt/provider/FunctionProvider.h>

#ifdef __clang__
#pragma clang diagnostic ignored "-Wsometimes-uninitialized"
#pragma clang diagnostic ignored "-Wunused-parameter"
#pragma clang diagnostic ignored "-Wunused-variable"
#else
#pragma GCC diagnostic ignored "-Wsometimes-uninitialized"
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wunused-variable"
#endif

using namespace duckdb_libpgquery;

namespace DB
{

void
TypeParser::typenameTypeIdAndMod(PGParseState *pstate, const PGTypeName *typeName,
					 Oid *typeid_p, int32 *typmod_p)
{
    PGTypePtr		tup;

	tup = typenameType(pstate, typeName, typmod_p);
	*typeid_p = tup->oid;
};

std::string
TypeParser::TypeNameToString(const PGTypeName *typeName)
{
    //StringInfoData string;

	//initStringInfo(&string);
	//appendTypeNameToBuffer(typeName, &string);

	std::string result = "";

	if (typeName->names != NIL)
	{
		/* Emit possibly-qualified name as-is */
		PGListCell   *l;

		foreach(l, typeName->names)
		{
			if (l != list_head(typeName->names))
			{
				//appendStringInfoChar(string, '.');
				result += ".";
			}
			//appendStringInfoString(string, strVal(lfirst(l)));
			result += std::string(strVal(lfirst(l)));
		}
	}
	else
	{
		/* Look up internally-specified type */
		//appendStringInfoString(string, format_type_be(typeName->typeOid));

		result += std::string(type_provider->format_type_be(typeName->typeOid));
	}

	/*
	 * Add decoration as needed, but only for fields considered by
	 * LookupTypeName
	 */
	if (typeName->pct_type)
	{
		//appendStringInfoString(string, "%TYPE");
		result += "%TYPE";
	}

	if (typeName->arrayBounds != NIL)
	{
		//appendStringInfoString(string, "[]");
		result += "[]";
	}

	return result;
};

PGTypePtr TypeParser::LookupTypeName(PGParseState * pstate, const duckdb_libpgquery::PGTypeName * typeName,
        int32 * typmod_p, bool missing_ok)
{
    return LookupTypeNameExtended(pstate, typeName, typmod_p, true, missing_ok);
};

PGTypePtr TypeParser::LookupTypeNameExtended(PGParseState * pstate, const PGTypeName * typeName,
        int32 * typmod_p, bool temp_ok, bool missing_ok)
{
    Oid typoid;
    int32 typmod;

    if (typeName->names == NIL)
    {
        /* We have the OID already if it's an internally generated TypeName */
        typoid = typeName->typeOid;
    }
    else if (typeName->pct_type)
    {
        /* Handle %TYPE reference to type of an existing field */
        PGRangeVar * rel = makeRangeVar(NULL, NULL, typeName->location);
        char * field = NULL;
        Oid relid;
        PGAttrNumber attnum;

        /* deconstruct the name list */
        switch (list_length(typeName->names))
        {
            case 1:
                ereport(
                    ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                     errmsg("improper %%TYPE reference (too few dotted names): %s", NameListToString(typeName->names)),
                     parser_errposition(pstate, typeName->location)));
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
                ereport(
                    ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                     errmsg("improper %%TYPE reference (too many dotted names): %s", NameListToString(typeName->names)),
                     parser_errposition(pstate, typeName->location)));
                break;
        }

        /*
		 * Look up the field.
		 *
		 * XXX: As no lock is taken here, this might fail in the presence of
		 * concurrent DDL.  But taking a lock would carry a performance
		 * penalty and would also require a permissions check.
		 */
        relid = relation_provider->RangeVarGetRelidExtended(rel, NoLock, missing_ok, false, NULL, NULL);
        attnum = relation_provider->get_attnum(relid, field);
        if (attnum == InvalidAttrNumber)
        {
            if (missing_ok)
                typoid = InvalidOid;
            else
                ereport(
                    ERROR,
                    (errcode(ERRCODE_UNDEFINED_COLUMN),
                     errmsg("column \"%s\" of relation \"%s\" does not exist", field, rel->relname),
                     parser_errposition(pstate, typeName->location)));
        }
        else
        {
            typoid = relation_provider->get_atttype(relid, attnum);

            /* this construct should never have an array indicator */
            Assert(typeName->arrayBounds == NIL)

            /* emit nuisance notice (intentionally not errposition'd) */
            ereport(NOTICE, (errmsg("type reference %s converted to %s", TypeNameToString(typeName).c_str(), type_provider->format_type_be(typoid))));
        }
    }
    else
    {
        /* Normal reference to a type name */
        char * schemaname;
        char * typname;

        /* deconstruct the name list */
        DeconstructQualifiedName(typeName->names, &schemaname, &typname);

        if (schemaname)
        {
            /* Look in specific schema only */
            Oid namespaceId;

            namespaceId = relation_provider->LookupExplicitNamespace(schemaname, missing_ok);
            if (OidIsValid(namespaceId))
			{
				typoid = type_provider->get_typeoid_by_typename_namespaceoid(typname, namespaceId);
			}
            else
                typoid = InvalidOid;
        }
        else
        {
            /* Unqualified type name, so search the search path */
            typoid = type_provider->TypenameGetTypidExtended(typname, temp_ok);
        }

        /* If an array reference, return the array type instead */
        if (typeName->arrayBounds != NIL)
            typoid = type_provider->get_array_type(typoid);
    }

    if (!OidIsValid(typoid))
    {
        if (typmod_p)
            *typmod_p = -1;
        return NULL;
    }

    PGTypePtr tup = type_provider->getTypeByOid(typoid);
    if (tup == NULL) /* should not happen */
        elog(ERROR, "cache lookup failed for type %u", typoid);

    typmod = typenameTypeMod(pstate, typeName, tup);

    if (typmod_p)
        *typmod_p = typmod;

    return tup;
};

int32
TypeParser::typenameTypeMod(PGParseState *pstate, const PGTypeName *typeName, PGTypePtr typ)
{
    int32 result;
    Oid typmodin;
    Datum * datums;
    int n;
    PGListCell * l;
    //ArrayType * arrtypmod;
    PGParseCallbackState pcbstate;

    /* Return prespecified typmod if no typmod expressions */
    if (typeName->typmods == NIL)
        return typeName->typemod;

    /*
	 * Else, type had better accept typmods.  We give a special error message
	 * for the shell-type case, since a shell couldn't possibly have a
	 * typmodin function.
	 */
    if (!typ->typisdefined)
        ereport(
            ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR),
             errmsg("type modifier cannot be specified for shell type \"%s\"", TypeNameToString(typeName).c_str()),
             parser_errposition(pstate, typeName->location)));

    typmodin = typ->typmodin;

    if (typmodin == InvalidOid)
        ereport(
            ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR),
             errmsg("type modifier is not allowed for type \"%s\"", TypeNameToString(typeName).c_str()),
             parser_errposition(pstate, typeName->location)));

    /*
	 * Convert the list of raw-grammar-output expressions to a cstring array.
	 * Currently, we allow simple numeric constants, string literals, and
	 * identifiers; possibly this list could be extended.
	 */
    datums = (Datum *)palloc(list_length(typeName->typmods) * sizeof(Datum));
    n = 0;
    foreach (l, typeName->typmods)
    {
        PGNode * tm = (PGNode *)lfirst(l);
        char * cstr = NULL;

        if (IsA(tm, PGAConst))
        {
            PGAConst * ac = (PGAConst *)tm;

            if (IsA(&ac->val, PGInteger))
            {
                cstr = psprintf("%ld", (long)ac->val.val.ival);
            }
            else if (IsA(&ac->val, PGFloat) || IsA(&ac->val, PGString))
            {
                /* we can just use the str field directly. */
                cstr = ac->val.val.str;
            }
        }
        else if (IsA(tm, PGColumnRef))
        {
            PGColumnRef * cr = (PGColumnRef *)tm;

            if (list_length(cr->fields) == 1 && IsA(linitial(cr->fields), PGString))
                cstr = strVal(linitial(cr->fields));
        }
        if (!cstr)
            ereport(
                ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                 errmsg("type modifiers must be simple constants or identifiers"),
                 parser_errposition(pstate, typeName->location)));
        datums[n++] = CStringGetDatum(cstr);
    }

    //TODO kindred
    // /* hardwired knowledge about cstring's representation details here */
    // arrtypmod = construct_array(datums, n, CSTRINGOID, -2, false, 'c');

    // /* arrange to report location if type's typmodin function fails */
    // setup_parser_errposition_callback(&pcbstate, pstate, typeName->location);

    // result = DatumGetInt32(OidFunctionCall1(typmodin, PointerGetDatum(arrtypmod)));

    // cancel_parser_errposition_callback(&pcbstate);

    result = DatumGetInt32(function_provider->OidFunctionCall1_DatumArr(typmodin, datums));

    pfree(datums);
    //pfree(arrtypmod);

    return result;
};

PGTypePtr
TypeParser::typenameType(PGParseState *pstate, const PGTypeName *typeName, int32 *typmod_p)
{
	PGTypePtr tup = LookupTypeName(pstate, typeName, typmod_p, false);
	if (tup == NULL)
	{
		parser_errposition(pstate, typeName->location);
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("type \"%s\" does not exist",
						TypeNameToString(typeName).c_str())));
	}
	if (!tup->typisdefined)
	{
		parser_errposition(pstate, typeName->location);
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("type \"%s\" is only a shell",
						TypeNameToString(typeName).c_str())));
	}
	return tup;
};

PGTypePtr
TypeParser::typeidType(Oid id)
{
	PGTypePtr tup = type_provider->getTypeByOid(id);
	if (tup == NULL)
		elog(ERROR, "cache lookup failed for type %u", id);
	return tup;
};

Oid TypeParser::typeidTypeRelid(Oid type_id)
{
    Oid result;

	PGTypePtr typeTuple = type_provider->getTypeByOid(type_id);
    if (typeTuple == NULL)
        elog(ERROR, "cache lookup failed for type %u", type_id);

    result = typeTuple->typrelid;
    return result;
};

Oid
TypeParser::typeTypeCollation(PGTypePtr typ)
{
	return typ->typcollation;
};

int16
TypeParser::typeLen(PGTypePtr t)
{
	return t->typlen;
};

bool
TypeParser::typeByVal(PGTypePtr t)
{
	return t->typbyval;
};

Datum
TypeParser::stringTypeDatum(PGTypePtr tp, const char *string, int32 atttypmod)
{
	Oid			typinput = tp->typinput;
	Oid			typioparam = type_provider->getTypeIOParam(tp);

	return function_provider->OidInputFunctionCall(typinput, string, typioparam, atttypmod);
};

Oid
TypeParser::typeOrDomainTypeRelid(Oid type_id)
{
	PGTypePtr typeTuple = nullptr;
	Oid			result;

	for (;;)
	{
		typeTuple = type_provider->getTypeByOid(type_id);
		if (typeTuple == nullptr)
			elog(ERROR, "cache lookup failed for type %u", type_id);
		if (typeTuple->typtype != TYPTYPE_DOMAIN)
		{
			/* Not a domain, so done looking through domains */
			break;
		}
		/* It is a domain, so examine the base type instead */
		type_id = typeTuple->typbasetype;
	}
	result = typeTuple->typrelid;
	return result;
};

Oid
TypeParser::typeTypeRelid(PGTypePtr typ)
{
	return typ->typrelid;
};

Oid
TypeParser::typeTypeId(PGTypePtr tp)
{
	if (tp == nullptr)				/* probably useless */
		elog(ERROR, "typeTypeId() called with NULL type struct");
	return tp->oid;
};

Oid
TypeParser::LookupCollation(PGParseState *pstate, PGList *collnames, int location)
{
	Oid			colloid = InvalidOid;
    //TODO kindred
    elog(ERROR, "Collation lookuping do not supported yet!");
	// PGParseCallbackState pcbstate;

	// if (pstate)
	// 	setup_parser_errposition_callback(&pcbstate, pstate, location);

	// colloid = get_collation_oid(collnames, false);

	// if (pstate)
	// 	cancel_parser_errposition_callback(&pcbstate);

	return colloid;
};

std::string TypeParser::format_type_with_typemod(Oid type_oid, int32 typemod)
{
	return format_type_internal(type_oid, typemod, true, false, false);
};

std::string TypeParser::printTypmod(const char * typname, int32 typmod, Oid typmodout)
{
    std::string res = "";

    /* Shouldn't be called if typmod is -1 */
    Assert(typmod >= 0)

    if (typmodout == InvalidOid)
    {
        /* Default behavior: just print the integer typmod with parens */
        //res = psprintf("%s(%d)", typname, (int)typmod);
        res = std::string(typname) + "(" + std::to_string(typmod) + ")";
    }
    else
    {
        /* Use the type-specific typmodout procedure */
        char * tmstr;

        tmstr = DatumGetCString(function_provider->OidFunctionCall1Coll(typmodout, InvalidOid, Int32GetDatum(typmod)));
        //res = psprintf("%s%s", typname, tmstr);
        res = std::string(typname) + std::string(tmstr);
    }

    return res;
};

std::string TypeParser::format_type_internal(Oid type_oid, int32 typemod, bool typemod_given, bool allow_invalid, bool force_qualify)
{
    bool with_typemod = typemod_given && (typemod >= 0);
    Oid array_base_type;
    bool is_array;
    std::string buf = "";

    if (type_oid == InvalidOid && allow_invalid)
    {
        return "-";
    }

	PGTypePtr tuple = type_provider->getTypeByOid(type_oid);
    if (tuple == NULL)
    {
        if (allow_invalid)
            return "???";
        else
            elog(ERROR, "cache lookup failed for type %u", type_oid);
    }

    /*
	 * Check if it's a regular (variable length) array type.  Fixed-length
	 * array types such as "name" shouldn't get deconstructed.  As of Postgres
	 * 8.1, rather than checking typlen we check the toast property, and don't
	 * deconstruct "plain storage" array types --- this is because we don't
	 * want to show oidvector as oid[].
	 */
    array_base_type = tuple->typelem;

    if (array_base_type != InvalidOid && tuple->typstorage != 'p')
    {
        /* Switch our attention to the array element type */
		tuple = type_provider->getTypeByOid(array_base_type);
        if (tuple == NULL)
        {
            if (allow_invalid)
                return "???[]";
            else
                elog(ERROR, "cache lookup failed for type %u", type_oid);
        }
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
    //buf = NULL; /* flag for no special case */

    switch (type_oid)
    {
        case BITOID:
            if (with_typemod)
                buf = printTypmod("bit", typemod, tuple->typmodout);
            else if (typemod_given)
            {
                /*
				 * bit with typmod -1 is not the same as BIT, which means
				 * BIT(1) per SQL spec.  Report it as the quoted typename so
				 * that parser will not assign a bogus typmod.
				 */
            }
            else
                buf = "bit";
            break;

        case BOOLOID:
            buf = "boolean";
            break;

        case BPCHAROID:
            if (with_typemod)
                buf = printTypmod("character", typemod, tuple->typmodout);
            else if (typemod_given)
            {
                /*
				 * bpchar with typmod -1 is not the same as CHARACTER, which
				 * means CHARACTER(1) per SQL spec.  Report it as bpchar so
				 * that parser will not assign a bogus typmod.
				 */
            }
            else
                buf = "character";
            break;

        case FLOAT4OID:
            buf = "real";
            break;

        case FLOAT8OID:
            buf = "double precision";
            break;

        case INT2OID:
            buf = "smallint";
            break;

        case INT4OID:
            buf = "integer";
            break;

        case INT8OID:
            buf = "bigint";
            break;

        case NUMERICOID:
            if (with_typemod)
                buf = printTypmod("numeric", typemod, tuple->typmodout);
            else
                buf = "numeric";
            break;

        case INTERVALOID:
            if (with_typemod)
                buf = printTypmod("interval", typemod, tuple->typmodout);
            else
                buf = "interval";
            break;

        case TIMEOID:
            if (with_typemod)
                buf = printTypmod("time", typemod, tuple->typmodout);
            else
                buf = "time without time zone";
            break;

        case TIMETZOID:
            if (with_typemod)
                buf = printTypmod("time", typemod, tuple->typmodout);
            else
                buf = "time with time zone";
            break;

        case TIMESTAMPOID:
            if (with_typemod)
                buf = printTypmod("timestamp", typemod, tuple->typmodout);
            else
                buf = "timestamp without time zone";
            break;

        case TIMESTAMPTZOID:
            if (with_typemod)
                buf = printTypmod("timestamp", typemod, tuple->typmodout);
            else
                buf = "timestamp with time zone";
            break;

        case VARBITOID:
            if (with_typemod)
                buf = printTypmod("bit varying", typemod, tuple->typmodout);
            else
                buf = "bit varying";
            break;

        case VARCHAROID:
            if (with_typemod)
                buf = printTypmod("character varying", typemod, tuple->typmodout);
            else
                buf = "character varying";
            break;
    }

    if (buf == "")
    {
        /*
		 * Default handling: report the name as it appears in the catalog.
		 * Here, we must qualify the name if it is not visible in the search
		 * path, and we must double-quote it if it's not a standard identifier
		 * or if it matches any keyword.
		 */
        std::string nspname;
        std::string typname;

        if (!force_qualify && type_provider->TypeIsVisible(type_oid))
            nspname = "";
        else
            nspname = relation_provider->get_namespace_name(tuple->typnamespace);

        typname = tuple->typname;

		//TODO kindred
        // buf = quote_qualified_identifier(nspname, typname);
		buf = pstrdup((std::string(nspname) + "." + std::string(typname)).c_str());

        if (with_typemod)
            buf = printTypmod(buf.c_str(), typemod, tuple->typmodout);
    }

    if (is_array)
    {
        //buf = psprintf("%s[]", buf);
        buf = buf + "[]";
    }

    return buf;
};

Oid TypeParser::typenameTypeId(PGParseState *pstate, const PGTypeName *typeName)
{
    Oid typoid;

    PGTypePtr tup = typenameType(pstate, typeName, NULL);
    typoid = tup->oid;

    return typoid;
};

}
