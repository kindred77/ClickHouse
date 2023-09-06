#include <Interpreters/orcaopt/TypeParser.h>

#include <Interpreters/orcaopt/RelationParser.h>
#include <Interpreters/orcaopt/NodeParser.h>
#include <Interpreters/orcaopt/provider/TypeProvider.h>
#include <Interpreters/orcaopt/provider/RelationProvider.h>
#include <Interpreters/orcaopt/provider/FunctionProvider.h>

#include <Interpreters/Context.h>

using namespace duckdb_libpgquery;

namespace DB
{

// TypeParser::TypeParser(const ContextPtr& context_) : context(context_)
// {
//     relation_parser = std::make_shared<RelationParser>(context);
//     node_parser = std::make_shared<NodeParser>(context);
// };

void
TypeParser::typenameTypeIdAndMod(PGParseState *pstate, const PGTypeName *typeName,
					 PGOid *typeid_p, int32 *typmod_p)
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

		result += std::string(TypeProvider::format_type_be(typeName->typeOid));
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
    PGOid typoid;
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
        PGOid relid;
        PGAttrNumber attnum;

        /* deconstruct the name list */
        switch (list_length(typeName->names))
        {
            case 1:
                ereport(
                    ERROR,
                    (errcode(PG_ERRCODE_SYNTAX_ERROR),
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
                    (errcode(PG_ERRCODE_SYNTAX_ERROR),
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
        relid = RelationProvider::RangeVarGetRelidExtended(rel, NoLock, missing_ok, false, NULL, NULL);
        attnum = RelationProvider::get_attnum(relid, field);
        if (attnum == InvalidAttrNumber)
        {
            if (missing_ok)
                typoid = InvalidOid;
            else
                ereport(
                    ERROR,
                    (errcode(PG_ERRCODE_UNDEFINED_COLUMN),
                     errmsg("column \"%s\" of relation \"%s\" does not exist", field, rel->relname),
                     parser_errposition(pstate, typeName->location)));
        }
        else
        {
            typoid = RelationProvider::get_atttype(relid, attnum);

            /* this construct should never have an array indicator */
            Assert(typeName->arrayBounds == NIL)

            /* emit nuisance notice (intentionally not errposition'd) */
            ereport(NOTICE, (errmsg("type reference %s converted to %s", TypeNameToString(typeName).c_str(), TypeProvider::format_type_be(typoid).c_str())));
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
            PGOid namespaceId;

            namespaceId = RelationProvider::LookupExplicitNamespace(schemaname, missing_ok);
            if (OidIsValid(namespaceId))
			{
				typoid = TypeProvider::get_typeoid_by_typename_namespaceoid(typname, namespaceId);
			}
            else
                typoid = InvalidOid;
        }
        else
        {
            /* Unqualified type name, so search the search path */
            typoid = TypeProvider::TypenameGetTypidExtended(typname, temp_ok);
        }

        /* If an array reference, return the array type instead */
        if (typeName->arrayBounds != NIL)
            typoid = TypeProvider::get_array_type(typoid);
    }

    if (!OidIsValid(typoid))
    {
        if (typmod_p)
            *typmod_p = -1;
        return NULL;
    }

    PGTypePtr tup = TypeProvider::getTypeByOid(typoid);
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
    int32 result = -1;
    PGOid typmodin;
    Datum * datums;
    int n;
    PGListCell * l;
    ArrayType * arrtypmod;
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
    {
        ereport(
            ERROR,
            (errcode(PG_ERRCODE_SYNTAX_ERROR),
             errmsg("type modifier cannot be specified for shell type \"%s\"", TypeNameToString(typeName).c_str()),
             parser_errposition(pstate, typeName->location)));
        
        return InvalidOid;
    }

    typmodin = typ->typmodin;

    if (typmodin == InvalidOid)
    {
        ereport(
            ERROR,
            (errcode(PG_ERRCODE_SYNTAX_ERROR),
             errmsg("type modifier is not allowed for type \"%s\"", TypeNameToString(typeName).c_str()),
             parser_errposition(pstate, typeName->location)));
        
        return InvalidOid;
    }

    //TODO kindred
    elog(ERROR, "extracting type modifier from typmods not supported yet!");

    /*
	 * Convert the list of raw-grammar-output expressions to a cstring array.
	 * Currently, we allow simple numeric constants, string literals, and
	 * identifiers; possibly this list could be extended.
	 */
    // datums = (Datum *)palloc(list_length(typeName->typmods) * sizeof(Datum));
    // n = 0;
    // foreach (l, typeName->typmods)
    // {
    //     PGNode * tm = (PGNode *)lfirst(l);
    //     char * cstr = NULL;

    //     if (IsA(tm, PGAConst))
    //     {
    //         PGAConst * ac = (PGAConst *)tm;

    //         if (IsA(&ac->val, PGInteger))
    //         {
    //             cstr = psprintf("%ld", (long)ac->val.val.ival);
    //         }
    //         else if (IsA(&ac->val, PGFloat) || IsA(&ac->val, PGString))
    //         {
    //             /* we can just use the str field directly. */
    //             cstr = ac->val.val.str;
    //         }
    //     }
    //     else if (IsA(tm, PGColumnRef))
    //     {
    //         PGColumnRef * cr = (PGColumnRef *)tm;

    //         if (list_length(cr->fields) == 1 && IsA(linitial(cr->fields), PGString))
    //             cstr = strVal(linitial(cr->fields));
    //     }
    //     if (!cstr)
    //         ereport(
    //             ERROR,
    //             (errcode(ERRCODE_SYNTAX_ERROR),
    //              errmsg("type modifiers must be simple constants or identifiers"),
    //              parser_errposition(pstate, typeName->location)));
    //     datums[n++] = CStringGetDatum(cstr);
    // }

    // /* hardwired knowledge about cstring's representation details here */
	// arrtypmod = construct_array(datums, n, CSTRINGOID,
	// 							-2, false, 'c');

	// /* arrange to report location if type's typmodin function fails */
	// setup_parser_errposition_callback(&pcbstate, pstate, typeName->location);

	// result = DatumGetInt32(FunctionProvider::OidFunctionCall1Coll(typmodin,InvalidOid,
	// 										PointerGetDatum(arrtypmod)));

	// cancel_parser_errposition_callback(&pcbstate);

	// pfree(datums);
	// pfree(arrtypmod);

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
				(errcode(PG_ERRCODE_UNDEFINED_OBJECT),
				 errmsg("type \"%s\" does not exist",
						TypeNameToString(typeName).c_str())));
	}
	if (!tup->typisdefined)
	{
		parser_errposition(pstate, typeName->location);
		ereport(ERROR,
				(errcode(PG_ERRCODE_UNDEFINED_OBJECT),
				 errmsg("type \"%s\" is only a shell",
						TypeNameToString(typeName).c_str())));
	}
	return tup;
};

PGTypePtr
TypeParser::typeidType(PGOid id)
{
	PGTypePtr tup = TypeProvider::getTypeByOid(id);
	if (tup == NULL)
		elog(ERROR, "cache lookup failed for type %u", id);
	return tup;
};

PGOid TypeParser::typeidTypeRelid(PGOid type_id)
{
    PGOid result;

	PGTypePtr typeTuple = TypeProvider::getTypeByOid(type_id);
    if (typeTuple == NULL)
        elog(ERROR, "cache lookup failed for type %u", type_id);

    result = typeTuple->typrelid;
    return result;
};

PGOid
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
TypeParser::stringTypeDatum(PGTypePtr tp, const char *str, int32 atttypmod)
{
    //TODO kindred
    elog(ERROR, "OidInputFunctionCall is not implemented yet!");

	PGOid			typinput = tp->typinput;
	PGOid			typioparam = TypeProvider::getTypeIOParam(tp);

	return FunctionProvider::OidInputFunctionCall(typinput, str, typioparam, atttypmod);
};

PGOid
TypeParser::typeOrDomainTypeRelid(PGOid type_id)
{
	PGTypePtr typeTuple = nullptr;
	PGOid			result;

	for (;;)
	{
		typeTuple = TypeProvider::getTypeByOid(type_id);
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

PGOid
TypeParser::typeTypeRelid(PGTypePtr typ)
{
	return typ->typrelid;
};

PGOid
TypeParser::typeTypeId(PGTypePtr tp)
{
	if (tp == nullptr)				/* probably useless */
		elog(ERROR, "typeTypeId() called with NULL type struct");
	return tp->oid;
};

PGOid
TypeParser::LookupCollation(PGParseState *pstate, PGList *collnames, int location)
{
	PGOid			colloid = InvalidOid;
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

PGOid TypeParser::typenameTypeId(PGParseState *pstate, const PGTypeName *typeName)
{
    PGOid typoid;

    PGTypePtr tup = typenameType(pstate, typeName, NULL);
    typoid = tup->oid;

    return typoid;
};

}
