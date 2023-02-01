#include <Interpreters/orcaopt/pgopt_hawq/TypeParser.h>

namespace DB
{
using namespace duckdb_libpgquery;

Oid TypeParser::typeidTypeRelid(Oid type_id)
{
    Oid result;
    int fetchCount = 0;

    result = caql_getoid_plus(
        NULL,
        &fetchCount,
        NULL,
        cql("SELECT typrelid FROM pg_type "
            " WHERE oid = :1 ",
            ObjectIdGetDatum(type_id)));

    if (0 == fetchCount)
        elog(ERROR, "cache lookup failed for type %u", type_id);

    return result;
};

Oid TypeParser::LookupTypeName(PGParseState * pstate, const PGTypeName * typname)
{
    Oid restype;

    /* Easy if it's an internally generated TypeName */
    if (typname->names == NIL)
        return typname->typid;

    if (typname->pct_type)
    {
        /* Handle %TYPE reference to type of an existing field */
        PGRangeVar * rel = makeRangeVar(NULL /*catalogname*/, NULL, NULL, typname->location);
        char * field = NULL;
        Oid relid;
        PGAttrNumber attnum;

        /* deconstruct the name list */
        switch (list_length(typname->names))
        {
            case 1:
                ereport(
                    ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                     errmsg("improper %%TYPE reference (too few dotted names): %s", NameListToString(typname->names)),
                     parser_errposition(pstate, typname->location)));
                break;
            case 2:
                rel->relname = strVal(linitial(typname->names));
                field = strVal(lsecond(typname->names));
                break;
            case 3:
                rel->schemaname = strVal(linitial(typname->names));
                rel->relname = strVal(lsecond(typname->names));
                field = strVal(lthird(typname->names));
                break;
            case 4:
                rel->catalogname = strVal(linitial(typname->names));
                rel->schemaname = strVal(lsecond(typname->names));
                rel->relname = strVal(lthird(typname->names));
                field = strVal(lfourth(typname->names));
                break;
            default:
                ereport(
                    ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                     errmsg("improper %%TYPE reference (too many dotted names): %s", NameListToString(typname->names)),
                     parser_errposition(pstate, typname->location)));
                break;
        }

        /* look up the field */
        relid = RangeVarGetRelid(rel, false, false /*allowHcatalog*/);
        attnum = get_attnum(relid, field);
        if (attnum == InvalidAttrNumber)
            ereport(
                ERROR,
                (errcode(ERRCODE_UNDEFINED_COLUMN),
                 errmsg("column \"%s\" of relation \"%s\" does not exist", field, rel->relname),
                 errOmitLocation(true),
                 parser_errposition(pstate, typname->location)));
        restype = get_atttype(relid, attnum);

        /* this construct should never have an array indicator */
        Assert(typname->arrayBounds == NIL);

        /* emit nuisance notice */
        ereport(NOTICE, (errmsg("type reference %s converted to %s", TypeNameToString(typname), format_type_be(restype))));
    }
    else
    {
        /* Normal reference to a type name */
        char * schemaname;
        char * tname;

        /* deconstruct the name list */
        DeconstructQualifiedName(typname->names, &schemaname, &tname);

        /* If an array reference, look up the array type instead */
        if (typname->arrayBounds != NIL)
            tname = makeArrayTypeName(tname);

        if (schemaname)
        {
            /* Look in specific schema only */
            Oid namespaceId;

            namespaceId = LookupExplicitNamespace(schemaname, NSPDBOID_CURRENT);

            restype = caql_getoid(
                NULL,
                cql("SELECT oid FROM pg_type "
                    " WHERE typname = :1 "
                    " AND typnamespace = :2 ",
                    PointerGetDatum(tname),
                    ObjectIdGetDatum(namespaceId)));
        }
        else
        {
            /* Unqualified type name, so search the search path */
            restype = TypenameGetTypid(tname);
        }
    }

    return restype;
};

Oid TypeParser::typenameTypeId(PGParseState * pstate, const PGTypeName * typname)
{
    Oid typoid;

    typoid = LookupTypeName(pstate, typname);
    if (!OidIsValid(typoid))
        ereport(
            ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT),
             errmsg("type \"%s\" does not exist", TypeNameToString(typname)),
             errOmitLocation(true),
             parser_errposition(pstate, typname->location)));

    if (!get_typisdefined(typoid))
        ereport(
            ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT),
             errmsg("type \"%s\" is only a shell", TypeNameToString(typname)),
             errOmitLocation(true),
             parser_errposition(pstate, typname->location)));
    return typoid;
};

}