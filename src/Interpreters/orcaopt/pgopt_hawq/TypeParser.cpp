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

}