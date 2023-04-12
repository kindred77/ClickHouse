#include <Interpreters/orcaopt/QueryRewriter.h>

#ifdef __clang__
#pragma clang diagnostic ignored "-Wunused-parameter"
#else
#pragma GCC diagnostic ignored "-Wunused-parameter"
#endif

using namespace duckdb_libpgquery;

namespace DB
{

PGList * QueryRewriter::rewrite(PGQuery *parsetree)
{
    return NULL;
};

};
