#pragma once

#include <Interpreters/orcaopt/parser_common.h>

namespace gpdxl
{

uint32
ListLength(duckdb_libpgquery::PGList *l);

void *
ListNth(duckdb_libpgquery::PGList *list, int n);

}
