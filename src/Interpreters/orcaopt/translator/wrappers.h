#pragma once

#include <Interpreters/orcaopt/parser_common.h>

#define MakeNode(_type_) ((_type_ *) NewNode(sizeof(_type_), T_##_type_))

namespace gpdxl
{

uint32
ListLength(duckdb_libpgquery::PGList *l);

void *
ListNth(duckdb_libpgquery::PGList *list, int n);

duckdb_libpgquery::PGList *
LAppend(duckdb_libpgquery::PGList *list, void *datum);

void
ListFree(duckdb_libpgquery::PGList *list);

}
