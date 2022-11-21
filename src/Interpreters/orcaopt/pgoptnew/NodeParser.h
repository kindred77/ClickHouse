#pragma once

#include <Interpreters/orcaopt/pgoptnew/parser_common.h>
#include <Interpreters/orcaopt/pgoptnew/CoerceParser.h>
#include <Interpreters/orcaopt/pgoptnew/ExprParser.h>

namespace DB
{

class NodeParser
{
private:
	CoerceParser coerce_parser;
	ExprParser expr_parser;
public:
	explicit NodeParser();

	SubscriptingRef *
	transformContainerSubscripts(PGParseState *pstate,
							 duckdb_libpgquery::PGNode *containerBase,
							 Oid containerType,
							 Oid elementType,
							 int32 containerTypMod,
							 duckdb_libpgquery::PGList *indirection,
							 duckdb_libpgquery::PGNode *assignFrom);
};

}