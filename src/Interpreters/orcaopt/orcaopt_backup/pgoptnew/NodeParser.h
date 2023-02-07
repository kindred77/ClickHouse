#pragma once

#include <Interpreters/orcaopt/pgoptnew/parser_common.h>
#include <Interpreters/orcaopt/pgoptnew/CoerceParser.h>
#include <Interpreters/orcaopt/pgoptnew/ExprParser.h>
#include <Interpreters/orcaopt/pgoptnew/RelationParser.h>

namespace DB
{

class NodeParser
{
private:
	CoerceParser coerce_parser;
	ExprParser expr_parser;
	RelationParser relation_parser;
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

	Oid
	transformContainerType(Oid *containerType, int32 *containerTypmod);

	// void
	// setup_parser_errposition_callback(PGParseCallbackState *pcbstate,
	// 							  PGParseState *pstate, int location);
	
	// void
	// cancel_parser_errposition_callback(PGParseCallbackState *pcbstate);
	
	duckdb_libpgquery::PGVar *
	make_var(PGParseState *pstate, duckdb_libpgquery::PGRangeTblEntry *rte, int attrno, int location);

	duckdb_libpgquery::PGConst *
	make_const(PGParseState *pstate, duckdb_libpgquery::PGValue *value, int location);

	// void parser_errposition(PGParseState *pstate, int location);
};

}