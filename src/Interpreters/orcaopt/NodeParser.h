#pragma once

#include <Interpreters/orcaopt/parser_common.h>

namespace DB
{
class CoerceParser;
class ExprParser;
class RelationParser;
class TypeProvider;

using CoerceParserPtr = std::unique_ptr<CoerceParser>;
using ExprParserPtr = std::unique_ptr<ExprParser>;
using RelationParserPtr = std::unique_ptr<RelationParser>;
using TypeProviderPtr = std::unique_ptr<TypeProvider>;

class NodeParser
{
private:
	CoerceParserPtr coerce_parser;
	ExprParserPtr expr_parser;
	RelationParserPtr relation_parser;
	TypeProviderPtr type_provider;
public:
	explicit NodeParser();

	//Oid
	//transformContainerType(Oid *containerType, int32 *containerTypmod);

	Oid transformArrayType(Oid *arrayType, int32 *arrayTypmod);

    duckdb_libpgquery::PGArrayRef * transformArraySubscripts(
        PGParseState * pstate, duckdb_libpgquery::PGNode * arrayBase, Oid arrayType,
		Oid elementType, int32 arrayTypMod, duckdb_libpgquery::PGList * indirection,
		duckdb_libpgquery::PGNode * assignFrom);

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
