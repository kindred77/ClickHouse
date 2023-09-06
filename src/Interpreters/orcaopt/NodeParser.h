#pragma once

#include <common/parser_common.hpp>

namespace DB
{
class CoerceParser;
class ExprParser;
class RelationParser;
class TypeProvider;

using CoerceParserPtr = std::shared_ptr<CoerceParser>;
using ExprParserPtr = std::shared_ptr<ExprParser>;
using RelationParserPtr = std::shared_ptr<RelationParser>;
// using TypeProviderPtr = std::shared_ptr<TypeProvider>;

class Context;
using ContextPtr = std::shared_ptr<const Context>;

class NodeParser
{
private:
	CoerceParserPtr coerce_parser;
	ExprParserPtr expr_parser;
	RelationParserPtr relation_parser;
	// TypeProviderPtr type_provider;

	ContextPtr context;
public:
	explicit NodeParser(const ContextPtr& context_);

	//Oid
	//transformContainerType(Oid *containerType, int32 *containerTypmod);

	duckdb_libpgquery::PGOid transformArrayType(duckdb_libpgquery::PGOid *arrayType, duckdb_libpgquery::int32 *arrayTypmod);

    duckdb_libpgquery::PGArrayRef * transformArraySubscripts(
        duckdb_libpgquery::PGParseState * pstate, duckdb_libpgquery::PGNode * arrayBase, duckdb_libpgquery::PGOid arrayType,
		duckdb_libpgquery::PGOid elementType, duckdb_libpgquery::int32 arrayTypMod, duckdb_libpgquery::PGList * indirection,
		duckdb_libpgquery::PGNode * assignFrom);

    // void
	// setup_parser_errposition_callback(PGParseCallbackState *pcbstate,
	// 							  PGParseState *pstate, int location);
	
	// void
	// cancel_parser_errposition_callback(PGParseCallbackState *pcbstate);
	
	duckdb_libpgquery::PGVar *
	make_var(duckdb_libpgquery::PGParseState *pstate, duckdb_libpgquery::PGRangeTblEntry *rte, int attrno, int location);

	duckdb_libpgquery::PGConst *
	make_const(duckdb_libpgquery::PGParseState *pstate, duckdb_libpgquery::PGValue *value, int location);

	// void parser_errposition(PGParseState *pstate, int location);
};

}
