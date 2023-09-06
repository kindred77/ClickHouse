#pragma once

#include <common/parser_common.hpp>

namespace DB
{

// class RelationParser;
// class NodeParser;
// class SelectParser;
// class TypeProvider;

// using RelationParserPtr = std::shared_ptr<RelationParser>;
// using NodeParserPtr = std::shared_ptr<NodeParser>;
// using SelectParserPtr = std::shared_ptr<SelectParser>;

// class Context;
// using ContextPtr = std::shared_ptr<const Context>;

class CTEParser
{
private:
	// RelationParserPtr relation_parser;
	// NodeParserPtr node_parser;
	// SelectParserPtr select_parser;
	// ContextPtr context;
public:
	//explicit CTEParser(const ContextPtr& context_);

	static duckdb_libpgquery::PGList *
	transformWithClause(duckdb_libpgquery::PGParseState *pstate, duckdb_libpgquery::PGWithClause *withClause);

    static void analyzeCTETargetList(duckdb_libpgquery::PGParseState * pstate, duckdb_libpgquery::PGCommonTableExpr * cte, duckdb_libpgquery::PGList * tlist);

    static void analyzeCTE(duckdb_libpgquery::PGParseState * pstate, duckdb_libpgquery::PGCommonTableExpr * cte);
};

}
