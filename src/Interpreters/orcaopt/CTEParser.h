#pragma once

#include <Interpreters/orcaopt/parser_common.h>

namespace DB
{

class RelationParser;
class NodeParser;

using RelationParserPtr = std::unique_ptr<RelationParser>;
using NodeParserPtr = std::unique_ptr<NodeParser>;

class CTEParser
{
private:
	RelationParserPtr relation_parser;
	NodeParserPtr node_parser;
public:
	explicit CTEParser();

	duckdb_libpgquery::PGList *
	transformWithClause(PGParseState *pstate, duckdb_libpgquery::PGWithClause *withClause);
};

}
