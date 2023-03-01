#pragma once

#include <Interpreters/orcaopt/parser_common.h>

namespace DB
{

class RelationParser;
class NodeParser;
class SelectParser;
class TypeParser;

using RelationParserPtr = std::shared_ptr<RelationParser>;
using NodeParserPtr = std::shared_ptr<NodeParser>;
using SelectParserPtr = std::shared_ptr<SelectParser>;
using TypeParserPtr = std::shared_ptr<TypeParser>;

class CTEParser
{
private:
	RelationParserPtr relation_parser;
	NodeParserPtr node_parser;
	SelectParserPtr select_parser;
	TypeParserPtr type_parser;
public:
	explicit CTEParser();

	duckdb_libpgquery::PGList *
	transformWithClause(PGParseState *pstate, duckdb_libpgquery::PGWithClause *withClause);

    void analyzeCTETargetList(PGParseState * pstate, duckdb_libpgquery::PGCommonTableExpr * cte, duckdb_libpgquery::PGList * tlist);

    void analyzeCTE(PGParseState * pstate, duckdb_libpgquery::PGCommonTableExpr * cte);
};

}
