#pragma once

#include <Interpreters/orcaopt/parser_common.h>
#include <Interpreters/orcaopt/RelationParser.h>
#include <Interpreters/orcaopt/NodeParser.h>
#include <Interpreters/orcaopt/SelectParser.h>

namespace DB
{

class CTEParser
{
private:
	RelationParserPtr relation_parser_ptr;
	NodeParserPtr node_parser_ptr;
	SelectParserPtr select_parser_ptr;
public:
	explicit CTEParser();

	void reportDuplicateNames(const char *queryName, duckdb_libpgquery::PGList *names);

	void analyzeCTETargetList(PGParseState *pstate, duckdb_libpgquery::PGCommonTableExpr *cte,
		duckdb_libpgquery::PGList *tlist);

	void analyzeCTE(PGParseState *pstate, duckdb_libpgquery::PGCommonTableExpr *cte);

	duckdb_libpgquery::PGList *
	transformWithClause(PGParseState *pstate, duckdb_libpgquery::PGWithClause *withClause);

    duckdb_libpgquery::PGCommonTableExpr * GetCTEForRTE(PGParseState * pstate,
		duckdb_libpgquery::PGRangeTblEntry * rte, int rtelevelsup);
};
}
