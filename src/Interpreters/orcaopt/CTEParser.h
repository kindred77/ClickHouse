#pragma once

#include <Interpreters/orcaopt/parser_common.h>

namespace DB
{

class RelationParser;
class SelectParser;
using RelationParserPtr = std::unique_ptr<RelationParser>;
using SelectParserPtr = std::unique_ptr<SelectParser>;

class CTEParser
{
private:
	RelationParserPtr relation_parser_ptr;
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
