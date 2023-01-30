#pragma once

#include <Interpreters/orcaopt/pgopt_hawq/parser_common.h>
#include <Interpreters/orcaopt/pgopt_hawq/RelationParser.h>
#include <Interpreters/orcaopt/pgopt_hawq/NodeParser.h>

namespace DB
{

class CTEParser
{
private:
	RelationParser relation_parser;
	NodeParser node_parser;
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