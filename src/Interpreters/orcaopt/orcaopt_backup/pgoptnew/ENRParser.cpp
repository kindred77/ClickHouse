#include <Interpreters/orcaopt/pgoptnew/ENRParser.h>

using namespace duckdb_libpgquery;

namespace DB
{

bool
ENRParser::name_matches_visible_ENR(PGParseState *pstate, const char *refname)
{
	return (get_visible_ENR_metadata(pstate->p_queryEnv, refname) != NULL);
};

EphemeralNamedRelationMetadata
ENRParser::get_visible_ENR(PGParseState *pstate, const char *refname)
{
	return get_visible_ENR_metadata(pstate->p_queryEnv, refname);
};

}