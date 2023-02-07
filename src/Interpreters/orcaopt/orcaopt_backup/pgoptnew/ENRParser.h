#pragma once

#include <Interpreters/orcaopt/pgoptnew/parser_common.h>

namespace DB
{

typedef enum EphemeralNameRelationType
{
	ENR_NAMED_TUPLESTORE		/* named tuplestore relation; e.g., deltas */
} EphemeralNameRelationType;

/*
 * Some ephemeral named relations must match some relation (e.g., trigger
 * transition tables), so to properly handle cached plans and DDL, we should
 * carry the OID of that relation.  In other cases an ENR might be independent
 * of any relation which is stored in the system catalogs, so we need to be
 * able to directly store the TupleDesc.  We never need both.
 */
typedef struct EphemeralNamedRelationMetadataData
{
	char	   *name;			/* name used to identify the relation */

	/* only one of the next two fields should be used */
	Oid			reliddesc;		/* oid of relation to get tupdesc */
	TupleDesc	tupdesc;		/* description of result rows */

	EphemeralNameRelationType enrtype;	/* to identify type of relation */
	double		enrtuples;		/* estimated number of tuples */
} EphemeralNamedRelationMetadataData;

typedef EphemeralNamedRelationMetadataData *EphemeralNamedRelationMetadata;

class ENRParser
{
private:

public:
	explicit ENRParser();

    bool
    name_matches_visible_ENR(PGParseState *pstate, const char *refname);

    EphemeralNamedRelationMetadata
    get_visible_ENR(PGParseState *pstate, const char *refname)
};

}