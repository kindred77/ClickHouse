#include <Interpreters/orcaopt/provider/AggProvider.h>

#include <Interpreters/Context.h>

using namespace duckdb_libpgquery;

#define NEW_AGG(AGGVARNM, OID, AGGKIND, AGGNUMDIRECTARGS, AGGTRANSFN, AGGFINALFN, AGGCOMBINEFN, AGGSERIALFN, AGGDESERIALFN, AGGMTRANSFN, AGGMINVTRANSFN, AGGMFINALFN, AGGFINALEXTRA, AGGMFINALEXTRA, AGGSORTOP, AGGTRANSTYPE, AGGTRANSSPACE, AGGMTRANSTYPE, AGGMTRANSSPACE) \
    std::pair<PGOid, PGAggPtr> AggProvider::AGG_##AGGVARNM = {PGOid(OID), \
        std::make_shared<Form_pg_agg>(Form_pg_agg{ \
            .aggfnoid = PGOid(OID), \
            /*aggkind*/ .aggkind = AGGKIND, \
            /*aggnumdirectargs*/ .aggnumdirectargs = AGGNUMDIRECTARGS, \
            /*aggtransfn*/ .aggtransfn = AGGTRANSFN, \
            /*aggfinalfn*/ .aggfinalfn = AGGFINALFN, \
            /*aggcombinefn*/ .aggcombinefn = AGGCOMBINEFN, \
            /*aggserialfn*/ .aggserialfn = AGGSERIALFN, \
            /*aggdeserialfn*/ .aggdeserialfn = AGGDESERIALFN, \
            /*aggmtransfn*/ .aggmtransfn = AGGMTRANSFN, \
            /*aggminvtransfn*/ .aggminvtransfn = AGGMINVTRANSFN, \
            /*aggmfinalfn*/ .aggmfinalfn = AGGMFINALFN, \
            /*aggfinalextra*/ .aggfinalextra = AGGFINALEXTRA, \
            /*aggmfinalextra*/ .aggmfinalextra = AGGMFINALEXTRA, \
            /*aggsortop*/ .aggsortop = AGGSORTOP, \
            /*aggtranstype*/ .aggtranstype = AGGTRANSTYPE, \
            /*aggtransspace*/ .aggtransspace = AGGTRANSSPACE, \
            /*aggmtranstype*/ .aggmtranstype = AGGMTRANSTYPE, \
            /*aggmtransspace*/ .aggmtransspace = AGGMTRANSSPACE})};

namespace DB
{

NEW_AGG(COUNTANY, 2147, 'n', 0, 2804, InvalidOid, 463, InvalidOid, InvalidOid, 2804, 3547, InvalidOid, false, false, InvalidOid, 20, InvalidOid, 20, InvalidOid)
NEW_AGG(COUNTSTAR, 2803, 'n', 0, 1219, InvalidOid, 463, InvalidOid, InvalidOid, 1219, 3546, InvalidOid, false, false, InvalidOid, 20, InvalidOid, 20, InvalidOid)

#undef NEW_AGG

AggProvider::OidAggMap AggProvider::oid_agg_map = {
	AggProvider::AGG_COUNTANY,
	AggProvider::AGG_COUNTSTAR,
};

// AggProvider::AggProvider(const ContextPtr& context_) : context(context_)
// {

// };

PGAggPtr AggProvider::getAggByFuncOid(PGOid func_oid)
{
	auto it = oid_agg_map.find(func_oid);
	if (it == oid_agg_map.end())
	    return nullptr;
	return it->second;
};

bool AggProvider::AggregateExists(PGOid func_oid)
{
	auto it = oid_agg_map.find(func_oid);
	if (it == oid_agg_map.end())
	    return false;
	return true;
};

}
