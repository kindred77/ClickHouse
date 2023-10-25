#include <Interpreters/orcaopt/provider/AggProvider.h>

#include <Interpreters/Context.h>

using namespace duckdb_libpgquery;

namespace DB
{

std::pair<PGOid, PGAggPtr> AggProvider::AGG_COUNTANY = std::pair<PGOid, PGAggPtr>(
    PGOid(2147),
    std::make_shared<Form_pg_agg>(Form_pg_agg{
        .aggfnoid = PGOid(2147),
        /*aggkind*/ .aggkind = 'n',
        /*aggnumdirectargs*/ .aggnumdirectargs = 0,
        /*aggtransfn*/ .aggtransfn = 2804,
        /*aggfinalfn*/ .aggfinalfn = InvalidOid,
        /*aggcombinefn*/ .aggcombinefn = 463,
        /*aggserialfn*/ .aggserialfn = InvalidOid,
        /*aggdeserialfn*/ .aggdeserialfn = InvalidOid,
        /*aggmtransfn*/ .aggmtransfn = 2804,
        /*aggminvtransfn*/ .aggminvtransfn = 3547,
        /*aggmfinalfn*/ .aggmfinalfn = InvalidOid,
        /*aggfinalextra*/ .aggfinalextra = false,
        /*aggmfinalextra*/ .aggmfinalextra = false,
        /*aggsortop*/ .aggsortop = InvalidOid,
        /*aggtranstype*/ .aggtranstype = 20,
        /*aggtransspace*/ .aggtransspace = 0,
        /*aggmtranstype*/ .aggmtranstype = 20,
        /*aggmtransspace*/ .aggmtransspace = 0}));

std::pair<PGOid, PGAggPtr> AggProvider::AGG_COUNTSTAR = std::pair<PGOid, PGAggPtr>(
    PGOid(2803),
    std::make_shared<Form_pg_agg>(Form_pg_agg{
        .aggfnoid = PGOid(2803),
        /*aggkind*/ .aggkind = 'n',
        /*aggnumdirectargs*/ .aggnumdirectargs = 0,
        /*aggtransfn*/ .aggtransfn = 1219,//int8inc
        /*aggfinalfn*/ .aggfinalfn = InvalidOid,
        /*aggcombinefn*/ .aggcombinefn = 463,//int8pl
        /*aggserialfn*/ .aggserialfn = InvalidOid,
        /*aggdeserialfn*/ .aggdeserialfn = InvalidOid,
        /*aggmtransfn*/ .aggmtransfn = 1219,//int8inc
        /*aggminvtransfn*/ .aggminvtransfn = 3546,//int8dec
        /*aggmfinalfn*/ .aggmfinalfn = InvalidOid,
        /*aggfinalextra*/ .aggfinalextra = false,
        /*aggmfinalextra*/ .aggmfinalextra = false,
        /*aggsortop*/ .aggsortop = InvalidOid,
        /*aggtranstype*/ .aggtranstype = 20,
        /*aggtransspace*/ .aggtransspace = InvalidOid,
        /*aggmtranstype*/ .aggmtranstype = 20,
        /*aggmtransspace*/ .aggmtransspace = InvalidOid}));

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
