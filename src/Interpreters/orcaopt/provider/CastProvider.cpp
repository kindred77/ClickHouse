#include <Interpreters/orcaopt/provider/CastProvider.h>

#include <Interpreters/Context.h>

using namespace duckdb_libpgquery;

#define NEW_CAST(CASTVARNM, CASTSOURCE, CASTTARGET, CASTFUNC, CASTCONTEXT, CASTMETHOD) \
    std::pair<PGOid, PGCastPtr> CastProvider::CAST_##CASTVARNM = {PGOid(++CastProvider::CAST_OID_ID), \
       std::make_shared<Form_pg_cast>(Form_pg_cast{ \
           /*oid*/ PGOid(CastProvider::CAST_OID_ID), \
           /*castsource*/ PGOid(CASTSOURCE), \
		   /*casttarget*/ PGOid(CASTTARGET), \
		   /*castfunc*/ PGOid(CASTFUNC), \
		   /*castcontext*/ CASTCONTEXT, \
		   /*castmethod*/ CASTMETHOD})};

namespace DB
{

int CastProvider::CAST_OID_ID = 1;

NEW_CAST(NUMERIC_TO_FLOAT8, 1700, 701, 1746, 'i', 'f')
NEW_CAST(NUMERIC_TO_FLOAT4, 1700, 700, 1745, 'i', 'f')
NEW_CAST(NUMERIC_TO_INT2, 1700, 21, 1783, 'a', 'f')
NEW_CAST(BOOL_TO_TEXT, 16, 25, 2971, 'a', 'f')
NEW_CAST(INT4_TO_INT2, 23, 21, 314, 'a', 'f')
NEW_CAST(INT2_TO_INT8, 21, 20, 754, 'i', 'f')
NEW_CAST(FLOAT4_TO_FLOAT8, 700, 701, 311, 'i', 'f')
NEW_CAST(INT4_TO_INT8, 23, 20, 481, 'i', 'f')
NEW_CAST(INT8_TO_INT4, 20, 23, 480, 'a', 'f')
NEW_CAST(FLOAT8_TO_FLOAT4, 701, 700, 312, 'a', 'f')
NEW_CAST(FLOAT8_TO_INT2, 701, 21, 237, 'a', 'f')
NEW_CAST(FLOAT8_TO_INT8, 701, 20, 483, 'a', 'f')
NEW_CAST(INT8_TO_FLOAT8, 20, 701, 482, 'i', 'f')
NEW_CAST(NUMERIC_TO_INT8, 1700, 20, 1779, 'a', 'f')
NEW_CAST(INT4_TO_FLOAT4, 23, 700, 318, 'i', 'f')
NEW_CAST(NUMERIC_TO_INT4, 1700, 23, 1744, 'a', 'f')
NEW_CAST(FLOAT4_TO_INT4, 700, 23, 319, 'a', 'f')
NEW_CAST(INT2_TO_INT4, 21, 23, 313, 'i', 'f')
NEW_CAST(BOOL_TO_INT4, 16, 23, 2558, 'e', 'f')
NEW_CAST(FLOAT4_TO_INT2, 700, 21, 238, 'a', 'f')
NEW_CAST(INT2_TO_FLOAT8, 21, 701, 235, 'i', 'f')
NEW_CAST(INT2_TO_FLOAT4, 21, 700, 236, 'i', 'f')
NEW_CAST(INT8_TO_FLOAT4, 20, 700, 652, 'i', 'f')
NEW_CAST(INT4_TO_FLOAT8, 23, 701, 316, 'i', 'f')
NEW_CAST(FLOAT4_TO_INT8, 700, 20, 653, 'a', 'f')
NEW_CAST(FLOAT8_TO_INT4, 701, 23, 317, 'a', 'f')
NEW_CAST(INT8_TO_INT2, 20, 21, 714, 'a', 'f')

#undef NEW_CAST

CastProvider::OidCastMap CastProvider::oid_cast_map = {
    CastProvider::CAST_NUMERIC_TO_FLOAT8,
	CastProvider::CAST_NUMERIC_TO_FLOAT4,
	CastProvider::CAST_NUMERIC_TO_INT2,
	CastProvider::CAST_BOOL_TO_TEXT,
	CastProvider::CAST_INT4_TO_INT2,
	CastProvider::CAST_INT2_TO_INT8,
	CastProvider::CAST_FLOAT4_TO_FLOAT8,
	CastProvider::CAST_INT4_TO_INT8,
	CastProvider::CAST_INT8_TO_INT4,
	CastProvider::CAST_FLOAT8_TO_FLOAT4,
	CastProvider::CAST_FLOAT8_TO_INT2,
	CastProvider::CAST_FLOAT8_TO_INT8,
	CastProvider::CAST_INT8_TO_FLOAT8,
	CastProvider::CAST_NUMERIC_TO_INT8,
	CastProvider::CAST_INT4_TO_FLOAT4,
	CastProvider::CAST_NUMERIC_TO_INT4,
	CastProvider::CAST_FLOAT4_TO_INT4,
	CastProvider::CAST_INT2_TO_INT4,
	CastProvider::CAST_BOOL_TO_INT4,
	CastProvider::CAST_FLOAT4_TO_INT2,
	CastProvider::CAST_INT2_TO_FLOAT8,
	CastProvider::CAST_INT2_TO_FLOAT4,
	CastProvider::CAST_INT8_TO_FLOAT4,
	CastProvider::CAST_INT4_TO_FLOAT8,
	CastProvider::CAST_FLOAT4_TO_INT8,
	CastProvider::CAST_FLOAT8_TO_INT4,
	CastProvider::CAST_INT8_TO_INT2
};

// CastProvider::CastProvider(const ContextPtr& context_) : context(context_)
// {
	// oid_cast_map.insert(INT64_TO_INT16);
	// oid_cast_map.insert(INT64_TO_INT32);
	// oid_cast_map.insert(INT64_TO_FLOAT32);
	// oid_cast_map.insert(INT64_TO_FLOAT64);
	// oid_cast_map.insert(INT16_TO_INT64);
	// oid_cast_map.insert(INT16_TO_INT32);
	// oid_cast_map.insert(INT16_TO_FLOAT32);
	// oid_cast_map.insert(INT16_TO_FLOAT64);
	// oid_cast_map.insert(BOOL_TO_INT32);
	// oid_cast_map.insert(BOOL_TO_STRING);
	// oid_cast_map.insert(FLOAT32_TO_INT64);
	// oid_cast_map.insert(FLOAT32_TO_INT16);
	// oid_cast_map.insert(FLOAT32_TO_INT32);
	// oid_cast_map.insert(FLOAT32_TO_FLOAT64);
	// oid_cast_map.insert(FLOAT64_TO_INT64);
	// oid_cast_map.insert(FLOAT64_TO_INT16);
	// oid_cast_map.insert(FLOAT64_TO_INT32);
	// oid_cast_map.insert(FLOAT64_TO_FLOAT32);
	// oid_cast_map.insert(INT32_TO_INT64);
	// oid_cast_map.insert(INT32_TO_INT16);
	// oid_cast_map.insert(INT32_TO_FLOAT32);
	// oid_cast_map.insert(INT32_TO_FLOAT64);
	// oid_cast_map.insert(DECIMAL64_TO_INT64);
	// oid_cast_map.insert(DECIMAL64_TO_INT16);
	// oid_cast_map.insert(DECIMAL64_TO_INT32);
	// oid_cast_map.insert(DECIMAL64_TO_FLOAT32);
	// oid_cast_map.insert(DECIMAL64_TO_FLOAT64);
// };

PGCastPtr CastProvider::getCastBySourceTypeAndTargetTypeOid(PGOid sourceTypeId, PGOid targetTypeId)
{
	for (auto pair : oid_cast_map)
	{
		if (pair.second->castsource == sourceTypeId
			&& pair.second->casttarget == targetTypeId)
		{
			return pair.second;
		}
	}

	return nullptr;
};

}
