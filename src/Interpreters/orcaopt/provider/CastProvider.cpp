#include <Interpreters/orcaopt/provider/CastProvider.h>

#include <Interpreters/Context.h>

using namespace duckdb_libpgquery;

namespace DB
{

int CastProvider::CAST_OID_ID = 1;

std::pair<PGOid, PGCastPtr> CastProvider::INT64_TO_INT16
    = {PGOid(++CastProvider::CAST_OID_ID),
       std::make_shared<Form_pg_cast>(Form_pg_cast{
           /*oid*/ PGOid(CastProvider::CAST_OID_ID),
           /*castsource*/ PGOid(20),
		   /*casttarget*/ PGOid(21),
		   /*castfunc*/ PGOid(714),
		   /*castcontext*/ 'a',
		   /*castmethod*/ 'f'})};

std::pair<PGOid, PGCastPtr> CastProvider::INT64_TO_INT32
    = {PGOid(++CastProvider::CAST_OID_ID),
       std::make_shared<Form_pg_cast>(Form_pg_cast{
           /*oid*/ PGOid(CastProvider::CAST_OID_ID),
           /*castsource*/ PGOid(20),
		   /*casttarget*/ PGOid(23),
		   /*castfunc*/ PGOid(480),
		   /*castcontext*/ 'a',
		   /*castmethod*/ 'f'})};

std::pair<PGOid, PGCastPtr> CastProvider::INT64_TO_FLOAT32
    = {PGOid(++CastProvider::CAST_OID_ID),
       std::make_shared<Form_pg_cast>(Form_pg_cast{
           /*oid*/ PGOid(CastProvider::CAST_OID_ID),
           /*castsource*/ PGOid(20),
		   /*casttarget*/ PGOid(700),
		   /*castfunc*/ PGOid(652),
		   /*castcontext*/ 'i',
		   /*castmethod*/ 'f'})};

std::pair<PGOid, PGCastPtr> CastProvider::INT64_TO_FLOAT64
    = {PGOid(++CastProvider::CAST_OID_ID),
       std::make_shared<Form_pg_cast>(Form_pg_cast{
           /*oid*/ PGOid(CastProvider::CAST_OID_ID),
           /*castsource*/ PGOid(20),
		   /*casttarget*/ PGOid(701),
		   /*castfunc*/ PGOid(482),
		   /*castcontext*/ 'i',
		   /*castmethod*/ 'f'})};

std::pair<PGOid, PGCastPtr> CastProvider::INT16_TO_INT64
    = {PGOid(++CastProvider::CAST_OID_ID),
       std::make_shared<Form_pg_cast>(Form_pg_cast{
           /*oid*/ PGOid(CastProvider::CAST_OID_ID),
           /*castsource*/ PGOid(21),
		   /*casttarget*/ PGOid(20),
		   /*castfunc*/ PGOid(754),
		   /*castcontext*/ 'i',
		   /*castmethod*/ 'f'})};

std::pair<PGOid, PGCastPtr> CastProvider::INT16_TO_INT32
    = {PGOid(++CastProvider::CAST_OID_ID),
       std::make_shared<Form_pg_cast>(Form_pg_cast{
           /*oid*/ PGOid(CastProvider::CAST_OID_ID),
           /*castsource*/ PGOid(21),
		   /*casttarget*/ PGOid(23),
		   /*castfunc*/ PGOid(313),
		   /*castcontext*/ 'i',
		   /*castmethod*/ 'f'})};

std::pair<PGOid, PGCastPtr> CastProvider::INT16_TO_FLOAT32
    = {PGOid(++CastProvider::CAST_OID_ID),
       std::make_shared<Form_pg_cast>(Form_pg_cast{
           /*oid*/ PGOid(CastProvider::CAST_OID_ID),
           /*castsource*/ PGOid(21),
		   /*casttarget*/ PGOid(700),
		   /*castfunc*/ PGOid(236),
		   /*castcontext*/ 'i',
		   /*castmethod*/ 'f'})};

std::pair<PGOid, PGCastPtr> CastProvider::INT16_TO_FLOAT64
    = {PGOid(++CastProvider::CAST_OID_ID),
       std::make_shared<Form_pg_cast>(Form_pg_cast{
           /*oid*/ PGOid(CastProvider::CAST_OID_ID),
           /*castsource*/ PGOid(21),
		   /*casttarget*/ PGOid(701),
		   /*castfunc*/ PGOid(235),
		   /*castcontext*/ 'i',
		   /*castmethod*/ 'f'})};

std::pair<PGOid, PGCastPtr> CastProvider::BOOL_TO_INT32
    = {PGOid(++CastProvider::CAST_OID_ID),
       std::make_shared<Form_pg_cast>(Form_pg_cast{
           /*oid*/ PGOid(CastProvider::CAST_OID_ID),
           /*castsource*/ PGOid(16),
		   /*casttarget*/ PGOid(23),
		   /*castfunc*/ PGOid(2558),
		   /*castcontext*/ 'e',
		   /*castmethod*/ 'f'})};

std::pair<PGOid, PGCastPtr> CastProvider::BOOL_TO_STRING
    = {PGOid(++CastProvider::CAST_OID_ID),
       std::make_shared<Form_pg_cast>(Form_pg_cast{
           /*oid*/ PGOid(CastProvider::CAST_OID_ID),
           /*castsource*/ PGOid(16),
		   /*casttarget*/ PGOid(25),
		   /*castfunc*/ PGOid(2971),
		   /*castcontext*/ 'a',
		   /*castmethod*/ 'f'})};

// std::pair<PGOid, PGCastPtr> CastProvider::BOOL_TO_FIXEDSTRING
//     = {PGOid(++CastProvider::CAST_OID_ID),
//        std::make_shared<Form_pg_cast>(Form_pg_cast{
//            /*oid*/ PGOid(CastProvider::CAST_OID_ID),
//            /*castsource*/ PGOid(16),
// 		   /*casttarget*/ PGOid(1042),
// 		   /*castfunc*/ PGOid(2971),
// 		   /*castcontext*/ 'a',
// 		   /*castmethod*/ 'f'})};

std::pair<PGOid, PGCastPtr> CastProvider::FLOAT32_TO_INT64
    = {PGOid(++CastProvider::CAST_OID_ID),
       std::make_shared<Form_pg_cast>(Form_pg_cast{
           /*oid*/ PGOid(CastProvider::CAST_OID_ID),
           /*castsource*/ PGOid(700),
		   /*casttarget*/ PGOid(20),
		   /*castfunc*/ PGOid(653),
		   /*castcontext*/ 'a',
		   /*castmethod*/ 'f'})};

std::pair<PGOid, PGCastPtr> CastProvider::FLOAT32_TO_INT16
    = {PGOid(++CastProvider::CAST_OID_ID),
       std::make_shared<Form_pg_cast>(Form_pg_cast{
           /*oid*/ PGOid(CastProvider::CAST_OID_ID),
           /*castsource*/ PGOid(700),
		   /*casttarget*/ PGOid(21),
		   /*castfunc*/ PGOid(238),
		   /*castcontext*/ 'a',
		   /*castmethod*/ 'f'})};

std::pair<PGOid, PGCastPtr> CastProvider::FLOAT32_TO_INT32
    = {PGOid(++CastProvider::CAST_OID_ID),
       std::make_shared<Form_pg_cast>(Form_pg_cast{
           /*oid*/ PGOid(CastProvider::CAST_OID_ID),
           /*castsource*/ PGOid(700),
		   /*casttarget*/ PGOid(23),
		   /*castfunc*/ PGOid(319),
		   /*castcontext*/ 'a',
		   /*castmethod*/ 'f'})};

std::pair<PGOid, PGCastPtr> CastProvider::FLOAT32_TO_FLOAT64
    = {PGOid(++CastProvider::CAST_OID_ID),
       std::make_shared<Form_pg_cast>(Form_pg_cast{
           /*oid*/ PGOid(CastProvider::CAST_OID_ID),
           /*castsource*/ PGOid(700),
		   /*casttarget*/ PGOid(701),
		   /*castfunc*/ PGOid(311),
		   /*castcontext*/ 'i',
		   /*castmethod*/ 'f'})};

std::pair<PGOid, PGCastPtr> CastProvider::FLOAT64_TO_INT64
    = {PGOid(++CastProvider::CAST_OID_ID),
       std::make_shared<Form_pg_cast>(Form_pg_cast{
           /*oid*/ PGOid(CastProvider::CAST_OID_ID),
           /*castsource*/ PGOid(701),
		   /*casttarget*/ PGOid(20),
		   /*castfunc*/ PGOid(483),
		   /*castcontext*/ 'a',
		   /*castmethod*/ 'f'})};

std::pair<PGOid, PGCastPtr> CastProvider::FLOAT64_TO_INT16
    = {PGOid(++CastProvider::CAST_OID_ID),
       std::make_shared<Form_pg_cast>(Form_pg_cast{
           /*oid*/ PGOid(CastProvider::CAST_OID_ID),
           /*castsource*/ PGOid(701),
		   /*casttarget*/ PGOid(21),
		   /*castfunc*/ PGOid(237),
		   /*castcontext*/ 'a',
		   /*castmethod*/ 'f'})};

std::pair<PGOid, PGCastPtr> CastProvider::FLOAT64_TO_INT32
    = {PGOid(++CastProvider::CAST_OID_ID),
       std::make_shared<Form_pg_cast>(Form_pg_cast{
           /*oid*/ PGOid(CastProvider::CAST_OID_ID),
           /*castsource*/ PGOid(701),
		   /*casttarget*/ PGOid(23),
		   /*castfunc*/ PGOid(317),
		   /*castcontext*/ 'a',
		   /*castmethod*/ 'f'})};

std::pair<PGOid, PGCastPtr> CastProvider::FLOAT64_TO_FLOAT32
    = {PGOid(++CastProvider::CAST_OID_ID),
       std::make_shared<Form_pg_cast>(Form_pg_cast{
           /*oid*/ PGOid(CastProvider::CAST_OID_ID),
           /*castsource*/ PGOid(701),
		   /*casttarget*/ PGOid(700),
		   /*castfunc*/ PGOid(312),
		   /*castcontext*/ 'a',
		   /*castmethod*/ 'f'})};

std::pair<PGOid, PGCastPtr> CastProvider::INT32_TO_INT64
    = {PGOid(++CastProvider::CAST_OID_ID),
       std::make_shared<Form_pg_cast>(Form_pg_cast{
           /*oid*/ PGOid(CastProvider::CAST_OID_ID),
           /*castsource*/ PGOid(23),
		   /*casttarget*/ PGOid(20),
		   /*castfunc*/ PGOid(481),
		   /*castcontext*/ 'i',
		   /*castmethod*/ 'f'})};

std::pair<PGOid, PGCastPtr> CastProvider::INT32_TO_INT16
    = {PGOid(++CastProvider::CAST_OID_ID),
       std::make_shared<Form_pg_cast>(Form_pg_cast{
           /*oid*/ PGOid(CastProvider::CAST_OID_ID),
           /*castsource*/ PGOid(23),
		   /*casttarget*/ PGOid(21),
		   /*castfunc*/ PGOid(314),
		   /*castcontext*/ 'a',
		   /*castmethod*/ 'f'})};

std::pair<PGOid, PGCastPtr> CastProvider::INT32_TO_FLOAT32
    = {PGOid(++CastProvider::CAST_OID_ID),
       std::make_shared<Form_pg_cast>(Form_pg_cast{
           /*oid*/ PGOid(CastProvider::CAST_OID_ID),
           /*castsource*/ PGOid(23),
		   /*casttarget*/ PGOid(700),
		   /*castfunc*/ PGOid(318),
		   /*castcontext*/ 'i',
		   /*castmethod*/ 'f'})};

std::pair<PGOid, PGCastPtr> CastProvider::INT32_TO_FLOAT64
    = {PGOid(++CastProvider::CAST_OID_ID),
       std::make_shared<Form_pg_cast>(Form_pg_cast{
           /*oid*/ PGOid(CastProvider::CAST_OID_ID),
           /*castsource*/ PGOid(23),
		   /*casttarget*/ PGOid(701),
		   /*castfunc*/ PGOid(316),
		   /*castcontext*/ 'i',
		   /*castmethod*/ 'f'})};

// std::pair<PGOid, PGCastPtr> CastProvider::DATE_TO_DATETIME
//     = {PGOid(++CastProvider::CAST_OID_ID),
//        std::make_shared<Form_pg_cast>(Form_pg_cast{
//            /*oid*/ PGOid(CastProvider::CAST_OID_ID),
//            /*castsource*/ PGOid(1082),
// 		   /*casttarget*/ PGOid(1114),
// 		   /*castfunc*/ PGOid(2024),
// 		   /*castcontext*/ 'i',
// 		   /*castmethod*/ 'f'})};

// std::pair<PGOid, PGCastPtr> CastProvider::DATE_TO_DATETIME64
//     = {PGOid(++CastProvider::CAST_OID_ID),
//        std::make_shared<Form_pg_cast>(Form_pg_cast{
//            /*oid*/ PGOid(CastProvider::CAST_OID_ID),
//            /*castsource*/ PGOid(1082),
// 		   /*casttarget*/ PGOid(1184),
// 		   /*castfunc*/ PGOid(1174),
// 		   /*castcontext*/ 'i',
// 		   /*castmethod*/ 'f'})};

std::pair<PGOid, PGCastPtr> CastProvider::DECIMAL64_TO_INT64
    = {PGOid(++CastProvider::CAST_OID_ID),
       std::make_shared<Form_pg_cast>(Form_pg_cast{
           /*oid*/ PGOid(CastProvider::CAST_OID_ID),
           /*castsource*/ PGOid(1700),
		   /*casttarget*/ PGOid(20),
		   /*castfunc*/ PGOid(1779),
		   /*castcontext*/ 'a',
		   /*castmethod*/ 'f'})};

std::pair<PGOid, PGCastPtr> CastProvider::DECIMAL64_TO_INT16
    = {PGOid(++CastProvider::CAST_OID_ID),
       std::make_shared<Form_pg_cast>(Form_pg_cast{
           /*oid*/ PGOid(CastProvider::CAST_OID_ID),
           /*castsource*/ PGOid(1700),
		   /*casttarget*/ PGOid(21),
		   /*castfunc*/ PGOid(1783),
		   /*castcontext*/ 'a',
		   /*castmethod*/ 'f'})};

std::pair<PGOid, PGCastPtr> CastProvider::DECIMAL64_TO_INT32
    = {PGOid(++CastProvider::CAST_OID_ID),
       std::make_shared<Form_pg_cast>(Form_pg_cast{
           /*oid*/ PGOid(CastProvider::CAST_OID_ID),
           /*castsource*/ PGOid(1700),
		   /*casttarget*/ PGOid(23),
		   /*castfunc*/ PGOid(1744),
		   /*castcontext*/ 'a',
		   /*castmethod*/ 'f'})};

std::pair<PGOid, PGCastPtr> CastProvider::DECIMAL64_TO_FLOAT32
    = {PGOid(++CastProvider::CAST_OID_ID),
       std::make_shared<Form_pg_cast>(Form_pg_cast{
           /*oid*/ PGOid(CastProvider::CAST_OID_ID),
           /*castsource*/ PGOid(1700),
		   /*casttarget*/ PGOid(700),
		   /*castfunc*/ PGOid(1745),
		   /*castcontext*/ 'i',
		   /*castmethod*/ 'f'})};

std::pair<PGOid, PGCastPtr> CastProvider::DECIMAL64_TO_FLOAT64
    = {PGOid(++CastProvider::CAST_OID_ID),
       std::make_shared<Form_pg_cast>(Form_pg_cast{
           /*oid*/ PGOid(CastProvider::CAST_OID_ID),
           /*castsource*/ PGOid(1700),
		   /*casttarget*/ PGOid(701),
		   /*castfunc*/ PGOid(1746),
		   /*castcontext*/ 'i',
		   /*castmethod*/ 'f'})};

CastProvider::OidCastMap CastProvider::oid_cast_map = {
    CastProvider::INT64_TO_INT16,
	CastProvider::INT64_TO_INT32,
	CastProvider::INT64_TO_FLOAT32,
	CastProvider::INT64_TO_FLOAT64,
	CastProvider::INT16_TO_INT64,
	CastProvider::INT16_TO_INT32,
	CastProvider::INT16_TO_FLOAT32,
	CastProvider::INT16_TO_FLOAT64,
	CastProvider::BOOL_TO_INT32,
	CastProvider::BOOL_TO_STRING,
	CastProvider::FLOAT32_TO_INT64,
	CastProvider::FLOAT32_TO_INT16,
	CastProvider::FLOAT32_TO_INT32,
	CastProvider::FLOAT32_TO_FLOAT64,
	CastProvider::FLOAT64_TO_INT64,
	CastProvider::FLOAT64_TO_INT16,
	CastProvider::FLOAT64_TO_INT32,
	CastProvider::FLOAT64_TO_FLOAT32,
	CastProvider::INT32_TO_INT64,
	CastProvider::INT32_TO_INT16,
	CastProvider::INT32_TO_FLOAT32,
	CastProvider::INT32_TO_FLOAT64,
	CastProvider::DECIMAL64_TO_INT64,
	CastProvider::DECIMAL64_TO_INT16,
	CastProvider::DECIMAL64_TO_INT32,
	CastProvider::DECIMAL64_TO_FLOAT32,
	CastProvider::DECIMAL64_TO_FLOAT64
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
