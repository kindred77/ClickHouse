#include <Interpreters/orcaopt/provider/CastProvider.h>

#ifdef __clang__
#pragma clang diagnostic ignored "-Wunused-variable"
#else
#pragma GCC diagnostic ignored "-Wunused-variable"
#endif

using namespace duckdb_libpgquery;

namespace DB
{

int CastProvider::CAST_OID_ID = 1;

std::pair<Oid, PGCastPtr> CastProvider::INT64_TO_INT16
    = {Oid(++CastProvider::CAST_OID_ID),
       std::make_shared<Form_pg_cast>(Form_pg_cast{
           /*oid*/ Oid(CastProvider::CAST_OID_ID),
           /*castsource*/ Oid(20),
		   /*casttarget*/ Oid(21),
		   /*castfunc*/ Oid(714),
		   /*castcontext*/ 'a',
		   /*castmethod*/ 'f'})};

std::pair<Oid, PGCastPtr> CastProvider::INT64_TO_INT32
    = {Oid(++CastProvider::CAST_OID_ID),
       std::make_shared<Form_pg_cast>(Form_pg_cast{
           /*oid*/ Oid(CastProvider::CAST_OID_ID),
           /*castsource*/ Oid(20),
		   /*casttarget*/ Oid(23),
		   /*castfunc*/ Oid(480),
		   /*castcontext*/ 'a',
		   /*castmethod*/ 'f'})};

std::pair<Oid, PGCastPtr> CastProvider::INT64_TO_FLOAT32
    = {Oid(++CastProvider::CAST_OID_ID),
       std::make_shared<Form_pg_cast>(Form_pg_cast{
           /*oid*/ Oid(CastProvider::CAST_OID_ID),
           /*castsource*/ Oid(20),
		   /*casttarget*/ Oid(700),
		   /*castfunc*/ Oid(652),
		   /*castcontext*/ 'i',
		   /*castmethod*/ 'f'})};

std::pair<Oid, PGCastPtr> CastProvider::INT64_TO_FLOAT64
    = {Oid(++CastProvider::CAST_OID_ID),
       std::make_shared<Form_pg_cast>(Form_pg_cast{
           /*oid*/ Oid(CastProvider::CAST_OID_ID),
           /*castsource*/ Oid(20),
		   /*casttarget*/ Oid(701),
		   /*castfunc*/ Oid(482),
		   /*castcontext*/ 'i',
		   /*castmethod*/ 'f'})};

std::pair<Oid, PGCastPtr> CastProvider::INT16_TO_INT64
    = {Oid(++CastProvider::CAST_OID_ID),
       std::make_shared<Form_pg_cast>(Form_pg_cast{
           /*oid*/ Oid(CastProvider::CAST_OID_ID),
           /*castsource*/ Oid(21),
		   /*casttarget*/ Oid(20),
		   /*castfunc*/ Oid(754),
		   /*castcontext*/ 'i',
		   /*castmethod*/ 'f'})};

std::pair<Oid, PGCastPtr> CastProvider::INT16_TO_INT32
    = {Oid(++CastProvider::CAST_OID_ID),
       std::make_shared<Form_pg_cast>(Form_pg_cast{
           /*oid*/ Oid(CastProvider::CAST_OID_ID),
           /*castsource*/ Oid(21),
		   /*casttarget*/ Oid(23),
		   /*castfunc*/ Oid(313),
		   /*castcontext*/ 'i',
		   /*castmethod*/ 'f'})};

std::pair<Oid, PGCastPtr> CastProvider::INT16_TO_FLOAT32
    = {Oid(++CastProvider::CAST_OID_ID),
       std::make_shared<Form_pg_cast>(Form_pg_cast{
           /*oid*/ Oid(CastProvider::CAST_OID_ID),
           /*castsource*/ Oid(21),
		   /*casttarget*/ Oid(700),
		   /*castfunc*/ Oid(236),
		   /*castcontext*/ 'i',
		   /*castmethod*/ 'f'})};

std::pair<Oid, PGCastPtr> CastProvider::INT16_TO_FLOAT64
    = {Oid(++CastProvider::CAST_OID_ID),
       std::make_shared<Form_pg_cast>(Form_pg_cast{
           /*oid*/ Oid(CastProvider::CAST_OID_ID),
           /*castsource*/ Oid(21),
		   /*casttarget*/ Oid(701),
		   /*castfunc*/ Oid(235),
		   /*castcontext*/ 'i',
		   /*castmethod*/ 'f'})};

std::pair<Oid, PGCastPtr> CastProvider::BOOL_TO_INT32
    = {Oid(++CastProvider::CAST_OID_ID),
       std::make_shared<Form_pg_cast>(Form_pg_cast{
           /*oid*/ Oid(CastProvider::CAST_OID_ID),
           /*castsource*/ Oid(16),
		   /*casttarget*/ Oid(23),
		   /*castfunc*/ Oid(2558),
		   /*castcontext*/ 'e',
		   /*castmethod*/ 'f'})};

std::pair<Oid, PGCastPtr> CastProvider::BOOL_TO_STRING
    = {Oid(++CastProvider::CAST_OID_ID),
       std::make_shared<Form_pg_cast>(Form_pg_cast{
           /*oid*/ Oid(CastProvider::CAST_OID_ID),
           /*castsource*/ Oid(16),
		   /*casttarget*/ Oid(25),
		   /*castfunc*/ Oid(2971),
		   /*castcontext*/ 'a',
		   /*castmethod*/ 'f'})};

// std::pair<Oid, PGCastPtr> CastProvider::BOOL_TO_FIXEDSTRING
//     = {Oid(++CastProvider::CAST_OID_ID),
//        std::make_shared<Form_pg_cast>(Form_pg_cast{
//            /*oid*/ Oid(CastProvider::CAST_OID_ID),
//            /*castsource*/ Oid(16),
// 		   /*casttarget*/ Oid(1042),
// 		   /*castfunc*/ Oid(2971),
// 		   /*castcontext*/ 'a',
// 		   /*castmethod*/ 'f'})};

std::pair<Oid, PGCastPtr> CastProvider::FLOAT32_TO_INT64
    = {Oid(++CastProvider::CAST_OID_ID),
       std::make_shared<Form_pg_cast>(Form_pg_cast{
           /*oid*/ Oid(CastProvider::CAST_OID_ID),
           /*castsource*/ Oid(700),
		   /*casttarget*/ Oid(20),
		   /*castfunc*/ Oid(653),
		   /*castcontext*/ 'a',
		   /*castmethod*/ 'f'})};

std::pair<Oid, PGCastPtr> CastProvider::FLOAT32_TO_INT16
    = {Oid(++CastProvider::CAST_OID_ID),
       std::make_shared<Form_pg_cast>(Form_pg_cast{
           /*oid*/ Oid(CastProvider::CAST_OID_ID),
           /*castsource*/ Oid(700),
		   /*casttarget*/ Oid(21),
		   /*castfunc*/ Oid(238),
		   /*castcontext*/ 'a',
		   /*castmethod*/ 'f'})};

std::pair<Oid, PGCastPtr> CastProvider::FLOAT32_TO_INT32
    = {Oid(++CastProvider::CAST_OID_ID),
       std::make_shared<Form_pg_cast>(Form_pg_cast{
           /*oid*/ Oid(CastProvider::CAST_OID_ID),
           /*castsource*/ Oid(700),
		   /*casttarget*/ Oid(23),
		   /*castfunc*/ Oid(319),
		   /*castcontext*/ 'a',
		   /*castmethod*/ 'f'})};

std::pair<Oid, PGCastPtr> CastProvider::FLOAT32_TO_FLOAT64
    = {Oid(++CastProvider::CAST_OID_ID),
       std::make_shared<Form_pg_cast>(Form_pg_cast{
           /*oid*/ Oid(CastProvider::CAST_OID_ID),
           /*castsource*/ Oid(700),
		   /*casttarget*/ Oid(701),
		   /*castfunc*/ Oid(311),
		   /*castcontext*/ 'i',
		   /*castmethod*/ 'f'})};

std::pair<Oid, PGCastPtr> CastProvider::FLOAT64_TO_INT64
    = {Oid(++CastProvider::CAST_OID_ID),
       std::make_shared<Form_pg_cast>(Form_pg_cast{
           /*oid*/ Oid(CastProvider::CAST_OID_ID),
           /*castsource*/ Oid(701),
		   /*casttarget*/ Oid(20),
		   /*castfunc*/ Oid(483),
		   /*castcontext*/ 'a',
		   /*castmethod*/ 'f'})};

std::pair<Oid, PGCastPtr> CastProvider::FLOAT64_TO_INT16
    = {Oid(++CastProvider::CAST_OID_ID),
       std::make_shared<Form_pg_cast>(Form_pg_cast{
           /*oid*/ Oid(CastProvider::CAST_OID_ID),
           /*castsource*/ Oid(701),
		   /*casttarget*/ Oid(21),
		   /*castfunc*/ Oid(237),
		   /*castcontext*/ 'a',
		   /*castmethod*/ 'f'})};

std::pair<Oid, PGCastPtr> CastProvider::FLOAT64_TO_INT32
    = {Oid(++CastProvider::CAST_OID_ID),
       std::make_shared<Form_pg_cast>(Form_pg_cast{
           /*oid*/ Oid(CastProvider::CAST_OID_ID),
           /*castsource*/ Oid(701),
		   /*casttarget*/ Oid(23),
		   /*castfunc*/ Oid(317),
		   /*castcontext*/ 'a',
		   /*castmethod*/ 'f'})};

std::pair<Oid, PGCastPtr> CastProvider::FLOAT64_TO_FLOAT32
    = {Oid(++CastProvider::CAST_OID_ID),
       std::make_shared<Form_pg_cast>(Form_pg_cast{
           /*oid*/ Oid(CastProvider::CAST_OID_ID),
           /*castsource*/ Oid(701),
		   /*casttarget*/ Oid(700),
		   /*castfunc*/ Oid(312),
		   /*castcontext*/ 'a',
		   /*castmethod*/ 'f'})};

std::pair<Oid, PGCastPtr> CastProvider::INT32_TO_INT64
    = {Oid(++CastProvider::CAST_OID_ID),
       std::make_shared<Form_pg_cast>(Form_pg_cast{
           /*oid*/ Oid(CastProvider::CAST_OID_ID),
           /*castsource*/ Oid(23),
		   /*casttarget*/ Oid(20),
		   /*castfunc*/ Oid(481),
		   /*castcontext*/ 'i',
		   /*castmethod*/ 'f'})};

std::pair<Oid, PGCastPtr> CastProvider::INT32_TO_INT16
    = {Oid(++CastProvider::CAST_OID_ID),
       std::make_shared<Form_pg_cast>(Form_pg_cast{
           /*oid*/ Oid(CastProvider::CAST_OID_ID),
           /*castsource*/ Oid(23),
		   /*casttarget*/ Oid(21),
		   /*castfunc*/ Oid(314),
		   /*castcontext*/ 'a',
		   /*castmethod*/ 'f'})};

std::pair<Oid, PGCastPtr> CastProvider::INT32_TO_FLOAT32
    = {Oid(++CastProvider::CAST_OID_ID),
       std::make_shared<Form_pg_cast>(Form_pg_cast{
           /*oid*/ Oid(CastProvider::CAST_OID_ID),
           /*castsource*/ Oid(23),
		   /*casttarget*/ Oid(700),
		   /*castfunc*/ Oid(318),
		   /*castcontext*/ 'i',
		   /*castmethod*/ 'f'})};

std::pair<Oid, PGCastPtr> CastProvider::INT32_TO_FLOAT64
    = {Oid(++CastProvider::CAST_OID_ID),
       std::make_shared<Form_pg_cast>(Form_pg_cast{
           /*oid*/ Oid(CastProvider::CAST_OID_ID),
           /*castsource*/ Oid(23),
		   /*casttarget*/ Oid(701),
		   /*castfunc*/ Oid(316),
		   /*castcontext*/ 'i',
		   /*castmethod*/ 'f'})};

// std::pair<Oid, PGCastPtr> CastProvider::DATE_TO_DATETIME
//     = {Oid(++CastProvider::CAST_OID_ID),
//        std::make_shared<Form_pg_cast>(Form_pg_cast{
//            /*oid*/ Oid(CastProvider::CAST_OID_ID),
//            /*castsource*/ Oid(1082),
// 		   /*casttarget*/ Oid(1114),
// 		   /*castfunc*/ Oid(2024),
// 		   /*castcontext*/ 'i',
// 		   /*castmethod*/ 'f'})};

// std::pair<Oid, PGCastPtr> CastProvider::DATE_TO_DATETIME64
//     = {Oid(++CastProvider::CAST_OID_ID),
//        std::make_shared<Form_pg_cast>(Form_pg_cast{
//            /*oid*/ Oid(CastProvider::CAST_OID_ID),
//            /*castsource*/ Oid(1082),
// 		   /*casttarget*/ Oid(1184),
// 		   /*castfunc*/ Oid(1174),
// 		   /*castcontext*/ 'i',
// 		   /*castmethod*/ 'f'})};

std::pair<Oid, PGCastPtr> CastProvider::DECIMAL64_TO_INT64
    = {Oid(++CastProvider::CAST_OID_ID),
       std::make_shared<Form_pg_cast>(Form_pg_cast{
           /*oid*/ Oid(CastProvider::CAST_OID_ID),
           /*castsource*/ Oid(1700),
		   /*casttarget*/ Oid(20),
		   /*castfunc*/ Oid(1779),
		   /*castcontext*/ 'a',
		   /*castmethod*/ 'f'})};

std::pair<Oid, PGCastPtr> CastProvider::DECIMAL64_TO_INT16
    = {Oid(++CastProvider::CAST_OID_ID),
       std::make_shared<Form_pg_cast>(Form_pg_cast{
           /*oid*/ Oid(CastProvider::CAST_OID_ID),
           /*castsource*/ Oid(1700),
		   /*casttarget*/ Oid(21),
		   /*castfunc*/ Oid(1783),
		   /*castcontext*/ 'a',
		   /*castmethod*/ 'f'})};

std::pair<Oid, PGCastPtr> CastProvider::DECIMAL64_TO_INT32
    = {Oid(++CastProvider::CAST_OID_ID),
       std::make_shared<Form_pg_cast>(Form_pg_cast{
           /*oid*/ Oid(CastProvider::CAST_OID_ID),
           /*castsource*/ Oid(1700),
		   /*casttarget*/ Oid(23),
		   /*castfunc*/ Oid(1744),
		   /*castcontext*/ 'a',
		   /*castmethod*/ 'f'})};

std::pair<Oid, PGCastPtr> CastProvider::DECIMAL64_TO_FLOAT32
    = {Oid(++CastProvider::CAST_OID_ID),
       std::make_shared<Form_pg_cast>(Form_pg_cast{
           /*oid*/ Oid(CastProvider::CAST_OID_ID),
           /*castsource*/ Oid(1700),
		   /*casttarget*/ Oid(700),
		   /*castfunc*/ Oid(1745),
		   /*castcontext*/ 'i',
		   /*castmethod*/ 'f'})};

std::pair<Oid, PGCastPtr> CastProvider::DECIMAL64_TO_FLOAT64
    = {Oid(++CastProvider::CAST_OID_ID),
       std::make_shared<Form_pg_cast>(Form_pg_cast{
           /*oid*/ Oid(CastProvider::CAST_OID_ID),
           /*castsource*/ Oid(1700),
		   /*casttarget*/ Oid(701),
		   /*castfunc*/ Oid(1746),
		   /*castcontext*/ 'i',
		   /*castmethod*/ 'f'})};

CastProvider::CastProvider()
{
	oid_cast_map.insert(INT64_TO_INT16);
	oid_cast_map.insert(INT64_TO_INT32);
	oid_cast_map.insert(INT64_TO_FLOAT32);
	oid_cast_map.insert(INT64_TO_FLOAT64);
	oid_cast_map.insert(INT16_TO_INT64);
	oid_cast_map.insert(INT16_TO_INT32);
	oid_cast_map.insert(INT16_TO_FLOAT32);
	oid_cast_map.insert(INT16_TO_FLOAT64);
	oid_cast_map.insert(BOOL_TO_INT32);
	oid_cast_map.insert(BOOL_TO_STRING);
	oid_cast_map.insert(FLOAT32_TO_INT64);
	oid_cast_map.insert(FLOAT32_TO_INT16);
	oid_cast_map.insert(FLOAT32_TO_INT32);
	oid_cast_map.insert(FLOAT32_TO_FLOAT64);
	oid_cast_map.insert(FLOAT64_TO_INT64);
	oid_cast_map.insert(FLOAT64_TO_INT16);
	oid_cast_map.insert(FLOAT64_TO_INT32);
	oid_cast_map.insert(FLOAT64_TO_FLOAT32);
	oid_cast_map.insert(INT32_TO_INT64);
	oid_cast_map.insert(INT32_TO_INT16);
	oid_cast_map.insert(INT32_TO_FLOAT32);
	oid_cast_map.insert(INT32_TO_FLOAT64);
	oid_cast_map.insert(DECIMAL64_TO_INT64);
	oid_cast_map.insert(DECIMAL64_TO_INT16);
	oid_cast_map.insert(DECIMAL64_TO_INT32);
	oid_cast_map.insert(DECIMAL64_TO_FLOAT32);
	oid_cast_map.insert(DECIMAL64_TO_FLOAT64);
};

PGCastPtr CastProvider::getCastBySourceTypeAndTargetTypeOid(Oid sourceTypeId, Oid targetTypeId) const
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
