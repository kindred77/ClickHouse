#include <Interpreters/orcaopt/provider/TypeProvider.h>

#include <Interpreters/orcaopt/TypeParser.h>
#include <Interpreters/orcaopt/provider/FunctionProvider.h>
#include <Interpreters/orcaopt/provider/RelationProvider.h>
#include <Interpreters/orcaopt/provider/OperProvider.h>
#include <Interpreters/orcaopt/provider/ProcProvider.h>

#include <Common/SipHash.h>

#include <naucrates/dxl/CDXLUtils.h>

#include <Interpreters/Context.h>

using namespace duckdb_libpgquery;

#define NEW_TYPE(TYPEVARNM, OID, TYPNAME, TYPNAMESPACE, TYPOWNER, TYPLEN, TYPBYVAL, TYPTYPE, TYPCATEGORY, TYPISPREFERRED, TYPISDEFINED, TYPDELIM, TYPRELID, TYPELEM, TYPARRAY, TYPINPUT, TYPOUTPUT, TYPRECEIVE, TYPSEND, TYPMODIN, TYPMODOUT, TYPANALYZE, TYPALIGN, TYPSTORAGE, TYPNOTNULL, TYPBASETYPE, TYPTYPMOD, TYPNDIMS, TYPCOLLATION, LT_OPR, EQ_OPR, GT_OPR, HASH_PROC, CMP_PROC) \
    std::pair<PGOid, PGTypePtr> TypeProvider::TYPE_##TYPEVARNM = {PGOid(OID), \
       std::make_shared<Form_pg_type>(Form_pg_type{ \
           /*oid*/ PGOid(OID), \
           /*typname*/ TYPNAME, \
		   /*typnamespace*/ PGOid(TYPNAMESPACE), \
		   /*typowner*/ PGOid(TYPOWNER), \
		   /*typlen*/ TYPLEN, \
		   /*typbyval*/ TYPBYVAL, \
		   /*typtype*/ TYPTYPE, \
		   /*typcategory*/ TYPCATEGORY, \
		   /*typispreferred*/ TYPISPREFERRED, \
		   /*typisdefined*/ TYPISDEFINED, \
		   /*typdelim*/ TYPDELIM, \
		   /*typrelid*/ PGOid(TYPRELID), \
		   /*typelem*/ PGOid(TYPELEM), \
		   /*typarray*/ PGOid(TYPARRAY), \
		   /*typinput*/ PGOid(TYPINPUT), \
		   /*typoutput*/ PGOid(TYPOUTPUT), \
		   /*typreceive*/ PGOid(TYPRECEIVE), \
		   /*typsend*/ PGOid(TYPSEND), \
		   /*typmodin*/ PGOid(TYPMODIN), \
		   /*typmodout*/ PGOid(TYPMODOUT), \
		   /*typanalyze*/ PGOid(TYPANALYZE), \
		   /*typalign*/ TYPALIGN, \
		   /*typstorage*/ TYPSTORAGE, \
		   /*typnotnull*/ TYPNOTNULL, \
		   /*typbasetype*/ PGOid(TYPBASETYPE), \
		   /*typtypmod*/ PGOid(TYPTYPMOD), \
		   /*typndims*/ PGOid(TYPNDIMS), \
		   /*typcollation*/ PGOid(TYPCOLLATION), \
		   /*lt_opr*/ PGOid(LT_OPR), \
		   /*eq_opr*/ PGOid(EQ_OPR), \
		   /*gt_opr*/ PGOid(GT_OPR), \
		   /*hash_proc*/ PGOid(HASH_PROC), \
           /*cmp_proc*/ PGOid(CMP_PROC)})};

namespace DB
{

int TypeProvider::TYPE_OID_ID = 3;
std::unordered_map<PGTupleDescPtr, const PGTupleDesc &, PGTupleDescHash, PGTupleDescEqual> TypeProvider::recordCacheHash = {};
size_t TypeProvider::NextRecordTypmod = 0;
std::vector<PGTupleDescPtr> TypeProvider::recordCacheArray = {};

NEW_TYPE(FLOAT32, 700, "Float32", 1, 1, 4, true, 'b', 'N', false, true, ',', 0, 1021, 0, 1, 1, 1, 1, 1, 1, 1, 'i', 'p', false, 1, 1, 1, 1, 622, 620, 623, 620, 0)

// std::pair<PGOid, PGTypePtr> TypeProvider::TYPE_FLOAT32
//     = {PGOid(700),
//        std::make_shared<Form_pg_type>(Form_pg_type{
//            /*oid*/ PGOid(700),
//            /*typname*/ getTypeName(TypeIndex::Float32),
// 		   /*typnamespace*/ PGOid(1),
// 		   /*typowner*/ PGOid(1),
// 		   /*typlen*/ 4,
// 		   /*typbyval*/ true,
// 		   /*typtype*/ 'b',
// 		   /*typcategory*/ 'N',
// 		   /*typispreferred*/ false,
// 		   /*typisdefined*/ true,
// 		   /*typdelim*/ ',',
// 		   /*typrelid*/ PGOid(0),
// 		   /*typelem*/ PGOid(1021),
// 		   /*typarray*/ PGOid(0),
// 		   /*typinput*/ PGOid(1),
// 		   /*typoutput*/ PGOid(1),
// 		   /*typreceive*/ PGOid(1),
// 		   /*typsend*/ PGOid(1),
// 		   /*typmodin*/ PGOid(1),
// 		   /*typmodout*/ PGOid(1),
// 		   /*typanalyze*/ PGOid(1),
// 		   /*typalign*/ 'i',
// 		   /*typstorage*/ 'p',
// 		   /*typnotnull*/ false,
// 		   /*typbasetype*/ PGOid(1),
// 		   /*typtypmod*/ PGOid(1),
// 		   /*typndims*/ PGOid(1),
// 		   /*typcollation*/ PGOid(1),
// 		   /*lt_opr*/ PGOid(622),
// 		   /*eq_opr*/ PGOid(620),
// 		   /*gt_opr*/ PGOid(623),
// 		   /*hash_proc*/ PGOid(620),
//            /*cmp_proc*/ PGOid(0)})};

std::pair<PGOid, PGTypePtr> TypeProvider::TYPE_FLOAT64
    = {PGOid(701),
       std::make_shared<Form_pg_type>(Form_pg_type{
           /*oid*/ PGOid(701),
           /*typname*/ getTypeName(TypeIndex::Float64),
		   /*typnamespace*/ PGOid(1),
		   /*typowner*/ PGOid(1),
		   /*typlen*/ 8,
		   /*typbyval*/ true,
		   /*typtype*/ 'b',
		   /*typcategory*/ 'N',
		   /*typispreferred*/ true,
		   /*typisdefined*/ false,
		   /*typdelim*/ ',',
		   /*typrelid*/ PGOid(0),
		   /*typelem*/ PGOid(1022),
		   /*typarray*/ PGOid(1),
		   /*typinput*/ PGOid(1),
		   /*typoutput*/ PGOid(1),
		   /*typreceive*/ PGOid(1),
		   /*typsend*/ PGOid(1),
		   /*typmodin*/ PGOid(1),
		   /*typmodout*/ PGOid(1),
		   /*typanalyze*/ PGOid(1),
		   /*typalign*/ 'i',
		   /*typstorage*/ 'p',
		   /*typnotnull*/ false,
		   /*typbasetype*/ PGOid(1),
		   /*typtypmod*/ PGOid(1),
		   /*typndims*/ PGOid(1),
		   /*typcollation*/ PGOid(1),
		   /*lt_opr*/ PGOid(672),
		   /*eq_opr*/ PGOid(670),
		   /*gt_opr*/ PGOid(674),
		   /*hash_proc*/ PGOid(670),
           /*cmp_proc*/ PGOid(0)})};

std::pair<PGOid, PGTypePtr> TypeProvider::TYPE_BOOLEAN
    = {PGOid(16),
       std::make_shared<Form_pg_type>(Form_pg_type{
           /*oid*/ PGOid(16),
           /*typname*/ getTypeName(TypeIndex::Int8),
		   /*typnamespace*/ PGOid(1),
		   /*typowner*/ PGOid(1),
		   /*typlen*/ 1,
		   /*typbyval*/ true,
		   /*typtype*/ 'b',
		   /*typcategory*/ 'B',
		   /*typispreferred*/ true,
		   /*typisdefined*/ true,
		   /*typdelim*/ ',',
		   /*typrelid*/ PGOid(0),
		   /*typelem*/ PGOid(1000),
		   /*typarray*/ PGOid(1),
		   /*typinput*/ PGOid(1),
		   /*typoutput*/ PGOid(1),
		   /*typreceive*/ PGOid(1),
		   /*typsend*/ PGOid(1),
		   /*typmodin*/ PGOid(1),
		   /*typmodout*/ PGOid(1),
		   /*typanalyze*/ PGOid(1),
		   /*typalign*/ 'i',
		   /*typstorage*/ 'p',
		   /*typnotnull*/ false,
		   /*typbasetype*/ PGOid(1),
		   /*typtypmod*/ PGOid(1),
		   /*typndims*/ PGOid(1),
		   /*typcollation*/ PGOid(1),
		   /*lt_opr*/ PGOid(58),
		   /*eq_opr*/ PGOid(91),
		   /*gt_opr*/ PGOid(59),
		   /*hash_proc*/ PGOid(91),
           /*cmp_proc*/ PGOid(0)})};

// std::pair<PGOid, PGTypePtr> TypeProvider::TYPE_UINT8
//     = {PGOid(++TypeProvider::TYPE_OID_ID),
//        std::make_shared<Form_pg_type>(Form_pg_type{
//            /*oid*/ PGOid(++TypeProvider::TYPE_OID_ID),
//            /*typname*/ getTypeName(TypeIndex::UInt8),
// 		   /*typnamespace*/ PGOid(1),
// 		   /*typowner*/ PGOid(1),
// 		   /*typlen*/ 1,
// 		   /*typbyval*/ true,
// 		   /*typtype*/ 'b',
// 		   /*typcategory*/ 'S',
// 		   /*typispreferred*/ true,
// 		   /*typisdefined*/ false,
// 		   /*typdelim*/ ',',
// 		   /*typrelid*/ PGOid(0),
// 		   /*typelem*/ PGOid(1002),
// 		   /*typarray*/ PGOid(1),
// 		   /*typinput*/ PGOid(1),
// 		   /*typoutput*/ PGOid(1),
// 		   /*typreceive*/ PGOid(1),
// 		   /*typsend*/ PGOid(1),
// 		   /*typmodin*/ PGOid(1),
// 		   /*typmodout*/ PGOid(1),
// 		   /*typanalyze*/ PGOid(1),
// 		   /*typalign*/ 'i',
// 		   /*typstorage*/ 'p',
// 		   /*typnotnull*/ false,
// 		   /*typbasetype*/ PGOid(1),
// 		   /*typtypmod*/ PGOid(1),
// 		   /*typndims*/ PGOid(1),
// 		   /*typcollation*/ PGOid(1),})};

// std::pair<PGOid, PGTypePtr> TypeProvider::TYPE_UINT16
//     = {PGOid(++TypeProvider::TYPE_OID_ID),
//        std::make_shared<Form_pg_type>(Form_pg_type{
//            /*oid*/ PGOid(++TypeProvider::TYPE_OID_ID),
//            /*typname*/ getTypeName(TypeIndex::UInt16),
// 		   /*typnamespace*/ PGOid(1),
// 		   /*typowner*/ PGOid(1),
// 		   /*typlen*/ 2,
// 		   /*typbyval*/ true,
// 		   /*typtype*/ 'b',
// 		   /*typcategory*/ 'N',
// 		   /*typispreferred*/ false,
// 		   /*typisdefined*/ true,
// 		   /*typdelim*/ ',',
// 		   /*typrelid*/ PGOid(0),
// 		   /*typelem*/ PGOid(1005),
// 		   /*typarray*/ PGOid(1),
// 		   /*typinput*/ PGOid(1),
// 		   /*typoutput*/ PGOid(1),
// 		   /*typreceive*/ PGOid(1),
// 		   /*typsend*/ PGOid(1),
// 		   /*typmodin*/ PGOid(1),
// 		   /*typmodout*/ PGOid(1),
// 		   /*typanalyze*/ PGOid(1),
// 		   /*typalign*/ 'i',
// 		   /*typstorage*/ 'p',
// 		   /*typnotnull*/ false,
// 		   /*typbasetype*/ PGOid(1),
// 		   /*typtypmod*/ PGOid(1),
// 		   /*typndims*/ PGOid(1),
// 		   /*typcollation*/ PGOid(1)})};

// std::pair<PGOid, PGTypePtr> TypeProvider::TYPE_UINT32
//     = {PGOid(++TypeProvider::TYPE_OID_ID),
//        std::make_shared<Form_pg_type>(Form_pg_type{
//            /*oid*/ PGOid(++TypeProvider::TYPE_OID_ID),
//            /*typname*/ getTypeName(TypeIndex::UInt32),
// 		   /*typnamespace*/ PGOid(1),
// 		   /*typowner*/ PGOid(1),
// 		   /*typlen*/ 4,
// 		   /*typbyval*/ true,
// 		   /*typtype*/ 'b',
// 		   /*typcategory*/ 'N',
// 		   /*typispreferred*/ false,
// 		   /*typisdefined*/ true,
// 		   /*typdelim*/ ',',
// 		   /*typrelid*/ PGOid(0),
// 		   /*typelem*/ PGOid(1007),
// 		   /*typarray*/ PGOid(1),
// 		   /*typinput*/ PGOid(1),
// 		   /*typoutput*/ PGOid(1),
// 		   /*typreceive*/ PGOid(1),
// 		   /*typsend*/ PGOid(1),
// 		   /*typmodin*/ PGOid(1),
// 		   /*typmodout*/ PGOid(1),
// 		   /*typanalyze*/ PGOid(1),
// 		   /*typalign*/ 'i',
// 		   /*typstorage*/ 'p',
// 		   /*typnotnull*/ false,
// 		   /*typbasetype*/ PGOid(1),
// 		   /*typtypmod*/ PGOid(1),
// 		   /*typndims*/ PGOid(1),
// 		   /*typcollation*/ PGOid(1)})};

// std::pair<PGOid, PGTypePtr> TypeProvider::TYPE_UINT64
//     = {PGOid(++TypeProvider::TYPE_OID_ID),
//        std::make_shared<Form_pg_type>(Form_pg_type{
//            /*oid*/ PGOid(++TypeProvider::TYPE_OID_ID),
//            /*typname*/ getTypeName(TypeIndex::UInt64),
// 		   /*typnamespace*/ PGOid(1),
// 		   /*typowner*/ PGOid(1),
// 		   /*typlen*/ 8,
// 		   /*typbyval*/ true,
// 		   /*typtype*/ 'b',
// 		   /*typcategory*/ 'N',
// 		   /*typispreferred*/ false,
// 		   /*typisdefined*/ true,
// 		   /*typdelim*/ ',',
// 		   /*typrelid*/ PGOid(0),
// 		   /*typelem*/ PGOid(1016),
// 		   /*typarray*/ PGOid(1),
// 		   /*typinput*/ PGOid(1),
// 		   /*typoutput*/ PGOid(1),
// 		   /*typreceive*/ PGOid(1),
// 		   /*typsend*/ PGOid(1),
// 		   /*typmodin*/ PGOid(1),
// 		   /*typmodout*/ PGOid(1),
// 		   /*typanalyze*/ PGOid(1),
// 		   /*typalign*/ 'i',
// 		   /*typstorage*/ 'p',
// 		   /*typnotnull*/ false,
// 		   /*typbasetype*/ PGOid(1),
// 		   /*typtypmod*/ PGOid(1),
// 		   /*typndims*/ PGOid(1),
// 		   /*typcollation*/ PGOid(1)})};

// std::pair<PGOid, PGTypePtr> TypeProvider::TYPE_UINT128
//     = {PGOid(++TypeProvider::TYPE_OID_ID),
//        std::make_shared<Form_pg_type>(Form_pg_type{
//            /*oid*/ PGOid(TypeProvider::TYPE_OID_ID),
//            /*typname*/ getTypeName(TypeIndex::UInt128),
// 		   /*typnamespace*/ PGOid(1),
// 		   /*typowner*/ PGOid(1),
// 		   /*typlen*/ 16,
// 		   /*typbyval*/ true,
// 		   /*typtype*/ 'b',
// 		   /*typcategory*/ 'A',
// 		   /*typispreferred*/ true,
// 		   /*typisdefined*/ false,
// 		   /*typdelim*/ ',',
// 		   /*typrelid*/ PGOid(0),
// 		   /*typelem*/ PGOid(1),
// 		   /*typarray*/ PGOid(1),
// 		   /*typinput*/ PGOid(1),
// 		   /*typoutput*/ PGOid(1),
// 		   /*typreceive*/ PGOid(1),
// 		   /*typsend*/ PGOid(1),
// 		   /*typmodin*/ PGOid(1),
// 		   /*typmodout*/ PGOid(1),
// 		   /*typanalyze*/ PGOid(1),
// 		   /*typalign*/ 'i',
// 		   /*typstorage*/ 'p',
// 		   /*typnotnull*/ false,
// 		   /*typbasetype*/ PGOid(1),
// 		   /*typtypmod*/ PGOid(1),
// 		   /*typndims*/ PGOid(1),
// 		   /*typcollation*/ PGOid(1)})};

// std::pair<PGOid, PGTypePtr> TypeProvider::TYPE_UINT256
//     = {PGOid(++TypeProvider::TYPE_OID_ID),
//        std::make_shared<Form_pg_type>(Form_pg_type{
//            /*oid*/ PGOid(TypeProvider::TYPE_OID_ID),
//            /*typname*/ getTypeName(TypeIndex::UInt256),
// 		   /*typnamespace*/ PGOid(1),
// 		   /*typowner*/ PGOid(1),
// 		   /*typlen*/ 32,
// 		   /*typbyval*/ true,
// 		   /*typtype*/ 'b',
// 		   /*typcategory*/ 'A',
// 		   /*typispreferred*/ true,
// 		   /*typisdefined*/ false,
// 		   /*typdelim*/ ',',
// 		   /*typrelid*/ PGOid(0),
// 		   /*typelem*/ PGOid(1),
// 		   /*typarray*/ PGOid(1),
// 		   /*typinput*/ PGOid(1),
// 		   /*typoutput*/ PGOid(1),
// 		   /*typreceive*/ PGOid(1),
// 		   /*typsend*/ PGOid(1),
// 		   /*typmodin*/ PGOid(1),
// 		   /*typmodout*/ PGOid(1),
// 		   /*typanalyze*/ PGOid(1),
// 		   /*typalign*/ 'i',
// 		   /*typstorage*/ 'p',
// 		   /*typnotnull*/ false,
// 		   /*typbasetype*/ PGOid(1),
// 		   /*typtypmod*/ PGOid(1),
// 		   /*typndims*/ PGOid(1),
// 		   /*typcollation*/ PGOid(1)})};

// std::pair<PGOid, PGTypePtr> TypeProvider::TYPE_INT8
//     = {PGOid(18),
//        std::make_shared<Form_pg_type>(Form_pg_type{
//            /*oid*/ PGOid(18),
//            /*typname*/ getTypeName(TypeIndex::Int8),
// 		   /*typnamespace*/ PGOid(1),
// 		   /*typowner*/ PGOid(1),
// 		   /*typlen*/ 1,
// 		   /*typbyval*/ true,
// 		   /*typtype*/ 'b',
// 		   /*typcategory*/ 'S',
// 		   /*typispreferred*/ false,
// 		   /*typisdefined*/ true,
// 		   /*typdelim*/ ',',
// 		   /*typrelid*/ PGOid(0),
// 		   /*typelem*/ PGOid(1002),
// 		   /*typarray*/ PGOid(1),
// 		   /*typinput*/ PGOid(1),
// 		   /*typoutput*/ PGOid(1),
// 		   /*typreceive*/ PGOid(1),
// 		   /*typsend*/ PGOid(1),
// 		   /*typmodin*/ PGOid(1),
// 		   /*typmodout*/ PGOid(1),
// 		   /*typanalyze*/ PGOid(1),
// 		   /*typalign*/ 'i',
// 		   /*typstorage*/ 'p',
// 		   /*typnotnull*/ false,
// 		   /*typbasetype*/ PGOid(1),
// 		   /*typtypmod*/ PGOid(1),
// 		   /*typndims*/ PGOid(1),
// 		   /*typcollation*/ PGOid(1),
// 		   /*lt_opr*/ PGOid(0),
// 		   /*eq_opr*/ PGOid(0),
// 		   /*gt_opr*/ PGOid(0),
// 		   /*hash_proc*/ PGOid(0)})};

std::pair<PGOid, PGTypePtr> TypeProvider::TYPE_INT16
    = {PGOid(21),
       std::make_shared<Form_pg_type>(Form_pg_type{
           /*oid*/ PGOid(21),
           /*typname*/ getTypeName(TypeIndex::Int16),
		   /*typnamespace*/ PGOid(1),
		   /*typowner*/ PGOid(1),
		   /*typlen*/ 2,
		   /*typbyval*/ true,
		   /*typtype*/ 'b',
		   /*typcategory*/ 'N',
		   /*typispreferred*/ false,
		   /*typisdefined*/ true,
		   /*typdelim*/ ',',
		   /*typrelid*/ PGOid(0),
		   /*typelem*/ PGOid(1005),
		   /*typarray*/ PGOid(1),
		   /*typinput*/ PGOid(1),
		   /*typoutput*/ PGOid(1),
		   /*typreceive*/ PGOid(1),
		   /*typsend*/ PGOid(1),
		   /*typmodin*/ PGOid(1),
		   /*typmodout*/ PGOid(1),
		   /*typanalyze*/ PGOid(1),
		   /*typalign*/ 'i',
		   /*typstorage*/ 'p',
		   /*typnotnull*/ false,
		   /*typbasetype*/ PGOid(1),
		   /*typtypmod*/ PGOid(1),
		   /*typndims*/ PGOid(1),
		   /*typcollation*/ PGOid(1),
		   /*lt_opr*/ PGOid(95),
		   /*eq_opr*/ PGOid(94),
		   /*gt_opr*/ PGOid(520),
		   /*hash_proc*/ PGOid(94),
           /*cmp_proc*/ PGOid(0)})};

std::pair<PGOid, PGTypePtr> TypeProvider::TYPE_INT32
    = {PGOid(23),
       std::make_shared<Form_pg_type>(Form_pg_type{
           /*oid*/ PGOid(23),
           /*typname*/ getTypeName(TypeIndex::Int32),
		   /*typnamespace*/ PGOid(1),
		   /*typowner*/ PGOid(1),
		   /*typlen*/ 4,
		   /*typbyval*/ true,
		   /*typtype*/ 'b',
		   /*typcategory*/ 'N',
		   /*typispreferred*/ false,
		   /*typisdefined*/ true,
		   /*typdelim*/ ',',
		   /*typrelid*/ PGOid(0),
		   /*typelem*/ PGOid(1007),
		   /*typarray*/ PGOid(1),
		   /*typinput*/ PGOid(1),
		   /*typoutput*/ PGOid(1),
		   /*typreceive*/ PGOid(1),
		   /*typsend*/ PGOid(1),
		   /*typmodin*/ PGOid(1),
		   /*typmodout*/ PGOid(1),
		   /*typanalyze*/ PGOid(1),
		   /*typalign*/ 'i',
		   /*typstorage*/ 'p',
		   /*typnotnull*/ false,
		   /*typbasetype*/ PGOid(1),
		   /*typtypmod*/ PGOid(1),
		   /*typndims*/ PGOid(1),
		   /*typcollation*/ PGOid(1),
		   /*lt_opr*/ PGOid(97),
		   /*eq_opr*/ PGOid(96),
		   /*gt_opr*/ PGOid(521),
		   /*hash_proc*/ PGOid(96),
           /*cmp_proc*/ PGOid(0)})};

std::pair<PGOid, PGTypePtr> TypeProvider::TYPE_INT64
    = {PGOid(20),
       std::make_shared<Form_pg_type>(Form_pg_type{
           /*oid*/ PGOid(20),
           /*typname*/ getTypeName(TypeIndex::Int64),
		   /*typnamespace*/ PGOid(1),
		   /*typowner*/ PGOid(1),
		   /*typlen*/ 8,
		   /*typbyval*/ true,
		   /*typtype*/ 'b',
		   /*typcategory*/ 'N',
		   /*typispreferred*/ false,
		   /*typisdefined*/ true,
		   /*typdelim*/ ',',
		   /*typrelid*/ PGOid(0),
		   /*typelem*/ PGOid(1016),
		   /*typarray*/ PGOid(1),
		   /*typinput*/ PGOid(1),
		   /*typoutput*/ PGOid(1),
		   /*typreceive*/ PGOid(1),
		   /*typsend*/ PGOid(1),
		   /*typmodin*/ PGOid(1),
		   /*typmodout*/ PGOid(1),
		   /*typanalyze*/ PGOid(1),
		   /*typalign*/ 'i',
		   /*typstorage*/ 'p',
		   /*typnotnull*/ false,
		   /*typbasetype*/ PGOid(1),
		   /*typtypmod*/ PGOid(1),
		   /*typndims*/ PGOid(1),
		   /*typcollation*/ PGOid(1),
		   /*lt_opr*/ PGOid(412),
		   /*eq_opr*/ PGOid(410),
		   /*gt_opr*/ PGOid(413),
		   /*hash_proc*/ PGOid(410),
           /*cmp_proc*/ PGOid(0)})};

// std::pair<PGOid, PGTypePtr> TypeProvider::TYPE_INT128
//     = {PGOid(++TypeProvider::TYPE_OID_ID),
//        std::make_shared<Form_pg_type>(Form_pg_type{
//            /*oid*/ PGOid(TypeProvider::TYPE_OID_ID),
//            /*typname*/ getTypeName(TypeIndex::Int128),
// 		   /*typnamespace*/ PGOid(1),
// 		   /*typowner*/ PGOid(1),
// 		   /*typlen*/ 16,
// 		   /*typbyval*/ true,
// 		   /*typtype*/ 'b',
// 		   /*typcategory*/ 'A',
// 		   /*typispreferred*/ true,
// 		   /*typisdefined*/ false,
// 		   /*typdelim*/ ',',
// 		   /*typrelid*/ PGOid(0),
// 		   /*typelem*/ PGOid(1),
// 		   /*typarray*/ PGOid(1),
// 		   /*typinput*/ PGOid(1),
// 		   /*typoutput*/ PGOid(1),
// 		   /*typreceive*/ PGOid(1),
// 		   /*typsend*/ PGOid(1),
// 		   /*typmodin*/ PGOid(1),
// 		   /*typmodout*/ PGOid(1),
// 		   /*typanalyze*/ PGOid(1),
// 		   /*typalign*/ 'i',
// 		   /*typstorage*/ 'p',
// 		   /*typnotnull*/ false,
// 		   /*typbasetype*/ PGOid(1),
// 		   /*typtypmod*/ PGOid(1),
// 		   /*typndims*/ PGOid(1),
// 		   /*typcollation*/ PGOid(1)})};

// std::pair<PGOid, PGTypePtr> TypeProvider::TYPE_INT256
//     = {PGOid(++TypeProvider::TYPE_OID_ID),
//        std::make_shared<Form_pg_type>(Form_pg_type{
//            /*oid*/ PGOid(TypeProvider::TYPE_OID_ID),
//            /*typname*/ getTypeName(TypeIndex::Int256),
// 		   /*typnamespace*/ PGOid(1),
// 		   /*typowner*/ PGOid(1),
// 		   /*typlen*/ 32,
// 		   /*typbyval*/ true,
// 		   /*typtype*/ 'b',
// 		   /*typcategory*/ 'A',
// 		   /*typispreferred*/ true,
// 		   /*typisdefined*/ false,
// 		   /*typdelim*/ ',',
// 		   /*typrelid*/ PGOid(0),
// 		   /*typelem*/ PGOid(1),
// 		   /*typarray*/ PGOid(1),
// 		   /*typinput*/ PGOid(1),
// 		   /*typoutput*/ PGOid(1),
// 		   /*typreceive*/ PGOid(1),
// 		   /*typsend*/ PGOid(1),
// 		   /*typmodin*/ PGOid(1),
// 		   /*typmodout*/ PGOid(1),
// 		   /*typanalyze*/ PGOid(1),
// 		   /*typalign*/ 'i',
// 		   /*typstorage*/ 'p',
// 		   /*typnotnull*/ false,
// 		   /*typbasetype*/ PGOid(1),
// 		   /*typtypmod*/ PGOid(1),
// 		   /*typndims*/ PGOid(1),
// 		   /*typcollation*/ PGOid(1)})};

std::pair<PGOid, PGTypePtr> TypeProvider::TYPE_STRING
    = {PGOid(25),
       std::make_shared<Form_pg_type>(Form_pg_type{
           /*oid*/ PGOid(25),
           /*typname*/ getTypeName(TypeIndex::String),
		   /*typnamespace*/ PGOid(1),
		   /*typowner*/ PGOid(1),
		   /*typlen*/ -1,
		   /*typbyval*/ false,
		   /*typtype*/ 'b',
		   /*typcategory*/ 'S',
		   /*typispreferred*/ true,
		   /*typisdefined*/ true,
		   /*typdelim*/ ',',
		   /*typrelid*/ PGOid(0),
		   /*typelem*/ PGOid(1009),
		   /*typarray*/ PGOid(1),
		   /*typinput*/ PGOid(1),
		   /*typoutput*/ PGOid(1),
		   /*typreceive*/ PGOid(1),
		   /*typsend*/ PGOid(1),
		   /*typmodin*/ PGOid(1),
		   /*typmodout*/ PGOid(1),
		   /*typanalyze*/ PGOid(1),
		   /*typalign*/ 'i',
		   /*typstorage*/ 'p',
		   /*typnotnull*/ false,
		   /*typbasetype*/ PGOid(1),
		   /*typtypmod*/ PGOid(1),
		   /*typndims*/ PGOid(1),
		   /*typcollation*/ PGOid(1),
		   /*lt_opr*/ PGOid(664),
		   /*eq_opr*/ PGOid(98),
		   /*gt_opr*/ PGOid(666),
		   /*hash_proc*/ PGOid(98),
           /*cmp_proc*/ PGOid(0)})};

std::pair<PGOid, PGTypePtr> TypeProvider::TYPE_FIXEDSTRING
    = {PGOid(1042),
       std::make_shared<Form_pg_type>(Form_pg_type{
           /*oid*/ PGOid(1042),
           /*typname*/ getTypeName(TypeIndex::FixedString),
		   /*typnamespace*/ PGOid(1),
		   /*typowner*/ PGOid(1),
		   /*typlen*/ -1,
		   /*typbyval*/ false,
		   /*typtype*/ 'b',
		   /*typcategory*/ 'S',
		   /*typispreferred*/ false,
		   /*typisdefined*/ true,
		   /*typdelim*/ ',',
		   /*typrelid*/ PGOid(0),
		   /*typelem*/ PGOid(1014),
		   /*typarray*/ PGOid(1),
		   /*typinput*/ PGOid(1),
		   /*typoutput*/ PGOid(1),
		   /*typreceive*/ PGOid(1),
		   /*typsend*/ PGOid(1),
		   /*typmodin*/ PGOid(1),
		   /*typmodout*/ PGOid(1),
		   /*typanalyze*/ PGOid(1),
		   /*typalign*/ 'i',
		   /*typstorage*/ 'p',
		   /*typnotnull*/ false,
		   /*typbasetype*/ PGOid(1),
		   /*typtypmod*/ PGOid(1),
		   /*typndims*/ PGOid(1),
		   /*typcollation*/ PGOid(1),
		   /*lt_opr*/ PGOid(1058),
		   /*eq_opr*/ PGOid(1054),
		   /*gt_opr*/ PGOid(1060),
		   /*hash_proc*/ PGOid(1054),
           /*cmp_proc*/ PGOid(0)})};

std::pair<PGOid, PGTypePtr> TypeProvider::TYPE_DATE
    = {PGOid(1082),
       std::make_shared<Form_pg_type>(Form_pg_type{
           /*oid*/ PGOid(1082),
           /*typname*/ getTypeName(TypeIndex::Date),
		   /*typnamespace*/ PGOid(1),
		   /*typowner*/ PGOid(1),
		   /*typlen*/ 4,
		   /*typbyval*/ true,
		   /*typtype*/ 'b',
		   /*typcategory*/ 'D',
		   /*typispreferred*/ true,
		   /*typisdefined*/ false,
		   /*typdelim*/ ',',
		   /*typrelid*/ PGOid(0),
		   /*typelem*/ PGOid(1182),
		   /*typarray*/ PGOid(1),
		   /*typinput*/ PGOid(1),
		   /*typoutput*/ PGOid(1),
		   /*typreceive*/ PGOid(1),
		   /*typsend*/ PGOid(1),
		   /*typmodin*/ PGOid(1),
		   /*typmodout*/ PGOid(1),
		   /*typanalyze*/ PGOid(1),
		   /*typalign*/ 'i',
		   /*typstorage*/ 'p',
		   /*typnotnull*/ false,
		   /*typbasetype*/ PGOid(1),
		   /*typtypmod*/ PGOid(1),
		   /*typndims*/ PGOid(1),
		   /*typcollation*/ PGOid(1),
		   /*lt_opr*/ PGOid(1095),
		   /*eq_opr*/ PGOid(1093),
		   /*gt_opr*/ PGOid(1097),
		   /*hash_proc*/ PGOid(1093),
           /*cmp_proc*/ PGOid(0)})};

std::pair<PGOid, PGTypePtr> TypeProvider::TYPE_DATETIME
    = {PGOid(1114),
       std::make_shared<Form_pg_type>(Form_pg_type{
           /*oid*/ PGOid(1114),
           /*typname*/ getTypeName(TypeIndex::DateTime),
		   /*typnamespace*/ PGOid(1),
		   /*typowner*/ PGOid(1),
		   /*typlen*/ 8,
		   /*typbyval*/ true,
		   /*typtype*/ 'b',
		   /*typcategory*/ 'D',
		   /*typispreferred*/ true,
		   /*typisdefined*/ false,
		   /*typdelim*/ ',',
		   /*typrelid*/ PGOid(0),
		   /*typelem*/ PGOid(1115),
		   /*typarray*/ PGOid(1),
		   /*typinput*/ PGOid(1),
		   /*typoutput*/ PGOid(1),
		   /*typreceive*/ PGOid(1),
		   /*typsend*/ PGOid(1),
		   /*typmodin*/ PGOid(1),
		   /*typmodout*/ PGOid(1),
		   /*typanalyze*/ PGOid(1),
		   /*typalign*/ 'i',
		   /*typstorage*/ 'p',
		   /*typnotnull*/ false,
		   /*typbasetype*/ PGOid(1),
		   /*typtypmod*/ PGOid(1),
		   /*typndims*/ PGOid(1),
		   /*typcollation*/ PGOid(1),
		   /*lt_opr*/ PGOid(2062),
		   /*eq_opr*/ PGOid(2060),
		   /*gt_opr*/ PGOid(2064),
		   /*hash_proc*/ PGOid(2060),
           /*cmp_proc*/ PGOid(0)})};

std::pair<PGOid, PGTypePtr> TypeProvider::TYPE_DATETIME64
    = {PGOid(1184),
       std::make_shared<Form_pg_type>(Form_pg_type{
           /*oid*/ PGOid(1184),
           /*typname*/ getTypeName(TypeIndex::DateTime64),
		   /*typnamespace*/ PGOid(1),
		   /*typowner*/ PGOid(1),
		   /*typlen*/ 8,
		   /*typbyval*/ true,
		   /*typtype*/ 'b',
		   /*typcategory*/ 'D',
		   /*typispreferred*/ true,
		   /*typisdefined*/ false,
		   /*typdelim*/ ',',
		   /*typrelid*/ PGOid(0),
		   /*typelem*/ PGOid(1185),
		   /*typarray*/ PGOid(1),
		   /*typinput*/ PGOid(1),
		   /*typoutput*/ PGOid(1),
		   /*typreceive*/ PGOid(1),
		   /*typsend*/ PGOid(1),
		   /*typmodin*/ PGOid(1),
		   /*typmodout*/ PGOid(1),
		   /*typanalyze*/ PGOid(1),
		   /*typalign*/ 'i',
		   /*typstorage*/ 'p',
		   /*typnotnull*/ false,
		   /*typbasetype*/ PGOid(1),
		   /*typtypmod*/ PGOid(1),
		   /*typndims*/ PGOid(1),
		   /*typcollation*/ PGOid(1),
		   /*lt_opr*/ PGOid(1322),
		   /*eq_opr*/ PGOid(1320),
		   /*gt_opr*/ PGOid(1324),
		   /*hash_proc*/ PGOid(1320),
           /*cmp_proc*/ PGOid(0)})};

// std::pair<PGOid, PGTypePtr> TypeProvider::TYPE_ARRAY
//     = {PGOid(++TypeProvider::TYPE_OID_ID),
//        std::make_shared<Form_pg_type>(Form_pg_type{
//            /*oid*/ PGOid(TypeProvider::TYPE_OID_ID),
//            /*typname*/ getTypeName(TypeIndex::Array),
// 		   /*typnamespace*/ PGOid(1),
// 		   /*typowner*/ PGOid(1),
// 		   /*typlen*/ 32,
// 		   /*typbyval*/ true,
// 		   /*typtype*/ 'b',
// 		   /*typcategory*/ 'A',
// 		   /*typispreferred*/ true,
// 		   /*typisdefined*/ false,
// 		   /*typdelim*/ ',',
// 		   /*typrelid*/ PGOid(0),
// 		   /*typelem*/ PGOid(1),
// 		   /*typarray*/ PGOid(1),
// 		   /*typinput*/ PGOid(1),
// 		   /*typoutput*/ PGOid(1),
// 		   /*typreceive*/ PGOid(1),
// 		   /*typsend*/ PGOid(1),
// 		   /*typmodin*/ PGOid(1),
// 		   /*typmodout*/ PGOid(1),
// 		   /*typanalyze*/ PGOid(1),
// 		   /*typalign*/ 'i',
// 		   /*typstorage*/ 'p',
// 		   /*typnotnull*/ false,
// 		   /*typbasetype*/ PGOid(1),
// 		   /*typtypmod*/ PGOid(1),
// 		   /*typndims*/ PGOid(1),
// 		   /*typcollation*/ PGOid(1)})};

// std::pair<PGOid, PGTypePtr> TypeProvider::TYPE_TUPLE
//     = {PGOid(++TypeProvider::TYPE_OID_ID),
//        std::make_shared<Form_pg_type>(Form_pg_type{
//            /*oid*/ PGOid(TypeProvider::TYPE_OID_ID),
//            /*typname*/ getTypeName(TypeIndex::Tuple),
// 		   /*typnamespace*/ PGOid(1),
// 		   /*typowner*/ PGOid(1),
// 		   /*typlen*/ 32,
// 		   /*typbyval*/ true,
// 		   /*typtype*/ 'b',
// 		   /*typcategory*/ 'A',
// 		   /*typispreferred*/ true,
// 		   /*typisdefined*/ false,
// 		   /*typdelim*/ ',',
// 		   /*typrelid*/ PGOid(0),
// 		   /*typelem*/ PGOid(1),
// 		   /*typarray*/ PGOid(1),
// 		   /*typinput*/ PGOid(1),
// 		   /*typoutput*/ PGOid(1),
// 		   /*typreceive*/ PGOid(1),
// 		   /*typsend*/ PGOid(1),
// 		   /*typmodin*/ PGOid(1),
// 		   /*typmodout*/ PGOid(1),
// 		   /*typanalyze*/ PGOid(1),
// 		   /*typalign*/ 'i',
// 		   /*typstorage*/ 'p',
// 		   /*typnotnull*/ false,
// 		   /*typbasetype*/ PGOid(1),
// 		   /*typtypmod*/ PGOid(1),
// 		   /*typndims*/ PGOid(1),
// 		   /*typcollation*/ PGOid(1)})};

// std::pair<PGOid, PGTypePtr> TypeProvider::TYPE_DECIMAL32
//     = {PGOid(++TypeProvider::TYPE_OID_ID),
//        std::make_shared<Form_pg_type>(Form_pg_type{
//            /*oid*/ PGOid(TypeProvider::TYPE_OID_ID),
//            /*typname*/ getTypeName(TypeIndex::Decimal32),
// 		   /*typnamespace*/ PGOid(1),
// 		   /*typowner*/ PGOid(1),
// 		   /*typlen*/ 4,
// 		   /*typbyval*/ true,
// 		   /*typtype*/ 'b',
// 		   /*typcategory*/ 'A',
// 		   /*typispreferred*/ true,
// 		   /*typisdefined*/ false,
// 		   /*typdelim*/ ',',
// 		   /*typrelid*/ PGOid(0),
// 		   /*typelem*/ PGOid(1),
// 		   /*typarray*/ PGOid(1),
// 		   /*typinput*/ PGOid(1),
// 		   /*typoutput*/ PGOid(1),
// 		   /*typreceive*/ PGOid(1),
// 		   /*typsend*/ PGOid(1),
// 		   /*typmodin*/ PGOid(1),
// 		   /*typmodout*/ PGOid(1),
// 		   /*typanalyze*/ PGOid(1),
// 		   /*typalign*/ 'i',
// 		   /*typstorage*/ 'p',
// 		   /*typnotnull*/ false,
// 		   /*typbasetype*/ PGOid(1),
// 		   /*typtypmod*/ PGOid(1),
// 		   /*typndims*/ PGOid(1),
// 		   /*typcollation*/ PGOid(1)})};

std::pair<PGOid, PGTypePtr> TypeProvider::TYPE_DECIMAL64
    = {PGOid(1700),
       std::make_shared<Form_pg_type>(Form_pg_type{
           /*oid*/ PGOid(1700),
           /*typname*/ getTypeName(TypeIndex::Decimal64),
		   /*typnamespace*/ PGOid(1),
		   /*typowner*/ PGOid(1),
		   /*typlen*/ 8,
		   /*typbyval*/ true,
		   /*typtype*/ 'b',
		   /*typcategory*/ 'N',
		   /*typispreferred*/ false,
		   /*typisdefined*/ true,
		   /*typdelim*/ ',',
		   /*typrelid*/ PGOid(0),
		   /*typelem*/ PGOid(0),
		   /*typarray*/ PGOid(1231),
		   /*typinput*/ PGOid(1),
		   /*typoutput*/ PGOid(1),
		   /*typreceive*/ PGOid(1),
		   /*typsend*/ PGOid(1),
		   /*typmodin*/ PGOid(1),
		   /*typmodout*/ PGOid(1),
		   /*typanalyze*/ PGOid(1),
		   /*typalign*/ 'i',
		   /*typstorage*/ 'm',
		   /*typnotnull*/ false,
		   /*typbasetype*/ PGOid(1),
		   /*typtypmod*/ PGOid(1),
		   /*typndims*/ PGOid(1),
		   /*typcollation*/ PGOid(1),
		   /*lt_opr*/ PGOid(1754),
		   /*eq_opr*/ PGOid(1752),
		   /*gt_opr*/ PGOid(1756),
		   /*hash_proc*/ PGOid(1752),
           /*cmp_proc*/ PGOid(0)})};

// std::pair<PGOid, PGTypePtr> TypeProvider::TYPE_DECIMAL128
//     = {PGOid(++TypeProvider::TYPE_OID_ID),
//        std::make_shared<Form_pg_type>(Form_pg_type{
//            /*oid*/ PGOid(TypeProvider::TYPE_OID_ID),
//            /*typname*/ getTypeName(TypeIndex::Decimal128),
// 		   /*typnamespace*/ PGOid(1),
// 		   /*typowner*/ PGOid(1),
// 		   /*typlen*/ 16,
// 		   /*typbyval*/ true,
// 		   /*typtype*/ 'b',
// 		   /*typcategory*/ 'A',
// 		   /*typispreferred*/ true,
// 		   /*typisdefined*/ false,
// 		   /*typdelim*/ ',',
// 		   /*typrelid*/ PGOid(0),
// 		   /*typelem*/ PGOid(1),
// 		   /*typarray*/ PGOid(1),
// 		   /*typinput*/ PGOid(1),
// 		   /*typoutput*/ PGOid(1),
// 		   /*typreceive*/ PGOid(1),
// 		   /*typsend*/ PGOid(1),
// 		   /*typmodin*/ PGOid(1),
// 		   /*typmodout*/ PGOid(1),
// 		   /*typanalyze*/ PGOid(1),
// 		   /*typalign*/ 'i',
// 		   /*typstorage*/ 'p',
// 		   /*typnotnull*/ false,
// 		   /*typbasetype*/ PGOid(1),
// 		   /*typtypmod*/ PGOid(1),
// 		   /*typndims*/ PGOid(1),
// 		   /*typcollation*/ PGOid(1)})};

// std::pair<PGOid, PGTypePtr> TypeProvider::TYPE_DECIMAL256
//     = {PGOid(++TypeProvider::TYPE_OID_ID),
//        std::make_shared<Form_pg_type>(Form_pg_type{
//            /*oid*/ PGOid(TypeProvider::TYPE_OID_ID),
//            /*typname*/ getTypeName(TypeIndex::Decimal256),
// 		   /*typnamespace*/ PGOid(1),
// 		   /*typowner*/ PGOid(1),
// 		   /*typlen*/ 32,
// 		   /*typbyval*/ true,
// 		   /*typtype*/ 'b',
// 		   /*typcategory*/ 'A',
// 		   /*typispreferred*/ true,
// 		   /*typisdefined*/ false,
// 		   /*typdelim*/ ',',
// 		   /*typrelid*/ PGOid(0),
// 		   /*typelem*/ PGOid(1),
// 		   /*typarray*/ PGOid(1),
// 		   /*typinput*/ PGOid(1),
// 		   /*typoutput*/ PGOid(1),
// 		   /*typreceive*/ PGOid(1),
// 		   /*typsend*/ PGOid(1),
// 		   /*typmodin*/ PGOid(1),
// 		   /*typmodout*/ PGOid(1),
// 		   /*typanalyze*/ PGOid(1),
// 		   /*typalign*/ 'i',
// 		   /*typstorage*/ 'p',
// 		   /*typnotnull*/ false,
// 		   /*typbasetype*/ PGOid(1),
// 		   /*typtypmod*/ PGOid(1),
// 		   /*typndims*/ PGOid(1),
// 		   /*typcollation*/ PGOid(1)})};

// std::pair<PGOid, PGTypePtr> TypeProvider::TYPE_AGGFUNCSTATE
//     = {PGOid(++TypeProvider::TYPE_OID_ID),
//        std::make_shared<Form_pg_type>(Form_pg_type{
//            /*oid*/ PGOid(TypeProvider::TYPE_OID_ID),
//            /*typname*/ getTypeName(TypeIndex::AggregateFunction),
// 		   /*typnamespace*/ PGOid(1),
// 		   /*typowner*/ PGOid(1),
// 		   /*typlen*/ 32,
// 		   /*typbyval*/ true,
// 		   /*typtype*/ 'b',
// 		   /*typcategory*/ 'A',
// 		   /*typispreferred*/ true,
// 		   /*typisdefined*/ false,
// 		   /*typdelim*/ ',',
// 		   /*typrelid*/ PGOid(0),
// 		   /*typelem*/ PGOid(1),
// 		   /*typarray*/ PGOid(1),
// 		   /*typinput*/ PGOid(1),
// 		   /*typoutput*/ PGOid(1),
// 		   /*typreceive*/ PGOid(1),
// 		   /*typsend*/ PGOid(1),
// 		   /*typmodin*/ PGOid(1),
// 		   /*typmodout*/ PGOid(1),
// 		   /*typanalyze*/ PGOid(1),
// 		   /*typalign*/ 'i',
// 		   /*typstorage*/ 'p',
// 		   /*typnotnull*/ false,
// 		   /*typbasetype*/ PGOid(1),
// 		   /*typtypmod*/ PGOid(1),
// 		   /*typndims*/ PGOid(1),
// 		   /*typcollation*/ PGOid(1)})};

// std::pair<PGOid, PGTypePtr> TypeProvider::TYPE_MAP
//     = {PGOid(++TypeProvider::TYPE_OID_ID),
//        std::make_shared<Form_pg_type>(Form_pg_type{
//            /*oid*/ PGOid(TypeProvider::TYPE_OID_ID),
//            /*typname*/ getTypeName(TypeIndex::Map),
// 		   /*typnamespace*/ PGOid(1),
// 		   /*typowner*/ PGOid(1),
// 		   /*typlen*/ 32,
// 		   /*typbyval*/ true,
// 		   /*typtype*/ 'b',
// 		   /*typcategory*/ 'A',
// 		   /*typispreferred*/ true,
// 		   /*typisdefined*/ false,
// 		   /*typdelim*/ ',',
// 		   /*typrelid*/ PGOid(0),
// 		   /*typelem*/ PGOid(1),
// 		   /*typarray*/ PGOid(1),
// 		   /*typinput*/ PGOid(1),
// 		   /*typoutput*/ PGOid(1),
// 		   /*typreceive*/ PGOid(1),
// 		   /*typsend*/ PGOid(1),
// 		   /*typmodin*/ PGOid(1),
// 		   /*typmodout*/ PGOid(1),
// 		   /*typanalyze*/ PGOid(1),
// 		   /*typalign*/ 'i',
// 		   /*typstorage*/ 'p',
// 		   /*typnotnull*/ false,
// 		   /*typbasetype*/ PGOid(1),
// 		   /*typtypmod*/ PGOid(1),
// 		   /*typndims*/ PGOid(1),
// 		   /*typcollation*/ PGOid(1)})};

// std::pair<PGOid, PGTypePtr> TypeProvider::TYPE_UUID
//     = {PGOid(++TypeProvider::TYPE_OID_ID),
//        std::make_shared<Form_pg_type>(Form_pg_type{
//            /*oid*/ PGOid(TypeProvider::TYPE_OID_ID),
//            /*typname*/ getTypeName(TypeIndex::UUID),
// 		   /*typnamespace*/ PGOid(1),
// 		   /*typowner*/ PGOid(1),
// 		   /*typlen*/ 32,
// 		   /*typbyval*/ true,
// 		   /*typtype*/ 'b',
// 		   /*typcategory*/ 'A',
// 		   /*typispreferred*/ true,
// 		   /*typisdefined*/ false,
// 		   /*typdelim*/ ',',
// 		   /*typrelid*/ PGOid(0),
// 		   /*typelem*/ PGOid(1),
// 		   /*typarray*/ PGOid(1),
// 		   /*typinput*/ PGOid(1),
// 		   /*typoutput*/ PGOid(1),
// 		   /*typreceive*/ PGOid(1),
// 		   /*typsend*/ PGOid(1),
// 		   /*typmodin*/ PGOid(1),
// 		   /*typmodout*/ PGOid(1),
// 		   /*typanalyze*/ PGOid(1),
// 		   /*typalign*/ 'i',
// 		   /*typstorage*/ 'p',
// 		   /*typnotnull*/ false,
// 		   /*typbasetype*/ PGOid(1),
// 		   /*typtypmod*/ PGOid(1),
// 		   /*typndims*/ PGOid(1),
// 		   /*typcollation*/ PGOid(1)})};

std::pair<PGOid, PGTypePtr> TypeProvider::TYPE_ANY
    = {PGOid(2276),
       std::make_shared<Form_pg_type>(Form_pg_type{
           /*oid*/ PGOid(2276),
           /*typname*/ "any",
		   /*typnamespace*/ PGOid(1),
		   /*typowner*/ PGOid(1),
		   /*typlen*/ 4,
		   /*typbyval*/ true,
		   /*typtype*/ 'p',
		   /*typcategory*/ 'P',
		   /*typispreferred*/ false,
		   /*typisdefined*/ true,
		   /*typdelim*/ ',',
		   /*typrelid*/ PGOid(0),
		   /*typelem*/ PGOid(0),
		   /*typarray*/ PGOid(0),
		   /*typinput*/ PGOid(1),
		   /*typoutput*/ PGOid(1),
		   /*typreceive*/ PGOid(1),
		   /*typsend*/ PGOid(1),
		   /*typmodin*/ PGOid(1),
		   /*typmodout*/ PGOid(1),
		   /*typanalyze*/ PGOid(1),
		   /*typalign*/ 'i',
		   /*typstorage*/ 'p',
		   /*typnotnull*/ false,
		   /*typbasetype*/ PGOid(0),
		   /*typtypmod*/ PGOid(0),
		   /*typndims*/ PGOid(0),
		   /*typcollation*/ PGOid(0),
		   /*lt_opr*/ PGOid(1754),
		   /*eq_opr*/ PGOid(1752),
		   /*gt_opr*/ PGOid(1756),
		   /*hash_proc*/ PGOid(1752),
           /*cmp_proc*/ PGOid(0)})};

std::pair<PGOid, PGTypePtr> TypeProvider::TYPE_INTERNAL
    = {PGOid(2281),
       std::make_shared<Form_pg_type>(Form_pg_type{
           /*oid*/ PGOid(2281),
           /*typname*/ "internal",
		   /*typnamespace*/ PGOid(1),
		   /*typowner*/ PGOid(1),
		   /*typlen*/ 8,
		   /*typbyval*/ true,
		   /*typtype*/ 'p',
		   /*typcategory*/ 'P',
		   /*typispreferred*/ false,
		   /*typisdefined*/ true,
		   /*typdelim*/ ',',
		   /*typrelid*/ PGOid(0),
		   /*typelem*/ PGOid(0),
		   /*typarray*/ PGOid(0),
		   /*typinput*/ PGOid(1),
		   /*typoutput*/ PGOid(1),
		   /*typreceive*/ InvalidOid,
		   /*typsend*/ InvalidOid,
		   /*typmodin*/ InvalidOid,
		   /*typmodout*/ InvalidOid,
		   /*typanalyze*/ InvalidOid,
		   /*typalign*/ 'd',
		   /*typstorage*/ 'P',
		   /*typnotnull*/ false,
		   /*typbasetype*/ InvalidOid,
		   /*typtypmod*/ InvalidOid,
		   /*typndims*/ InvalidOid,
		   /*typcollation*/ InvalidOid,
		   /*lt_opr*/ InvalidOid,
		   /*eq_opr*/ InvalidOid,
		   /*gt_opr*/ InvalidOid,
		   /*hash_proc*/ InvalidOid,
           /*cmp_proc*/ InvalidOid})};

std::pair<PGOid, PGTypePtr> TypeProvider::TYPE_OID
    = {PGOid(26),
       std::make_shared<Form_pg_type>(Form_pg_type{
           /*oid*/ PGOid(26),
           /*typname*/ "oid",
		   /*typnamespace*/ PGOid(1),
		   /*typowner*/ PGOid(1),
		   /*typlen*/ 4,
		   /*typbyval*/ true,
		   /*typtype*/ 'b',
		   /*typcategory*/ 'N',
		   /*typispreferred*/ true,
		   /*typisdefined*/ true,
		   /*typdelim*/ ',',
		   /*typrelid*/ PGOid(0),
		   /*typelem*/ PGOid(0),
		   /*typarray*/ PGOid(1028),
		   /*typinput*/ PGOid(1),
		   /*typoutput*/ PGOid(1),
		   /*typreceive*/ InvalidOid,
		   /*typsend*/ InvalidOid,
		   /*typmodin*/ InvalidOid,
		   /*typmodout*/ InvalidOid,
		   /*typanalyze*/ InvalidOid,
		   /*typalign*/ 'i',
		   /*typstorage*/ 'P',
		   /*typnotnull*/ false,
		   /*typbasetype*/ InvalidOid,
		   /*typtypmod*/ InvalidOid,
		   /*typndims*/ InvalidOid,
		   /*typcollation*/ InvalidOid,
		   /*lt_opr*/ InvalidOid,
		   /*eq_opr*/ InvalidOid,
		   /*gt_opr*/ InvalidOid,
		   /*hash_proc*/ InvalidOid,
           /*cmp_proc*/ InvalidOid})};

// gpdxl::CMDName *
// TypeProvider::CreateMDName(const char *name_str)
// {
// 	gpos::CWStringDynamic *str_name =
// 			gpdxl::CDXLUtils::CreateDynamicStringFromCharArray(mp, name_str);
// 	gpdxl::CMDName *mdname = GPOS_NEW(mp) gpdxl::CMDName(mp, str_name);

// 	// cleanup
// 	GPOS_DELETE(str_name);
// 	return mdname;
// }

TypeProvider::OidTypeMap TypeProvider::oid_type_map = {
    TypeProvider::TYPE_FLOAT32,
	TypeProvider::TYPE_FLOAT64,
	TypeProvider::TYPE_BOOLEAN,
	// TypeProvider::TYPE_UINT8,
	// TypeProvider::TYPE_UINT16,
	// TypeProvider::TYPE_UINT32,
	// TypeProvider::TYPE_UINT64,
	// TypeProvider::TYPE_UINT128,
	// TypeProvider::TYPE_UINT256,
	// TypeProvider::TYPE_INT8,
	TypeProvider::TYPE_INT16,
	TypeProvider::TYPE_INT32,
	TypeProvider::TYPE_INT64,
	// TypeProvider::TYPE_INT128,
	// TypeProvider::TYPE_INT256,
	TypeProvider::TYPE_STRING,
	TypeProvider::TYPE_FIXEDSTRING,
	TypeProvider::TYPE_DATE,
	TypeProvider::TYPE_DATETIME,
	TypeProvider::TYPE_DATETIME64,
	// TypeProvider::TYPE_ARRAY,
	// TypeProvider::TYPE_TUPLE,
	// TypeProvider::TYPE_DECIMAL32,
	TypeProvider::TYPE_DECIMAL64,
	// TypeProvider::TYPE_DECIMAL128,
	// TypeProvider::TYPE_DECIMAL256,
	// TypeProvider::TYPE_AGGFUNCSTATE,
	// TypeProvider::TYPE_MAP,
	// TypeProvider::TYPE_UUID
    TypeProvider::TYPE_ANY,
    TypeProvider::TYPE_INTERNAL,
    TypeProvider::TYPE_OID,
};

// TypeProvider::TypeProvider(const ContextPtr& context_) : context(context_)
// {
	// oid_type_map.insert(TYPE_FLOAT32);
	// oid_type_map.insert(TYPE_FLOAT64);
	// oid_type_map.insert(TYPE_BOOLEAN);
	// // oid_type_map.insert(TYPE_UINT8);
	// // oid_type_map.insert(TYPE_UINT16);
	// // oid_type_map.insert(TYPE_UINT32);
	// // oid_type_map.insert(TYPE_UINT64);
	// // oid_type_map.insert(TYPE_UINT128);
	// // oid_type_map.insert(TYPE_UINT256);
	// // oid_type_map.insert(TYPE_INT8);
	// oid_type_map.insert(TYPE_INT16);
	// oid_type_map.insert(TYPE_INT32);
	// oid_type_map.insert(TYPE_INT64);
	// // oid_type_map.insert(TYPE_INT128);
	// // oid_type_map.insert(TYPE_INT256);
	// oid_type_map.insert(TYPE_STRING);
	// oid_type_map.insert(TYPE_FIXEDSTRING);
	// oid_type_map.insert(TYPE_DATE);
	// oid_type_map.insert(TYPE_DATETIME);
	// oid_type_map.insert(TYPE_DATETIME64);
	// // oid_type_map.insert(TYPE_ARRAY);
	// // oid_type_map.insert(TYPE_TUPLE);
	// // oid_type_map.insert(TYPE_DECIMAL32);
	// oid_type_map.insert(TYPE_DECIMAL64);
	// // oid_type_map.insert(TYPE_DECIMAL128);
	// // oid_type_map.insert(TYPE_DECIMAL256);
	// // oid_type_map.insert(TYPE_AGGFUNCSTATE);
	// // oid_type_map.insert(TYPE_MAP);
	// // oid_type_map.insert(TYPE_UUID);
//};

// IMDTypePtr
// TypeProvider::getTypeByOID(OID oid)
// {
// 	auto it = oid_type_map.find(oid);
// 	if (it == oid_type_map.end())
// 	    return {};
// 	return it->second;
// }

// IMDTypePtr
// TypeProvider::getType(Field::Types::Which which)
// {
// 	return nullptr;
// 	switch (which)
// 	{
// 	case Null:
// 		return 
//     case UInt64:
// 		return TYPE_UINT64.second;
//     case Int64:
// 		return TYPE_INT64.second;
//     case Float64:
// 		return TYPE_FLOAT64.second;
//     case UInt128:
// 		return TYPE_UINT128.second;
//     case Int128:
// 		return TYPE_INT128.second;
//     case String:
// 		return TYPE_STRING.second;
// 	case FixedString:
// 		return TYPE_FIXEDSTRING.second;
// 	case Date:
// 		return TYPE_DATE.second;
// 	case DateTime:
// 		return TYPE_DATETIME.second;
// 	case DateTime64:
// 		return TYPE_DATETIME64.second;
//     case Array:
// 		return TYPE_ARRAY.second;
//     case Tuple:
// 		return TYPE_TUPLE.second;
//     case Decimal32:
// 		return TYPE_DECIMAL32.second;
//     case Decimal64:
// 		return TYPE_DECIMAL64.second;
//     case Decimal128:
// 		return TYPE_DECIMAL128.second;
//     //case AggregateFunctionState:
// 		//return TYPE_AGGFUNCSTATE.second;
//     case Decimal256:
// 		return TYPE_DECIMAL256.second;
//     case UInt256:
// 		return TYPE_UINT256.second;
//     case Int256:
// 		return TYPE_INT256.second;
//     case Map:
// 		return TYPE_MAP.second;
//     case UUID:
// 		return TYPE_UUID.second;
// 	default:
// 		throw Exception("Unsupported type yet.", ErrorCodes::SYNTAX_ERROR);
// 	}
// };

PGTypePtr TypeProvider::getTypeByOid(PGOid oid)
{
	auto it = oid_type_map.find(oid);
	if (it == oid_type_map.end())
	    return nullptr;
	return it->second;
};

bool TypeProvider::TypeExists(PGOid oid)
{
    auto it = oid_type_map.find(oid);
	if (it == oid_type_map.end())
	    return false;
	return true;
};

std::optional<std::string> TypeProvider::get_type_name(PGOid oid)
{
    auto type = getTypeByOid(oid);
    if (type != nullptr)
    {
        return type->typname;
    }
    return std::nullopt;
};

PGTypePtr TypeProvider::get_type_by_typename_namespaceoid(const std::string& type_name, PGOid namespace_oid)
{
    for (auto pair : oid_type_map)
	{
		if (pair.second->typname == type_name)
		{
			return pair.second;
		}
	}
	return nullptr;
}

/*
 * getBaseType
 *		If the given type is a domain, return its base type;
 *		otherwise return the type's own OID.
 */
PGOid TypeProvider::getBaseType(PGOid typid)
{
    int32 typmod = -1;

    return getBaseTypeAndTypmod(typid, &typmod);
};

/*
 * getBaseTypeAndTypmod
 *		If the given type is a domain, return its base type and typmod;
 *		otherwise return the type's own OID, and leave *typmod unchanged.
 *
 * Note that the "applied typmod" should be -1 for every domain level
 * above the bottommost; therefore, if the passed-in typid is indeed
 * a domain, *typmod should be -1.
 */
PGOid TypeProvider::getBaseTypeAndTypmod(PGOid typid, int32 * typmod)
{
	/*
	 * We loop to find the bottom base type in a stack of domains.
	 */
    for (;;)
    {
		PGTypePtr tup = getTypeByOid(typid);
		if (tup == nullptr)
		{
			elog(ERROR, "Lookup failed for type %u", typid);
			return InvalidOid;
		}

		if (tup->typtype != TYPTYPE_DOMAIN)
		{
			break;
		}

		Assert(*typmod == -1)
		typid = tup->typbasetype;
		*typmod = tup->typtypmod;

    }

    return typid;
};

/*
 * get_type_category_preferred
 *
 *		Given the type OID, fetch its category and preferred-type status.
 *		Throws error on failure.
 */
void TypeProvider::get_type_category_preferred(PGOid typid, char * typcategory, bool * typispreferred)
{
	PGTypePtr tup = getTypeByOid(typid);
	if (tup == NULL)
	{
		elog(ERROR, "Lookup failed for type %u", typid);
		return;
	}
	*typcategory = tup->typcategory;
    *typispreferred = tup->typispreferred;
};

std::string TypeProvider::format_type_be(PGOid type_oid)
{
    return format_type_internal(type_oid, -1, false, false, false);
};

std::string TypeProvider::printTypmod(const char * typname, int32 typmod, PGOid typmodout)
{
    std::string res = "";

    /* Shouldn't be called if typmod is -1 */
    Assert(typmod >= 0)

    if (typmodout == InvalidOid)
    {
        /* Default behavior: just print the integer typmod with parens */
        //res = psprintf("%s(%d)", typname, (int)typmod);
        res = std::string(typname) + "(" + std::to_string(typmod) + ")";
    }
    else
    {
        /* Use the type-specific typmodout procedure */
        // char * tmstr;

        // tmstr = DatumGetCString(FunctionProvider::OidFunctionCall1Coll(typmodout, InvalidOid, Int32GetDatum(typmod)));
        // //res = psprintf("%s%s", typname, tmstr);
        // res = std::string(typname) + std::string(tmstr);

        res = std::string(typname) + "(typmodout: " + std::to_string(typmodout) + ")";
    }

    return res;
};

bool TypeProvider::TypeIsVisible(PGOid typid)
{
    //HeapTuple typtup;
    //Form_pg_type typform;
    //PGOid typnamespace;
    bool visible = true;

    PGTypePtr typtup = getTypeByOid(typid);
    if (typtup == nullptr)
	{
        elog(ERROR, "Lookup failed for type %u", typid);
		return false;
	}

    //recomputeNamespacePath();

    // /*
	//  * Quick check: if it ain't in the path at all, it ain't visible. Items in
	//  * the system namespace are surely in the path and so we needn't even do
	//  * list_member_oid() for them.
	//  */
    // typnamespace = typtup->typnamespace;
    // if (typnamespace != PG_CATALOG_NAMESPACE && !list_member_oid(activeSearchPath, typnamespace))
    //     visible = false;
    // else
    // {
    //     /*
	// 	 * If it is in the path, it might still not be visible; it could be
	// 	 * hidden by another type of the same name earlier in the path. So we
	// 	 * must do a slow check for conflicting types.
	// 	 */
    //     char * typname = NameStr(typtup->typname);
    //     PGListCell * l;

    //     visible = false;
    //     foreach (l, activeSearchPath)
    //     {
    //         PGOid namespaceId = lfirst_oid(l);

    //         if (namespaceId == typnamespace)
    //         {
    //             /* Found it first in path */
    //             visible = true;
    //             break;
    //         }
    //         if (SearchSysCacheExists2(TYPENAMENSP, PointerGetDatum(typname), ObjectIdGetDatum(namespaceId)))
    //         {
    //             /* Found something else first in path */
    //             break;
    //         }
    //     }
    // }

    return visible;
};

std::string TypeProvider::format_type_with_typemod(PGOid type_oid, int32 typemod)
{
	return format_type_internal(type_oid, typemod, true, false, false);
};

std::string TypeProvider::format_type_internal(PGOid type_oid, int32 typemod, bool typemod_given, bool allow_invalid, bool force_qualify)
{
    bool with_typemod = typemod_given && (typemod >= 0);
    PGOid array_base_type;
    bool is_array;
    std::string buf = "";

    if (type_oid == InvalidOid && allow_invalid)
    {
        return "-";
    }

    PGTypePtr tuple = getTypeByOid(type_oid);
    if (tuple == NULL)
    {
        if (allow_invalid)
            return "???";
        else
            elog(ERROR, "Lookup failed for type %u", type_oid);
    }

    /*
	 * Check if it's a regular (variable length) array type.  Fixed-length
	 * array types such as "name" shouldn't get deconstructed.  As of Postgres
	 * 8.1, rather than checking typlen we check the toast property, and don't
	 * deconstruct "plain storage" array types --- this is because we don't
	 * want to show oidvector as oid[].
	 */
    array_base_type = tuple->typelem;

    if (array_base_type != InvalidOid && tuple->typstorage != 'p')
    {
        /* Switch our attention to the array element type */
        tuple = getTypeByOid(array_base_type);
        if (tuple == NULL)
        {
            if (allow_invalid)
                return "???[]";
            else
                elog(ERROR, "cache lookup failed for type %u", type_oid);
        }
        type_oid = array_base_type;
        is_array = true;
    }
    else
        is_array = false;

    /*
	 * See if we want to special-case the output for certain built-in types.
	 * Note that these special cases should all correspond to special
	 * productions in gram.y, to ensure that the type name will be taken as a
	 * system type, not a user type of the same name.
	 *
	 * If we do not provide a special-case output here, the type name will be
	 * handled the same way as a user type name --- in particular, it will be
	 * double-quoted if it matches any lexer keyword.  This behavior is
	 * essential for some cases, such as types "bit" and "char".
	 */
    //buf = NULL; /* flag for no special case */

    switch (type_oid)
    {
        case BITOID:
            if (with_typemod)
                buf = printTypmod("bit", typemod, tuple->typmodout);
            else if (typemod_given)
            {
                /*
				 * bit with typmod -1 is not the same as BIT, which means
				 * BIT(1) per SQL spec.  Report it as the quoted typename so
				 * that parser will not assign a bogus typmod.
				 */
            }
            else
                buf = "bit";
            break;

        case BOOLOID:
            buf = "boolean";
            break;

        case BPCHAROID:
            if (with_typemod)
                buf = printTypmod("character", typemod, tuple->typmodout);
            else if (typemod_given)
            {
                /*
				 * bpchar with typmod -1 is not the same as CHARACTER, which
				 * means CHARACTER(1) per SQL spec.  Report it as bpchar so
				 * that parser will not assign a bogus typmod.
				 */
            }
            else
                buf = "character";
            break;

        case FLOAT4OID:
            buf = "real";
            break;

        case FLOAT8OID:
            buf = "double precision";
            break;

        case INT2OID:
            buf = "smallint";
            break;

        case INT4OID:
            buf = "integer";
            break;

        case INT8OID:
            buf = "bigint";
            break;

        case NUMERICOID:
            if (with_typemod)
                buf = printTypmod("numeric", typemod, tuple->typmodout);
            else
                buf = "numeric";
            break;

        case INTERVALOID:
            if (with_typemod)
                buf = printTypmod("interval", typemod, tuple->typmodout);
            else
                buf = "interval";
            break;

        case TIMEOID:
            if (with_typemod)
                buf = printTypmod("time", typemod, tuple->typmodout);
            else
                buf = "time without time zone";
            break;

        case TIMETZOID:
            if (with_typemod)
                buf = printTypmod("time", typemod, tuple->typmodout);
            else
                buf = "time with time zone";
            break;

        case TIMESTAMPOID:
            if (with_typemod)
                buf = printTypmod("timestamp", typemod, tuple->typmodout);
            else
                buf = "timestamp without time zone";
            break;

        case TIMESTAMPTZOID:
            if (with_typemod)
                buf = printTypmod("timestamp", typemod, tuple->typmodout);
            else
                buf = "timestamp with time zone";
            break;

        case VARBITOID:
            if (with_typemod)
                buf = printTypmod("bit varying", typemod, tuple->typmodout);
            else
                buf = "bit varying";
            break;

        case VARCHAROID:
            if (with_typemod)
                buf = printTypmod("character varying", typemod, tuple->typmodout);
            else
                buf = "character varying";
            break;
    }

    if (buf == "")
    {
        /*
		 * Default handling: report the name as it appears in the catalog.
		 * Here, we must qualify the name if it is not visible in the search
		 * path, and we must double-quote it if it's not a standard identifier
		 * or if it matches any keyword.
		 */
        std::string nspname;
        std::string typname;

        if (!force_qualify && TypeIsVisible(type_oid))
            nspname = "";
        else
            nspname = RelationProvider::get_namespace_name(tuple->typnamespace);

        typname = tuple->typname;

        //TODO kindred
        // buf = quote_qualified_identifier(nspname, typname);
        buf = pstrdup((std::string(nspname) + "." + std::string(typname)).c_str());

        if (with_typemod)
            buf = printTypmod(buf.c_str(), typemod, tuple->typmodout);
    }

    if (is_array)
    {
        //buf = psprintf("%s[]", buf);
        buf = buf + "[]";
    }

    return buf;
};

PGTupleDescPtr TypeProvider::get_tupdesc_by_type_relid(PGTypePtr type)
{
    if (!OidIsValid(type->typrelid)) /* should not happen */
	{
        elog(ERROR, "invalid typrelid for composite type %u", type->oid);
		return nullptr;
	}
    PGRelationPtr rel = RelationProvider::relation_open(type->typrelid, AccessShareLock);
    Assert(rel->rd_rel->reltype == type->oid)

    /*
	 * Link to the tupdesc and increment its refcount (we assert it's a
	 * refcounted descriptor).  We don't use IncrTupleDescRefCount() for this,
	 * because the reference mustn't be entered in the current resource owner;
	 * it can outlive the current query.
	 */
	//TODO kindred
    //typentry->tupDesc = rel->rd_att;

    //Assert(typentry->tupDesc->tdrefcount > 0);
    //typentry->tupDesc->tdrefcount++;

	RelationProvider::relation_close(rel, AccessShareLock);

	return rel->rd_att;
};

PGTupleDescPtr TypeProvider::lookup_rowtype_tupdesc_internal(PGOid type_id, int32 typmod, bool noError)
{
    if (type_id != RECORDOID)
    {
        /*
		 * It's a named composite type, so use the regular typcache.
		 */
        // TypeCacheEntry * typentry;

        // typentry = lookup_type_cache(type_id, TYPECACHE_TUPDESC);
        // if (typentry->tupDesc == NULL && !noError)
        //     ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("type %s is not composite", format_type_be(type_id))));
        // return typentry->tupDesc;

		PGTypePtr type = getTypeByOid(type_id);
        if (type->typtype == TYPTYPE_COMPOSITE)
        {
            return get_tupdesc_by_type_relid(type);
        }
    }
    else
    {
        /*
		 * It's a transient record type, so look in our record-type table.
		 */
        if (typmod < 0 || (size_t)typmod >= NextRecordTypmod)
        {
            if (!noError)
            {
                ereport(ERROR, (errcode(PG_ERRCODE_WRONG_OBJECT_TYPE), errmsg("record type has not been registered")));
            }
            return nullptr;
        }
        return recordCacheArray[typmod];
    }

	return nullptr;
};

PGTupleDescPtr TypeProvider::lookup_rowtype_tupdesc(PGOid type_id, int32 typmod)
{
	//TODO kindred
    //PGTupleDescPtr tupDesc = lookup_rowtype_tupdesc_internal(type_id, typmod, false);
    //IncrTupleDescRefCount(tupDesc);
    //return tupDesc;

	return lookup_rowtype_tupdesc_internal(type_id, typmod, false);
};

PGOid TypeProvider::get_element_type(PGOid typid)
{
	PGTypePtr tup = getTypeByOid(typid);

    if (tup != NULL)
    {
        PGOid result;

        if (tup->typlen == -1)
            result = tup->typelem;
        else
            result = InvalidOid;

        return result;
    }
    else
        return InvalidOid;
};

void TypeProvider::getTypeOutputInfo(PGOid type, PGOid * typOutput, bool * typIsVarlena)
{
    // HeapTuple typeTuple;
    // Form_pg_type pt;

    // typeTuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(type));
    // if (!HeapTupleIsValid(typeTuple))
    //     elog(ERROR, "cache lookup failed for type %u", type);
    // pt = (Form_pg_type)GETSTRUCT(typeTuple);

    // if (!pt->typisdefined)
    //     ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("type %s is only a shell", format_type_be(type))));
    // if (!OidIsValid(pt->typoutput))
    //     ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION), errmsg("no output function available for type %s", format_type_be(type))));

    // *typOutput = pt->typoutput;
    // *typIsVarlena = (!pt->typbyval) && (pt->typlen == -1);

    // ReleaseSysCache(typeTuple);

	PGTypePtr tup = getTypeByOid(type);
	if (tup == nullptr)
	{
		elog(ERROR, "cache lookup failed for type %u", type);
		return;
	}

	if (!tup->typisdefined)
	{
		ereport(ERROR, (errcode(PG_ERRCODE_UNDEFINED_OBJECT), errmsg("type %s is only a shell", format_type_be(type).c_str())));
		return;
	}

	if (!OidIsValid(tup->typoutput))
	{
		ereport(ERROR, (errcode(PG_ERRCODE_UNDEFINED_FUNCTION), errmsg("no output function available for type %s", format_type_be(type).c_str())));
		return;
	}

	*typOutput = tup->typoutput;
    *typIsVarlena = (!tup->typbyval) && (tup->typlen == -1);

};

void TypeProvider::getTypeInputInfo(PGOid type, PGOid * typInput, PGOid * typIOParam)
{
    // HeapTuple typeTuple;
    // Form_pg_type pt;

    // typeTuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(type));
    // if (!HeapTupleIsValid(typeTuple))
    //     elog(ERROR, "cache lookup failed for type %u", type);
    // pt = (Form_pg_type)GETSTRUCT(typeTuple);

    // if (!pt->typisdefined)
    //     ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("type %s is only a shell", format_type_be(type))));
    // if (!OidIsValid(pt->typinput))
    //     ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION), errmsg("no input function available for type %s", format_type_be(type))));

    // *typInput = pt->typinput;
    // *typIOParam = getTypeIOParam(typeTuple);

    // ReleaseSysCache(typeTuple);

	PGTypePtr tup = getTypeByOid(type);
	if (tup == nullptr)
	{
		elog(ERROR, "cache lookup failed for type %u", type);
		return;
	}

	if (!tup->typisdefined)
	{
		ereport(ERROR, (errcode(PG_ERRCODE_UNDEFINED_OBJECT), errmsg("type %s is only a shell", format_type_be(type).c_str())));
		return;
	}

	if (!OidIsValid(tup->typinput))
	{
		ereport(ERROR, (errcode(PG_ERRCODE_UNDEFINED_FUNCTION), errmsg("no typinput function available for type %s", format_type_be(type).c_str())));
		return;
	}

	*typInput = tup->typinput;
    *typIOParam = getTypeIOParam(tup);

};

bool TypeProvider::typeInheritsFrom(PGOid subclassTypeId, PGOid superclassTypeId)
{
    bool result = false;
	// TODO kindred
    // PGOid subclassRelid;
    // PGOid superclassRelid;
    // PGRelation inhrel;
    // PGList *visited, *queue;
    // PGListCell * queue_item;

    // /* We need to work with the associated relation OIDs */
    // subclassRelid = type_parser->typeidTypeRelid(subclassTypeId);
    // if (subclassRelid == InvalidOid)
	// {
    //     return false; /* not a complex type */
	// }
    // superclassRelid = type_parser->typeidTypeRelid(superclassTypeId);
    // if (superclassRelid == InvalidOid)
	// {
    //     return false; /* not a complex type */
	// }

    // /* No point in searching if the superclass has no subclasses */
    // if (!RelationProvider::has_subclass(superclassRelid))
    //     return false;

    // /*
	//  * Begin the search at the relation itself, so add its relid to the queue.
	//  */
    // queue = list_make1_oid(subclassRelid);
    // visited = NIL;

    // inhrel = RelationProvider::heap_open(InheritsRelationId, AccessShareLock);

    // /*
	//  * Use queue to do a breadth-first traversal of the inheritance graph from
	//  * the relid supplied up to the root.  Notice that we append to the queue
	//  * inside the loop --- this is okay because the foreach() macro doesn't
	//  * advance queue_item until the next loop iteration begins.
	//  */
    // foreach (queue_item, queue)
    // {
    //     PGOid this_relid = lfirst_oid(queue_item);
    //     ScanKeyData skey;
    //     SysScanDesc inhscan;
    //     HeapTuple inhtup;

    //     /*
	// 	 * If we've seen this relid already, skip it.  This avoids extra work
	// 	 * in multiple-inheritance scenarios, and also protects us from an
	// 	 * infinite loop in case there is a cycle in pg_inherits (though
	// 	 * theoretically that shouldn't happen).
	// 	 */
    //     if (list_member_oid(visited, this_relid))
    //         continue;

    //     /*
	// 	 * Okay, this is a not-yet-seen relid. Add it to the list of
	// 	 * already-visited OIDs, then find all the types this relid inherits
	// 	 * from and add them to the queue.
	// 	 */
    //     visited = lappend_oid(visited, this_relid);

    //     ScanKeyInit(&skey, Anum_pg_inherits_inhrelid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(this_relid));

    //     inhscan = systable_beginscan(inhrel, InheritsRelidSeqnoIndexId, true, NULL, 1, &skey);

    //     while ((inhtup = systable_getnext(inhscan)) != NULL)
    //     {
    //         Form_pg_inherits inh = (Form_pg_inherits)GETSTRUCT(inhtup);
    //         PGOid inhparent = inh->inhparent;

    //         /* If this is the target superclass, we're done */
    //         if (inhparent == superclassRelid)
    //         {
    //             result = true;
    //             break;
    //         }

    //         /* Else add to queue */
    //         queue = lappend_oid(queue, inhparent);
    //     }

    //     systable_endscan(inhscan);

    //     if (result)
    //         break;
    // }

    // /* clean up ... */
    // heap_close(inhrel, AccessShareLock);

    // list_free(visited);
    // list_free(queue);

    return result;
};

PGOid TypeProvider::get_range_subtype(PGOid rangeOid)
{
    // HeapTuple tp;

    // tp = SearchSysCache1(RANGETYPE, ObjectIdGetDatum(rangeOid));
    // if (HeapTupleIsValid(tp))
    // {
    //     Form_pg_range rngtup = (Form_pg_range)GETSTRUCT(tp);
    //     PGOid result;

    //     result = rngtup->rngsubtype;
    //     ReleaseSysCache(tp);
    //     return result;
    // }
    // else
    //     return InvalidOid;

	// TODO kindred
	elog(ERROR, "range type not supported yet: %u", rangeOid);

	return InvalidOid;
};

PGOid TypeProvider::get_base_element_type(PGOid typid)
{
    /*
	 * We loop to find the bottom base type in a stack of domains.
	 */
    for (;;)
    {
		PGTypePtr tup = getTypeByOid(typid);
		if (tup == nullptr)
		{
			break;
		}
		if (tup->typtype != TYPTYPE_DOMAIN)
		{
			/* Not a domain, so stop descending */
            PGOid result;

            /* This test must match get_element_type */
            if (tup->typlen == -1)
                result = tup->typelem;
            else
                result = InvalidOid;
            return result;
		}

        typid = tup->typbasetype;
    }

    /* Like get_element_type, silently return InvalidOid for bogus input */
    return InvalidOid;
};

char TypeProvider::get_typtype(PGOid typid)
{
	PGTypePtr tp = getTypeByOid(typid);
	if (nullptr != tp)
	{
		char result;
        result = tp->typtype;
        return result;
	}
	else
	{
		return '\0';
	}
};

bool TypeProvider::type_is_enum(PGOid typid)
{
    return (get_typtype(typid) == TYPTYPE_ENUM);
};

PGOid TypeProvider::get_array_type(PGOid typid)
{
	PGOid			result = InvalidOid;

	PGTypePtr tp = getTypeByOid(typid);
	if (tp != NULL)
	{
		result = tp->typarray;
	}
	return result;
};

bool TypeProvider::type_is_range(PGOid typid)
{
	return (get_typtype(typid) == TYPTYPE_RANGE);
};

bool TypeProvider::type_is_rowtype(PGOid typid)
{
	return (typid == RECORDOID || get_typtype(typid) == TYPTYPE_COMPOSITE);
};

PGOid TypeProvider::get_typcollation(PGOid typid)
{
	PGTypePtr tp = getTypeByOid(typid);
    if (tp != NULL)
    {
        PGOid result;

        result = tp->typcollation;
        return result;
    }
    else
        return InvalidOid;
};

bool
TypeProvider::type_is_collatable(PGOid typid)
{
    return OidIsValid(get_typcollation(typid));
};

void TypeProvider::PGTupleDescInitEntry(
        PGTupleDescPtr desc, PGAttrNumber attributeNumber, const std::string & attributeName,
		PGOid oidtypeid, int32 typmod, int attdim)
{
	//HeapTuple tuple;
    //Form_pg_type typeForm;

    /*
	 * sanity checks
	 */
    Assert(PointerIsValid(desc.get()))
    Assert(attributeNumber >= 1)
    Assert(attributeNumber <= desc->natts)
    /*
	 * initialize the attribute fields
	 */
    PGAttrPtr & att = desc->attrs[attributeNumber - 1];
    att = std::make_shared<Form_pg_attribute>();
    att->attrelid = 0; /* dummy value */

    /*
	 * Note: attributeName can be NULL, because the planner doesn't always
	 * fill in valid resname values in targetlists, particularly for resjunk
	 * attributes. Also, do nothing if caller wants to re-use the old attname.
	 */
    // if (attributeName == "")
	// {
	// 	MemSet(NameStr(att->attname), 0, NAMEDATALEN);
	// }
    // else if (attributeName != std::string(NameStr(att->attname)))
	// {
	// 	namestrcpy(&(att->attname), attributeName.c_str());
	// }

	att->attname = attributeName;

    att->attstattarget = -1;
    att->attcacheoff = -1;
    att->atttypmod = typmod;

    att->attnum = attributeNumber;
    att->attndims = attdim;

    att->attnotnull = false;
    att->atthasdef = false;
    att->attisdropped = false;
    att->attislocal = true;
    att->attinhcount = 0;
    /* attacl, attoptions and attfdwoptions are not present in tupledescs */
    PGTypePtr tuple = getTypeByOid(oidtypeid);
    if (tuple == NULL)
        elog(ERROR, "cache lookup failed for type %u", oidtypeid);
    //typeForm = (Form_pg_type)GETSTRUCT(tuple);
    
    att->atttypid = oidtypeid;
    att->attlen = tuple->typlen;
    att->attbyval = tuple->typbyval;
    att->attalign = tuple->typalign;
    att->attstorage = tuple->typstorage;
    //att->attcollation = tuple->typcollation;
    //ReleaseSysCache(tuple);
};

PGTupleDescPtr TypeProvider::build_function_result_tupdesc_t(PGProcPtr & procTuple)
{
    //Form_pg_proc procform = (Form_pg_proc)GETSTRUCT(procTuple);
    //Datum proallargtypes;
    //Datum proargmodes;
    //Datum proargnames;
    //bool isnull;

    /* Return NULL if the function isn't declared to return RECORD */
    if (procTuple->prorettype != RECORDOID)
	{
        return nullptr;
	}

    /* If there are no OUT parameters, return NULL */
    // if (heap_attisnull(procTuple, Anum_pg_proc_proallargtypes) || heap_attisnull(procTuple, Anum_pg_proc_proargmodes))
    //     return NULL;
	//TODO kindred
	if (procTuple->proargtypes.size() == 0)
	{
		return nullptr;
	}

    // /* Get the data out of the tuple */
    // proallargtypes = SysCacheGetAttr(PROCOID, procTuple, Anum_pg_proc_proallargtypes, &isnull);
    // Assert(!isnull)
    // proargmodes = SysCacheGetAttr(PROCOID, procTuple, Anum_pg_proc_proargmodes, &isnull);
    // Assert(!isnull)
    // proargnames = SysCacheGetAttr(PROCOID, procTuple, Anum_pg_proc_proargnames, &isnull);
    // if (isnull)
	// {
    //     proargnames = PointerGetDatum(NULL); /* just to be sure */
	// }

    //return build_function_result_tupdesc_d(proallargtypes, proargmodes, proargnames);

	return build_function_result_tupdesc_d(procTuple);
};

PGTupleDescPtr TypeProvider::build_function_result_tupdesc_d(PGProcPtr & procTuple)
{
	if (procTuple->proallargtypes.size() == 0
	    || procTuple->proargmodes.size() == 0)
	{
		return nullptr;
	}

	auto numargs = procTuple->proallargtypes.size();

	/* zero elements probably shouldn't happen, but handle it gracefully */
    if (numargs <= 0)
	{
        return nullptr;
	}

	if (numargs != procTuple->proargmodes.size())
	{
		elog(ERROR, "proargmodes is not a 1-D char array");
		return nullptr;
	}
	/* extract output-argument types and names */
    //PGOid * outargtypes = (PGOid *)palloc(numargs * sizeof(PGOid));
	std::vector<PGOid> outargtypes;
	std::vector<std::string> outargnames;
    //char ** outargnames = (char **)palloc(numargs * sizeof(char *));
    int numoutargs = 0;
    for (size_t i = 0; i < numargs; i++)
    {
        std::string pname = "";

        switch (procTuple->proargmodes[i])
        {
            /* input modes */
            case PG_PROARGMODE_IN:
            case PG_PROARGMODE_VARIADIC:
                break;

            /* input and output */
            case PG_PROARGMODE_INOUT:
                /* fallthrough */

            /* output modes */
            case PG_PROARGMODE_OUT:
            case PG_PROARGMODE_TABLE:
				outargtypes.push_back(procTuple->proallargtypes[i]);
                if (procTuple->proargnames.size() > i)
                    pname = procTuple->proargnames[i];
                else
                    pname = "";

                if (pname == "")
                {
                    /* Parameter is not named, so gin up a column name */
					pname = "column" + std::to_string(numoutargs + 1);
                }
				outargnames.push_back(pname);
                numoutargs++;
        }
    }

    /*
	 * If there is no output argument, or only one, the function does not
	 * return tuples.
	 */
    if (numoutargs < 2)
        return nullptr;

    PGTupleDescPtr desc = PGCreateTemplateTupleDesc(numoutargs, false);
    for (int i = 0; i < numoutargs; i++)
    {
        PGTupleDescInitEntry(desc, i + 1, outargnames[i], outargtypes[i], -1, 0);
    }

    return desc;
};

PGOid TypeProvider::get_call_expr_argtype(PGNode * expr, int argnum)
{
    PGList * args;
    PGOid argtype;

    if (expr == NULL)
        return InvalidOid;

    if (IsA(expr, PGFuncExpr))
        args = ((PGFuncExpr *)expr)->args;
    else if (IsA(expr, PGOpExpr))
        args = ((PGOpExpr *)expr)->args;
    else if (IsA(expr, PGDistinctExpr))
        args = ((PGDistinctExpr *)expr)->args;
    else if (IsA(expr, PGScalarArrayOpExpr))
        args = ((PGScalarArrayOpExpr *)expr)->args;
    else if (IsA(expr, PGArrayCoerceExpr))
        args = list_make1(((PGArrayCoerceExpr *)expr)->arg);
    else if (IsA(expr, PGNullIfExpr))
        args = ((PGNullIfExpr *)expr)->args;
    else if (IsA(expr, PGWindowFunc))
        args = ((PGWindowFunc *)expr)->args;
    else
        return InvalidOid;

    if (argnum < 0 || argnum >= list_length(args))
        return InvalidOid;

    argtype = exprType((PGNode *)list_nth(args, argnum));

    /*
	 * special hack for ScalarArrayOpExpr and ArrayCoerceExpr: what the
	 * underlying function will actually get passed is the element type of the
	 * array.
	 */
    if (IsA(expr, PGScalarArrayOpExpr) && argnum == 1)
        argtype = get_base_element_type(argtype);
    else if (IsA(expr, PGArrayCoerceExpr) && argnum == 0)
        argtype = get_base_element_type(argtype);

    return argtype;
};

PGOid TypeProvider::resolve_generic_type(PGOid declared_type, PGOid context_actual_type, PGOid context_declared_type)
{
    if (declared_type == ANYARRAYOID)
    {
        if (context_declared_type == ANYARRAYOID)
        {
            /*
			 * Use actual type, but it must be an array; or if it's a domain
			 * over array, use the base array type.
			 */
            PGOid context_base_type = getBaseType(context_actual_type);
            PGOid array_typelem = get_element_type(context_base_type);

            if (!OidIsValid(array_typelem))
                ereport(
                    ERROR,
                    (errcode(PG_ERRCODE_DATATYPE_MISMATCH),
                     errmsg("argument declared \"anyarray\" is not an array but type %s", format_type_be(context_base_type).c_str())));
            return context_base_type;
        }
        else if (
            context_declared_type == ANYELEMENTOID || context_declared_type == ANYNONARRAYOID || context_declared_type == ANYENUMOID
            || context_declared_type == ANYRANGEOID)
        {
            /* Use the array type corresponding to actual type */
            PGOid array_typeid = get_array_type(context_actual_type);

            if (!OidIsValid(array_typeid))
                ereport(
                    ERROR,
                    (errcode(PG_ERRCODE_UNDEFINED_OBJECT),
                     errmsg("could not find array type for data type %s", format_type_be(context_actual_type).c_str())));
            return array_typeid;
        }
    }
    else if (
        declared_type == ANYELEMENTOID || declared_type == ANYNONARRAYOID || declared_type == ANYENUMOID || declared_type == ANYRANGEOID)
    {
        if (context_declared_type == ANYARRAYOID)
        {
            /* Use the element type corresponding to actual type */
            PGOid context_base_type = getBaseType(context_actual_type);
            PGOid array_typelem = get_element_type(context_base_type);

            if (!OidIsValid(array_typelem))
                ereport(
                    ERROR,
                    (errcode(PG_ERRCODE_DATATYPE_MISMATCH),
                     errmsg("argument declared \"anyarray\" is not an array but type %s", format_type_be(context_base_type).c_str())));
            return array_typelem;
        }
        else if (context_declared_type == ANYRANGEOID)
        {
            /* Use the element type corresponding to actual type */
            PGOid context_base_type = getBaseType(context_actual_type);
            PGOid range_typelem = get_range_subtype(context_base_type);

            if (!OidIsValid(range_typelem))
                ereport(
                    ERROR,
                    (errcode(PG_ERRCODE_DATATYPE_MISMATCH),
                     errmsg("argument declared \"anyrange\" is not a range type but type %s", format_type_be(context_base_type).c_str())));
            return range_typelem;
        }
        else if (context_declared_type == ANYELEMENTOID || context_declared_type == ANYNONARRAYOID || context_declared_type == ANYENUMOID)
        {
            /* Use the actual type; it doesn't matter if array or not */
            return context_actual_type;
        }
    }
    else
    {
        /* declared_type isn't polymorphic, so return it as-is */
        return declared_type;
    }
    /* If we get here, declared_type is polymorphic and context isn't */
    /* NB: this is a calling-code logic error, not a user error */
    elog(ERROR, "could not determine polymorphic type because context isn't polymorphic");
    return InvalidOid; /* keep compiler quiet */
};

bool TypeProvider::resolve_polymorphic_tupdesc(PGTupleDescPtr tupdesc, std::vector<PGOid> & declared_args, PGNode * call_expr)
{
    int natts = tupdesc->natts;
    //int nargs = declared_args->dim1;
    bool have_anyelement_result = false;
    bool have_anyarray_result = false;
    bool have_anyrange_result = false;
    bool have_anynonarray = false;
    bool have_anyenum = false;
    PGOid anyelement_type = InvalidOid;
    PGOid anyarray_type = InvalidOid;
    PGOid anyrange_type = InvalidOid;
    PGOid anycollation = InvalidOid;
    int i;

    /* See if there are any polymorphic outputs; quick out if not */
    for (i = 0; i < natts; i++)
    {
        switch (tupdesc->attrs[i]->atttypid)
        {
            case ANYELEMENTOID:
                have_anyelement_result = true;
                break;
            case ANYARRAYOID:
                have_anyarray_result = true;
                break;
            case ANYNONARRAYOID:
                have_anyelement_result = true;
                have_anynonarray = true;
                break;
            case ANYENUMOID:
                have_anyelement_result = true;
                have_anyenum = true;
                break;
            case ANYRANGEOID:
                have_anyrange_result = true;
                break;
            default:
                break;
        }
    }
    if (!have_anyelement_result && !have_anyarray_result && !have_anyrange_result)
        return true;

    /*
	 * Otherwise, extract actual datatype(s) from input arguments.  (We assume
	 * the parser already validated consistency of the arguments.)
	 */
    if (!call_expr)
	{
        return false; /* no hope */
	}

    // for (i = 0; i < nargs; i++)
    // {
    //     switch (declared_args->values[i])
    //     {
    //         case ANYELEMENTOID:
    //         case ANYNONARRAYOID:
    //         case ANYENUMOID:
    //             if (!OidIsValid(anyelement_type))
    //                 anyelement_type = get_call_expr_argtype(call_expr, i);
    //             break;
    //         case ANYARRAYOID:
    //             if (!OidIsValid(anyarray_type))
    //                 anyarray_type = get_call_expr_argtype(call_expr, i);
    //             break;
    //         case ANYRANGEOID:
    //             if (!OidIsValid(anyrange_type))
    //                 anyrange_type = get_call_expr_argtype(call_expr, i);
    //             break;
    //         default:
    //             break;
    //     }
    // }

	for (const auto oid : declared_args)
    {
        switch (oid)
        {
            case ANYELEMENTOID:
            case ANYNONARRAYOID:
            case ANYENUMOID:
                if (!OidIsValid(anyelement_type))
                    anyelement_type = get_call_expr_argtype(call_expr, i);
                break;
            case ANYARRAYOID:
                if (!OidIsValid(anyarray_type))
                    anyarray_type = get_call_expr_argtype(call_expr, i);
                break;
            case ANYRANGEOID:
                if (!OidIsValid(anyrange_type))
                    anyrange_type = get_call_expr_argtype(call_expr, i);
                break;
            default:
                break;
        }
    }

    /* If nothing found, parser messed up */
    if (!OidIsValid(anyelement_type) && !OidIsValid(anyarray_type) && !OidIsValid(anyrange_type))
        return false;

    /* If needed, deduce one polymorphic type from others */
    if (have_anyelement_result && !OidIsValid(anyelement_type))
    {
        if (OidIsValid(anyarray_type))
            anyelement_type = resolve_generic_type(ANYELEMENTOID, anyarray_type, ANYARRAYOID);
        if (OidIsValid(anyrange_type))
        {
            PGOid subtype = resolve_generic_type(ANYELEMENTOID, anyrange_type, ANYRANGEOID);

            /* check for inconsistent array and range results */
            if (OidIsValid(anyelement_type) && anyelement_type != subtype)
                return false;
            anyelement_type = subtype;
        }
    }

    if (have_anyarray_result && !OidIsValid(anyarray_type))
        anyarray_type = resolve_generic_type(ANYARRAYOID, anyelement_type, ANYELEMENTOID);

    /*
	 * We can't deduce a range type from other polymorphic inputs, because
	 * there may be multiple range types for the same subtype.
	 */
    if (have_anyrange_result && !OidIsValid(anyrange_type))
        return false;

    /* Enforce ANYNONARRAY if needed */
    if (have_anynonarray && get_element_type(anyelement_type) != InvalidOid)
        return false;

    /* Enforce ANYENUM if needed */
    if (have_anyenum && !type_is_enum(anyelement_type))
        return false;

    /*
	 * Identify the collation to use for polymorphic OUT parameters. (It'll
	 * necessarily be the same for both anyelement and anyarray.)  Note that
	 * range types are not collatable, so any possible internal collation of a
	 * range type is not considered here.
	 */
    if (OidIsValid(anyelement_type))
        anycollation = get_typcollation(anyelement_type);
    else if (OidIsValid(anyarray_type))
        anycollation = get_typcollation(anyarray_type);

    if (OidIsValid(anycollation))
    {
        /*
		 * The types are collatable, so consider whether to use a nondefault
		 * collation.  We do so if we can identify the input collation used
		 * for the function.
		 */
        PGOid inputcollation = exprInputCollation(call_expr);

        if (OidIsValid(inputcollation))
            anycollation = inputcollation;
    }

    /* And finally replace the tuple column types as needed */
    for (i = 0; i < natts; i++)
    {
        switch (tupdesc->attrs[i]->atttypid)
        {
            case ANYELEMENTOID:
            case ANYNONARRAYOID:
            case ANYENUMOID:
                PGTupleDescInitEntry(tupdesc, i + 1, tupdesc->attrs[i]->attname, anyelement_type, -1, 0);
                PGTupleDescInitEntryCollation(tupdesc, i + 1, anycollation);
                break;
            case ANYARRAYOID:
                PGTupleDescInitEntry(tupdesc, i + 1, tupdesc->attrs[i]->attname, anyarray_type, -1, 0);
                PGTupleDescInitEntryCollation(tupdesc, i + 1, anycollation);
                break;
            case ANYRANGEOID:
                PGTupleDescInitEntry(tupdesc, i + 1, tupdesc->attrs[i]->attname, anyrange_type, -1, 0);
                /* no collation should be attached to a range type */
                break;
            default:
                break;
        }
    }

    return true;
};

std::size_t PGTupleDescHash::operator()(const PGTupleDescPtr & key) const
{
	bool strict = false;
	SipHash hash;
    hash.update<int>(key->natts);
	hash.update<PGOid>(key->tdtypeid);
	hash.update<bool>(key->tdhasoid);
	for (int i = 0; i < key->natts; i++)
	{
		PGAttrPtr attr = key->attrs[i];
		hash.update(attr->attname);
		hash.update<PGOid>(attr->atttypid);
		hash.update<int4>(attr->attstattarget);
		hash.update<int2>(attr->attlen);
		hash.update<int4>(attr->attndims);
		hash.update<int4>(attr->atttypmod);
		hash.update<bool>(attr->attbyval);
		hash.update<char>(attr->attstorage);
		hash.update<char>(attr->attalign);
		if (strict)
		{
			hash.update<bool>(attr->attnotnull);
			hash.update<bool>(attr->atthasdef);
			hash.update<bool>(attr->attisdropped);
			hash.update<bool>(attr->attislocal);
			hash.update<int4>(attr->attinhcount);
			hash.update<PGOid>(attr->attcollation);
		}
	}

	if (key->constr != nullptr)
	{
		PGTupleConstrPtr constr = key->constr;
		hash.update<bool>(constr->has_not_null);
		hash.update<uint16>(constr->num_defval);
		hash.update<uint16>(constr->num_check);
	}

    return hash.get64();
};

bool PGTupleDescEqual::operator()(const PGTupleDescPtr & lhs, const PGTupleDescPtr & rhs) const
{
    return equalTupleDescs(lhs, rhs, true);
};

void TypeProvider::assign_record_type_typmod(PGTupleDescPtr tupDesc)
{
	auto it = recordCacheHash.find(tupDesc);
	if (it != recordCacheHash.end())
	{
		tupDesc->tdtypmod = it->second.tdtypmod;
		return;
	}

    if (recordCacheArray.size() <= 0)
    {
        recordCacheArray.resize(64);
    }
    else if (NextRecordTypmod >= recordCacheArray.size())
    {
        int32 newlen = recordCacheArray.size() * 2;

        recordCacheArray.resize(newlen);
    }

    PGTupleDescPtr entDesc = PGCreateTupleDescCopy(tupDesc);

	int32 newtypmod = NextRecordTypmod++;
	entDesc->tdtypmod = newtypmod;
	recordCacheArray[newtypmod] = entDesc;

	/* report to caller as well */
	tupDesc->tdtypmod = newtypmod;
};

TypeFuncClass
TypeProvider::internal_get_result_type(PGOid funcid, duckdb_libpgquery::PGNode * call_expr,
        PGOid * resultTypeId, PGTupleDescPtr & resultTupleDesc)
{
    TypeFuncClass result;
    //HeapTuple tp;
    //Form_pg_proc procform;
    PGOid rettype;
    PGTupleDescPtr tupdesc = nullptr;

    /* First fetch the function's pg_proc row to inspect its rettype */
    // tp = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
    // if (!HeapTupleIsValid(tp))
    //     elog(ERROR, "cache lookup failed for function %u", funcid);
    // procform = (Form_pg_proc)GETSTRUCT(tp);

	PGProcPtr tp = ProcProvider::getProcByOid(funcid);
	if (nullptr == tp)
	{
		elog(ERROR, "cache lookup failed for function %u", funcid);
		return TYPEFUNC_OTHER;
	}

    rettype = tp->prorettype;

    /* Check for OUT parameters defining a RECORD result */
    tupdesc = build_function_result_tupdesc_t(tp);
    if (tupdesc)
    {
        /*
		 * It has OUT parameters, so it's basically like a regular composite
		 * type, except we have to be able to resolve any polymorphic OUT
		 * parameters.
		 */
        if (resultTypeId)
            *resultTypeId = rettype;

        if (resolve_polymorphic_tupdesc(tupdesc, tp->proargtypes, call_expr))
        {
            if (tupdesc->tdtypeid == RECORDOID && tupdesc->tdtypmod < 0)
                assign_record_type_typmod(tupdesc);
            if (resultTupleDesc)
                resultTupleDesc = tupdesc;
            result = TYPEFUNC_COMPOSITE;
        }
        else
        {
            if (resultTupleDesc)
                resultTupleDesc = nullptr;
            result = TYPEFUNC_RECORD;
        }

        //ReleaseSysCache(tp);

        return result;
    }

    /*
	 * If scalar polymorphic result, try to resolve it.
	 */
    if (IsPolymorphicType(rettype))
    {
        PGOid newrettype = exprType(call_expr);

        if (!OidIsValid(newrettype)) /* this probably should not happen */
            ereport(
                ERROR,
                (errcode(PG_ERRCODE_DATATYPE_MISMATCH),
                 errmsg(
                     "could not determine actual result type for function \"%s\" declared to return type %s",
                     tp->proname.c_str(),
                     format_type_be(rettype).c_str())));
        rettype = newrettype;
    }

    if (resultTypeId)
        *resultTypeId = rettype;
    if (resultTupleDesc)
        resultTupleDesc = nullptr; /* default result */

    /* Classify the result type */
    result = get_type_func_class(rettype);
    switch (result)
    {
        case TYPEFUNC_COMPOSITE:
            if (resultTupleDesc)
                resultTupleDesc = lookup_rowtype_tupdesc_copy(rettype, -1);
            /* Named composite types can't have any polymorphic columns */
            break;
        case TYPEFUNC_SCALAR:
            break;
        case TYPEFUNC_RECORD:
            /* We must get the tupledesc from call context */
			// TODO kindred
            // if (rsinfo && IsA(rsinfo, ReturnSetInfo) && rsinfo->expectedDesc != NULL)
            // {
            //     result = TYPEFUNC_COMPOSITE;
            //     if (resultTupleDesc)
            //         *resultTupleDesc = rsinfo->expectedDesc;
            //     /* Assume no polymorphic columns here, either */
            // }
			elog(ERROR, "return set of record type not supported yet: %u", funcid);
            break;
        default:
            break;
    }

    //ReleaseSysCache(tp);

    return result;
};

TypeFuncClass TypeProvider::get_type_func_class(PGOid typid)
{
    switch (get_typtype(typid))
    {
        case TYPTYPE_COMPOSITE:
            return TYPEFUNC_COMPOSITE;
        case TYPTYPE_BASE:
        case TYPTYPE_DOMAIN:
        case TYPTYPE_ENUM:
        case TYPTYPE_RANGE:
            return TYPEFUNC_SCALAR;
        case TYPTYPE_PSEUDO:
            if (typid == RECORDOID)
                return TYPEFUNC_RECORD;

            /*
			 * We treat VOID and CSTRING as legitimate scalar datatypes,
			 * mostly for the convenience of the JDBC driver (which wants to
			 * be able to do "SELECT * FROM foo()" for all legitimately
			 * user-callable functions).
			 */
            if (typid == VOIDOID || typid == CSTRINGOID)
                return TYPEFUNC_SCALAR;
            return TYPEFUNC_OTHER;
    }
    /* shouldn't get here, probably */
    return TYPEFUNC_OTHER;
};

TypeFuncClass TypeProvider::get_expr_result_type(PGNode * expr, PGOid * resultTypeId, PGTupleDescPtr & resultTupleDesc)
{
    //TypeFuncClass result;

    // if (expr && IsA(expr, PGFuncExpr))
    //     result = internal_get_result_type(((PGFuncExpr *)expr)->funcid, expr, NULL, resultTypeId, resultTupleDesc);
    // else if (expr && IsA(expr, PGOpExpr))
    //     result = internal_get_result_type(get_opcode(((PGOpExpr *)expr)->opno), expr, NULL, resultTypeId, resultTupleDesc);
    // else
    // {
    //     /* handle as a generic expression; no chance to resolve RECORD */
    //     PGOid typid = exprType(expr);

    //     if (resultTypeId)
    //         *resultTypeId = typid;
    //     if (resultTupleDesc)
    //         *resultTupleDesc = NULL;
    //     result = get_type_func_class(typid);
    //     if (result == TYPEFUNC_COMPOSITE && resultTupleDesc)
    //         *resultTupleDesc = lookup_rowtype_tupdesc_copy(typid, -1);
    // }

    TypeFuncClass result;

    if (expr && IsA(expr, PGFuncExpr))
    {
        result = internal_get_result_type(((PGFuncExpr *)expr)->funcid, expr, resultTypeId, resultTupleDesc);
    }
    else if (expr && IsA(expr, PGOpExpr))
    {
        result = internal_get_result_type(OperProvider::get_opcode(((PGOpExpr *)expr)->opno), expr, resultTypeId, resultTupleDesc);
    }
    else
    {
        /* handle as a generic expression; no chance to resolve RECORD */
        PGOid typid = exprType(expr);

        if (resultTypeId)
            *resultTypeId = typid;
        if (resultTupleDesc)
            resultTupleDesc = nullptr;
        result = get_type_func_class(typid);
        if (result == TYPEFUNC_COMPOSITE && resultTupleDesc)
            resultTupleDesc = lookup_rowtype_tupdesc_copy(typid, -1);
    }

    return result;
};

PGTupleDescPtr TypeProvider::lookup_rowtype_tupdesc_copy(PGOid type_id, int32 typmod)
{
    PGTupleDescPtr tmp = lookup_rowtype_tupdesc_internal(type_id, typmod, false);
    return PGCreateTupleDescCopyConstr(tmp);
};

PGOid TypeProvider::get_typeoid_by_typename_namespaceoid(const char * type_name, PGOid namespace_oid)
{
	for (auto pair : oid_type_map)
	{
		if (pair.second->typname == std::string(type_name))
		{
			return pair.first;
		}
	}
	return InvalidOid;
};

PGOid TypeProvider::TypenameGetTypidExtended(const char * typname, bool temp_ok)
{
    // PGOid typid;
    // PGListCell * l;

    // recomputeNamespacePath();

    // foreach (l, activeSearchPath)
    // {
    //     PGOid namespaceId = lfirst_oid(l);

    //     if (!temp_ok && namespaceId == myTempNamespace)
    //         continue; /* do not look in temp namespace */

    //     typid = GetSysCacheOid2(TYPENAMENSP, PointerGetDatum(typname), ObjectIdGetDatum(namespaceId));
    //     if (OidIsValid(typid))
    //         return typid;
    // }

    /* Not found in path */
	return get_typeoid_by_typename_namespaceoid(typname, PGOid(0));
};

PGOid TypeProvider::getTypeIOParam(PGTypePtr typeTuple)
{
    /*
	 * Array types get their typelem as parameter; everybody else gets their
	 * own type OID as parameter.
	 */
    if (OidIsValid(typeTuple->typelem))
        return typeTuple->typelem;
    else
        return typeTuple->oid;
};

void TypeProvider::get_typlenbyval(PGOid typid, int16 *typlen, bool *typbyval)
{
    //HeapTuple	tp;
    auto typtup = getTypeByOid(typid);

    //tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
    if (!typtup)
    {
        elog(ERROR, "cache lookup failed for type %u", typid);
    }
    *typlen = typtup->typlen;
    *typbyval = typtup->typbyval;
};

}
