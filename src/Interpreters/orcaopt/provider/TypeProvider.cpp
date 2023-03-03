#include <Interpreters/orcaopt/provider/TypeProvider.h>

#include <Interpreters/orcaopt/TypeParser.h>
#include <Interpreters/orcaopt/provider/FunctionProvider.h>
#include <Interpreters/orcaopt/provider/RelationProvider.h>
#include <Interpreters/orcaopt/provider/ClassProvider.h>
#include <Interpreters/orcaopt/provider/OperProvider.h>
#include <Interpreters/orcaopt/provider/ProcProvider.h>

#include "naucrates/dxl/CDXLUtils.h"

#ifdef __clang__
#pragma clang diagnostic ignored "-Wunused-parameter"
#else
#pragma GCC diagnostic ignored "-Wunused-parameter"
#endif

using namespace duckdb_libpgquery;

namespace DB
{

int TypeProvider::TYPE_OID_ID = 3;

std::pair<Oid, PGTypePtr> TypeProvider::TYPE_FLOAT32
    = {Oid(700),
       std::make_shared<Form_pg_type>(Form_pg_type{
           /*oid*/ Oid(700),
           /*typname*/ getTypeName(TypeIndex::Float32),
		   /*typnamespace*/ Oid(1),
		   /*typowner*/ Oid(1),
		   /*typlen*/ 4,
		   /*typbyval*/ true,
		   /*typtype*/ 'b',
		   /*typcategory*/ 'N',
		   /*typispreferred*/ false,
		   /*typisdefined*/ true,
		   /*typdelim*/ ',',
		   /*typrelid*/ Oid(0),
		   /*typelem*/ Oid(1021),
		   /*typarray*/ Oid(0),
		   /*typinput*/ Oid(1),
		   /*typoutput*/ Oid(1),
		   /*typreceive*/ Oid(1),
		   /*typsend*/ Oid(1),
		   /*typmodin*/ Oid(1),
		   /*typmodout*/ Oid(1),
		   /*typanalyze*/ Oid(1),
		   /*typalign*/ 'i',
		   /*typstorage*/ 'p',
		   /*typnotnull*/ false,
		   /*typbasetype*/ Oid(1),
		   /*typtypmod*/ Oid(1),
		   /*typndims*/ Oid(1),
		   /*typcollation*/ Oid(1)})};

std::pair<Oid, PGTypePtr> TypeProvider::TYPE_FLOAT64
    = {Oid(701),
       std::make_shared<Form_pg_type>(Form_pg_type{
           /*oid*/ Oid(701),
           /*typname*/ getTypeName(TypeIndex::Float64),
		   /*typnamespace*/ Oid(1),
		   /*typowner*/ Oid(1),
		   /*typlen*/ 8,
		   /*typbyval*/ true,
		   /*typtype*/ 'b',
		   /*typcategory*/ 'N',
		   /*typispreferred*/ true,
		   /*typisdefined*/ false,
		   /*typdelim*/ ',',
		   /*typrelid*/ Oid(0),
		   /*typelem*/ Oid(1022),
		   /*typarray*/ Oid(1),
		   /*typinput*/ Oid(1),
		   /*typoutput*/ Oid(1),
		   /*typreceive*/ Oid(1),
		   /*typsend*/ Oid(1),
		   /*typmodin*/ Oid(1),
		   /*typmodout*/ Oid(1),
		   /*typanalyze*/ Oid(1),
		   /*typalign*/ 'i',
		   /*typstorage*/ 'p',
		   /*typnotnull*/ false,
		   /*typbasetype*/ Oid(1),
		   /*typtypmod*/ Oid(1),
		   /*typndims*/ Oid(1),
		   /*typcollation*/ Oid(1)})};

std::pair<Oid, PGTypePtr> TypeProvider::TYPE_BOOLEAN
    = {Oid(16),
       std::make_shared<Form_pg_type>(Form_pg_type{
           /*oid*/ Oid(16),
           /*typname*/ getTypeName(TypeIndex::Int8),
		   /*typnamespace*/ Oid(1),
		   /*typowner*/ Oid(1),
		   /*typlen*/ 1,
		   /*typbyval*/ true,
		   /*typtype*/ 'b',
		   /*typcategory*/ 'B',
		   /*typispreferred*/ true,
		   /*typisdefined*/ true,
		   /*typdelim*/ ',',
		   /*typrelid*/ Oid(0),
		   /*typelem*/ Oid(1000),
		   /*typarray*/ Oid(1),
		   /*typinput*/ Oid(1),
		   /*typoutput*/ Oid(1),
		   /*typreceive*/ Oid(1),
		   /*typsend*/ Oid(1),
		   /*typmodin*/ Oid(1),
		   /*typmodout*/ Oid(1),
		   /*typanalyze*/ Oid(1),
		   /*typalign*/ 'i',
		   /*typstorage*/ 'p',
		   /*typnotnull*/ false,
		   /*typbasetype*/ Oid(1),
		   /*typtypmod*/ Oid(1),
		   /*typndims*/ Oid(1),
		   /*typcollation*/ Oid(1)})};

std::pair<Oid, PGTypePtr> TypeProvider::TYPE_UINT8
    = {Oid(++TypeProvider::TYPE_OID_ID),
       std::make_shared<Form_pg_type>(Form_pg_type{
           /*oid*/ Oid(++TypeProvider::TYPE_OID_ID),
           /*typname*/ getTypeName(TypeIndex::UInt8),
		   /*typnamespace*/ Oid(1),
		   /*typowner*/ Oid(1),
		   /*typlen*/ 1,
		   /*typbyval*/ true,
		   /*typtype*/ 'b',
		   /*typcategory*/ 'S',
		   /*typispreferred*/ true,
		   /*typisdefined*/ false,
		   /*typdelim*/ ',',
		   /*typrelid*/ Oid(0),
		   /*typelem*/ Oid(1002),
		   /*typarray*/ Oid(1),
		   /*typinput*/ Oid(1),
		   /*typoutput*/ Oid(1),
		   /*typreceive*/ Oid(1),
		   /*typsend*/ Oid(1),
		   /*typmodin*/ Oid(1),
		   /*typmodout*/ Oid(1),
		   /*typanalyze*/ Oid(1),
		   /*typalign*/ 'i',
		   /*typstorage*/ 'p',
		   /*typnotnull*/ false,
		   /*typbasetype*/ Oid(1),
		   /*typtypmod*/ Oid(1),
		   /*typndims*/ Oid(1),
		   /*typcollation*/ Oid(1)})};

std::pair<Oid, PGTypePtr> TypeProvider::TYPE_UINT16
    = {Oid(++TypeProvider::TYPE_OID_ID),
       std::make_shared<Form_pg_type>(Form_pg_type{
           /*oid*/ Oid(++TypeProvider::TYPE_OID_ID),
           /*typname*/ getTypeName(TypeIndex::UInt16),
		   /*typnamespace*/ Oid(1),
		   /*typowner*/ Oid(1),
		   /*typlen*/ 2,
		   /*typbyval*/ true,
		   /*typtype*/ 'b',
		   /*typcategory*/ 'N',
		   /*typispreferred*/ false,
		   /*typisdefined*/ true,
		   /*typdelim*/ ',',
		   /*typrelid*/ Oid(0),
		   /*typelem*/ Oid(1005),
		   /*typarray*/ Oid(1),
		   /*typinput*/ Oid(1),
		   /*typoutput*/ Oid(1),
		   /*typreceive*/ Oid(1),
		   /*typsend*/ Oid(1),
		   /*typmodin*/ Oid(1),
		   /*typmodout*/ Oid(1),
		   /*typanalyze*/ Oid(1),
		   /*typalign*/ 'i',
		   /*typstorage*/ 'p',
		   /*typnotnull*/ false,
		   /*typbasetype*/ Oid(1),
		   /*typtypmod*/ Oid(1),
		   /*typndims*/ Oid(1),
		   /*typcollation*/ Oid(1)})};

std::pair<Oid, PGTypePtr> TypeProvider::TYPE_UINT32
    = {Oid(++TypeProvider::TYPE_OID_ID),
       std::make_shared<Form_pg_type>(Form_pg_type{
           /*oid*/ Oid(++TypeProvider::TYPE_OID_ID),
           /*typname*/ getTypeName(TypeIndex::UInt32),
		   /*typnamespace*/ Oid(1),
		   /*typowner*/ Oid(1),
		   /*typlen*/ 4,
		   /*typbyval*/ true,
		   /*typtype*/ 'b',
		   /*typcategory*/ 'N',
		   /*typispreferred*/ false,
		   /*typisdefined*/ true,
		   /*typdelim*/ ',',
		   /*typrelid*/ Oid(0),
		   /*typelem*/ Oid(1007),
		   /*typarray*/ Oid(1),
		   /*typinput*/ Oid(1),
		   /*typoutput*/ Oid(1),
		   /*typreceive*/ Oid(1),
		   /*typsend*/ Oid(1),
		   /*typmodin*/ Oid(1),
		   /*typmodout*/ Oid(1),
		   /*typanalyze*/ Oid(1),
		   /*typalign*/ 'i',
		   /*typstorage*/ 'p',
		   /*typnotnull*/ false,
		   /*typbasetype*/ Oid(1),
		   /*typtypmod*/ Oid(1),
		   /*typndims*/ Oid(1),
		   /*typcollation*/ Oid(1)})};

std::pair<Oid, PGTypePtr> TypeProvider::TYPE_UINT64
    = {Oid(++TypeProvider::TYPE_OID_ID),
       std::make_shared<Form_pg_type>(Form_pg_type{
           /*oid*/ Oid(++TypeProvider::TYPE_OID_ID),
           /*typname*/ getTypeName(TypeIndex::UInt64),
		   /*typnamespace*/ Oid(1),
		   /*typowner*/ Oid(1),
		   /*typlen*/ 8,
		   /*typbyval*/ true,
		   /*typtype*/ 'b',
		   /*typcategory*/ 'N',
		   /*typispreferred*/ false,
		   /*typisdefined*/ true,
		   /*typdelim*/ ',',
		   /*typrelid*/ Oid(0),
		   /*typelem*/ Oid(1016),
		   /*typarray*/ Oid(1),
		   /*typinput*/ Oid(1),
		   /*typoutput*/ Oid(1),
		   /*typreceive*/ Oid(1),
		   /*typsend*/ Oid(1),
		   /*typmodin*/ Oid(1),
		   /*typmodout*/ Oid(1),
		   /*typanalyze*/ Oid(1),
		   /*typalign*/ 'i',
		   /*typstorage*/ 'p',
		   /*typnotnull*/ false,
		   /*typbasetype*/ Oid(1),
		   /*typtypmod*/ Oid(1),
		   /*typndims*/ Oid(1),
		   /*typcollation*/ Oid(1)})};

std::pair<Oid, PGTypePtr> TypeProvider::TYPE_UINT128
    = {Oid(++TypeProvider::TYPE_OID_ID),
       std::make_shared<Form_pg_type>(Form_pg_type{
           /*oid*/ Oid(TypeProvider::TYPE_OID_ID),
           /*typname*/ getTypeName(TypeIndex::UInt128),
		   /*typnamespace*/ Oid(1),
		   /*typowner*/ Oid(1),
		   /*typlen*/ 16,
		   /*typbyval*/ true,
		   /*typtype*/ 'b',
		   /*typcategory*/ 'A',
		   /*typispreferred*/ true,
		   /*typisdefined*/ false,
		   /*typdelim*/ ',',
		   /*typrelid*/ Oid(0),
		   /*typelem*/ Oid(1),
		   /*typarray*/ Oid(1),
		   /*typinput*/ Oid(1),
		   /*typoutput*/ Oid(1),
		   /*typreceive*/ Oid(1),
		   /*typsend*/ Oid(1),
		   /*typmodin*/ Oid(1),
		   /*typmodout*/ Oid(1),
		   /*typanalyze*/ Oid(1),
		   /*typalign*/ 'i',
		   /*typstorage*/ 'p',
		   /*typnotnull*/ false,
		   /*typbasetype*/ Oid(1),
		   /*typtypmod*/ Oid(1),
		   /*typndims*/ Oid(1),
		   /*typcollation*/ Oid(1)})};

std::pair<Oid, PGTypePtr> TypeProvider::TYPE_UINT256
    = {Oid(++TypeProvider::TYPE_OID_ID),
       std::make_shared<Form_pg_type>(Form_pg_type{
           /*oid*/ Oid(TypeProvider::TYPE_OID_ID),
           /*typname*/ getTypeName(TypeIndex::UInt256),
		   /*typnamespace*/ Oid(1),
		   /*typowner*/ Oid(1),
		   /*typlen*/ 32,
		   /*typbyval*/ true,
		   /*typtype*/ 'b',
		   /*typcategory*/ 'A',
		   /*typispreferred*/ true,
		   /*typisdefined*/ false,
		   /*typdelim*/ ',',
		   /*typrelid*/ Oid(0),
		   /*typelem*/ Oid(1),
		   /*typarray*/ Oid(1),
		   /*typinput*/ Oid(1),
		   /*typoutput*/ Oid(1),
		   /*typreceive*/ Oid(1),
		   /*typsend*/ Oid(1),
		   /*typmodin*/ Oid(1),
		   /*typmodout*/ Oid(1),
		   /*typanalyze*/ Oid(1),
		   /*typalign*/ 'i',
		   /*typstorage*/ 'p',
		   /*typnotnull*/ false,
		   /*typbasetype*/ Oid(1),
		   /*typtypmod*/ Oid(1),
		   /*typndims*/ Oid(1),
		   /*typcollation*/ Oid(1)})};

std::pair<Oid, PGTypePtr> TypeProvider::TYPE_INT8
    = {Oid(18),
       std::make_shared<Form_pg_type>(Form_pg_type{
           /*oid*/ Oid(18),
           /*typname*/ getTypeName(TypeIndex::Int8),
		   /*typnamespace*/ Oid(1),
		   /*typowner*/ Oid(1),
		   /*typlen*/ 1,
		   /*typbyval*/ true,
		   /*typtype*/ 'b',
		   /*typcategory*/ 'S',
		   /*typispreferred*/ false,
		   /*typisdefined*/ true,
		   /*typdelim*/ ',',
		   /*typrelid*/ Oid(0),
		   /*typelem*/ Oid(1002),
		   /*typarray*/ Oid(1),
		   /*typinput*/ Oid(1),
		   /*typoutput*/ Oid(1),
		   /*typreceive*/ Oid(1),
		   /*typsend*/ Oid(1),
		   /*typmodin*/ Oid(1),
		   /*typmodout*/ Oid(1),
		   /*typanalyze*/ Oid(1),
		   /*typalign*/ 'i',
		   /*typstorage*/ 'p',
		   /*typnotnull*/ false,
		   /*typbasetype*/ Oid(1),
		   /*typtypmod*/ Oid(1),
		   /*typndims*/ Oid(1),
		   /*typcollation*/ Oid(1)})};

std::pair<Oid, PGTypePtr> TypeProvider::TYPE_INT16
    = {Oid(21),
       std::make_shared<Form_pg_type>(Form_pg_type{
           /*oid*/ Oid(21),
           /*typname*/ getTypeName(TypeIndex::Int16),
		   /*typnamespace*/ Oid(1),
		   /*typowner*/ Oid(1),
		   /*typlen*/ 2,
		   /*typbyval*/ true,
		   /*typtype*/ 'b',
		   /*typcategory*/ 'N',
		   /*typispreferred*/ false,
		   /*typisdefined*/ true,
		   /*typdelim*/ ',',
		   /*typrelid*/ Oid(0),
		   /*typelem*/ Oid(1005),
		   /*typarray*/ Oid(1),
		   /*typinput*/ Oid(1),
		   /*typoutput*/ Oid(1),
		   /*typreceive*/ Oid(1),
		   /*typsend*/ Oid(1),
		   /*typmodin*/ Oid(1),
		   /*typmodout*/ Oid(1),
		   /*typanalyze*/ Oid(1),
		   /*typalign*/ 'i',
		   /*typstorage*/ 'p',
		   /*typnotnull*/ false,
		   /*typbasetype*/ Oid(1),
		   /*typtypmod*/ Oid(1),
		   /*typndims*/ Oid(1),
		   /*typcollation*/ Oid(1)})};

std::pair<Oid, PGTypePtr> TypeProvider::TYPE_INT32
    = {Oid(23),
       std::make_shared<Form_pg_type>(Form_pg_type{
           /*oid*/ Oid(23),
           /*typname*/ getTypeName(TypeIndex::Int32),
		   /*typnamespace*/ Oid(1),
		   /*typowner*/ Oid(1),
		   /*typlen*/ 4,
		   /*typbyval*/ true,
		   /*typtype*/ 'b',
		   /*typcategory*/ 'N',
		   /*typispreferred*/ false,
		   /*typisdefined*/ true,
		   /*typdelim*/ ',',
		   /*typrelid*/ Oid(0),
		   /*typelem*/ Oid(1007),
		   /*typarray*/ Oid(1),
		   /*typinput*/ Oid(1),
		   /*typoutput*/ Oid(1),
		   /*typreceive*/ Oid(1),
		   /*typsend*/ Oid(1),
		   /*typmodin*/ Oid(1),
		   /*typmodout*/ Oid(1),
		   /*typanalyze*/ Oid(1),
		   /*typalign*/ 'i',
		   /*typstorage*/ 'p',
		   /*typnotnull*/ false,
		   /*typbasetype*/ Oid(1),
		   /*typtypmod*/ Oid(1),
		   /*typndims*/ Oid(1),
		   /*typcollation*/ Oid(1)})};

std::pair<Oid, PGTypePtr> TypeProvider::TYPE_INT64
    = {Oid(20),
       std::make_shared<Form_pg_type>(Form_pg_type{
           /*oid*/ Oid(20),
           /*typname*/ getTypeName(TypeIndex::Int64),
		   /*typnamespace*/ Oid(1),
		   /*typowner*/ Oid(1),
		   /*typlen*/ 8,
		   /*typbyval*/ true,
		   /*typtype*/ 'b',
		   /*typcategory*/ 'N',
		   /*typispreferred*/ false,
		   /*typisdefined*/ true,
		   /*typdelim*/ ',',
		   /*typrelid*/ Oid(0),
		   /*typelem*/ Oid(1016),
		   /*typarray*/ Oid(1),
		   /*typinput*/ Oid(1),
		   /*typoutput*/ Oid(1),
		   /*typreceive*/ Oid(1),
		   /*typsend*/ Oid(1),
		   /*typmodin*/ Oid(1),
		   /*typmodout*/ Oid(1),
		   /*typanalyze*/ Oid(1),
		   /*typalign*/ 'i',
		   /*typstorage*/ 'p',
		   /*typnotnull*/ false,
		   /*typbasetype*/ Oid(1),
		   /*typtypmod*/ Oid(1),
		   /*typndims*/ Oid(1),
		   /*typcollation*/ Oid(1)})};

std::pair<Oid, PGTypePtr> TypeProvider::TYPE_INT128
    = {Oid(++TypeProvider::TYPE_OID_ID),
       std::make_shared<Form_pg_type>(Form_pg_type{
           /*oid*/ Oid(TypeProvider::TYPE_OID_ID),
           /*typname*/ getTypeName(TypeIndex::Int128),
		   /*typnamespace*/ Oid(1),
		   /*typowner*/ Oid(1),
		   /*typlen*/ 16,
		   /*typbyval*/ true,
		   /*typtype*/ 'b',
		   /*typcategory*/ 'A',
		   /*typispreferred*/ true,
		   /*typisdefined*/ false,
		   /*typdelim*/ ',',
		   /*typrelid*/ Oid(0),
		   /*typelem*/ Oid(1),
		   /*typarray*/ Oid(1),
		   /*typinput*/ Oid(1),
		   /*typoutput*/ Oid(1),
		   /*typreceive*/ Oid(1),
		   /*typsend*/ Oid(1),
		   /*typmodin*/ Oid(1),
		   /*typmodout*/ Oid(1),
		   /*typanalyze*/ Oid(1),
		   /*typalign*/ 'i',
		   /*typstorage*/ 'p',
		   /*typnotnull*/ false,
		   /*typbasetype*/ Oid(1),
		   /*typtypmod*/ Oid(1),
		   /*typndims*/ Oid(1),
		   /*typcollation*/ Oid(1)})};

std::pair<Oid, PGTypePtr> TypeProvider::TYPE_INT256
    = {Oid(++TypeProvider::TYPE_OID_ID),
       std::make_shared<Form_pg_type>(Form_pg_type{
           /*oid*/ Oid(TypeProvider::TYPE_OID_ID),
           /*typname*/ getTypeName(TypeIndex::Int256),
		   /*typnamespace*/ Oid(1),
		   /*typowner*/ Oid(1),
		   /*typlen*/ 32,
		   /*typbyval*/ true,
		   /*typtype*/ 'b',
		   /*typcategory*/ 'A',
		   /*typispreferred*/ true,
		   /*typisdefined*/ false,
		   /*typdelim*/ ',',
		   /*typrelid*/ Oid(0),
		   /*typelem*/ Oid(1),
		   /*typarray*/ Oid(1),
		   /*typinput*/ Oid(1),
		   /*typoutput*/ Oid(1),
		   /*typreceive*/ Oid(1),
		   /*typsend*/ Oid(1),
		   /*typmodin*/ Oid(1),
		   /*typmodout*/ Oid(1),
		   /*typanalyze*/ Oid(1),
		   /*typalign*/ 'i',
		   /*typstorage*/ 'p',
		   /*typnotnull*/ false,
		   /*typbasetype*/ Oid(1),
		   /*typtypmod*/ Oid(1),
		   /*typndims*/ Oid(1),
		   /*typcollation*/ Oid(1)})};

std::pair<Oid, PGTypePtr> TypeProvider::TYPE_STRING
    = {Oid(25),
       std::make_shared<Form_pg_type>(Form_pg_type{
           /*oid*/ Oid(25),
           /*typname*/ getTypeName(TypeIndex::String),
		   /*typnamespace*/ Oid(1),
		   /*typowner*/ Oid(1),
		   /*typlen*/ -1,
		   /*typbyval*/ false,
		   /*typtype*/ 'b',
		   /*typcategory*/ 'S',
		   /*typispreferred*/ true,
		   /*typisdefined*/ true,
		   /*typdelim*/ ',',
		   /*typrelid*/ Oid(0),
		   /*typelem*/ Oid(1009),
		   /*typarray*/ Oid(1),
		   /*typinput*/ Oid(1),
		   /*typoutput*/ Oid(1),
		   /*typreceive*/ Oid(1),
		   /*typsend*/ Oid(1),
		   /*typmodin*/ Oid(1),
		   /*typmodout*/ Oid(1),
		   /*typanalyze*/ Oid(1),
		   /*typalign*/ 'i',
		   /*typstorage*/ 'p',
		   /*typnotnull*/ false,
		   /*typbasetype*/ Oid(1),
		   /*typtypmod*/ Oid(1),
		   /*typndims*/ Oid(1),
		   /*typcollation*/ Oid(1)})};

std::pair<Oid, PGTypePtr> TypeProvider::TYPE_FIXEDSTRING
    = {Oid(1024),
       std::make_shared<Form_pg_type>(Form_pg_type{
           /*oid*/ Oid(1024),
           /*typname*/ getTypeName(TypeIndex::FixedString),
		   /*typnamespace*/ Oid(1),
		   /*typowner*/ Oid(1),
		   /*typlen*/ -1,
		   /*typbyval*/ false,
		   /*typtype*/ 'b',
		   /*typcategory*/ 'S',
		   /*typispreferred*/ false,
		   /*typisdefined*/ true,
		   /*typdelim*/ ',',
		   /*typrelid*/ Oid(0),
		   /*typelem*/ Oid(1014),
		   /*typarray*/ Oid(1),
		   /*typinput*/ Oid(1),
		   /*typoutput*/ Oid(1),
		   /*typreceive*/ Oid(1),
		   /*typsend*/ Oid(1),
		   /*typmodin*/ Oid(1),
		   /*typmodout*/ Oid(1),
		   /*typanalyze*/ Oid(1),
		   /*typalign*/ 'i',
		   /*typstorage*/ 'p',
		   /*typnotnull*/ false,
		   /*typbasetype*/ Oid(1),
		   /*typtypmod*/ Oid(1),
		   /*typndims*/ Oid(1),
		   /*typcollation*/ Oid(1)})};

std::pair<Oid, PGTypePtr> TypeProvider::TYPE_DATE
    = {Oid(1082),
       std::make_shared<Form_pg_type>(Form_pg_type{
           /*oid*/ Oid(1082),
           /*typname*/ getTypeName(TypeIndex::Date),
		   /*typnamespace*/ Oid(1),
		   /*typowner*/ Oid(1),
		   /*typlen*/ 4,
		   /*typbyval*/ true,
		   /*typtype*/ 'b',
		   /*typcategory*/ 'D',
		   /*typispreferred*/ true,
		   /*typisdefined*/ false,
		   /*typdelim*/ ',',
		   /*typrelid*/ Oid(0),
		   /*typelem*/ Oid(1182),
		   /*typarray*/ Oid(1),
		   /*typinput*/ Oid(1),
		   /*typoutput*/ Oid(1),
		   /*typreceive*/ Oid(1),
		   /*typsend*/ Oid(1),
		   /*typmodin*/ Oid(1),
		   /*typmodout*/ Oid(1),
		   /*typanalyze*/ Oid(1),
		   /*typalign*/ 'i',
		   /*typstorage*/ 'p',
		   /*typnotnull*/ false,
		   /*typbasetype*/ Oid(1),
		   /*typtypmod*/ Oid(1),
		   /*typndims*/ Oid(1),
		   /*typcollation*/ Oid(1)})};

std::pair<Oid, PGTypePtr> TypeProvider::TYPE_DATETIME
    = {Oid(1114),
       std::make_shared<Form_pg_type>(Form_pg_type{
           /*oid*/ Oid(1114),
           /*typname*/ getTypeName(TypeIndex::DateTime),
		   /*typnamespace*/ Oid(1),
		   /*typowner*/ Oid(1),
		   /*typlen*/ 8,
		   /*typbyval*/ true,
		   /*typtype*/ 'b',
		   /*typcategory*/ 'D',
		   /*typispreferred*/ true,
		   /*typisdefined*/ false,
		   /*typdelim*/ ',',
		   /*typrelid*/ Oid(0),
		   /*typelem*/ Oid(1115),
		   /*typarray*/ Oid(1),
		   /*typinput*/ Oid(1),
		   /*typoutput*/ Oid(1),
		   /*typreceive*/ Oid(1),
		   /*typsend*/ Oid(1),
		   /*typmodin*/ Oid(1),
		   /*typmodout*/ Oid(1),
		   /*typanalyze*/ Oid(1),
		   /*typalign*/ 'i',
		   /*typstorage*/ 'p',
		   /*typnotnull*/ false,
		   /*typbasetype*/ Oid(1),
		   /*typtypmod*/ Oid(1),
		   /*typndims*/ Oid(1),
		   /*typcollation*/ Oid(1)})};

std::pair<Oid, PGTypePtr> TypeProvider::TYPE_DATETIME64
    = {Oid(1184),
       std::make_shared<Form_pg_type>(Form_pg_type{
           /*oid*/ Oid(1184),
           /*typname*/ getTypeName(TypeIndex::DateTime64),
		   /*typnamespace*/ Oid(1),
		   /*typowner*/ Oid(1),
		   /*typlen*/ 8,
		   /*typbyval*/ true,
		   /*typtype*/ 'b',
		   /*typcategory*/ 'D',
		   /*typispreferred*/ true,
		   /*typisdefined*/ false,
		   /*typdelim*/ ',',
		   /*typrelid*/ Oid(0),
		   /*typelem*/ Oid(1185),
		   /*typarray*/ Oid(1),
		   /*typinput*/ Oid(1),
		   /*typoutput*/ Oid(1),
		   /*typreceive*/ Oid(1),
		   /*typsend*/ Oid(1),
		   /*typmodin*/ Oid(1),
		   /*typmodout*/ Oid(1),
		   /*typanalyze*/ Oid(1),
		   /*typalign*/ 'i',
		   /*typstorage*/ 'p',
		   /*typnotnull*/ false,
		   /*typbasetype*/ Oid(1),
		   /*typtypmod*/ Oid(1),
		   /*typndims*/ Oid(1),
		   /*typcollation*/ Oid(1)})};

std::pair<Oid, PGTypePtr> TypeProvider::TYPE_ARRAY
    = {Oid(++TypeProvider::TYPE_OID_ID),
       std::make_shared<Form_pg_type>(Form_pg_type{
           /*oid*/ Oid(TypeProvider::TYPE_OID_ID),
           /*typname*/ getTypeName(TypeIndex::Array),
		   /*typnamespace*/ Oid(1),
		   /*typowner*/ Oid(1),
		   /*typlen*/ 32,
		   /*typbyval*/ true,
		   /*typtype*/ 'b',
		   /*typcategory*/ 'A',
		   /*typispreferred*/ true,
		   /*typisdefined*/ false,
		   /*typdelim*/ ',',
		   /*typrelid*/ Oid(0),
		   /*typelem*/ Oid(1),
		   /*typarray*/ Oid(1),
		   /*typinput*/ Oid(1),
		   /*typoutput*/ Oid(1),
		   /*typreceive*/ Oid(1),
		   /*typsend*/ Oid(1),
		   /*typmodin*/ Oid(1),
		   /*typmodout*/ Oid(1),
		   /*typanalyze*/ Oid(1),
		   /*typalign*/ 'i',
		   /*typstorage*/ 'p',
		   /*typnotnull*/ false,
		   /*typbasetype*/ Oid(1),
		   /*typtypmod*/ Oid(1),
		   /*typndims*/ Oid(1),
		   /*typcollation*/ Oid(1)})};

std::pair<Oid, PGTypePtr> TypeProvider::TYPE_TUPLE
    = {Oid(++TypeProvider::TYPE_OID_ID),
       std::make_shared<Form_pg_type>(Form_pg_type{
           /*oid*/ Oid(TypeProvider::TYPE_OID_ID),
           /*typname*/ getTypeName(TypeIndex::Tuple),
		   /*typnamespace*/ Oid(1),
		   /*typowner*/ Oid(1),
		   /*typlen*/ 32,
		   /*typbyval*/ true,
		   /*typtype*/ 'b',
		   /*typcategory*/ 'A',
		   /*typispreferred*/ true,
		   /*typisdefined*/ false,
		   /*typdelim*/ ',',
		   /*typrelid*/ Oid(0),
		   /*typelem*/ Oid(1),
		   /*typarray*/ Oid(1),
		   /*typinput*/ Oid(1),
		   /*typoutput*/ Oid(1),
		   /*typreceive*/ Oid(1),
		   /*typsend*/ Oid(1),
		   /*typmodin*/ Oid(1),
		   /*typmodout*/ Oid(1),
		   /*typanalyze*/ Oid(1),
		   /*typalign*/ 'i',
		   /*typstorage*/ 'p',
		   /*typnotnull*/ false,
		   /*typbasetype*/ Oid(1),
		   /*typtypmod*/ Oid(1),
		   /*typndims*/ Oid(1),
		   /*typcollation*/ Oid(1)})};

std::pair<Oid, PGTypePtr> TypeProvider::TYPE_DECIMAL32
    = {Oid(++TypeProvider::TYPE_OID_ID),
       std::make_shared<Form_pg_type>(Form_pg_type{
           /*oid*/ Oid(TypeProvider::TYPE_OID_ID),
           /*typname*/ getTypeName(TypeIndex::Decimal32),
		   /*typnamespace*/ Oid(1),
		   /*typowner*/ Oid(1),
		   /*typlen*/ 4,
		   /*typbyval*/ true,
		   /*typtype*/ 'b',
		   /*typcategory*/ 'A',
		   /*typispreferred*/ true,
		   /*typisdefined*/ false,
		   /*typdelim*/ ',',
		   /*typrelid*/ Oid(0),
		   /*typelem*/ Oid(1),
		   /*typarray*/ Oid(1),
		   /*typinput*/ Oid(1),
		   /*typoutput*/ Oid(1),
		   /*typreceive*/ Oid(1),
		   /*typsend*/ Oid(1),
		   /*typmodin*/ Oid(1),
		   /*typmodout*/ Oid(1),
		   /*typanalyze*/ Oid(1),
		   /*typalign*/ 'i',
		   /*typstorage*/ 'p',
		   /*typnotnull*/ false,
		   /*typbasetype*/ Oid(1),
		   /*typtypmod*/ Oid(1),
		   /*typndims*/ Oid(1),
		   /*typcollation*/ Oid(1)})};

std::pair<Oid, PGTypePtr> TypeProvider::TYPE_DECIMAL64
    = {Oid(++TypeProvider::TYPE_OID_ID),
       std::make_shared<Form_pg_type>(Form_pg_type{
           /*oid*/ Oid(TypeProvider::TYPE_OID_ID),
           /*typname*/ getTypeName(TypeIndex::Decimal64),
		   /*typnamespace*/ Oid(1),
		   /*typowner*/ Oid(1),
		   /*typlen*/ 8,
		   /*typbyval*/ true,
		   /*typtype*/ 'b',
		   /*typcategory*/ 'A',
		   /*typispreferred*/ true,
		   /*typisdefined*/ false,
		   /*typdelim*/ ',',
		   /*typrelid*/ Oid(0),
		   /*typelem*/ Oid(1),
		   /*typarray*/ Oid(1),
		   /*typinput*/ Oid(1),
		   /*typoutput*/ Oid(1),
		   /*typreceive*/ Oid(1),
		   /*typsend*/ Oid(1),
		   /*typmodin*/ Oid(1),
		   /*typmodout*/ Oid(1),
		   /*typanalyze*/ Oid(1),
		   /*typalign*/ 'i',
		   /*typstorage*/ 'p',
		   /*typnotnull*/ false,
		   /*typbasetype*/ Oid(1),
		   /*typtypmod*/ Oid(1),
		   /*typndims*/ Oid(1),
		   /*typcollation*/ Oid(1)})};

std::pair<Oid, PGTypePtr> TypeProvider::TYPE_DECIMAL128
    = {Oid(++TypeProvider::TYPE_OID_ID),
       std::make_shared<Form_pg_type>(Form_pg_type{
           /*oid*/ Oid(TypeProvider::TYPE_OID_ID),
           /*typname*/ getTypeName(TypeIndex::Decimal128),
		   /*typnamespace*/ Oid(1),
		   /*typowner*/ Oid(1),
		   /*typlen*/ 16,
		   /*typbyval*/ true,
		   /*typtype*/ 'b',
		   /*typcategory*/ 'A',
		   /*typispreferred*/ true,
		   /*typisdefined*/ false,
		   /*typdelim*/ ',',
		   /*typrelid*/ Oid(0),
		   /*typelem*/ Oid(1),
		   /*typarray*/ Oid(1),
		   /*typinput*/ Oid(1),
		   /*typoutput*/ Oid(1),
		   /*typreceive*/ Oid(1),
		   /*typsend*/ Oid(1),
		   /*typmodin*/ Oid(1),
		   /*typmodout*/ Oid(1),
		   /*typanalyze*/ Oid(1),
		   /*typalign*/ 'i',
		   /*typstorage*/ 'p',
		   /*typnotnull*/ false,
		   /*typbasetype*/ Oid(1),
		   /*typtypmod*/ Oid(1),
		   /*typndims*/ Oid(1),
		   /*typcollation*/ Oid(1)})};

std::pair<Oid, PGTypePtr> TypeProvider::TYPE_DECIMAL256
    = {Oid(++TypeProvider::TYPE_OID_ID),
       std::make_shared<Form_pg_type>(Form_pg_type{
           /*oid*/ Oid(TypeProvider::TYPE_OID_ID),
           /*typname*/ getTypeName(TypeIndex::Decimal256),
		   /*typnamespace*/ Oid(1),
		   /*typowner*/ Oid(1),
		   /*typlen*/ 32,
		   /*typbyval*/ true,
		   /*typtype*/ 'b',
		   /*typcategory*/ 'A',
		   /*typispreferred*/ true,
		   /*typisdefined*/ false,
		   /*typdelim*/ ',',
		   /*typrelid*/ Oid(0),
		   /*typelem*/ Oid(1),
		   /*typarray*/ Oid(1),
		   /*typinput*/ Oid(1),
		   /*typoutput*/ Oid(1),
		   /*typreceive*/ Oid(1),
		   /*typsend*/ Oid(1),
		   /*typmodin*/ Oid(1),
		   /*typmodout*/ Oid(1),
		   /*typanalyze*/ Oid(1),
		   /*typalign*/ 'i',
		   /*typstorage*/ 'p',
		   /*typnotnull*/ false,
		   /*typbasetype*/ Oid(1),
		   /*typtypmod*/ Oid(1),
		   /*typndims*/ Oid(1),
		   /*typcollation*/ Oid(1)})};

std::pair<Oid, PGTypePtr> TypeProvider::TYPE_AGGFUNCSTATE
    = {Oid(++TypeProvider::TYPE_OID_ID),
       std::make_shared<Form_pg_type>(Form_pg_type{
           /*oid*/ Oid(TypeProvider::TYPE_OID_ID),
           /*typname*/ getTypeName(TypeIndex::AggregateFunction),
		   /*typnamespace*/ Oid(1),
		   /*typowner*/ Oid(1),
		   /*typlen*/ 32,
		   /*typbyval*/ true,
		   /*typtype*/ 'b',
		   /*typcategory*/ 'A',
		   /*typispreferred*/ true,
		   /*typisdefined*/ false,
		   /*typdelim*/ ',',
		   /*typrelid*/ Oid(0),
		   /*typelem*/ Oid(1),
		   /*typarray*/ Oid(1),
		   /*typinput*/ Oid(1),
		   /*typoutput*/ Oid(1),
		   /*typreceive*/ Oid(1),
		   /*typsend*/ Oid(1),
		   /*typmodin*/ Oid(1),
		   /*typmodout*/ Oid(1),
		   /*typanalyze*/ Oid(1),
		   /*typalign*/ 'i',
		   /*typstorage*/ 'p',
		   /*typnotnull*/ false,
		   /*typbasetype*/ Oid(1),
		   /*typtypmod*/ Oid(1),
		   /*typndims*/ Oid(1),
		   /*typcollation*/ Oid(1)})};

std::pair<Oid, PGTypePtr> TypeProvider::TYPE_MAP
    = {Oid(++TypeProvider::TYPE_OID_ID),
       std::make_shared<Form_pg_type>(Form_pg_type{
           /*oid*/ Oid(TypeProvider::TYPE_OID_ID),
           /*typname*/ getTypeName(TypeIndex::Map),
		   /*typnamespace*/ Oid(1),
		   /*typowner*/ Oid(1),
		   /*typlen*/ 32,
		   /*typbyval*/ true,
		   /*typtype*/ 'b',
		   /*typcategory*/ 'A',
		   /*typispreferred*/ true,
		   /*typisdefined*/ false,
		   /*typdelim*/ ',',
		   /*typrelid*/ Oid(0),
		   /*typelem*/ Oid(1),
		   /*typarray*/ Oid(1),
		   /*typinput*/ Oid(1),
		   /*typoutput*/ Oid(1),
		   /*typreceive*/ Oid(1),
		   /*typsend*/ Oid(1),
		   /*typmodin*/ Oid(1),
		   /*typmodout*/ Oid(1),
		   /*typanalyze*/ Oid(1),
		   /*typalign*/ 'i',
		   /*typstorage*/ 'p',
		   /*typnotnull*/ false,
		   /*typbasetype*/ Oid(1),
		   /*typtypmod*/ Oid(1),
		   /*typndims*/ Oid(1),
		   /*typcollation*/ Oid(1)})};

std::pair<Oid, PGTypePtr> TypeProvider::TYPE_UUID
    = {Oid(++TypeProvider::TYPE_OID_ID),
       std::make_shared<Form_pg_type>(Form_pg_type{
           /*oid*/ Oid(TypeProvider::TYPE_OID_ID),
           /*typname*/ getTypeName(TypeIndex::UUID),
		   /*typnamespace*/ Oid(1),
		   /*typowner*/ Oid(1),
		   /*typlen*/ 32,
		   /*typbyval*/ true,
		   /*typtype*/ 'b',
		   /*typcategory*/ 'A',
		   /*typispreferred*/ true,
		   /*typisdefined*/ false,
		   /*typdelim*/ ',',
		   /*typrelid*/ Oid(0),
		   /*typelem*/ Oid(1),
		   /*typarray*/ Oid(1),
		   /*typinput*/ Oid(1),
		   /*typoutput*/ Oid(1),
		   /*typreceive*/ Oid(1),
		   /*typsend*/ Oid(1),
		   /*typmodin*/ Oid(1),
		   /*typmodout*/ Oid(1),
		   /*typanalyze*/ Oid(1),
		   /*typalign*/ 'i',
		   /*typstorage*/ 'p',
		   /*typnotnull*/ false,
		   /*typbasetype*/ Oid(1),
		   /*typtypmod*/ Oid(1),
		   /*typndims*/ Oid(1),
		   /*typcollation*/ Oid(1)})};

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

TypeProvider::TypeProvider()
{
	oid_type_map.insert(TYPE_FLOAT32);
	oid_type_map.insert(TYPE_FLOAT64);
	oid_type_map.insert(TYPE_BOOLEAN);
	oid_type_map.insert(TYPE_UINT8);
	oid_type_map.insert(TYPE_UINT16);
	oid_type_map.insert(TYPE_UINT32);
	oid_type_map.insert(TYPE_UINT64);
	oid_type_map.insert(TYPE_UINT128);
	oid_type_map.insert(TYPE_UINT256);
	oid_type_map.insert(TYPE_INT8);
	oid_type_map.insert(TYPE_INT16);
	oid_type_map.insert(TYPE_INT32);
	oid_type_map.insert(TYPE_INT64);
	oid_type_map.insert(TYPE_INT128);
	oid_type_map.insert(TYPE_INT256);
	oid_type_map.insert(TYPE_STRING);
	oid_type_map.insert(TYPE_FIXEDSTRING);
	oid_type_map.insert(TYPE_DATE);
	oid_type_map.insert(TYPE_DATETIME);
	oid_type_map.insert(TYPE_DATETIME64);
	oid_type_map.insert(TYPE_ARRAY);
	oid_type_map.insert(TYPE_TUPLE);
	oid_type_map.insert(TYPE_DECIMAL32);
	oid_type_map.insert(TYPE_DECIMAL64);
	oid_type_map.insert(TYPE_DECIMAL128);
	oid_type_map.insert(TYPE_DECIMAL256);
	oid_type_map.insert(TYPE_AGGFUNCSTATE);
	oid_type_map.insert(TYPE_MAP);
	oid_type_map.insert(TYPE_UUID);
};

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

PGTypePtr TypeProvider::getTypeByOid(Oid oid) const
{
	auto it = oid_type_map.find(oid);
	if (it == oid_type_map.end())
	    return nullptr;
	return it->second;
};

/*
 * getBaseType
 *		If the given type is a domain, return its base type;
 *		otherwise return the type's own OID.
 */
Oid TypeProvider::getBaseType(Oid typid)
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
Oid TypeProvider::getBaseTypeAndTypmod(Oid typid, int32 * typmod)
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
void TypeProvider::get_type_category_preferred(Oid typid, char * typcategory, bool * typispreferred)
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

std::string TypeProvider::format_type_be(Oid type_oid)
{
    return format_type_internal(type_oid, -1, false, false, false);
};

std::string TypeProvider::printTypmod(const char * typname, int32 typmod, Oid typmodout)
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
        char * tmstr;

        tmstr = DatumGetCString(function_provider->OidFunctionCall1Coll(typmodout, InvalidOid, Int32GetDatum(typmod)));
        //res = psprintf("%s%s", typname, tmstr);
        res = std::string(typname) + std::string(tmstr);
    }

    return res;
};

bool TypeProvider::TypeIsVisible(Oid typid)
{
    //HeapTuple typtup;
    //Form_pg_type typform;
    //Oid typnamespace;
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
    //         Oid namespaceId = lfirst_oid(l);

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

std::string TypeProvider::format_type_with_typemod(Oid type_oid, int32 typemod)
{
	return format_type_internal(type_oid, typemod, true, false, false);
};

std::string TypeProvider::format_type_internal(Oid type_oid, int32 typemod, bool typemod_given, bool allow_invalid, bool force_qualify)
{
    bool with_typemod = typemod_given && (typemod >= 0);
    Oid array_base_type;
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
            nspname = relation_provider->get_namespace_name(tuple->typnamespace);

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
    PGRelationPtr rel = relation_provider->relation_open(type->typrelid, AccessShareLock);
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

	relation_provider->relation_close(rel, AccessShareLock);

	return rel->rd_att;
};

PGTupleDescPtr TypeProvider::lookup_rowtype_tupdesc_internal(Oid type_id, int32 typmod, bool noError)
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
            return get_tupdesc_by_type_relid(type->typrelid);
        }
    }
    else
    {
        /*
		 * It's a transient record type, so look in our record-type table.
		 */
        if (typmod < 0 || typmod >= NextRecordTypmod)
        {
            if (!noError)
            {
                ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("record type has not been registered")));
            }
            return nullptr;
        }
		//TODO kindred
        //return RecordCacheArray[typmod];
		ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("record type cache not supported yet!")));
    }

	return nullptr;
};

PGTupleDescPtr TypeProvider::lookup_rowtype_tupdesc(Oid type_id, int32 typmod)
{
	//TODO kindred
    //PGTupleDescPtr tupDesc = lookup_rowtype_tupdesc_internal(type_id, typmod, false);
    //IncrTupleDescRefCount(tupDesc);
    //return tupDesc;

	return lookup_rowtype_tupdesc_internal(type_id, typmod, false);
};

Oid TypeProvider::get_element_type(Oid typid)
{
	PGTypePtr tup = getTypeByOid(typid);

    if (tup != NULL)
    {
        Oid result;

        if (tup->typlen == -1)
            result = tup->typelem;
        else
            result = InvalidOid;

        return result;
    }
    else
        return InvalidOid;
};

void TypeProvider::getTypeOutputInfo(Oid type, Oid * typOutput, bool * typIsVarlena)
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
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("type %s is only a shell", format_type_be(type).c_str())));
		return;
	}

	if (!OidIsValid(tup->typoutput))
	{
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION), errmsg("no output function available for type %s", format_type_be(type).c_str())));
		return;
	}

	*typOutput = tup->typoutput;
    *typIsVarlena = (!tup->typbyval) && (tup->typlen == -1);

};

void TypeProvider::getTypeInputInfo(Oid type, Oid * typInput, Oid * typIOParam)
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
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("type %s is only a shell", format_type_be(type).c_str())));
		return;
	}

	if (!OidIsValid(tup->typinput))
	{
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION), errmsg("no typinput function available for type %s", format_type_be(type).c_str())));
		return;
	}

	*typInput = tup->typinput;
    *typIOParam = getTypeIOParam(typeTuple);

};

bool TypeProvider::typeInheritsFrom(Oid subclassTypeId, Oid superclassTypeId)
{
    bool result = false;
	// TODO kindred
    // Oid subclassRelid;
    // Oid superclassRelid;
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
    // if (!class_provider->has_subclass(superclassRelid))
    //     return false;

    // /*
	//  * Begin the search at the relation itself, so add its relid to the queue.
	//  */
    // queue = list_make1_oid(subclassRelid);
    // visited = NIL;

    // inhrel = relation_provider->heap_open(InheritsRelationId, AccessShareLock);

    // /*
	//  * Use queue to do a breadth-first traversal of the inheritance graph from
	//  * the relid supplied up to the root.  Notice that we append to the queue
	//  * inside the loop --- this is okay because the foreach() macro doesn't
	//  * advance queue_item until the next loop iteration begins.
	//  */
    // foreach (queue_item, queue)
    // {
    //     Oid this_relid = lfirst_oid(queue_item);
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
    //         Oid inhparent = inh->inhparent;

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

Oid TypeProvider::get_range_subtype(Oid rangeOid)
{
    // HeapTuple tp;

    // tp = SearchSysCache1(RANGETYPE, ObjectIdGetDatum(rangeOid));
    // if (HeapTupleIsValid(tp))
    // {
    //     Form_pg_range rngtup = (Form_pg_range)GETSTRUCT(tp);
    //     Oid result;

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

Oid TypeProvider::get_base_element_type(Oid typid)
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
            Oid result;

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

char TypeProvider::get_typtype(Oid typid)
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

bool TypeProvider::type_is_enum(Oid typid)
{
    return (get_typtype(typid) == TYPTYPE_ENUM);
};

Oid TypeProvider::get_array_type(Oid typid)
{
	Oid			result = InvalidOid;

	PGTypePtr tp = getTypeByOid(typid);
	if (tp != NULL)
	{
		result = tp->typarray;
	}
	return result;
};

bool TypeProvider::type_is_range(Oid typid)
{
	return (get_typtype(typid) == TYPTYPE_RANGE);
};

bool TypeProvider::type_is_rowtype(Oid typid)
{
	return (typid == RECORDOID || get_typtype(typid) == TYPTYPE_COMPOSITE);
};

Oid TypeProvider::get_typcollation(Oid typid)
{
	PGTypePtr tp = getTypeByOid(typid);
    if (tp != NULL)
    {
        Oid result;

        result = tp->typcollation;
        return result;
    }
    else
        return InvalidOid;
};

bool
TypeProvider::type_is_collatable(Oid typid)
{
    return OidIsValid(get_typcollation(typid));
};

void TypeProvider::PGTupleDescInitEntry(
        PGTupleDescPtr desc, PGAttrNumber attributeNumber, const char * attributeName,
		Oid oidtypeid, int32 typmod, int attdim)
{
	//HeapTuple tuple;
    //Form_pg_type typeForm;
    PGAttrPtr att;

    /*
	 * sanity checks
	 */
    Assert(PointerIsValid(desc.get()))
    Assert(attributeNumber >= 1)
    Assert(attributeNumber <= desc->natts)

    /*
	 * initialize the attribute fields
	 */
    att = desc->attrs[attributeNumber - 1];

    att->attrelid = 0; /* dummy value */

    /*
	 * Note: attributeName can be NULL, because the planner doesn't always
	 * fill in valid resname values in targetlists, particularly for resjunk
	 * attributes. Also, do nothing if caller wants to re-use the old attname.
	 */
    if (attributeName == NULL)
        MemSet(NameStr(att->attname), 0, NAMEDATALEN);
    else if (attributeName != NameStr(att->attname))
        namestrcpy(&(att->attname), attributeName);

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

TypeFuncClass
TypeProvider::internal_get_result_type(Oid funcid, duckdb_libpgquery::PGNode * call_expr,
        Oid * resultTypeId, PGTupleDescPtr & resultTupleDesc)
{
    TypeFuncClass result;
    //HeapTuple tp;
    //Form_pg_proc procform;
    Oid rettype;
    PGTupleDescPtr tupdesc = nullptr;

    /* First fetch the function's pg_proc row to inspect its rettype */
    // tp = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
    // if (!HeapTupleIsValid(tp))
    //     elog(ERROR, "cache lookup failed for function %u", funcid);
    // procform = (Form_pg_proc)GETSTRUCT(tp);

	PGProcPtr tp = proc_provider->getProcByOid(funcid);
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

        if (resolve_polymorphic_tupdesc(tupdesc, &procform->proargtypes, call_expr))
        {
            if (tupdesc->tdtypeid == RECORDOID && tupdesc->tdtypmod < 0)
                assign_record_type_typmod(tupdesc);
            if (resultTupleDesc)
                *resultTupleDesc = tupdesc;
            result = TYPEFUNC_COMPOSITE;
        }
        else
        {
            if (resultTupleDesc)
                *resultTupleDesc = NULL;
            result = TYPEFUNC_RECORD;
        }

        ReleaseSysCache(tp);

        return result;
    }

    /*
	 * If scalar polymorphic result, try to resolve it.
	 */
    if (IsPolymorphicType(rettype))
    {
        Oid newrettype = exprType(call_expr);

        if (!OidIsValid(newrettype)) /* this probably should not happen */
            ereport(
                ERROR,
                (errcode(ERRCODE_DATATYPE_MISMATCH),
                 errmsg(
                     "could not determine actual result type for function \"%s\" declared to return type %s",
                     NameStr(procform->proname),
                     format_type_be(rettype))));
        rettype = newrettype;
    }

    if (resultTypeId)
        *resultTypeId = rettype;
    if (resultTupleDesc)
        *resultTupleDesc = NULL; /* default result */

    /* Classify the result type */
    result = get_type_func_class(rettype);
    switch (result)
    {
        case TYPEFUNC_COMPOSITE:
            if (resultTupleDesc)
                *resultTupleDesc = lookup_rowtype_tupdesc_copy(rettype, -1);
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

    ReleaseSysCache(tp);

    return result;
};

TypeFuncClass TypeProvider::get_type_func_class(Oid typid)
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

TypeFuncClass TypeProvider::get_expr_result_type(PGNode * expr, Oid * resultTypeId, PGTupleDescPtr & resultTupleDesc)
{
    //TypeFuncClass result;

    // if (expr && IsA(expr, PGFuncExpr))
    //     result = internal_get_result_type(((PGFuncExpr *)expr)->funcid, expr, NULL, resultTypeId, resultTupleDesc);
    // else if (expr && IsA(expr, PGOpExpr))
    //     result = internal_get_result_type(get_opcode(((PGOpExpr *)expr)->opno), expr, NULL, resultTypeId, resultTupleDesc);
    // else
    // {
    //     /* handle as a generic expression; no chance to resolve RECORD */
    //     Oid typid = exprType(expr);

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
        result = internal_get_result_type(oper_provider->get_opcode(((PGOpExpr *)expr)->opno), expr, resultTypeId, resultTupleDesc);
    }
    else
    {
        /* handle as a generic expression; no chance to resolve RECORD */
        Oid typid = exprType(expr);

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

PGTupleDescPtr TypeProvider::lookup_rowtype_tupdesc_copy(Oid type_id, int32 typmod)
{
    PGTupleDescPtr tmp = lookup_rowtype_tupdesc_internal(type_id, typmod, false);
    return PGCreateTupleDescCopyConstr(tmp);
};

Oid TypeProvider::get_typeoid_by_typename_namespaceoid(const char * type_name, Oid namespace_oid)
{
	return Oid(0);
};

Oid TypeProvider::TypenameGetTypidExtended(const char * typname, bool temp_ok)
{
    // Oid typid;
    // PGListCell * l;

    // recomputeNamespacePath();

    // foreach (l, activeSearchPath)
    // {
    //     Oid namespaceId = lfirst_oid(l);

    //     if (!temp_ok && namespaceId == myTempNamespace)
    //         continue; /* do not look in temp namespace */

    //     typid = GetSysCacheOid2(TYPENAMENSP, PointerGetDatum(typname), ObjectIdGetDatum(namespaceId));
    //     if (OidIsValid(typid))
    //         return typid;
    // }

    /* Not found in path */
    return InvalidOid;
};

Oid TypeProvider::getTypeIOParam(PGTypePtr typeTuple)
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

}
