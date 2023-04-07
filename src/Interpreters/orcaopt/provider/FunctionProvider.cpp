#include <Interpreters/orcaopt/provider/FunctionProvider.h>

#include "gpos/base.h"

#include "naucrates/dxl/gpdb_types.h"
#include "naucrates/md/CMDScCmpGPDB.h"
#include "naucrates/md/CMDTypeBoolGPDB.h"
#include "naucrates/md/CMDTypeGenericGPDB.h"
#include "naucrates/md/CMDTypeInt2GPDB.h"
#include "naucrates/md/CMDTypeInt4GPDB.h"
#include "naucrates/md/CMDTypeInt8GPDB.h"
#include "naucrates/md/CMDTypeOidGPDB.h"
//#include "naucrates/md/CMDidGPDB.h"
#include "naucrates/md/CMDName.h"
#include "naucrates/md/IMDId.h"
#include "naucrates/md/IMDAggregate.h"

#include "naucrates/dxl/CDXLUtils.h"

#include <Interpreters/Context.h>

#ifdef __clang__
#pragma clang diagnostic ignored "-Wunused-parameter"
#else
#pragma GCC diagnostic ignored "-Wunused-parameter"
#endif

using namespace duckdb_libpgquery;

namespace DB
{

gpdxl::CMDName *
FunctionProvider::CreateMDName(const char *name_str)
{
	gpos::CWStringDynamic *str_name =
			gpdxl::CDXLUtils::CreateDynamicStringFromCharArray(mp, name_str);
	gpdxl::CMDName *mdname = GPOS_NEW(mp) gpdxl::CMDName(mp, str_name);

	// cleanup
	GPOS_DELETE(str_name);
	return mdname;
};

FunctionProvider::FunctionProvider(const ContextPtr& context_) : context(context_)
{

};

// FunctionProvider::FunctionProvider(gpos::CMemoryPool *mp_, ContextPtr context_)
// 		: context(std::move(context_)),
// 		  mp(std::move(mp_))
// {
// 	/*-------------plus start---------------------------*/
// 	//float32+float32=float32
// 	gpmd::IMdIdArray *arg_type_mdids = GPOS_NEW(mp) gpmd::IMdIdArray(mp);
// 	arg_type_mdids->Append(GPOS_NEW(mp) gpmd::CMDIdGPDB(OID(3)));
// 	oid_fun_map.insert(std::pair<OID, IMDFunctionPtr>(OID(1), GPOS_NEW(mp) gpos::CMDFunctionGPDB(
// 			mp, gpos::CMDIdGPDB(OID(1)), /*mdname*/ CreateMDName("plus"),
// 			/*result_type_mdid*/ gpos::CMDIdGPDB(OID(3)),
// 			/*arg_type_mdids*/ arg_type_mdids, /*returns_set*/ false,
// 			/*stability*/ gpmd::IMDFunction::EfsStable,
// 			/*access*/ gpmd::IMDFunction::EfdaNoSQL, /*is_strict*/ true,
// 			/*is_ndv_preserving*/ false, /*is_allowed_for_PS*/ false)));

// 	//Float64+Float64=Float64
// 	gpmd::IMdIdArray *arg_type_mdids = GPOS_NEW(mp) gpmd::IMdIdArray(mp);
// 	arg_type_mdids->Append(GPOS_NEW(mp) gpmd::CMDIdGPDB(OID(4)));
// 	oid_fun_map.insert(std::pair<OID, IMDFunctionPtr>(OID(2), GPOS_NEW(mp) gpos::CMDFunctionGPDB(
// 			mp, gpos::CMDIdGPDB(OID(2)), /*mdname*/ CreateMDName("plus"),
// 			/*result_type_mdid*/ gpos::CMDIdGPDB(OID(4)),
// 			/*arg_type_mdids*/ arg_type_mdids, /*returns_set*/ false,
// 			/*stability*/ gpmd::IMDFunction::EfsStable,
// 			/*access*/ gpmd::IMDFunction::EfdaNoSQL, /*is_strict*/ true,
// 			/*is_ndv_preserving*/ false, /*is_allowed_for_PS*/ false)));

// 	//Boolean+Boolean=UInt64
// 	gpmd::IMdIdArray *arg_type_mdids = GPOS_NEW(mp) gpmd::IMdIdArray(mp);
// 	arg_type_mdids->Append(GPOS_NEW(mp) gpmd::CMDIdGPDB(OID(5)));
// 	oid_fun_map.insert(std::pair<OID, IMDFunctionPtr>(OID(3), GPOS_NEW(mp) gpos::CMDFunctionGPDB(
// 			mp, gpos::CMDIdGPDB(OID(3)), /*mdname*/ CreateMDName("plus"),
// 			/*result_type_mdid*/ gpos::CMDIdGPDB(OID(9)),
// 			/*arg_type_mdids*/ arg_type_mdids, /*returns_set*/ false,
// 			/*stability*/ gpmd::IMDFunction::EfsStable,
// 			/*access*/ gpmd::IMDFunction::EfdaNoSQL, /*is_strict*/ true,
// 			/*is_ndv_preserving*/ false, /*is_allowed_for_PS*/ false)));

// 	//UInt8+UInt8=UInt64
// 	gpmd::IMdIdArray *arg_type_mdids = GPOS_NEW(mp) gpmd::IMdIdArray(mp);
// 	arg_type_mdids->Append(GPOS_NEW(mp) gpmd::CMDIdGPDB(OID(6)));
// 	oid_fun_map.insert(std::pair<OID, IMDFunctionPtr>(OID(4), GPOS_NEW(mp) gpos::CMDFunctionGPDB(
// 			mp, gpos::CMDIdGPDB(OID(4)), /*mdname*/ CreateMDName("plus"),
// 			/*result_type_mdid*/ gpos::CMDIdGPDB(OID(9)),
// 			/*arg_type_mdids*/ arg_type_mdids, /*returns_set*/ false,
// 			/*stability*/ gpmd::IMDFunction::EfsStable,
// 			/*access*/ gpmd::IMDFunction::EfdaNoSQL, /*is_strict*/ true,
// 			/*is_ndv_preserving*/ false, /*is_allowed_for_PS*/ false)));

// 	//UInt16+UInt16=UInt64
// 	gpmd::IMdIdArray *arg_type_mdids = GPOS_NEW(mp) gpmd::IMdIdArray(mp);
// 	arg_type_mdids->Append(GPOS_NEW(mp) gpmd::CMDIdGPDB(OID(7)));
// 	oid_fun_map.insert(std::pair<OID, IMDFunctionPtr>(OID(5), GPOS_NEW(mp) gpos::CMDFunctionGPDB(
// 			mp, gpos::CMDIdGPDB(OID(5)), /*mdname*/ CreateMDName("plus"),
// 			/*result_type_mdid*/ gpos::CMDIdGPDB(OID(9)),
// 			/*arg_type_mdids*/ arg_type_mdids, /*returns_set*/ false,
// 			/*stability*/ gpmd::IMDFunction::EfsStable,
// 			/*access*/ gpmd::IMDFunction::EfdaNoSQL, /*is_strict*/ true,
// 			/*is_ndv_preserving*/ false, /*is_allowed_for_PS*/ false)));

// 	//UInt32+UInt32=UInt64
// 	gpmd::IMdIdArray *arg_type_mdids = GPOS_NEW(mp) gpmd::IMdIdArray(mp);
// 	arg_type_mdids->Append(GPOS_NEW(mp) gpmd::CMDIdGPDB(OID(8)));
// 	oid_fun_map.insert(std::pair<OID, IMDFunctionPtr>(OID(6), GPOS_NEW(mp) gpos::CMDFunctionGPDB(
// 			mp, gpos::CMDIdGPDB(OID(6)), /*mdname*/ CreateMDName("plus"),
// 			/*result_type_mdid*/ gpos::CMDIdGPDB(OID(9)),
// 			/*arg_type_mdids*/ arg_type_mdids, /*returns_set*/ false,
// 			/*stability*/ gpmd::IMDFunction::EfsStable,
// 			/*access*/ gpmd::IMDFunction::EfdaNoSQL, /*is_strict*/ true,
// 			/*is_ndv_preserving*/ false, /*is_allowed_for_PS*/ false)));

// 	//UInt64+UInt64=UInt64
// 	gpmd::IMdIdArray *arg_type_mdids = GPOS_NEW(mp) gpmd::IMdIdArray(mp);
// 	arg_type_mdids->Append(GPOS_NEW(mp) gpmd::CMDIdGPDB(OID(9)));
// 	oid_fun_map.insert(std::pair<OID, IMDFunctionPtr>(OID(7), GPOS_NEW(mp) gpos::CMDFunctionGPDB(
// 			mp, gpos::CMDIdGPDB(OID(7)), /*mdname*/ CreateMDName("plus"),
// 			/*result_type_mdid*/ gpos::CMDIdGPDB(OID(9)),
// 			/*arg_type_mdids*/ arg_type_mdids, /*returns_set*/ false,
// 			/*stability*/ gpmd::IMDFunction::EfsStable,
// 			/*access*/ gpmd::IMDFunction::EfdaNoSQL, /*is_strict*/ true,
// 			/*is_ndv_preserving*/ false, /*is_allowed_for_PS*/ false)));

// 	//UInt128+UInt128=UInt128
// 	gpmd::IMdIdArray *arg_type_mdids = GPOS_NEW(mp) gpmd::IMdIdArray(mp);
// 	arg_type_mdids->Append(GPOS_NEW(mp) gpmd::CMDIdGPDB(OID(10)));
// 	oid_fun_map.insert(std::pair<OID, IMDFunctionPtr>(OID(8), GPOS_NEW(mp) gpos::CMDFunctionGPDB(
// 			mp, gpos::CMDIdGPDB(OID(8)), /*mdname*/ CreateMDName("plus"),
// 			/*result_type_mdid*/ gpos::CMDIdGPDB(OID(10)),
// 			/*arg_type_mdids*/ arg_type_mdids, /*returns_set*/ false,
// 			/*stability*/ gpmd::IMDFunction::EfsStable,
// 			/*access*/ gpmd::IMDFunction::EfdaNoSQL, /*is_strict*/ true,
// 			/*is_ndv_preserving*/ false, /*is_allowed_for_PS*/ false)));

// 	//UInt256+UInt256=UInt256
// 	gpmd::IMdIdArray *arg_type_mdids = GPOS_NEW(mp) gpmd::IMdIdArray(mp);
// 	arg_type_mdids->Append(GPOS_NEW(mp) gpmd::CMDIdGPDB(OID(11)));
// 	oid_fun_map.insert(std::pair<OID, IMDFunctionPtr>(OID(9), GPOS_NEW(mp) gpos::CMDFunctionGPDB(
// 			mp, gpos::CMDIdGPDB(OID(9)), /*mdname*/ CreateMDName("plus"),
// 			/*result_type_mdid*/ gpos::CMDIdGPDB(OID(11)),
// 			/*arg_type_mdids*/ arg_type_mdids, /*returns_set*/ false,
// 			/*stability*/ gpmd::IMDFunction::EfsStable,
// 			/*access*/ gpmd::IMDFunction::EfdaNoSQL, /*is_strict*/ true,
// 			/*is_ndv_preserving*/ false, /*is_allowed_for_PS*/ false)));

// 	//Int8+Int8=Int64
// 	gpmd::IMdIdArray *arg_type_mdids = GPOS_NEW(mp) gpmd::IMdIdArray(mp);
// 	arg_type_mdids->Append(GPOS_NEW(mp) gpmd::CMDIdGPDB(OID(12)));
// 	oid_fun_map.insert(std::pair<OID, IMDFunctionPtr>(OID(10), GPOS_NEW(mp) gpos::CMDFunctionGPDB(
// 			mp, gpos::CMDIdGPDB(OID(10)), /*mdname*/ CreateMDName("plus"),
// 			/*result_type_mdid*/ gpos::CMDIdGPDB(OID(15)),
// 			/*arg_type_mdids*/ arg_type_mdids, /*returns_set*/ false,
// 			/*stability*/ gpmd::IMDFunction::EfsStable,
// 			/*access*/ gpmd::IMDFunction::EfdaNoSQL, /*is_strict*/ true,
// 			/*is_ndv_preserving*/ false, /*is_allowed_for_PS*/ false)));

// 	//Int16+Int16=Int64
// 	gpmd::IMdIdArray *arg_type_mdids = GPOS_NEW(mp) gpmd::IMdIdArray(mp);
// 	arg_type_mdids->Append(GPOS_NEW(mp) gpmd::CMDIdGPDB(OID(13)));
// 	oid_fun_map.insert(std::pair<OID, IMDFunctionPtr>(OID(11), GPOS_NEW(mp) gpos::CMDFunctionGPDB(
// 			mp, gpos::CMDIdGPDB(OID(11)), /*mdname*/ CreateMDName("plus"),
// 			/*result_type_mdid*/ gpos::CMDIdGPDB(OID(15)),
// 			/*arg_type_mdids*/ arg_type_mdids, /*returns_set*/ false,
// 			/*stability*/ gpmd::IMDFunction::EfsStable,
// 			/*access*/ gpmd::IMDFunction::EfdaNoSQL, /*is_strict*/ true,
// 			/*is_ndv_preserving*/ false, /*is_allowed_for_PS*/ false)));

// 	//Int32+Int32=Int64
// 	gpmd::IMdIdArray *arg_type_mdids = GPOS_NEW(mp) gpmd::IMdIdArray(mp);
// 	arg_type_mdids->Append(GPOS_NEW(mp) gpmd::CMDIdGPDB(OID(14)));
// 	oid_fun_map.insert(std::pair<OID, IMDFunctionPtr>(OID(12), GPOS_NEW(mp) gpos::CMDFunctionGPDB(
// 			mp, gpos::CMDIdGPDB(OID(12)), /*mdname*/ CreateMDName("plus"),
// 			/*result_type_mdid*/ gpos::CMDIdGPDB(OID(15)),
// 			/*arg_type_mdids*/ arg_type_mdids, /*returns_set*/ false,
// 			/*stability*/ gpmd::IMDFunction::EfsStable,
// 			/*access*/ gpmd::IMDFunction::EfdaNoSQL, /*is_strict*/ true,
// 			/*is_ndv_preserving*/ false, /*is_allowed_for_PS*/ false)));

// 	//Int64+Int64=Int64
// 	gpmd::IMdIdArray *arg_type_mdids = GPOS_NEW(mp) gpmd::IMdIdArray(mp);
// 	arg_type_mdids->Append(GPOS_NEW(mp) gpmd::CMDIdGPDB(OID(15)));
// 	oid_fun_map.insert(std::pair<OID, IMDFunctionPtr>(OID(13), GPOS_NEW(mp) gpos::CMDFunctionGPDB(
// 			mp, gpos::CMDIdGPDB(OID(13)), /*mdname*/ CreateMDName("plus"),
// 			/*result_type_mdid*/ gpos::CMDIdGPDB(OID(15)),
// 			/*arg_type_mdids*/ arg_type_mdids, /*returns_set*/ false,
// 			/*stability*/ gpmd::IMDFunction::EfsStable,
// 			/*access*/ gpmd::IMDFunction::EfdaNoSQL, /*is_strict*/ true,
// 			/*is_ndv_preserving*/ false, /*is_allowed_for_PS*/ false)));

// 	//Int128+Int128=Int128
// 	gpmd::IMdIdArray *arg_type_mdids = GPOS_NEW(mp) gpmd::IMdIdArray(mp);
// 	arg_type_mdids->Append(GPOS_NEW(mp) gpmd::CMDIdGPDB(OID(16)));
// 	oid_fun_map.insert(std::pair<OID, IMDFunctionPtr>(OID(14), GPOS_NEW(mp) gpos::CMDFunctionGPDB(
// 			mp, gpos::CMDIdGPDB(OID(14)), /*mdname*/ CreateMDName("plus"),
// 			/*result_type_mdid*/ gpos::CMDIdGPDB(OID(16)),
// 			/*arg_type_mdids*/ arg_type_mdids, /*returns_set*/ false,
// 			/*stability*/ gpmd::IMDFunction::EfsStable,
// 			/*access*/ gpmd::IMDFunction::EfdaNoSQL, /*is_strict*/ true,
// 			/*is_ndv_preserving*/ false, /*is_allowed_for_PS*/ false)));

// 	//Int256+Int256=Int256
// 	gpmd::IMdIdArray *arg_type_mdids = GPOS_NEW(mp) gpmd::IMdIdArray(mp);
// 	arg_type_mdids->Append(GPOS_NEW(mp) gpmd::CMDIdGPDB(OID(17)));
// 	oid_fun_map.insert(std::pair<OID, IMDFunctionPtr>(OID(15), GPOS_NEW(mp) gpos::CMDFunctionGPDB(
// 			mp, gpos::CMDIdGPDB(OID(15)), /*mdname*/ CreateMDName("plus"),
// 			/*result_type_mdid*/ gpos::CMDIdGPDB(OID(17)),
// 			/*arg_type_mdids*/ arg_type_mdids, /*returns_set*/ false,
// 			/*stability*/ gpmd::IMDFunction::EfsStable,
// 			/*access*/ gpmd::IMDFunction::EfdaNoSQL, /*is_strict*/ true,
// 			/*is_ndv_preserving*/ false, /*is_allowed_for_PS*/ false)));

// 	/*-------------plus end---------------------------*/

// 	//Float64
// 	arg_type_mdids = GPOS_NEW(mp) gpmd::IMdIdArray(mp);
// 	arg_type_mdids->Append(GPOS_NEW(mp) gpmd::CMDIdGPDB(OID(4)));
// 	oid_fun_map.insert(std::pair<OID, IMDFunctionPtr>(OID(2), GPOS_NEW(mp) gpos::CMDFunctionGPDB(
// 			mp, gpos::CMDIdGPDB(OID(2)), /*mdname*/ CreateMDName("minus"),
// 			/*result_type_mdid*/ OID(4),
// 			/*arg_type_mdids*/ arg_type_mdids, /*returns_set*/ false,
// 			/*stability*/ gpmd::IMDFunction::EfsStable,
// 			/*access*/ gpmd::IMDFunction::EfdaNoSQL, /*is_strict*/ true,
// 			/*is_ndv_preserving*/ false, /*is_allowed_for_PS*/ false)));

// 	//Boolean
// 	arg_type_mdids = GPOS_NEW(mp) gpmd::IMdIdArray(mp);
// 	arg_type_mdids->Append(GPOS_NEW(mp) gpmd::CMDIdGPDB(OID(5)));
// 	oid_fun_map.insert(std::pair<OID, IMDFunctionPtr>(OID(3), GPOS_NEW(mp) gpos::CMDFunctionGPDB(
// 			mp, gpos::CMDIdGPDB(OID(3)), /*mdname*/ CreateMDName("multiply"), /*result_type_mdid*/ OID(3),
// 			/*arg_type_mdids*/ arg_type_mdids, /*returns_set*/ false,
// 			/*stability*/ gpmd::IMDFunction::EfsStable, /*access*/ gpmd::IMDFunction::EfdaNoSQL, /*is_strict*/ true,
// 			/*is_ndv_preserving*/ false, /*is_allowed_for_PS*/ false)));

// 	arg_type_mdids = GPOS_NEW(mp) gpmd::IMdIdArray(mp);
// 	arg_type_mdids->Append(GPOS_NEW(mp) gpmd::CMDIdGPDB(OID(2)));
// 	//float32
// 	oid_fun_map.insert(std::pair<OID, IMDFunctionPtr>(OID(4), GPOS_NEW(mp) gpos::CMDFunctionGPDB(
// 			mp, gpos::CMDIdGPDB(OID(4)), /*mdname*/ CreateMDName("divide"), /*result_type_mdid*/ OID(3),
// 			/*arg_type_mdids*/ arg_type_mdids, /*returns_set*/ false,
// 			/*stability*/ gpmd::IMDFunction::EfsStable, /*access*/ gpmd::IMDFunction::EfdaNoSQL, /*is_strict*/ true,
// 			/*is_ndv_preserving*/ false, /*is_allowed_for_PS*/ false)));

// 	arg_type_mdids = GPOS_NEW(mp) gpmd::IMdIdArray(mp);
// 	arg_type_mdids->Append(GPOS_NEW(mp) gpmd::CMDIdGPDB(OID(2)));
// 	//float64
// 	oid_fun_map.insert(std::pair<OID, IMDFunctionPtr>(OID(5), GPOS_NEW(mp) gpos::CMDFunctionGPDB(
// 			mp, gpos::CMDIdGPDB(OID(5)), /*mdname*/ CreateMDName("SUM"), /*result_type_mdid*/ OID(4),
// 			/*arg_type_mdids*/ arg_type_mdids, /*returns_set*/ false,
// 			/*stability*/ gpmd::IMDFunction::EfsStable, /*access*/ gpmd::IMDFunction::EfdaNoSQL, /*is_strict*/ true,
// 			/*is_ndv_preserving*/ false, /*is_allowed_for_PS*/ false)));

// 	arg_type_mdids = GPOS_NEW(mp) gpmd::IMdIdArray(mp);
// 	arg_type_mdids->Append(GPOS_NEW(mp) gpmd::CMDIdGPDB(OID(2)));
// 	//Int64
// 	oid_fun_map.insert(std::pair<OID, IMDFunctionPtr>(OID(6), GPOS_NEW(mp) gpos::CMDFunctionGPDB(
// 			mp, gpos::CMDIdGPDB(OID(6)), /*mdname*/ CreateMDName("SUM"), /*result_type_mdid*/ OID(15),
// 			/*arg_type_mdids*/ arg_type_mdids, /*returns_set*/ false,
// 			/*stability*/ gpmd::IMDFunction::EfsStable, /*access*/ gpmd::IMDFunction::EfdaNoSQL, /*is_strict*/ true,
// 			/*is_ndv_preserving*/ false, /*is_allowed_for_PS*/ false)));
// };

IMDFunctionPtr
FunctionProvider::getFunctionByOID(OID oid) const
{
	auto it = oid_fun_map.find(oid);
	if (it == oid_fun_map.end())
	    return {};
	return it->second;
};

// Datum FunctionProvider::OidFunctionCall2(Oid functionId, Datum arg1, Datum arg2)
// {
//     // FmgrInfo flinfo;
//     // FunctionCallInfoData fcinfo;
//     // Datum result;

//     // fmgr_info(functionId, &flinfo);

//     // InitFunctionCallInfoData(fcinfo, &flinfo, 2, InvalidOid, NULL, NULL);

//     // fcinfo.arg[0] = arg1;
//     // fcinfo.arg[1] = arg2;
//     // fcinfo.argnull[0] = false;
//     // fcinfo.argnull[1] = false;

//     // result = FunctionCallInvoke(&fcinfo);

//     // /* Check for null result, since caller is clearly not expecting one */
//     // if (fcinfo.isnull)
//     //     elog(ERROR, "function %u returned NULL", flinfo.fn_oid);

//     // return result;

//     Datum result = 0;
//     return result;
// };

// Datum FunctionProvider::OidFunctionCall1Coll(Oid functionId, Datum arg1)
// {
//     // FmgrInfo flinfo;
//     // FunctionCallInfoData fcinfo;
//     // Datum result;

//     // fmgr_info(functionId, &flinfo);

//     // InitFunctionCallInfoData(fcinfo, &flinfo, 1, InvalidOid, NULL, NULL);

//     // fcinfo.arg[0] = arg1;
//     // fcinfo.argnull[0] = false;

//     // result = FunctionCallInvoke(&fcinfo);

//     // /* Check for null result, since caller is clearly not expecting one */
//     // if (fcinfo.isnull)
//     //     elog(ERROR, "function %u returned NULL", flinfo.fn_oid);

//     // return result;

//     Datum result = 0;
//     return result;
// };

// Datum FunctionProvider::OidFunctionCall1Coll(Oid functionId, Oid collation, Datum arg1)
// {
//     // FmgrInfo flinfo;
//     // FunctionCallInfoData fcinfo;
//     // Datum result;

//     // fmgr_info(functionId, &flinfo);

//     // InitFunctionCallInfoData(fcinfo, &flinfo, 1, collation, NULL, NULL);

//     // fcinfo.arg[0] = arg1;
//     // fcinfo.argnull[0] = false;

//     // result = FunctionCallInvoke(&fcinfo);

//     // /* Check for null result, since caller is clearly not expecting one */
//     // if (fcinfo.isnull)
//     //     elog(ERROR, "function %u returned NULL", flinfo.fn_oid);

//     // return result;

//     Datum result = 0;
//     return result;
// };

// Datum FunctionProvider::OidFunctionCall1_DatumArr(Oid functionId, Datum * datums)
// {
//     /* hardwired knowledge about cstring's representation details here */
//     // ArrayType * arrtypmod = construct_array(datums, n, CSTRINGOID, -2, false, 'c');

//     // /* arrange to report location if type's typmodin function fails */
//     // setup_parser_errposition_callback(&pcbstate, pstate, typeName->location);

//     // result = DatumGetInt32(OidFunctionCall1(typmodin, PointerGetDatum(arrtypmod)));

//     // cancel_parser_errposition_callback(&pcbstate);

//     // pfree(arrtypmod);

//     // return result;

//     Datum result = 0;
//     return result;
// };

Datum FunctionProvider::OidInputFunctionCall(Oid functionId, const char * str, Oid typioparam, int32 typmod)
{
    // FmgrInfo	flinfo;

	// fmgr_info(functionId, &flinfo);
	// return InputFunctionCall(&flinfo, str, typioparam, typmod);

    Datum result = 0;
    return result;
};

FuncCandidateListPtr
FunctionProvider::FuncnameGetCandidates(PGList * names, int nargs, PGList * argnames,
		bool expand_variadic, bool expand_defaults, bool missing_ok)
{
    // FuncCandidateList resultList = NULL;
    // bool any_special = false;
    // char * schemaname;
    // char * funcname;
    // Oid namespaceId;
    // CatCList * catlist;
    // int i;

    // /* check for caller error */
    // Assert(nargs >= 0 || !(expand_variadic | expand_defaults));

    // /* deconstruct the name list */
    // DeconstructQualifiedName(names, &schemaname, &funcname);

    // if (schemaname)
    // {
    //     /* use exact schema given */
    //     namespaceId = LookupExplicitNamespace(schemaname, missing_ok);
    //     if (!OidIsValid(namespaceId))
    //         return NULL;
    // }
    // else
    // {
    //     /* flag to indicate we need namespace search */
    //     namespaceId = InvalidOid;
    //     recomputeNamespacePath();
    // }

    // /* Search syscache by name only */
    // catlist = SearchSysCacheList1(PROCNAMEARGSNSP, CStringGetDatum(funcname));

    // for (i = 0; i < catlist->n_members; i++)
    // {
    //     HeapTuple proctup = &catlist->members[i]->tuple;
    //     Form_pg_proc procform = (Form_pg_proc)GETSTRUCT(proctup);
    //     int pronargs = procform->pronargs;
    //     int effective_nargs;
    //     int pathpos = 0;
    //     bool variadic;
    //     bool use_defaults;
    //     Oid va_elem_type;
    //     int * argnumbers = NULL;
    //     FuncCandidateList newResult;

    //     if (OidIsValid(namespaceId))
    //     {
    //         /* Consider only procs in specified namespace */
    //         if (procform->pronamespace != namespaceId)
    //             continue;
    //     }
    //     else
    //     {
    //         /*
	// 		 * Consider only procs that are in the search path and are not in
	// 		 * the temp namespace.
	// 		 */
    //         PGListCell * nsp;

    //         foreach (nsp, activeSearchPath)
    //         {
    //             if (procform->pronamespace == lfirst_oid(nsp) && procform->pronamespace != myTempNamespace)
    //                 break;
    //             pathpos++;
    //         }
    //         if (nsp == NULL)
    //             continue; /* proc is not in search path */
    //     }

    //     if (argnames != NIL)
    //     {
    //         /*
	// 		 * Call uses named or mixed notation
	// 		 *
	// 		 * Named or mixed notation can match a variadic function only if
	// 		 * expand_variadic is off; otherwise there is no way to match the
	// 		 * presumed-nameless parameters expanded from the variadic array.
	// 		 */
    //         if (OidIsValid(procform->provariadic) && expand_variadic)
    //             continue;
    //         va_elem_type = InvalidOid;
    //         variadic = false;

    //         /*
	// 		 * Check argument count.
	// 		 */
    //         Assert(nargs >= 0); /* -1 not supported with argnames */

    //         if (pronargs > nargs && expand_defaults)
    //         {
    //             /* Ignore if not enough default expressions */
    //             if (nargs + procform->pronargdefaults < pronargs)
    //                 continue;
    //             use_defaults = true;
    //         }
    //         else
    //             use_defaults = false;

    //         /* Ignore if it doesn't match requested argument count */
    //         if (pronargs != nargs && !use_defaults)
    //             continue;

    //         /* Check for argument name match, generate positional mapping */
    //         if (!MatchNamedCall(proctup, nargs, argnames, &argnumbers))
    //             continue;

    //         /* Named argument matching is always "special" */
    //         any_special = true;
    //     }
    //     else
    //     {
    //         /*
	// 		 * Call uses positional notation
	// 		 *
	// 		 * Check if function is variadic, and get variadic element type if
	// 		 * so.  If expand_variadic is false, we should just ignore
	// 		 * variadic-ness.
	// 		 */
    //         if (pronargs <= nargs && expand_variadic)
    //         {
    //             va_elem_type = procform->provariadic;
    //             variadic = OidIsValid(va_elem_type);
    //             any_special |= variadic;
    //         }
    //         else
    //         {
    //             va_elem_type = InvalidOid;
    //             variadic = false;
    //         }

    //         /*
	// 		 * Check if function can match by using parameter defaults.
	// 		 */
    //         if (pronargs > nargs && expand_defaults)
    //         {
    //             /* Ignore if not enough default expressions */
    //             if (nargs + procform->pronargdefaults < pronargs)
    //                 continue;
    //             use_defaults = true;
    //             any_special = true;
    //         }
    //         else
    //             use_defaults = false;

    //         /* Ignore if it doesn't match requested argument count */
    //         if (nargs >= 0 && pronargs != nargs && !variadic && !use_defaults)
    //             continue;
    //     }

    //     /*
	// 	 * We must compute the effective argument list so that we can easily
	// 	 * compare it to earlier results.  We waste a palloc cycle if it gets
	// 	 * masked by an earlier result, but really that's a pretty infrequent
	// 	 * case so it's not worth worrying about.
	// 	 */
    //     effective_nargs = Max(pronargs, nargs);
    //     newResult = (FuncCandidateList)palloc(sizeof(struct _FuncCandidateList) - sizeof(Oid) + effective_nargs * sizeof(Oid));
    //     newResult->pathpos = pathpos;
    //     newResult->oid = HeapTupleGetOid(proctup);
    //     newResult->nargs = effective_nargs;
    //     newResult->argnumbers = argnumbers;
    //     if (argnumbers)
    //     {
    //         /* Re-order the argument types into call's logical order */
    //         Oid * proargtypes = procform->proargtypes.values;
    //         int i;

    //         for (i = 0; i < pronargs; i++)
    //             newResult->args[i] = proargtypes[argnumbers[i]];
    //     }
    //     else
    //     {
    //         /* Simple positional case, just copy proargtypes as-is */
    //         memcpy(newResult->args, procform->proargtypes.values, pronargs * sizeof(Oid));
    //     }
    //     if (variadic)
    //     {
    //         int i;

    //         newResult->nvargs = effective_nargs - pronargs + 1;
    //         /* Expand variadic argument into N copies of element type */
    //         for (i = pronargs - 1; i < effective_nargs; i++)
    //             newResult->args[i] = va_elem_type;
    //     }
    //     else
    //         newResult->nvargs = 0;
    //     newResult->ndargs = use_defaults ? pronargs - nargs : 0;

    //     /*
	// 	 * Does it have the same arguments as something we already accepted?
	// 	 * If so, decide what to do to avoid returning duplicate argument
	// 	 * lists.  We can skip this check for the single-namespace case if no
	// 	 * special (named, variadic or defaults) match has been made, since
	// 	 * then the unique index on pg_proc guarantees all the matches have
	// 	 * different argument lists.
	// 	 */
    //     if (resultList != NULL && (any_special || !OidIsValid(namespaceId)))
    //     {
    //         /*
	// 		 * If we have an ordered list from SearchSysCacheList (the normal
	// 		 * case), then any conflicting proc must immediately adjoin this
	// 		 * one in the list, so we only need to look at the newest result
	// 		 * item.  If we have an unordered list, we have to scan the whole
	// 		 * result list.  Also, if either the current candidate or any
	// 		 * previous candidate is a special match, we can't assume that
	// 		 * conflicts are adjacent.
	// 		 *
	// 		 * We ignore defaulted arguments in deciding what is a match.
	// 		 */
    //         FuncCandidateList prevResult;

    //         if (catlist->ordered && !any_special)
    //         {
    //             /* ndargs must be 0 if !any_special */
    //             if (effective_nargs == resultList->nargs && memcmp(newResult->args, resultList->args, effective_nargs * sizeof(Oid)) == 0)
    //                 prevResult = resultList;
    //             else
    //                 prevResult = NULL;
    //         }
    //         else
    //         {
    //             int cmp_nargs = newResult->nargs - newResult->ndargs;

    //             for (prevResult = resultList; prevResult; prevResult = prevResult->next)
    //             {
    //                 if (cmp_nargs == prevResult->nargs - prevResult->ndargs
    //                     && memcmp(newResult->args, prevResult->args, cmp_nargs * sizeof(Oid)) == 0)
    //                     break;
    //             }
    //         }

    //         if (prevResult)
    //         {
    //             /*
	// 			 * We have a match with a previous result.  Decide which one
	// 			 * to keep, or mark it ambiguous if we can't decide.  The
	// 			 * logic here is preference > 0 means prefer the old result,
	// 			 * preference < 0 means prefer the new, preference = 0 means
	// 			 * ambiguous.
	// 			 */
    //             int preference;

    //             if (pathpos != prevResult->pathpos)
    //             {
    //                 /*
	// 				 * Prefer the one that's earlier in the search path.
	// 				 */
    //                 preference = pathpos - prevResult->pathpos;
    //             }
    //             else if (variadic && prevResult->nvargs == 0)
    //             {
    //                 /*
	// 				 * With variadic functions we could have, for example,
	// 				 * both foo(numeric) and foo(variadic numeric[]) in the
	// 				 * same namespace; if so we prefer the non-variadic match
	// 				 * on efficiency grounds.
	// 				 */
    //                 preference = 1;
    //             }
    //             else if (!variadic && prevResult->nvargs > 0)
    //             {
    //                 preference = -1;
    //             }
    //             else
    //             {
    //                 /*----------
	// 				 * We can't decide.  This can happen with, for example,
	// 				 * both foo(numeric, variadic numeric[]) and
	// 				 * foo(variadic numeric[]) in the same namespace, or
	// 				 * both foo(int) and foo (int, int default something)
	// 				 * in the same namespace, or both foo(a int, b text)
	// 				 * and foo(b text, a int) in the same namespace.
	// 				 *----------
	// 				 */
    //                 preference = 0;
    //             }

    //             if (preference > 0)
    //             {
    //                 /* keep previous result */
    //                 pfree(newResult);
    //                 continue;
    //             }
    //             else if (preference < 0)
    //             {
    //                 /* remove previous result from the list */
    //                 if (prevResult == resultList)
    //                     resultList = prevResult->next;
    //                 else
    //                 {
    //                     FuncCandidateList prevPrevResult;

    //                     for (prevPrevResult = resultList; prevPrevResult; prevPrevResult = prevPrevResult->next)
    //                     {
    //                         if (prevResult == prevPrevResult->next)
    //                         {
    //                             prevPrevResult->next = prevResult->next;
    //                             break;
    //                         }
    //                     }
    //                     Assert(prevPrevResult); /* assert we found it */
    //                 }
    //                 pfree(prevResult);
    //                 /* fall through to add newResult to list */
    //             }
    //             else
    //             {
    //                 /* mark old result as ambiguous, discard new */
    //                 prevResult->oid = InvalidOid;
    //                 pfree(newResult);
    //                 continue;
    //             }
    //         }
    //     }

    //     /*
	// 	 * Okay to add it to result list
	// 	 */
    //     newResult->next = resultList;
    //     resultList = newResult;
    // }

    // ReleaseSysCacheList(catlist);

    // return resultList;

    return nullptr;
};

PGList * FunctionProvider::SystemFuncName(const char * name)
{
	return list_make2(makeString("pg_catalog"), makeString(name));
};

char * FunctionProvider::get_func_result_name(Oid functionId)
{
    // char * result;
    // HeapTuple procTuple;
    // Datum proargmodes;
    // Datum proargnames;
    // bool isnull;
    // ArrayType * arr;
    // int numargs;
    // char * argmodes;
    // Datum * argnames;
    // int numoutargs;
    // int nargnames;
    // int i;

    // /* First fetch the function's pg_proc row */
    // procTuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(functionId));
    // if (!HeapTupleIsValid(procTuple))
    //     elog(ERROR, "cache lookup failed for function %u", functionId);

    // /* If there are no named OUT parameters, return NULL */
    // if (heap_attisnull(procTuple, Anum_pg_proc_proargmodes) || heap_attisnull(procTuple, Anum_pg_proc_proargnames))
    //     result = NULL;
    // else
    // {
    //     /* Get the data out of the tuple */
    //     proargmodes = SysCacheGetAttr(PROCOID, procTuple, Anum_pg_proc_proargmodes, &isnull);
    //     Assert(!isnull)
    //     proargnames = SysCacheGetAttr(PROCOID, procTuple, Anum_pg_proc_proargnames, &isnull);
    //     Assert(!isnull)

    //     /*
	// 	 * We expect the arrays to be 1-D arrays of the right types; verify
	// 	 * that.  For the char array, we don't need to use deconstruct_array()
	// 	 * since the array data is just going to look like a C array of
	// 	 * values.
	// 	 */
    //     arr = DatumGetArrayTypeP(proargmodes); /* ensure not toasted */
    //     numargs = ARR_DIMS(arr)[0];
    //     if (ARR_NDIM(arr) != 1 || numargs < 0 || ARR_HASNULL(arr) || ARR_ELEMTYPE(arr) != CHAROID)
    //         elog(ERROR, "proargmodes is not a 1-D char array");
    //     argmodes = (char *)ARR_DATA_PTR(arr);
    //     arr = DatumGetArrayTypeP(proargnames); /* ensure not toasted */
    //     if (ARR_NDIM(arr) != 1 || ARR_DIMS(arr)[0] != numargs || ARR_HASNULL(arr) || ARR_ELEMTYPE(arr) != TEXTOID)
    //         elog(ERROR, "proargnames is not a 1-D text array");
    //     deconstruct_array(arr, TEXTOID, -1, false, 'i', &argnames, NULL, &nargnames);
    //     Assert(nargnames == numargs)

    //     /* scan for output argument(s) */
    //     result = NULL;
    //     numoutargs = 0;
    //     for (i = 0; i < numargs; i++)
    //     {
    //         if (argmodes[i] == PROARGMODE_IN || argmodes[i] == PROARGMODE_VARIADIC)
    //             continue;
    //         Assert(argmodes[i] == PROARGMODE_OUT || argmodes[i] == PROARGMODE_INOUT || argmodes[i] == PROARGMODE_TABLE)
    //         if (++numoutargs > 1)
    //         {
    //             /* multiple out args, so forget it */
    //             result = NULL;
    //             break;
    //         }
    //         result = TextDatumGetCString(argnames[i]);
    //         if (result == NULL || result[0] == '\0')
    //         {
    //             /* Parameter is not named, so forget it */
    //             result = NULL;
    //             break;
    //         }
    //     }
    // }

    // ReleaseSysCache(procTuple);

    // return result;

    return pstrdup("");
};

}
