#pragma once

#include <gpopt/mdcache/CMDAccessor.h>

#include <naucrates/dxl/CDXLUtils.h>

#include <Parsers/IAST_fwd.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>

#include <Interpreters/orcaopt/ContextQueryToDXL.h>

namespace Poco
{
class Logger;
}

namespace DB
{

class gpdxl::CDXLNode;

using ASTsArr = std::vector<ASTs>;

class TranslatorScalarToDXL {

private:
    gpopt::CMDAccessor * metadata_accessor;
    gpos::ULONG m_query_level;
    CMemoryPool * memory_pool;
    ContextQueryToDXL * context;
    Poco::Logger * log;
    ScalarOperatorProvider * scalar_op_provider;
    gpdxl::EdxlBoolExprType EdxlbooltypeFromGPDBBoolType(BoolExprType boolexprtype) const;
public:
    TranslatorScalarToDXL();

    TranslatorScalarToDXL(
    	ContextQueryToDXL * context_,
        gpopt::CMDAccessor * metadata_accessor_,
        gpos::ULONG m_query_level_);

    virtual ~TranslatorScalarToDXL();

    gpdxl::CDXLNode * translateVarToDXL(
        ASTPtr expr,
        const MappingVarColId *var_colid_mapping);
    
    // datum to oid CDXLDatum
	static CDXLDatum *translateOidDatumToDXL(CMemoryPool *mp,
											 const IMDType *md_type,
											 BOOL is_null, ULONG len,
											 Datum datum);

	// datum to int2 CDXLDatum
	static CDXLDatum *translateInt2DatumToDXL(CMemoryPool *mp,
											  const IMDType *md_type,
											  BOOL is_null, ULONG len,
											  Datum datum);

	// datum to int4 CDXLDatum
	static CDXLDatum *translateInt4DatumToDXL(CMemoryPool *mp,
											  const IMDType *md_type,
											  BOOL is_null, ULONG len,
											  Datum datum);

	// datum to int8 CDXLDatum
	static CDXLDatum *translateInt8DatumToDXL(CMemoryPool *mp,
											  const IMDType *md_type,
											  BOOL is_null, ULONG len,
											  Datum datum);

	// datum to bool CDXLDatum
	static CDXLDatum *translateBoolDatumToDXL(CMemoryPool *mp,
											  const IMDType *md_type,
											  BOOL is_null, ULONG len,
											  Datum datum);

    BYTE* extractByteArrayFromDatum(CMemoryPool *mp,
												 //const IMDType *md_type,
                                                 BOOL is_null,
												 const ASTLiteral* datum);

    gpos::CDouble extractDoubleValueFromDatum(//CMemoryPool *mp,
												   const BOOL is_null,
                                                   //BYTE *bytes,
												   const ASTLiteral* datum);

    gpos::LINT extractLintValueFromDatum(const BOOL is_null,
                                        const ASTLiteral* datum);

    gpos::ULONG getDatumLength(BOOL is_null, const ASTLiteral* datum);
	// datum to generic CDXLDatum
	static CDXLDatum *translateGenericDatumToDXL(CMemoryPool *mp,
												 const IMDType *md_type,
												 INT type_modifier,
												 BOOL is_null,
												 const ASTLiteral* datum);

    gpdxl::CDXLNode * translateConstToDXL(
        const ASTPtr *expr,
        const CMappingVarColId * var_colid_mapping,
        gpmd::CMDIdGPDB & mid_type_ret
    );
    
    gpdxl::CDXLNode * translateAndOrNotExprToDXL(
        ASTPtr expr,
        const MappingVarColId *var_colid_mapping,
        gpmd::CMDIdGPDB & mid_type_ret);

    gpdxl::CDXLNode * translateOpExprToDXL(
        ASTPtr expr,
        const MappingVarColId *var_colid_mapping,
        gpmd::CMDIdGPDB & mid_type_ret);

    gpdxl::CDXLNode * translateExprToDXL(
        ASTPtr expr,
        const MappingVarColId *var_colid_mapping,
        gpmd::CMDIdGPDB & mid_type_ret);

    static TranslatorScalarToDXL *ScalarToDXLInstance(
        CMemoryPool * memory_pool_,
        gpdxl::CMDAccessor * md_accessor_,
        ASTPtr query);
};

}
