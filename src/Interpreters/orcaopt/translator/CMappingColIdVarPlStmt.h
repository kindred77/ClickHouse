#pragma once

//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CMappingColIdVarPlStmt.h
//
//	@doc:
//		Class defining the functions that provide the mapping between Var, Param
//		and variables of Sub-query to CDXLNode during Query->DXL translation
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include <common/parser_common.hpp>

#include "gpos/base.h"
#include "gpos/common/CDynamicPtrArray.h"
#include "gpos/common/CHashMap.h"

#include "Interpreters/orcaopt/translator/CDXLTranslateContext.h"
#include "Interpreters/orcaopt/translator/CMappingColIdVar.h"

//fwd decl
// struct Var;
// struct Plan;
namespace DB
{
	class TypeProvider;
};

namespace gpdxl
{
// fwd decl
class CDXLTranslateContextBaseTable;
class CContextDXLToPlStmt;

using TypeProviderPtr = std::shared_ptr<DB::TypeProvider>;

//---------------------------------------------------------------------------
//	@class:
//		CMappingColIdVarPlStmt
//
//	@doc:
//	Class defining functions that provide the mapping between Var, Param
//	and variables of Sub-query to CDXLNode during Query->DXL translation
//
//---------------------------------------------------------------------------
class CMappingColIdVarPlStmt : public CMappingColIdVar
{
private:
	const CDXLTranslateContextBaseTable *m_base_table_context;

	// the array of translator context (one for each child of the DXL operator)
	CDXLTranslationContextArray *m_child_contexts;

	CDXLTranslateContext *m_output_context;

	// translator context used to translate initplan and subplans associated
	// with a param node
	CContextDXLToPlStmt *m_dxl_to_plstmt_context;

	TypeProviderPtr type_provider;
public:
	CMappingColIdVarPlStmt(
		CMemoryPool *mp,
		const CDXLTranslateContextBaseTable *base_table_context,
		CDXLTranslationContextArray *child_contexts,
		CDXLTranslateContext *output_context,
		CContextDXLToPlStmt *dxl_to_plstmt_context);

	// translate DXL ScalarIdent node into GPDB Var node
	virtual duckdb_libpgquery::PGVar *VarFromDXLNodeScId(const CDXLScalarIdent *dxlop);

	// translate DXL ScalarIdent node into GPDB Param node
	duckdb_libpgquery::PGParam *ParamFromDXLNodeScId(const CDXLScalarIdent *dxlop);

	// get the output translator context
	CDXLTranslateContext *GetOutputContext();

	// return the context of the DXL->PlStmt translation
	CContextDXLToPlStmt *GetDXLToPlStmtContext();
};
}  // namespace gpdxl

// EOF
