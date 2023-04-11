#pragma once

//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLTranslateContext.h
//
//	@doc:
//		Class providing access to translation context, such as mappings between
//		table names, operator names, etc. and oids
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include <Interpreters/orcaopt/parser_common.h>

#include "gpos/base.h"
#include "gpos/common/CHashMap.h"
#include "gpos/common/CHashMapIter.h"

#include "Interpreters/orcaopt/translator/CMappingElementColIdParamId.h"


// fwd decl
//struct TargetEntry;

namespace gpdxl
{
using namespace gpos;

// hash maps mapping ULONG -> TargetEntry
typedef CHashMap<ULONG, duckdb_libpgquery::PGTargetEntry, gpos::HashValue<ULONG>,
				 gpos::Equals<ULONG>, CleanupDelete<ULONG>, CleanupNULL>
	ULongToTargetEntryMap;

// hash maps mapping ULONG -> CMappingElementColIdParamId
typedef CHashMap<ULONG, CMappingElementColIdParamId, gpos::HashValue<ULONG>,
				 gpos::Equals<ULONG>, CleanupDelete<ULONG>,
				 CleanupRelease<CMappingElementColIdParamId> >
	ULongToColParamMap;

typedef CHashMapIter<ULONG, CMappingElementColIdParamId, gpos::HashValue<ULONG>,
					 gpos::Equals<ULONG>, CleanupDelete<ULONG>,
					 CleanupRelease<CMappingElementColIdParamId> >
	ULongToColParamMapIter;


//---------------------------------------------------------------------------
//	@class:
//		CDXLTranslateContext
//
//	@doc:
//		Class providing access to translation context, such as mappings between
//		ColIds and target entries
//
//---------------------------------------------------------------------------
class CDXLTranslateContext
{
private:
	CMemoryPool *m_mp;

	// private copy ctor
	CDXLTranslateContext(const CDXLTranslateContext &);

	// mappings ColId->TargetEntry used for intermediate DXL nodes
	ULongToTargetEntryMap *m_colid_to_target_entry_map;

	// mappings ColId->ParamId used for outer refs in subplans
	ULongToColParamMap *m_colid_to_paramid_map;

	// is the node for which this context is built a child of an aggregate node
	// This is used to assign 0 instead of OUTER for the varno value of columns
	// in an Agg node, as expected in GPDB
	// TODO: antovl - Jan 26, 2011; remove this when Agg node in GPDB is fixed
	// to use OUTER instead of 0 for Var::varno in Agg target lists (MPP-12034)
	BOOL m_is_child_agg_node;

	// copy the params hashmap
	void CopyParamHashmap(ULongToColParamMap *original);

public:
	// ctor/dtor
	CDXLTranslateContext(CMemoryPool *mp, BOOL is_child_agg_node);

	CDXLTranslateContext(CMemoryPool *mp, BOOL is_child_agg_node,
						 ULongToColParamMap *original);

	~CDXLTranslateContext();

	// is parent an aggregate node
	BOOL IsParentAggNode() const;

	// return the params hashmap
	ULongToColParamMap *
	GetColIdToParamIdMap()
	{
		return m_colid_to_paramid_map;
	}

	// return the target entry corresponding to the given ColId
	const duckdb_libpgquery::PGTargetEntry *GetTargetEntry(ULONG colid) const;

	// return the param id corresponding to the given ColId
	const CMappingElementColIdParamId *GetParamIdMappingElement(
		ULONG colid) const;

	// store the mapping of the given column id and target entry
	void InsertMapping(ULONG colid, duckdb_libpgquery::PGTargetEntry *target_entry);

	// store the mapping of the given column id and param id
	BOOL FInsertParamMapping(ULONG colid,
							 CMappingElementColIdParamId *pmecolidparamid);
};


// array of dxl translation context
typedef CDynamicPtrArray<const CDXLTranslateContext, CleanupNULL>
	CDXLTranslationContextArray;
}  // namespace gpdxl

// EOF
