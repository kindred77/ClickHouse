#pragma once

//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CCTEListEntry.h
//
//	@doc:
//		Class representing the list of common table expression defined at a
//		query level
//
//	@test:
//
//---------------------------------------------------------------------------
#include <common/parser_common.hpp>

#include "gpos/base.h"
#include "gpos/common/CHashMap.h"

#include "naucrates/dxl/operators/CDXLNode.h"

// fwd declaration
// struct Query;
// struct List;
// struct RangeTblEntry;
// struct CommonTableExpr;


//using namespace gpos;

namespace gpdxl
{
// hash on character arrays
inline ULONG
HashStr(const CHAR *str)
{
	return gpos::HashByteArray((const BYTE *) str, clib::Strlen(str));
}

// equality on character arrays
inline BOOL
StrEqual(const CHAR *str_a, const CHAR *str_b)
{
	return (0 == clib::Strcmp(str_a, str_b));
}


//---------------------------------------------------------------------------
//	@class:
//		CCTEListEntry
//
//	@doc:
//		Class representing the list of common table expression defined at a
//		query level
//
//---------------------------------------------------------------------------
class CCTEListEntry : public CRefCount
{
private:
	// pair of DXL CTE producer and target list of the original CTE query
	struct SCTEProducerInfo
	{
		const CDXLNode *m_cte_producer;
		duckdb_libpgquery::PGList *m_target_list;

		// ctor
		SCTEProducerInfo(const CDXLNode *cte_producer, duckdb_libpgquery::PGList *target_list)
			: m_cte_producer(cte_producer), m_target_list(target_list)
		{
		}
	};

	// hash maps mapping CHAR *->SCTEProducerInfo
	typedef CHashMap<CHAR, SCTEProducerInfo, HashStr, StrEqual, CleanupNULL,
					 CleanupDelete>
		HMSzCTEInfo;

	// query level where the CTEs are defined
	ULONG m_query_level;

	// CTE producers at that level indexed by their name
	HMSzCTEInfo *m_cte_info;

public:
	// ctor: single CTE
	CCTEListEntry(CMemoryPool *mp, ULONG query_level, duckdb_libpgquery::PGCommonTableExpr *cte,
				  CDXLNode *cte_producer);

	// ctor: multiple CTEs
	CCTEListEntry(CMemoryPool *mp, ULONG query_level, duckdb_libpgquery::PGList *cte_list,
				  CDXLNodeArray *dxlnodes);

	// dtor
	virtual ~CCTEListEntry() override
	{
		m_cte_info->Release();
	}

	// the query level
	ULONG
	GetQueryLevel() const
	{
		return m_query_level;
	}

	// lookup CTE producer by its name
	const CDXLNode *GetCTEProducer(const CHAR *cte_str) const;

	// lookup CTE producer target list by its name
	duckdb_libpgquery::PGList *GetCTEProducerTargetList(const CHAR *cte_str) const;

	// add a new CTE producer for this level
	void AddCTEProducer(CMemoryPool *mp, duckdb_libpgquery::PGCommonTableExpr *cte,
						const CDXLNode *cte_producer);
};

// hash maps mapping ULONG -> CCTEListEntry
typedef CHashMap<ULONG, CCTEListEntry, gpos::HashValue<ULONG>,
				 gpos::Equals<ULONG>, CleanupDelete<ULONG>, CleanupRelease>
	HMUlCTEListEntry;

// iterator
typedef CHashMapIter<ULONG, CCTEListEntry, gpos::HashValue<ULONG>,
					 gpos::Equals<ULONG>, CleanupDelete<ULONG>, CleanupRelease>
	HMIterUlCTEListEntry;

}  // namespace gpdxl

//EOF
