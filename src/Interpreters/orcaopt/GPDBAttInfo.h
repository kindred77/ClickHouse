#pragma once

#include "gpos/base.h"
#include "gpos/common/CRefCount.h"
#include "gpos/utils.h"

#include "naucrates/dxl/gpdb_types.h"

namespace DB
{

//---------------------------------------------------------------------------
//	@class:
//		CGPDBAttInfo
//
//	@doc:
//		Class to uniquely identify a column in GPDB
//
//---------------------------------------------------------------------------
class GPDBAttInfo : public CRefCount
{
private:
	// query level number
	ULONG m_query_level;

	// varno in the rtable
	ULONG m_varno;

	// attno
	INT m_attno;

	// copy c'tor
	GPDBAttInfo(const GPDBAttInfo &);

public:
	// ctor
	GPDBAttInfo(ULONG query_level, ULONG var_no, INT attrnum)
		: m_query_level(query_level), m_varno(var_no), m_attno(attrnum)
	{
	}

	// d'tor
	virtual ~GPDBAttInfo()
	{
	}

	// accessor
	ULONG
	GetQueryLevel() const
	{
		return m_query_level;
	}

	// accessor
	ULONG
	GetVarNo() const
	{
		return m_varno;
	}

	// accessor
	INT
	GetAttNo() const
	{
		return m_attno;
	}

	// equality check
	BOOL
	Equals(const GPDBAttInfo &gpdb_att_info) const
	{
		return m_query_level == gpdb_att_info.m_query_level &&
			   m_varno == gpdb_att_info.m_varno &&
			   m_attno == gpdb_att_info.m_attno;
	}

	// hash value
	ULONG
	HashValue() const
	{
		return gpos::CombineHashes(
			gpos::HashValue(&m_query_level),
			gpos::CombineHashes(gpos::HashValue(&m_varno),
								gpos::HashValue(&m_attno)));
	}
};

// hash function
inline ULONG
HashGPDBAttInfo(const GPDBAttInfo *gpdb_att_info)
{
	GPOS_ASSERT(NULL != gpdb_att_info);
	return gpdb_att_info->HashValue();
}

// equality function
inline BOOL
EqualGPDBAttInfo(const GPDBAttInfo *gpdb_att_info_a,
				 const GPDBAttInfo *gpdb_att_info_b)
{
	GPOS_ASSERT(NULL != gpdb_att_info_a && NULL != gpdb_att_info_b);
	return gpdb_att_info_a->Equals(*gpdb_att_info_b);
}

}  // namespace DB
