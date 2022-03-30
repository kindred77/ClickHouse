#pragma once

#include "gpos/base.h"
#include "gpos/common/CRefCount.h"
#include "gpos/utils.h"

namespace DB
{

//---------------------------------------------------------------------------
//	@class:
//		COptColInfo
//
//	@doc:
//		pair of column id and column name
//
//---------------------------------------------------------------------------
class OptColInfo : public gpos::CRefCount
{
private:
	// column id
	ULONG m_colid;

	// column name
	gpos::CWStringBase *m_str;

	// private copy c'tor
	OptColInfo(const OptColInfo &);

public:
	// ctor
	OptColInfo(ULONG colid, gpos::CWStringBase *str) : m_colid(colid), m_str(str)
	{
		GPOS_ASSERT(m_str);
	}

	// dtor
	virtual ~OptColInfo()
	{
		GPOS_DELETE(m_str);
	}

	// accessors
	ULONG
	GetColId() const
	{
		return m_colid;
	}

	gpos::CWStringBase *
	GetOptColName() const
	{
		return m_str;
	}

	// equality check
	BOOL
	Equals(const OptColInfo &optcolinfo) const
	{
		// don't need to check name as column id is unique
		return m_colid == optcolinfo.m_colid;
	}

	// hash value
	ULONG
	HashValue() const
	{
		return gpos::HashValue(&m_colid);
	}
};

// hash function
inline ULONG
UlHashOptColInfo(const OptColInfo *opt_col_info)
{
	GPOS_ASSERT(NULL != opt_col_info);
	return opt_col_info->HashValue();
}

// equality function
inline BOOL
FEqualOptColInfo(const OptColInfo *opt_col_infoA,
				 const OptColInfo *opt_col_infoB)
{
	GPOS_ASSERT(NULL != opt_col_infoA && NULL != opt_col_infoB);
	return opt_col_infoA->Equals(*opt_col_infoB);
}

}  // namespace DB
