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

	OID type_oid;

	int typemod;

	int attno;

	// private copy c'tor
	OptColInfo(const OptColInfo &);

public:
	// ctor
	OptColInfo(ULONG colid, gpos::CWStringBase *str,
		OID type_oid_ = InvalidOid, int typemod_ = -1,
		int attno_ = -1
	)
	: m_colid(colid)
	, m_str(str)
	, type_oid(type_oid_)
	, typemod(typemod_)
	, attno(attno_)
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
	
	OID
	GetTypeOid() const
	{
		return type_oid;
	}

	int 
	GetTypeMod() const
	{
		return typemod;
	}

	int 
	GetAttNo() const
	{
		return attno;
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
