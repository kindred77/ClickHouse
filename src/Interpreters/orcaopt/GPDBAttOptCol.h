#pragma once

#include "gpos/common/CRefCount.h"

#include "gpopt/translate/CGPDBAttInfo.h"
#include "gpopt/translate/COptColInfo.h"

namespace DB
{


//---------------------------------------------------------------------------
//	@class:
//		GPDBAttOptCol
//
//	@doc:
//		Class to represent pair of GPDB var info to optimizer col info
//
//---------------------------------------------------------------------------
class GPDBAttOptCol : public gpos::CRefCount
{
private:
	// gpdb att info
	GPDBAttInfo *m_gpdb_att_info;

	// optimizer col info
	COptColInfo *m_opt_col_info;

	// copy c'tor
	GPDBAttOptCol(const GPDBAttOptCol &);

public:
	// ctor
	GPDBAttOptCol(GPDBAttInfo *gpdb_att_info, COptColInfo *opt_col_info)
		: m_gpdb_att_info(gpdb_att_info), m_opt_col_info(opt_col_info)
	{
		GPOS_ASSERT(NULL != m_gpdb_att_info);
		GPOS_ASSERT(NULL != m_opt_col_info);
	}

	// d'tor
	virtual ~GPDBAttOptCol()
	{
		m_gpdb_att_info->Release();
		m_opt_col_info->Release();
	}

	// accessor
	const GPDBAttInfo *
	GetGPDBAttInfo() const
	{
		return m_gpdb_att_info;
	}

	// accessor
	const COptColInfo *
	GetOptColInfo() const
	{
		return m_opt_col_info;
	}
};

}  // namespace DB
