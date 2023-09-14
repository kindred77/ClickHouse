#pragma once

#include "gpos/base.h"
#include "naucrates/md/IMDId.h"
#include "naucrates/md/CMDIdGPDB.h"

namespace gpmd
{

class CMDIdCKDB : public CMDIdGPDB
{

public:

    static CMDIdCKDB *
	Cast(IMDId *mdid)
	{
		return dynamic_cast<CMDIdCKDB *>(mdid);
	}

    enum ECKDBMDOIdType
	{
		ECKDBMDOIdTypeType = 0,
		ECKDBMDOIdTypeRelation = 1,
		ECKDBMDOIdTypeOperator = 2,
		ECKDBMDOIdTypeAggregate = 3,
		ECKDBMDOIdTypeFunction = 4,
		ECKDBMDOIdTypeIndex = 5
	};

    virtual ECKDBMDOIdType
	MDOIdType() const
	{
		return mdoid_type;
	}

    explicit CMDIdCKDB(const ECKDBMDOIdType type, OID oid);
private:
    ECKDBMDOIdType mdoid_type;
};

}
