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
		ECKDBMDOIdTypeInvalid = 0,
		ECKDBMDOIdTypeType = 1,
		ECKDBMDOIdTypeRelation = 2,
		ECKDBMDOIdTypeOperator = 3,
		ECKDBMDOIdTypeAggregate = 4,
		ECKDBMDOIdTypeFunction = 5,
		ECKDBMDOIdTypeIndex = 6
	};

    virtual ECKDBMDOIdType
	MDOIdType() const
	{
		return mdoid_type;
	}

    explicit CMDIdCKDB(const ECKDBMDOIdType type, OID oid);

	CMDIdCKDB(const SINT type, OID oid, ULONG version_major, ULONG version_minor);

	// equality check
	virtual BOOL Equals(const IMDId *mdid) const;

	virtual ULONG
	HashValue() const
	{
		return gpos::CombineHashes(
			MdidType(),
			gpos::CombineHashes(
				MDOIdType(),
				gpos::CombineHashes(
					gpos::HashValue(&m_oid),
					gpos::CombineHashes(gpos::HashValue(&m_major_version),
									gpos::HashValue(&m_minor_version)))));
	}
	
	virtual IOstream &OsPrint(IOstream &os) const;


	// invalid mdid
	static CMDIdCKDB m_mdid_invalid_key;

	// int2 mdid
	static CMDIdCKDB m_mdid_int2;

	// int4 mdid
	static CMDIdCKDB m_mdid_int4;

	// int8 mdid
	static CMDIdCKDB m_mdid_int8;

	// oid mdid
	static CMDIdCKDB m_mdid_oid;

	// bool mdid
	static CMDIdCKDB m_mdid_bool;

	// numeric mdid
	static CMDIdCKDB m_mdid_numeric;

	// date mdid
	static CMDIdCKDB m_mdid_date;

	// time mdid
	static CMDIdCKDB m_mdid_time;

	// time with time zone mdid
	static CMDIdCKDB m_mdid_timeTz;

	// timestamp mdid
	static CMDIdCKDB m_mdid_timestamp;

	// timestamp with time zone mdid
	static CMDIdCKDB m_mdid_timestampTz;

	// absolute time mdid
	static CMDIdCKDB m_mdid_abs_time;

	// relative time mdid
	static CMDIdCKDB m_mdid_relative_time;

	// interval mdid
	static CMDIdCKDB m_mdid_interval;

	// time interval mdid
	static CMDIdCKDB m_mdid_time_interval;

	// char mdid
	static CMDIdCKDB m_mdid_char;

	// bpchar mdid
	static CMDIdCKDB m_mdid_bpchar;

	// varchar mdid
	static CMDIdCKDB m_mdid_varchar;

	// text mdid
	static CMDIdCKDB m_mdid_text;

	// name mdid
	static CMDIdCKDB m_mdid_name;

	// float4 mdid
	static CMDIdCKDB m_mdid_float4;

	// float8 mdid
	static CMDIdCKDB m_mdid_float8;

	// cash mdid
	static CMDIdCKDB m_mdid_cash;

	// inet mdid
	static CMDIdCKDB m_mdid_inet;

	// cidr mdid
	static CMDIdCKDB m_mdid_cidr;

	// macaddr mdid
	static CMDIdCKDB m_mdid_macaddr;

	// count(*) mdid
	static CMDIdCKDB m_mdid_count_star;

	// count(Any) mdid
	static CMDIdCKDB m_mdid_count_any;

	// uuid mdid
	static CMDIdCKDB m_mdid_uuid;

	// unknown datatype mdid
	static CMDIdCKDB m_mdid_unknown;

protected:
    ECKDBMDOIdType mdoid_type;
	virtual void Serialize();
};

}
