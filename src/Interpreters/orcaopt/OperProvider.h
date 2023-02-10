#pragma once

#include <Interpreters/orcaopt/parser_common.h>
#include <Interpreters/Context.h>

#include <gpos/memory/CMemoryPool.h>

#include <map>

namespace DB
{

class OperProvider
{
private:
	using Map = std::map<Oid, PGOperatorPtr>;

	Map oid_oper_map;
	ContextPtr context;
	gpos::CMemoryPool *mp;
public:
	explicit OperProvider(gpos::CMemoryPool *mp_, ContextPtr context);
	PGOperatorPtr getOperByOID(Oid oid) const;
	Oid getOperByName(duckdb_libpgquery::PGList *names, Oid oprleft, Oid oprright) const;

    FuncCandidateList OpernameGetCandidates(duckdb_libpgquery::PGList * names, char oprkind, bool missing_schema_ok);

	Oid get_opcode(Oid opno);
};

}
