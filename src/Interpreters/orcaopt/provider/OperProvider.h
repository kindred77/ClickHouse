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

	static std::pair<Oid, PGOperatorPtr> OPER_INT2PL;
	static std::pair<Oid, PGOperatorPtr> OPER_INT4PL;
	static std::pair<Oid, PGOperatorPtr> OPER_INT24PL;
	static std::pair<Oid, PGOperatorPtr> OPER_INT42PL;
	static std::pair<Oid, PGOperatorPtr> OPER_INT2MI;
	static std::pair<Oid, PGOperatorPtr> OPER_INT4MI;
	static std::pair<Oid, PGOperatorPtr> OPER_INT24MI;
	static std::pair<Oid, PGOperatorPtr> OPER_INT42MI;
	static std::pair<Oid, PGOperatorPtr> OPER_INT2MUL;
	static std::pair<Oid, PGOperatorPtr> OPER_INT4MUL;
	static std::pair<Oid, PGOperatorPtr> OPER_INT24MUL;
	static std::pair<Oid, PGOperatorPtr> OPER_INT42MUL;
	static std::pair<Oid, PGOperatorPtr> OPER_INT2DIV;
	static std::pair<Oid, PGOperatorPtr> OPER_INT4DIV;
	static std::pair<Oid, PGOperatorPtr> OPER_INT24DIV;
	static std::pair<Oid, PGOperatorPtr> OPER_INT42DIV;
public:
	//explicit OperProvider(gpos::CMemoryPool *mp_, ContextPtr context);
	explicit OperProvider();
	PGOperatorPtr getOperByOID(Oid oid) const;
	Oid getOperByName(duckdb_libpgquery::PGList *names, Oid oprleft, Oid oprright) const;

    FuncCandidateListPtr OpernameGetCandidates(duckdb_libpgquery::PGList * names, char oprkind, bool missing_schema_ok);

	Oid get_opcode(Oid opno);

	Oid get_commutator(Oid opno);

    Oid get_opfamily_member(Oid opfamily, Oid lefttype, Oid righttype, int16 strategy);

    bool get_ordering_op_properties(Oid opno, Oid * opfamily, Oid * opcintype, int16 * strategy);

    Oid get_equality_op_for_ordering_op(Oid opno, bool * reverse);

    bool op_hashjoinable(Oid opno, Oid inputtype);

	PGSortGroupOperPtr get_sort_group_operators(Oid type_id);
};

}
