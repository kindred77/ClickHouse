#pragma once

#include <Interpreters/orcaopt/parser_common.h>
#include <Interpreters/Context.h>

#include <gpos/memory/CMemoryPool.h>

#include <map>

namespace DB
{

class TypeProvider;
using TypeProviderPtr = std::shared_ptr<TypeProvider>;

class OperProvider
{
private:
	using Map = std::map<Oid, PGOperatorPtr>;

	Map oid_oper_map;
	//ContextPtr context;
	//gpos::CMemoryPool *mp;

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
	static std::pair<Oid, PGOperatorPtr> OPER_FLOAT32EQ;
    static std::pair<Oid, PGOperatorPtr> OPER_FLOAT32NE;
    static std::pair<Oid, PGOperatorPtr> OPER_FLOAT32LT;
    static std::pair<Oid, PGOperatorPtr> OPER_FLOAT32LE;
    static std::pair<Oid, PGOperatorPtr> OPER_FLOAT32GT;
    static std::pair<Oid, PGOperatorPtr> OPER_FLOAT32GE;
    static std::pair<Oid, PGOperatorPtr> OPER_FLOAT64EQ;
    static std::pair<Oid, PGOperatorPtr> OPER_FLOAT64NE;
    static std::pair<Oid, PGOperatorPtr> OPER_FLOAT64LT;
    static std::pair<Oid, PGOperatorPtr> OPER_FLOAT64LE;
    static std::pair<Oid, PGOperatorPtr> OPER_FLOAT64GT;
    static std::pair<Oid, PGOperatorPtr> OPER_FLOAT64GE;
    static std::pair<Oid, PGOperatorPtr> OPER_BOOLEQ;
    static std::pair<Oid, PGOperatorPtr> OPER_BOOLNE;
    static std::pair<Oid, PGOperatorPtr> OPER_BOOLLT;
    static std::pair<Oid, PGOperatorPtr> OPER_BOOLLE;
    static std::pair<Oid, PGOperatorPtr> OPER_BOOLGT;
    static std::pair<Oid, PGOperatorPtr> OPER_BOOLGE;
    static std::pair<Oid, PGOperatorPtr> OPER_INT16EQ;
    static std::pair<Oid, PGOperatorPtr> OPER_INT16NE;
    static std::pair<Oid, PGOperatorPtr> OPER_INT16LT;
    static std::pair<Oid, PGOperatorPtr> OPER_INT16LE;
    static std::pair<Oid, PGOperatorPtr> OPER_INT16GT;
    static std::pair<Oid, PGOperatorPtr> OPER_INT16GE;
    static std::pair<Oid, PGOperatorPtr> OPER_INT32EQ;
    static std::pair<Oid, PGOperatorPtr> OPER_INT32NE;
    static std::pair<Oid, PGOperatorPtr> OPER_INT32LT;
    static std::pair<Oid, PGOperatorPtr> OPER_INT32LE;
    static std::pair<Oid, PGOperatorPtr> OPER_INT32GT;
    static std::pair<Oid, PGOperatorPtr> OPER_INT32GE;
    static std::pair<Oid, PGOperatorPtr> OPER_INT64EQ;
    static std::pair<Oid, PGOperatorPtr> OPER_INT64NE;
    static std::pair<Oid, PGOperatorPtr> OPER_INT64LT;
    static std::pair<Oid, PGOperatorPtr> OPER_INT64LE;
    static std::pair<Oid, PGOperatorPtr> OPER_INT64GT;
    static std::pair<Oid, PGOperatorPtr> OPER_INT64GE;
    static std::pair<Oid, PGOperatorPtr> OPER_STRINGEQ;
    static std::pair<Oid, PGOperatorPtr> OPER_STRINGNE;
    static std::pair<Oid, PGOperatorPtr> OPER_STRINGLT;
    static std::pair<Oid, PGOperatorPtr> OPER_STRINGLE;
    static std::pair<Oid, PGOperatorPtr> OPER_STRINGGT;
    static std::pair<Oid, PGOperatorPtr> OPER_STRINGGE;
    static std::pair<Oid, PGOperatorPtr> OPER_FIXEDSTRINGEQ;
    static std::pair<Oid, PGOperatorPtr> OPER_FIXEDSTRINGNE;
    static std::pair<Oid, PGOperatorPtr> OPER_FIXEDSTRINGLT;
    static std::pair<Oid, PGOperatorPtr> OPER_FIXEDSTRINGLE;
    static std::pair<Oid, PGOperatorPtr> OPER_FIXEDSTRINGGT;
    static std::pair<Oid, PGOperatorPtr> OPER_FIXEDSTRINGGE;
    static std::pair<Oid, PGOperatorPtr> OPER_DATEEQ;
    static std::pair<Oid, PGOperatorPtr> OPER_DATENE;
    static std::pair<Oid, PGOperatorPtr> OPER_DATELT;
    static std::pair<Oid, PGOperatorPtr> OPER_DATELE;
    static std::pair<Oid, PGOperatorPtr> OPER_DATEGT;
    static std::pair<Oid, PGOperatorPtr> OPER_DATEGE;
    static std::pair<Oid, PGOperatorPtr> OPER_DATETIMEEQ;
    static std::pair<Oid, PGOperatorPtr> OPER_DATETIMENE;
    static std::pair<Oid, PGOperatorPtr> OPER_DATETIMELT;
    static std::pair<Oid, PGOperatorPtr> OPER_DATETIMELE;
    static std::pair<Oid, PGOperatorPtr> OPER_DATETIMEGT;
    static std::pair<Oid, PGOperatorPtr> OPER_DATETIMEGE;
    static std::pair<Oid, PGOperatorPtr> OPER_DATETIME64EQ;
    static std::pair<Oid, PGOperatorPtr> OPER_DATETIME64NE;
    static std::pair<Oid, PGOperatorPtr> OPER_DATETIME64LT;
    static std::pair<Oid, PGOperatorPtr> OPER_DATETIME64LE;
    static std::pair<Oid, PGOperatorPtr> OPER_DATETIME64GT;
    static std::pair<Oid, PGOperatorPtr> OPER_DATETIME64GE;
    static std::pair<Oid, PGOperatorPtr> OPER_DECIMAL64EQ;
    static std::pair<Oid, PGOperatorPtr> OPER_DECIMAL64NE;
    static std::pair<Oid, PGOperatorPtr> OPER_DECIMAL64LT;
    static std::pair<Oid, PGOperatorPtr> OPER_DECIMAL64LE;
    static std::pair<Oid, PGOperatorPtr> OPER_DECIMAL64GT;
    static std::pair<Oid, PGOperatorPtr> OPER_DECIMAL64GE;

	TypeProviderPtr type_provider;
public:
	//explicit OperProvider(gpos::CMemoryPool *mp_, ContextPtr context);
	explicit OperProvider();
	PGOperatorPtr getOperByOID(Oid oid) const;
	Oid getOperByName(duckdb_libpgquery::PGList *names, Oid oprleft, Oid oprright) const;

    FuncCandidateListPtr OpernameGetCandidates(duckdb_libpgquery::PGList * names, char oprkind, bool missing_schema_ok);

	Oid get_opcode(Oid opno);

	Oid get_commutator(Oid opno);

    // Oid get_opfamily_member(Oid opfamily, Oid lefttype, Oid righttype, int16 strategy);

    // bool get_ordering_op_properties(Oid opno, Oid * opfamily, Oid * opcintype, int16 * strategy);

    // Oid get_equality_op_for_ordering_op(Oid opno, bool * reverse);

    // bool op_hashjoinable(Oid opno, Oid inputtype);

	PGSortGroupOperPtr get_sort_grp_oper_by_typeid(Oid type_id);
};

}
