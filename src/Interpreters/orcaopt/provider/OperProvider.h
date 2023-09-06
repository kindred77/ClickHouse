#pragma once

#include <common/parser_common.hpp>
#include <Interpreters/Context.h>

#include <gpos/memory/CMemoryPool.h>

#include <map>

namespace DB
{

class TypeProvider;
using TypeProviderPtr = std::shared_ptr<TypeProvider>;

class Context;
using ContextPtr = std::shared_ptr<const Context>;

class OperProvider
{
private:
	//ContextPtr context;
	//gpos::CMemoryPool *mp;
    using OidOperatorMap = std::map<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr>;
    static OidOperatorMap oid_oper_map;

	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_INT2PL;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_INT4PL;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_INT24PL;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_INT42PL;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_INT2MI;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_INT4MI;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_INT24MI;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_INT42MI;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_INT2MUL;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_INT4MUL;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_INT24MUL;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_INT42MUL;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_INT2DIV;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_INT4DIV;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_INT24DIV;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_INT42DIV;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_FLOAT32EQ;
    static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_FLOAT32NE;
    static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_FLOAT32LT;
    static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_FLOAT32LE;
    static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_FLOAT32GT;
    static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_FLOAT32GE;
    static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_FLOAT64EQ;
    static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_FLOAT64NE;
    static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_FLOAT64LT;
    static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_FLOAT64LE;
    static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_FLOAT64GT;
    static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_FLOAT64GE;
    static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_BOOLEQ;
    static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_BOOLNE;
    static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_BOOLLT;
    static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_BOOLLE;
    static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_BOOLGT;
    static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_BOOLGE;
    static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_INT16EQ;
    static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_INT16NE;
    static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_INT16LT;
    static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_INT16LE;
    static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_INT16GT;
    static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_INT16GE;
    static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_INT32EQ;
    static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_INT32NE;
    static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_INT32LT;
    static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_INT32LE;
    static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_INT32GT;
    static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_INT32GE;
    static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_INT64EQ;
    static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_INT64NE;
    static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_INT64LT;
    static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_INT64LE;
    static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_INT64GT;
    static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_INT64GE;
    static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_STRINGEQ;
    static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_STRINGNE;
    static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_STRINGLT;
    static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_STRINGLE;
    static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_STRINGGT;
    static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_STRINGGE;
    static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_FIXEDSTRINGEQ;
    static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_FIXEDSTRINGNE;
    static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_FIXEDSTRINGLT;
    static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_FIXEDSTRINGLE;
    static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_FIXEDSTRINGGT;
    static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_FIXEDSTRINGGE;
    static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_DATEEQ;
    static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_DATENE;
    static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_DATELT;
    static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_DATELE;
    static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_DATEGT;
    static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_DATEGE;
    static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_DATETIMEEQ;
    static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_DATETIMENE;
    static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_DATETIMELT;
    static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_DATETIMELE;
    static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_DATETIMEGT;
    static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_DATETIMEGE;
    static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_DATETIME64EQ;
    static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_DATETIME64NE;
    static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_DATETIME64LT;
    static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_DATETIME64LE;
    static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_DATETIME64GT;
    static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_DATETIME64GE;
    static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_DECIMAL64EQ;
    static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_DECIMAL64NE;
    static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_DECIMAL64LT;
    static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_DECIMAL64LE;
    static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_DECIMAL64GT;
    static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGOperatorPtr> OPER_DECIMAL64GE;

	//TypeProviderPtr type_provider;

    //ContextPtr context;
public:
	//explicit OperProvider(gpos::CMemoryPool *mp_, ContextPtr context);
	//explicit OperProvider(const ContextPtr& context_);
	static duckdb_libpgquery::PGOperatorPtr getOperByOID(duckdb_libpgquery::PGOid oid);
	static duckdb_libpgquery::PGOid getOperByName(duckdb_libpgquery::PGList *names, duckdb_libpgquery::PGOid oprleft, duckdb_libpgquery::PGOid oprright);

    static duckdb_libpgquery::FuncCandidateListPtr OpernameGetCandidates(duckdb_libpgquery::PGList * names, char oprkind, bool missing_schema_ok);

	static duckdb_libpgquery::PGOid get_opcode(duckdb_libpgquery::PGOid opno);

	static duckdb_libpgquery::PGOid get_commutator(duckdb_libpgquery::PGOid opno);

    // Oid get_opfamily_member(Oid opfamily, Oid lefttype, Oid righttype, int16 strategy);

    // bool get_ordering_op_properties(Oid opno, Oid * opfamily, Oid * opcintype, int16 * strategy);

    // Oid get_equality_op_for_ordering_op(Oid opno, bool * reverse);

    // bool op_hashjoinable(Oid opno, Oid inputtype);

	static duckdb_libpgquery::PGSortGroupOperPtr get_sort_grp_oper_by_typeid(duckdb_libpgquery::PGOid type_id);
};

}
