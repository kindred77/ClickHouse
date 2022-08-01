#pragma once

#include <Interpreters/orcaopt/pgopt/parser_common.h>

namespace DB
{

class OperParser
{
private:
    

public:
	explicit OperParser();

    void
    get_sort_group_operators(Oid argtype,
						 bool needLT, bool needEQ, bool needGT,
						 Oid *ltOpr, Oid *eqOpr, Oid *gtOpr,
						 bool *isHashable);
	
	HeapTuple
	oper(PGParseState *pstate, duckdb_libpgquery::PGList *opname, Oid ltypeId, Oid rtypeId,
		bool noError, int location);
};

}