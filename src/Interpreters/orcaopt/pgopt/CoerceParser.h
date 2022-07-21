#pragma once

#include <parser_common.h>

#include <RelationParser.h>
#include <SelectParser.h>
#include <ExprParser.h>

namespace DB
{

/* Result codes for find_coercion_pathway */
typedef enum CoercionPathType
{
	COERCION_PATH_NONE,			/* failed to find any coercion pathway */
	COERCION_PATH_FUNC,			/* apply the specified coercion function */
	COERCION_PATH_RELABELTYPE,	/* binary-compatible cast, no function */
	COERCION_PATH_ARRAYCOERCE,	/* need an ArrayCoerceExpr node */
	COERCION_PATH_COERCEVIAIO	/* need a CoerceViaIO node */
} CoercionPathType;

class CoerceParser
{
private:

public:
	explicit CoerceParser();

    bool can_coerce_type(int nargs, Oid *input_typeids, Oid *target_typeids,
				duckdb_libpgquery::PGCoercionContext ccontext);
    
    CoercionPathType
    find_coercion_pathway(Oid targetTypeId, Oid sourceTypeId,
					  duckdb_libpgquery::PGCoercionContext ccontext,
					  Oid *funcid);

    void fixup_unknown_vars_in_targetlist(PGParseState *pstate, duckdb_libpgquery::PGList *targetlist);

};

}