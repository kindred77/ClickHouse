#pragma once

#include <Interpreters/orcaopt/pgopt/parser_common.h>

#include <Interpreters/orcaopt/pgopt/RelationParser.h>
#include <Interpreters/orcaopt/pgopt/SelectParser.h>
#include <Interpreters/orcaopt/pgopt/ExprParser.h>

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
	RelationParser relation_parser;
public:
	explicit CoerceParser();

    bool can_coerce_type(int nargs, Oid *input_typeids, Oid *target_typeids,
				duckdb_libpgquery::PGCoercionContext ccontext);
    
    CoercionPathType
    find_coercion_pathway(Oid targetTypeId, Oid sourceTypeId,
					  duckdb_libpgquery::PGCoercionContext ccontext,
					  Oid *funcid);

    void fixup_unknown_vars_in_targetlist(PGParseState *pstate, duckdb_libpgquery::PGList *targetlist);

	void
	fixup_unknown_vars_in_exprlist(PGParseState *pstate, duckdb_libpgquery::PGList *exprlist);

	duckdb_libpgquery::PGVar *
	coerce_unknown_var(PGParseState *pstate, duckdb_libpgquery::PGVar *var,
                   Oid targetTypeId, int32 targetTypeMod,
			       duckdb_libpgquery::PGCoercionContext ccontext,
				   duckdb_libpgquery::PGCoercionForm cformat,
                   int levelsup);
	
	duckdb_libpgquery::PGNode *
	coerce_type(PGParseState *pstate, duckdb_libpgquery::PGNode *node,
			Oid inputTypeId, Oid targetTypeId, int32 targetTypeMod,
			duckdb_libpgquery::PGCoercionContext ccontext, duckdb_libpgquery::PGCoercionForm cformat, int location);
	
	duckdb_libpgquery::PGNode *
	coerce_to_boolean(PGParseState *pstate, duckdb_libpgquery::PGNode *node,
				  const char *constructName);
	
	Oid select_common_type(PGParseState *pstate,
		duckdb_libpgquery::PGList *exprs, const char *context,
		duckdb_libpgquery::PGNode **which_expr);

};

}