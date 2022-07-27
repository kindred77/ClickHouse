#pragma once

#include <Interpreters/orcaopt/pgopt/parser_common.h>

#include <Interpreters/orcaopt/pgopt/RelationParser.h>
#include <Interpreters/orcaopt/pgopt/SelectParser.h>
#include <Interpreters/orcaopt/pgopt/ExprParser.h>

namespace DB
{

/* Result codes for find_coercion_pathway */
enum class CoercionPathType
{
	COERCION_PATH_NONE,			/* failed to find any coercion pathway */
	COERCION_PATH_FUNC,			/* apply the specified coercion function */
	COERCION_PATH_RELABELTYPE,	/* binary-compatible cast, no function */
	COERCION_PATH_ARRAYCOERCE,	/* need an ArrayCoerceExpr node */
	COERCION_PATH_COERCEVIAIO	/* need a CoerceViaIO node */
};

enum class CoercionCodes
{
	COERCION_CODE_IMPLICIT = 'i',		/* coercion in context of expression */
	COERCION_CODE_ASSIGNMENT = 'a',		/* coercion in context of assignment */
	COERCION_CODE_EXPLICIT = 'e'	/* explicit cast operation */
};

/*
 * The allowable values for pg_cast.castmethod are specified by this enum.
 * Since castmethod is stored as a "char", we use ASCII codes for human
 * convenience in reading the table.
 */
enum class CoercionMethod
{
	COERCION_METHOD_FUNCTION = 'f',		/* use a function */
	COERCION_METHOD_BINARY = 'b',		/* types are binary-compatible */
	COERCION_METHOD_INOUT = 'i' /* use input/output functions */
};

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
	
	duckdb_libpgquery::PGNode *
	coerce_to_specific_type(PGParseState *pstate, duckdb_libpgquery::PGNode *node,
						Oid targetTypeId,
						const char *constructName);

	duckdb_libpgquery::PGNode *
	coerce_to_target_type(PGParseState *pstate, duckdb_libpgquery::PGNode *expr, Oid exprtype,
					  Oid targettype, int32 targettypmod,
					  duckdb_libpgquery::PGCoercionContext ccontext,
					  duckdb_libpgquery::PGCoercionForm cformat,
					  int location);

	duckdb_libpgquery::PGNode *
	coerce_type_typmod(duckdb_libpgquery::PGNode *node, Oid targetTypeId, int32 targetTypMod,
				   duckdb_libpgquery::PGCoercionForm cformat, int location,
				   bool isExplicit, bool hideInputCoercion);

	CoercionPathType
	find_typmod_coercion_function(Oid typeId,
							  Oid *funcid);

	void
	hide_coercion_node(duckdb_libpgquery::PGNode *node);

	duckdb_libpgquery::PGNode *
	build_coercion_expression(duckdb_libpgquery::PGNode *node,
						  CoercionPathType pathtype,
						  Oid funcId,
						  Oid targetTypeId, int32 targetTypMod,
						  duckdb_libpgquery::PGCoercionForm cformat, int location,
						  bool isExplicit);
	
	duckdb_libpgquery::PGNode *
	coerce_to_domain(duckdb_libpgquery::PGNode *arg, Oid baseTypeId, int32 baseTypeMod, Oid typeId,
				 duckdb_libpgquery::PGCoercionForm cformat, int location,
				 bool hideInputCoercion,
				 bool lengthCoercionDone);

	duckdb_libpgquery::PGNode *
	coerce_to_common_type(PGParseState *pstate, duckdb_libpgquery::PGNode *node,
					  Oid targetTypeId, const char *context);

};

}