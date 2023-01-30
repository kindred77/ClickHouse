#pragma once

#include <Interpreters/orcaopt/pgopt_hawq/parser_common.h>

#include <Interpreters/orcaopt/pgopt_hawq/FuncParser.h>
#include <Interpreters/orcaopt/pgopt_hawq/TypeParser.h>
#include <Interpreters/orcaopt/pgopt_hawq/RelationParser.h>

namespace DB
{

/* Result codes for find_coercion_pathway */
typedef enum
{
	COERCION_PATH_NONE,			/* failed to find any coercion pathway */
	COERCION_PATH_FUNC,			/* apply the specified coercion function */
	COERCION_PATH_RELABELTYPE,	/* binary-compatible cast, no function */
	COERCION_PATH_ARRAYCOERCE,	/* need an ArrayCoerceExpr node */
	COERCION_PATH_COERCEVIAIO	/* need a CoerceViaIO node */
} CoercionPathType;

typedef enum
{
	COERCION_CODE_IMPLICIT = 'i',		/* coercion in context of expression */
	COERCION_CODE_ASSIGNMENT = 'a',		/* coercion in context of assignment */
	COERCION_CODE_EXPLICIT = 'e'	/* explicit cast operation */
} CoercionCodes;

/*
 * The allowable values for pg_cast.castmethod are specified by this enum.
 * Since castmethod is stored as a "char", we use ASCII codes for human
 * convenience in reading the table.
 */
typedef enum
{
	COERCION_METHOD_FUNCTION = 'f',		/* use a function */
	COERCION_METHOD_BINARY = 'b',		/* types are binary-compatible */
	COERCION_METHOD_INOUT = 'i' /* use input/output functions */
} CoercionMethod;

class CoerceParser
{
private:
	FuncParser func_parser;
	TypeParser type_parser;
	RelationParser relation_parser;
public:
	explicit CoerceParser();

    bool check_generic_type_consistency(Oid * actual_arg_types, Oid * declared_arg_types, int nargs);

    bool IsPreferredType(CATEGORY category, Oid type);

    bool find_coercion_pathway(Oid targetTypeId, Oid sourceTypeId,
		CoercionContext ccontext, Oid * funcid);

    bool can_coerce_type(int nargs, Oid * input_typeids, Oid * target_typeids, duckdb_libpgquery::PGCoercionContext ccontext);

    CATEGORY TypeCategory(Oid inType);

    Oid select_common_type(duckdb_libpgquery::PGList *typeids, const char *context);

    duckdb_libpgquery::PGNode * coerce_to_boolean(PGParseState * pstate,
		duckdb_libpgquery::PGNode * node, const char * constructName);

    duckdb_libpgquery::PGVar * coerce_unknown_var(
        PGParseState * pstate,
        duckdb_libpgquery::PGVar * var,
        Oid targetTypeId,
        int32 targetTypeMod,
        PGCoercionContext ccontext,
        PGCoercionForm cformat,
        int levelsup);

    void fixup_unknown_vars_in_targetlist(PGParseState * pstate,
		duckdb_libpgquery::PGList * targetlist);
};

}