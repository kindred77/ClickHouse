#pragma once

#include <Interpreters/orcaopt/pgoptnew/parser_common.h>

#include <Interpreters/orcaopt/pgoptnew/RelationParser.h>
#include <Interpreters/orcaopt/pgoptnew/SelectParser.h>
#include <Interpreters/orcaopt/pgoptnew/ExprParser.h>
#include <Interpreters/orcaopt/pgoptnew/NodeParser.h>
#include <Interpreters/orcaopt/pgoptnew/TypeParser.h>

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
	RelationParser relation_parser;
	NodeParser node_parser;
	TypeParser type_parser;
public:
	explicit CoerceParser();

	Oid
	select_common_type(PGParseState *pstate, duckdb_libpgquery::PGList *exprs, const char *context,
				   duckdb_libpgquery::PGNode **which_expr);
	
	duckdb_libpgquery::PGNode *
	coerce_type(PGParseState *pstate, duckdb_libpgquery::PGNode *node,
			Oid inputTypeId, Oid targetTypeId, int32 targetTypeMod,
			duckdb_libpgquery::PGCoercionContext ccontext, duckdb_libpgquery::PGCoercionForm cformat, int location);

	duckdb_libpgquery::PGNode *
	coerce_to_target_type(PGParseState *pstate, duckdb_libpgquery::PGNode *expr, Oid exprtype,
					  Oid targettype, int32 targettypmod,
					  duckdb_libpgquery::PGCoercionContext ccontext,
					  duckdb_libpgquery::PGCoercionForm cformat,
					  int location);
	
	duckdb_libpgquery::PGNode *
	coerce_to_common_type(PGParseState *pstate, duckdb_libpgquery::PGNode *node,
					  Oid targetTypeId, const char *context);

	duckdb_libpgquery::PGNode *
	coerce_to_boolean(PGParseState *pstate, duckdb_libpgquery::PGNode *node,
				  const char *constructName);

	bool
	can_coerce_type(int nargs, const Oid *input_typeids, const Oid *target_typeids,
				duckdb_libpgquery::PGCoercionContext ccontext);

	CoercionPathType
	find_coercion_pathway(Oid targetTypeId, Oid sourceTypeId,
					  duckdb_libpgquery::PGCoercionContext ccontext,
					  Oid *funcid);

	CoercionPathType
	find_typmod_coercion_function(Oid typeId,
							  Oid *funcid);

	duckdb_libpgquery::PGNode *
	coerce_type_typmod(duckdb_libpgquery::PGNode *node, Oid targetTypeId, int32 targetTypMod,
				   duckdb_libpgquery::PGCoercionContext ccontext, duckdb_libpgquery::PGCoercionForm cformat,
				   int location,
				   bool hideInputCoercion);

	void
	hide_coercion_node(duckdb_libpgquery::PGNode *node);

	duckdb_libpgquery::PGNode *
	coerce_record_to_complex(PGParseState *pstate, duckdb_libpgquery::PGNode *node,
						 Oid targetTypeId,
						 duckdb_libpgquery::PGCoercionContext ccontext,
						 duckdb_libpgquery::PGCoercionForm cformat,
						 int location);

	duckdb_libpgquery::PGNode *
	build_coercion_expression(duckdb_libpgquery::PGNode *node,
						  CoercionPathType pathtype,
						  Oid funcId,
						  Oid targetTypeId, int32 targetTypMod,
						  duckdb_libpgquery::PGCoercionContext ccontext, duckdb_libpgquery::PGCoercionForm cformat,
						  int location);

	void
	parser_coercion_errposition(PGParseState *pstate,
							int coerce_location,
							duckdb_libpgquery::PGNode *input_expr);

	duckdb_libpgquery::PGNode *
	coerce_to_domain(duckdb_libpgquery::PGNode *arg, Oid baseTypeId, int32 baseTypeMod, Oid typeId,
				 duckdb_libpgquery::PGCoercionContext ccontext, duckdb_libpgquery::PGCoercionForm cformat, int location,
				 bool hideInputCoercion);

	void
	parser_coercion_errposition(PGParseState *pstate,
							int coerce_location,
							duckdb_libpgquery::PGNode *input_expr);

	TYPCATEGORY
	TypeCategory(Oid type);

	bool
	is_complex_array(Oid typid);

	bool
	IsPreferredType(TYPCATEGORY category, Oid type);

	Oid
	enforce_generic_type_consistency(const Oid *actual_arg_types,
								 Oid *declared_arg_types,
								 int nargs,
								 Oid rettype,
								 bool allow_poly);

	bool
	check_generic_type_consistency(const Oid *actual_arg_types,
							   const Oid *declared_arg_types,
							   int nargs);

	duckdb_libpgquery::PGNode *
	coerce_to_specific_type_typmod(PGParseState *pstate, duckdb_libpgquery::PGNode *node,
							   Oid targetTypeId, int32 targetTypmod,
							   const char *constructName);

	duckdb_libpgquery::PGNode *
	coerce_to_specific_type(PGParseState *pstate, duckdb_libpgquery::PGNode *node,
						Oid targetTypeId,
						const char *constructName);
};

}