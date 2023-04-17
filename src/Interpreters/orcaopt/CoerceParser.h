#pragma once

#include <Interpreters/orcaopt/parser_common.h>

namespace DB
{

class RelationParser;
class TypeParser;
class NodeParser;
class TypeProvider;
class ProcProvider;
class CastProvider;
class RelationProvider;

using RelationParserPtr = std::shared_ptr<RelationParser>;
using TypeParserPtr = std::shared_ptr<TypeParser>;
using NodeParserPtr = std::shared_ptr<NodeParser>;
using TypeProviderPtr = std::shared_ptr<TypeProvider>;
using ProcProviderPtr = std::shared_ptr<ProcProvider>;
using CastProviderPtr = std::shared_ptr<CastProvider>;
using RelationProviderPtr = std::shared_ptr<RelationProvider>;

class Context;
using ContextPtr = std::shared_ptr<const Context>;

class CoerceParser
{
private:
	RelationParserPtr relation_parser;
	NodeParserPtr node_parser;
	TypeParserPtr type_parser;
	TypeProviderPtr type_provider;
	ProcProviderPtr proc_provider;
	CastProviderPtr cast_provider;
	RelationProviderPtr relation_provider;

	ContextPtr context;
public:
	explicit CoerceParser(ContextPtr& context_);

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

	PGCoercionPathType
	find_coercion_pathway(Oid targetTypeId, Oid sourceTypeId,
					  duckdb_libpgquery::PGCoercionContext ccontext,
					  Oid *funcid);

	PGCoercionPathType
	find_typmod_coercion_function(Oid typeId,
							  Oid *funcid);

    duckdb_libpgquery::PGNode * coerce_type_typmod(
        duckdb_libpgquery::PGNode * node, Oid targetTypeId, int32 targetTypMod,
		duckdb_libpgquery::PGCoercionForm cformat, int location, bool isExplicit, bool hideInputCoercion);

    void
	hide_coercion_node(duckdb_libpgquery::PGNode *node);

	duckdb_libpgquery::PGNode *
	coerce_record_to_complex(PGParseState *pstate, duckdb_libpgquery::PGNode *node,
						 Oid targetTypeId,
						 duckdb_libpgquery::PGCoercionContext ccontext,
						 duckdb_libpgquery::PGCoercionForm cformat,
						 int location);

    duckdb_libpgquery::PGNode * build_coercion_expression(
        duckdb_libpgquery::PGNode * node,
        PGCoercionPathType pathtype,
        Oid funcId,
        Oid targetTypeId,
        int32 targetTypMod,
        duckdb_libpgquery::PGCoercionForm cformat,
        int location,
        bool isExplicit);

    int
	parser_coercion_errposition(PGParseState *pstate,
							int coerce_location,
							duckdb_libpgquery::PGNode *input_expr);

    duckdb_libpgquery::PGNode * coerce_to_domain(
        duckdb_libpgquery::PGNode * arg,
        Oid baseTypeId,
        int32 baseTypeMod,
        Oid typeId,
        duckdb_libpgquery::PGCoercionForm cformat,
        int location,
        bool hideInputCoercion,
        bool lengthCoercionDone);

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

    bool IsBinaryCoercible(Oid srctype, Oid targettype);

    bool typeIsOfTypedTable(Oid reltypeId, Oid reloftypeId);

    duckdb_libpgquery::PGVar * coerce_unknown_var(
        PGParseState * pstate,
        duckdb_libpgquery::PGVar * var,
        Oid targetTypeId,
        int32 targetTypeMod,
        duckdb_libpgquery::PGCoercionContext ccontext,
        duckdb_libpgquery::PGCoercionForm cformat,
        int levelsup);

    void fixup_unknown_vars_in_targetlist(PGParseState * pstate, duckdb_libpgquery::PGList * targetlist);
};

}
