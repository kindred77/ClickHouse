#pragma once

#include <common/parser_common.hpp>

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
//using TypeProviderPtr = std::shared_ptr<TypeProvider>;
//using ProcProviderPtr = std::shared_ptr<ProcProvider>;
//using CastProviderPtr = std::shared_ptr<CastProvider>;
//using RelationProviderPtr = std::shared_ptr<RelationProvider>;

class Context;
using ContextPtr = std::shared_ptr<const Context>;

class CoerceParser
{
private:
	RelationParserPtr relation_parser;
	NodeParserPtr node_parser;
	TypeParserPtr type_parser;
	// TypeProviderPtr type_provider;
	// ProcProviderPtr proc_provider;
	// CastProviderPtr cast_provider;
	// RelationProviderPtr relation_provider;

	ContextPtr context;
public:
	explicit CoerceParser(ContextPtr& context_);

	duckdb_libpgquery::PGOid
	select_common_type(duckdb_libpgquery::PGParseState *pstate, duckdb_libpgquery::PGList *exprs, const char *context,
				   duckdb_libpgquery::PGNode **which_expr);
	
	duckdb_libpgquery::PGNode *
	coerce_type(duckdb_libpgquery::PGParseState *pstate, duckdb_libpgquery::PGNode *node,
			duckdb_libpgquery::PGOid inputTypeId, duckdb_libpgquery::PGOid targetTypeId, duckdb_libpgquery::int32 targetTypeMod,
			duckdb_libpgquery::PGCoercionContext ccontext, duckdb_libpgquery::PGCoercionForm cformat, int location);

    duckdb_libpgquery::PGNode *
	coerce_to_target_type(duckdb_libpgquery::PGParseState *pstate, duckdb_libpgquery::PGNode *expr, duckdb_libpgquery::PGOid exprtype,
					  duckdb_libpgquery::PGOid targettype, duckdb_libpgquery::int32 targettypmod,
					  duckdb_libpgquery::PGCoercionContext ccontext,
					  duckdb_libpgquery::PGCoercionForm cformat,
					  int location);
	
	duckdb_libpgquery::PGNode *
	coerce_to_common_type(duckdb_libpgquery::PGParseState *pstate, duckdb_libpgquery::PGNode *node,
					  duckdb_libpgquery::PGOid targetTypeId, const char *context);

	duckdb_libpgquery::PGNode *
	coerce_to_boolean(duckdb_libpgquery::PGParseState *pstate, duckdb_libpgquery::PGNode *node,
				  const char *constructName);

	bool
	can_coerce_type(int nargs, const duckdb_libpgquery::PGOid *input_typeids, const duckdb_libpgquery::PGOid *target_typeids,
				duckdb_libpgquery::PGCoercionContext ccontext);

	duckdb_libpgquery::PGCoercionPathType
	find_coercion_pathway(duckdb_libpgquery::PGOid targetTypeId, duckdb_libpgquery::PGOid sourceTypeId,
					  duckdb_libpgquery::PGCoercionContext ccontext,
					  duckdb_libpgquery::PGOid *funcid);

	duckdb_libpgquery::PGCoercionPathType
	find_typmod_coercion_function(duckdb_libpgquery::PGOid typeId,
							  duckdb_libpgquery::PGOid *funcid);

    duckdb_libpgquery::PGNode * coerce_type_typmod(
        duckdb_libpgquery::PGNode * node, duckdb_libpgquery::PGOid targetTypeId, duckdb_libpgquery::int32 targetTypMod,
		duckdb_libpgquery::PGCoercionForm cformat, int location, bool isExplicit, bool hideInputCoercion);

    void
	hide_coercion_node(duckdb_libpgquery::PGNode *node);

	duckdb_libpgquery::PGNode *
	coerce_record_to_complex(duckdb_libpgquery::PGParseState *pstate, duckdb_libpgquery::PGNode *node,
						 duckdb_libpgquery::PGOid targetTypeId,
						 duckdb_libpgquery::PGCoercionContext ccontext,
						 duckdb_libpgquery::PGCoercionForm cformat,
						 int location);

    duckdb_libpgquery::PGNode * build_coercion_expression(
        duckdb_libpgquery::PGNode * node,
        duckdb_libpgquery::PGCoercionPathType pathtype,
        duckdb_libpgquery::PGOid funcId,
        duckdb_libpgquery::PGOid targetTypeId,
        duckdb_libpgquery::int32 targetTypMod,
        duckdb_libpgquery::PGCoercionForm cformat,
        int location,
        bool isExplicit);

    int
	parser_coercion_errposition(duckdb_libpgquery::PGParseState *pstate,
							int coerce_location,
							duckdb_libpgquery::PGNode *input_expr);

    duckdb_libpgquery::PGNode * coerce_to_domain(
        duckdb_libpgquery::PGNode * arg,
        duckdb_libpgquery::PGOid baseTypeId,
        duckdb_libpgquery::int32 baseTypeMod,
        duckdb_libpgquery::PGOid typeId,
        duckdb_libpgquery::PGCoercionForm cformat,
        int location,
        bool hideInputCoercion,
        bool lengthCoercionDone);

    duckdb_libpgquery::TYPCATEGORY
	TypeCategory(duckdb_libpgquery::PGOid type);

	bool
	is_complex_array(duckdb_libpgquery::PGOid typid);

	bool
	IsPreferredType(duckdb_libpgquery::TYPCATEGORY category, duckdb_libpgquery::PGOid type);

	duckdb_libpgquery::PGOid
	enforce_generic_type_consistency(const duckdb_libpgquery::PGOid *actual_arg_types,
								 duckdb_libpgquery::PGOid *declared_arg_types,
								 int nargs,
								 duckdb_libpgquery::PGOid rettype,
								 bool allow_poly);

	bool
	check_generic_type_consistency(const duckdb_libpgquery::PGOid *actual_arg_types,
							   const duckdb_libpgquery::PGOid *declared_arg_types,
							   int nargs);

	duckdb_libpgquery::PGNode *
	coerce_to_specific_type_typmod(duckdb_libpgquery::PGParseState *pstate, duckdb_libpgquery::PGNode *node,
							   duckdb_libpgquery::PGOid targetTypeId, duckdb_libpgquery::int32 targetTypmod,
							   const char *constructName);

	duckdb_libpgquery::PGNode *
	coerce_to_specific_type(duckdb_libpgquery::PGParseState *pstate, duckdb_libpgquery::PGNode *node,
						duckdb_libpgquery::PGOid targetTypeId,
						const char *constructName);

    bool IsBinaryCoercible(duckdb_libpgquery::PGOid srctype, duckdb_libpgquery::PGOid targettype);

    bool typeIsOfTypedTable(duckdb_libpgquery::PGOid reltypeId, duckdb_libpgquery::PGOid reloftypeId);

    duckdb_libpgquery::PGVar * coerce_unknown_var(
        duckdb_libpgquery::PGParseState * pstate,
        duckdb_libpgquery::PGVar * var,
        duckdb_libpgquery::PGOid targetTypeId,
        duckdb_libpgquery::int32 targetTypeMod,
        duckdb_libpgquery::PGCoercionContext ccontext,
        duckdb_libpgquery::PGCoercionForm cformat,
        int levelsup);

    void fixup_unknown_vars_in_targetlist(duckdb_libpgquery::PGParseState * pstate, duckdb_libpgquery::PGList * targetlist);
};

}
