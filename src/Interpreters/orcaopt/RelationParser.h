#pragma once

#include <Interpreters/orcaopt/parser_common.h>

//#include <Storages/IStorage.h>

//#include <optional>

namespace DB
{
class CoerceParser;
class NodeParser;
class TypeParser;
class RelationProvider;
class TypeProvider;
class FunctionProvider;

using CoerceParserPtr = std::shared_ptr<CoerceParser>;
using NodeParserPtr = std::shared_ptr<NodeParser>;
using TypeParserPtr = std::shared_ptr<TypeParser>;
using RelationProviderPtr = std::shared_ptr<RelationProvider>;
using TypeProviderPtr = std::shared_ptr<TypeProvider>;
using FunctionProviderPtr = std::shared_ptr<FunctionProvider>;

class Context;
using ContextPtr = std::shared_ptr<const Context>;

class RelationParser
{
private:
    CoerceParserPtr coerce_parser;
	//std::shared_ptr<RelationProvider> relation_provider;
	NodeParserPtr node_parser;
	TypeParserPtr type_parser;

	RelationProviderPtr relation_provider;
	TypeProviderPtr type_provider;
	FunctionProviderPtr function_provider;
	//ENRParser enr_parser;

    //int specialAttNum(const char * attname);

	ContextPtr context;

public:
	explicit RelationParser(const ContextPtr& context_);

	void
	expandRTE(duckdb_libpgquery::PGRangeTblEntry *rte, int rtindex, int sublevels_up,
		  int location, bool include_dropped,
		  duckdb_libpgquery::PGList **colnames, duckdb_libpgquery::PGList **colvars);

	duckdb_libpgquery::PGCommonTableExpr *
	scanNameSpaceForCTE(PGParseState *pstate, const char *refname,
					Index *ctelevelsup);

	duckdb_libpgquery::PGRangeTblEntry *
	addRangeTableEntryForCTE(PGParseState *pstate,
						 duckdb_libpgquery::PGCommonTableExpr *cte,
						 Index levelsup,
						 duckdb_libpgquery::PGRangeVar *rv,
						 bool inFromCl);

	void
	expandRelation(Oid relid, duckdb_libpgquery::PGAlias *eref, int rtindex, int sublevels_up,
			   int location, bool include_dropped,
			   duckdb_libpgquery::PGList **colnames, duckdb_libpgquery::PGList **colvars);

	void
	expandTupleDesc(PGTupleDescPtr tupdesc, duckdb_libpgquery::PGAlias *eref, int count, int offset,
				int rtindex, int sublevels_up,
				int location, bool include_dropped,
				duckdb_libpgquery::PGList **colnames, duckdb_libpgquery::PGList **colvars);

	void
	buildRelationAliases(PGTupleDescPtr tupdesc, duckdb_libpgquery::PGAlias *alias,
				duckdb_libpgquery::PGAlias *eref);

	duckdb_libpgquery::PGRangeTblEntry *
	addRangeTableEntry(PGParseState *pstate,
				   duckdb_libpgquery::PGRangeVar *relation,
				   duckdb_libpgquery::PGAlias *alias,
				   bool inh,
				   bool inFromCl);

	duckdb_libpgquery::PGRangeTblEntry *
	addRangeTableEntryForSubquery(PGParseState *pstate,
							  duckdb_libpgquery::PGQuery *subquery,
							  duckdb_libpgquery::PGAlias *alias,
							  bool lateral,
							  bool inFromCl);
	
	duckdb_libpgquery::PGLockingClause *
	getLockedRefname(PGParseState *pstate, const char *refname);

	void
	checkNameSpaceConflicts(PGParseState *pstate, duckdb_libpgquery::PGList *namespace1,
						duckdb_libpgquery::PGList *namespace2);

	duckdb_libpgquery::PGRangeTblEntry *
	addRangeTableEntryForJoin(PGParseState *pstate,
						  duckdb_libpgquery::PGList *colnames,
						  duckdb_libpgquery::PGJoinType jointype,
						  duckdb_libpgquery::PGList *aliasvars,
						  duckdb_libpgquery::PGAlias *alias,
						  bool inFromCl);

	duckdb_libpgquery::PGRangeTblEntry *
	GetRTEByRangeTablePosn(PGParseState *pstate,
					   int varno,
					   int sublevels_up);

    int32 * getValuesTypmods(duckdb_libpgquery::PGRangeTblEntry * rte);

    duckdb_libpgquery::PGNode * scanRTEForColumn(PGParseState * pstate,
		duckdb_libpgquery::PGRangeTblEntry * rte, const char * colname, int location);

    duckdb_libpgquery::PGNode *
	colNameToVar(PGParseState *pstate, const char *colname, bool localonly,
			 int location);

    duckdb_libpgquery::PGRangeTblEntry * addRangeTableEntryForFunction(
        PGParseState * pstate,
        duckdb_libpgquery::PGList * funcnames,
        duckdb_libpgquery::PGList * funcexprs,
        duckdb_libpgquery::PGList * coldeflists,
        duckdb_libpgquery::PGRangeFunction * rangefunc,
        bool lateral,
        bool inFromCl);

    void
	markRTEForSelectPriv(PGParseState *pstate, duckdb_libpgquery::PGRangeTblEntry *rte,
					 int rtindex, PGAttrNumber col);

	void
	markVarForSelectPriv(PGParseState *pstate, duckdb_libpgquery::PGVar *var, duckdb_libpgquery::PGRangeTblEntry *rte);

	duckdb_libpgquery::PGRangeTblEntry *
	scanNameSpaceForRelid(PGParseState *pstate, Oid relid, int location);

	duckdb_libpgquery::PGRangeTblEntry *
	scanNameSpaceForRefname(PGParseState *pstate, const char *refname, int location);

	duckdb_libpgquery::PGRangeTblEntry *
	refnameRangeTblEntry(PGParseState *pstate,
					 const char *schemaname,
					 const char *refname,
					 int location,
					 int *sublevels_up);

	void
	errorMissingRTE(PGParseState *pstate, duckdb_libpgquery::PGRangeVar *relation);

	void
	errorMissingColumn(PGParseState *pstate,
				   const char *relname, char *colname, int location);

    duckdb_libpgquery::PGRangeTblEntry * searchRangeTableForCol(PGParseState * pstate, char * colname, int location);

    int
	RTERangeTablePosn(PGParseState *pstate, duckdb_libpgquery::PGRangeTblEntry *rte, int *sublevels_up);

	duckdb_libpgquery::PGList *
	expandRelAttrs(PGParseState *pstate, duckdb_libpgquery::PGRangeTblEntry *rte,
			   int rtindex, int sublevels_up, int location);

	duckdb_libpgquery::PGTargetEntry *
	get_tle_by_resno(duckdb_libpgquery::PGList *tlist, PGAttrNumber resno);

	duckdb_libpgquery::PGCommonTableExpr *
	GetCTEForRTE(PGParseState *pstate, duckdb_libpgquery::PGRangeTblEntry *rte, int rtelevelsup);

	duckdb_libpgquery::PGRangeTblEntry *
	searchRangeTableForRel(PGParseState *pstate, duckdb_libpgquery::PGRangeVar *relation);

	bool
	isSimplyUpdatableRelation(Oid relid, bool noerror);

	void
	get_rte_attribute_type(duckdb_libpgquery::PGRangeTblEntry *rte, PGAttrNumber attnum,
					   Oid *vartype, int32 *vartypmod, Oid *varcollid);
	
	void
	check_lateral_ref_ok(PGParseState *pstate, PGParseNamespaceItem *nsitem,
					 int location);

    String chooseScalarFunctionAlias(duckdb_libpgquery::PGNode * funcexpr, char * funcname,
		duckdb_libpgquery::PGAlias * alias, int nfuncs);

    bool isFutureCTE(PGParseState * pstate, const char * refname);

    PGRelationPtr parserOpenTable(PGParseState * pstate, const duckdb_libpgquery::PGRangeVar * relation, int lockmode, bool * lockUpgraded);
};

}
