#pragma once

#include <Interpreters/orcaopt/pgopt/parser_common.h>
#include <Interpreters/orcaopt/pgopt/CoerceParser.h>
#include <Interpreters/orcaopt/pgopt/RelationProvider.h>

#include <Storages/IStorage.h>

#include <optional>

namespace DB
{

class RelationParser
{
private:
    CoerceParser coerce_parser;
	std::shared_ptr<RelationProvider> relation_provider;
public:
	explicit RelationParser();

	void expandTupleDesc(StoragePtr storage, duckdb_libpgquery::PGAlias *eref, int offset,
				int rtindex, int sublevels_up,
				int location, bool include_dropped,
				duckdb_libpgquery::PGList **colnames, duckdb_libpgquery::PGList **colvars);
    
    void expandRelation(Oid relid, duckdb_libpgquery::PGAlias *eref, int rtindex, int sublevels_up,
			   int location, bool include_dropped,
			   duckdb_libpgquery::PGList **colnames, duckdb_libpgquery::PGList **colvars);
    
    void expandRTE(duckdb_libpgquery::PGRangeTblEntry *rte, int rtindex, int sublevels_up,
		  int location, bool include_dropped,
		  duckdb_libpgquery::PGList **colnames, duckdb_libpgquery::PGList **colvars);
    
    void checkNameSpaceConflicts(PGParseState *pstate, duckdb_libpgquery::PGList *namespace1,
						duckdb_libpgquery::PGList *namespace2);
    
    duckdb_libpgquery::PGRangeTblEntry *
    addRangeTableEntryForCTE(PGParseState *pstate,
						 duckdb_libpgquery::PGCommonTableExpr *cte,
						 Index levelsup,
						 duckdb_libpgquery::PGRangeVar *rv,
						 bool inFromCl);
    
    duckdb_libpgquery::PGCommonTableExpr *
    scanNameSpaceForCTE(PGParseState *pstate, const char *refname,
					Index *ctelevelsup);

	duckdb_libpgquery::PGNode *
	scanRTEForColumn(PGParseState *pstate, duckdb_libpgquery::PGRangeTblEntry *rte, char *colname,
				 int location);
	
	int RTERangeTablePosn(PGParseState *pstate, duckdb_libpgquery::PGRangeTblEntry *rte, int *sublevels_up);

	duckdb_libpgquery::PGRangeTblEntry * addRangeTableEntry(PGParseState *pstate,
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
	
	void markRTEForSelectPriv(PGParseState *pstate, duckdb_libpgquery::PGRangeTblEntry *rte,
					 int rtindex, PGAttrNumber col);
	
	void markVarForSelectPriv(PGParseState *pstate, duckdb_libpgquery::PGVar *var, duckdb_libpgquery::PGRangeTblEntry *rte);

	duckdb_libpgquery::PGNode *
	colNameToVar(PGParseState *pstate, char *colname, bool localonly,
			 int location);

	duckdb_libpgquery::PGVar *
	make_var(PGParseState *pstate, duckdb_libpgquery::PGRangeTblEntry *rte, int attrno, int location);

	void
	get_rte_attribute_type(duckdb_libpgquery::PGRangeTblEntry *rte, PGAttrNumber attnum,
					   Oid *vartype, int32 *vartypmod, Oid *varcollid);
	
	duckdb_libpgquery::PGRangeTblEntry *
	GetRTEByRangeTablePosn(PGParseState *pstate,
					   int varno,
					   int sublevels_up);

	duckdb_libpgquery::PGCommonTableExpr *
	GetCTEForRTE(PGParseState *pstate, duckdb_libpgquery::PGRangeTblEntry *rte, int rtelevelsup);

	duckdb_libpgquery::PGRangeTblEntry *
	addRangeTableEntryForJoin(PGParseState *pstate,
						  duckdb_libpgquery::PGList *colnames,
						  duckdb_libpgquery::PGJoinType jointype,
						  duckdb_libpgquery::PGList *aliasvars,
						  duckdb_libpgquery::PGAlias *alias,
						  bool inFromCl);
	
	duckdb_libpgquery::PGTargetEntry *
	get_tle_by_resno(duckdb_libpgquery::PGList *tlist, PGAttrNumber resno);

	void buildRelationAliases(StoragePtr storage_ptr,
		duckdb_libpgquery::PGAlias *alias, duckdb_libpgquery::PGAlias *eref);

	duckdb_libpgquery::PGRangeTblEntry *
	refnameRangeTblEntry(PGParseState *pstate,
					 const char *schemaname,
					 const char *refname,
					 int location,
					 int *sublevels_up);
	
	void
	check_lateral_ref_ok(PGParseState *pstate, PGParseNamespaceItem *nsitem,
					 int location);

	duckdb_libpgquery::PGRangeTblEntry *
	scanNameSpaceForRelid(PGParseState *pstate, Oid relid, int location);

	duckdb_libpgquery::PGRangeTblEntry *
	scanNameSpaceForRefname(PGParseState *pstate, const char *refname, int location);
};

}