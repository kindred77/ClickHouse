#pragma once

#include <parser_common.h>

namespace DB
{

class RelationParser
{
private:
    

public:
	explicit RelationParser();

	void expandTupleDesc(TupleDesc tupdesc, duckdb_libpgquery::PGAlias *eref, int count, int offset,
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
};

}