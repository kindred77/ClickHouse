#pragma once

#include <nodes/parsenodes.hpp>
#include <nodes/makefuncs.hpp>
#include <nodes/nodeFuncs.hpp>
#include <nodes/nodes.hpp>
#include <nodes/primnodes.hpp>
#include <nodes/pg_list.hpp>
#include <nodes/bitmapset.hpp>
#include <nodes/lockoptions.hpp>
#include <nodes/value.hpp>
#include <pg_functions.hpp>
#include <access/attnum.hpp>
#include <common/common_def.hpp>
#include <common/common_macro.hpp>
#include <common/common_walkers.hpp>
#include <common/common_equalfuncs.hpp>
#include <common/common_datum.hpp>
//#include <c.h>
//#include <funcapi.h>
//#include <Common/Exception.h>

#include <string.h>
#include <boost/algorithm/string.hpp>
#include <optional>

namespace duckdb_libpgquery {

extern bool SQL_inheritance;

// namespace duckdb_libpgquery
// {
//     typedef DistinctExpr PGDistinctExpr;

//     typedef NullIfExpr PGNullIfExpr;
// };

extern PGTupleDescPtr PGCreateTemplateTupleDesc(int natts, bool hasoid);

/*
 * CreateTupleDescCopy
 *		This function creates a new TupleDesc by copying from an existing
 *		TupleDesc.
 *
 * !!! Constraints and defaults are not copied !!!
 */
extern PGTupleDescPtr PGCreateTupleDescCopy(PGTupleDescPtr tupdesc);

/*
 * CreateTupleDescCopyConstr
 *		This function creates a new TupleDesc by copying from an existing
 *		TupleDesc (including its constraints and defaults).
 */
extern PGTupleDescPtr PGCreateTupleDescCopyConstr(PGTupleDescPtr tupdesc);

// int
// namestrcpy(Name name, const char *str)
// {
// 	if (!name || !str)
// 		return -1;
// 	StrNCpy(NameStr(*name), str, NAMEDATALEN);
// 	return 0;
// };

extern void PGTupleDescInitEntryCollation(PGTupleDescPtr desc, PGAttrNumber attributeNumber, PGOid collationid);

extern void PGTupleDescCopyEntry(PGTupleDescPtr dst, PGAttrNumber dstAttno, PGTupleDescPtr src, PGAttrNumber srcAttno);

extern PGOid MyDatabaseId;

extern size_t strlcpy(char *dst, const char *src, size_t siz);

/*
 * count_nonjunk_tlist_entries
 *		What it says ...
 */
extern int count_nonjunk_tlist_entries(duckdb_libpgquery::PGList *tlist);

extern PGParseState * make_parsestate(PGParseState *parentParseState);

extern duckdb_libpgquery::PGValue * makeString(char * str);

extern int parser_errposition(PGParseState *pstate, int location);

extern void free_parsestate(PGParseState *pstate);

/* Global variables */
extern ErrorContextCallback *error_context_stack;

extern void pcb_error_callback(void * arg);

extern void setup_parser_errposition_callback(PGParseCallbackState *pcbstate,
								  PGParseState *pstate, int location);

extern void cancel_parser_errposition_callback(PGParseCallbackState *pcbstate);

extern duckdb_libpgquery::PGTargetEntry * get_sortgroupref_tle(PGIndex sortref, duckdb_libpgquery::PGList * targetList);

extern duckdb_libpgquery::PGTargetEntry * get_sortgroupclause_tle(
	duckdb_libpgquery::PGSortGroupClause * sgClause, 
	duckdb_libpgquery::PGList * targetList);

extern duckdb_libpgquery::PGNode *
get_sortgroupclause_expr(duckdb_libpgquery::PGSortGroupClause * sgClause, duckdb_libpgquery::PGList * targetList);

extern bool scanint8(const char * str, bool errorOK, int64 * result);

extern std::string PGNameListToString(duckdb_libpgquery::PGList * names);

/*
 * DeconstructQualifiedName
 *		Given a possibly-qualified name expressed as a list of String nodes,
 *		extract the schema name and object name.
 *
 * *nspname_p is set to NULL if there is no explicit schema name.
 */
extern void DeconstructQualifiedName(duckdb_libpgquery::PGList * names, char ** nspname_p, char ** objname_p);

extern duckdb_libpgquery::PGList * stringToQualifiedNameList(const char * string);

extern duckdb_libpgquery::PGRangeVar * makeRangeVarFromNameList(duckdb_libpgquery::PGList * names);

extern int pg_strcasecmp(const char * s1, const char * s2);

extern bool optimizer_multilevel_partitioning;

}
