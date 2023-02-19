#pragma once

#include <Interpreters/orcaopt/parser_common.h>

namespace DB
{

class RelationParser;
class SelectParser;
class CoerceParser;
class ExprParser;
class TargetParser;
class OperParser;
class NodeParser;
class TypeParser;
class OperProvider;
class FunctionProvider;

using RelationParserPtr = std::unique_ptr<RelationParser>;
using SelectParserPtr = std::unique_ptr<SelectParser>;
using CoerceParserPtr = std::unique_ptr<CoerceParser>;
using ExprParserPtr = std::unique_ptr<ExprParser>;
using TargetParserPtr = std::unique_ptr<TargetParser>;
using OperParserPtr = std::unique_ptr<OperParser>;
using NodeParserPtr = std::unique_ptr<NodeParser>;
using TypeParserPtr = std::unique_ptr<TypeParser>;
using OperProviderPtr = std::unique_ptr<OperProvider>;
using FunctionProviderPtr = std::unique_ptr<FunctionProvider>;

class ClauseParser
{
private:
    RelationParserPtr relation_parser;
    SelectParserPtr select_parser;
    CoerceParserPtr coerce_parser;
    ExprParserPtr expr_parser;
    TargetParserPtr target_parser;
    //CollationParserPtr collation_parser;
    OperParserPtr oper_parser;
    NodeParserPtr node_parser;
    TypeParserPtr type_parser;
    OperProviderPtr oper_provider;
    FunctionProviderPtr function_provider;
    //std::shared_ptr<ScalarOperatorProvider> scalar_operator_provider = nullptr;
public:
	explicit ClauseParser();
    
    void
    transformFromClause(PGParseState *pstate, duckdb_libpgquery::PGList *frmList);

    duckdb_libpgquery::PGNode *
    transformWhereClause(PGParseState *pstate, duckdb_libpgquery::PGNode *clause,
					 PGParseExprKind exprKind, const char *constructName);
    
    duckdb_libpgquery::PGTargetEntry *
    findTargetlistEntrySQL99(PGParseState *pstate, duckdb_libpgquery::PGNode *node, duckdb_libpgquery::PGList **tlist,
						 PGParseExprKind exprKind);

    duckdb_libpgquery::PGTargetEntry *
    findTargetlistEntrySQL92(PGParseState *pstate, duckdb_libpgquery::PGNode *node, duckdb_libpgquery::PGList **tlist,
						 PGParseExprKind exprKind);

    duckdb_libpgquery::PGList *
    addTargetToSortList(PGParseState * pstate, duckdb_libpgquery::PGTargetEntry * tle,
        duckdb_libpgquery::PGList * sortlist, duckdb_libpgquery::PGList * targetlist,
        duckdb_libpgquery::PGSortBy * sortby, bool resolveUnknown);

    duckdb_libpgquery::PGList * transformSortClause(
        PGParseState * pstate,
        duckdb_libpgquery::PGList * orderlist,
        duckdb_libpgquery::PGList ** targetlist,
        PGParseExprKind exprKind, bool resolveUnknown, bool useSQL99);
    
    duckdb_libpgquery::PGNode *
    transformFromClauseItem(PGParseState *pstate, duckdb_libpgquery::PGNode *n,
						duckdb_libpgquery::PGRangeTblEntry **top_rte, int *top_rti,
						duckdb_libpgquery::PGList **namespace_ptr);

    duckdb_libpgquery::PGRangeTblEntry *
    getRTEForSpecialRelationTypes(PGParseState *pstate, duckdb_libpgquery::PGRangeVar *rv);

    duckdb_libpgquery::PGRangeTblEntry *
    transformTableEntry(PGParseState *pstate, duckdb_libpgquery::PGRangeVar *r);

    PGParseNamespaceItem *
    makeNamespaceItem(duckdb_libpgquery::PGRangeTblEntry *rte, bool rel_visible, bool cols_visible,
				  bool lateral_only, bool lateral_ok);

    duckdb_libpgquery::PGRangeTblEntry *
    transformRangeSubselect(PGParseState *pstate, duckdb_libpgquery::PGRangeSubselect *r);

    void
    setNamespaceLateralState(duckdb_libpgquery::PGList *namespace_ptr,
                    bool lateral_only, bool lateral_ok);

    duckdb_libpgquery::PGNode *
    buildMergedJoinVar(PGParseState *pstate, duckdb_libpgquery::PGJoinType jointype,
				   duckdb_libpgquery::PGVar *l_colvar, duckdb_libpgquery::PGVar *r_colvar);

    duckdb_libpgquery::PGNode *
    transformJoinUsingClause(PGParseState *pstate,
						 duckdb_libpgquery::PGRangeTblEntry *leftRTE, duckdb_libpgquery::PGRangeTblEntry *rightRTE,
						 duckdb_libpgquery::PGList *leftVars, duckdb_libpgquery::PGList *rightVars);
    
    duckdb_libpgquery::PGNode *
    transformJoinOnClause(PGParseState *pstate, duckdb_libpgquery::PGJoinExpr *j, duckdb_libpgquery::PGList *namespace_ptr);

    void
    extractRemainingColumns(duckdb_libpgquery::PGList *common_colnames,
						duckdb_libpgquery::PGList *src_colnames, duckdb_libpgquery::PGList *src_colvars,
						duckdb_libpgquery::PGList **res_colnames, duckdb_libpgquery::PGList **res_colvars);

    void
    setNamespaceColumnVisibility(duckdb_libpgquery::PGList *namespace_ptr, bool cols_visible);

    void
    checkTargetlistEntrySQL92(PGParseState *pstate, duckdb_libpgquery::PGTargetEntry *tle,
						  PGParseExprKind exprKind);

    bool
    targetIsInSortList(duckdb_libpgquery::PGTargetEntry *tle, Oid sortop, duckdb_libpgquery::PGList *sortList);

    Index
    assignSortGroupRef(duckdb_libpgquery::PGTargetEntry *tle, duckdb_libpgquery::PGList *tlist);

    duckdb_libpgquery::PGList *
    addTargetToGroupList(PGParseState *pstate, duckdb_libpgquery::PGTargetEntry *tle,
					 duckdb_libpgquery::PGList *grouplist,
                     duckdb_libpgquery::PGList *targetlist, int location);

    duckdb_libpgquery::PGList *
    transformDistinctClause(PGParseState *pstate,
						duckdb_libpgquery::PGList **targetlist,
                        duckdb_libpgquery::PGList *sortClause, bool is_agg);

    void freeGroupList(duckdb_libpgquery::PGList * grouplist);

    duckdb_libpgquery::PGList * transformRowExprToGroupClauses(PGParseState * pstate,
        duckdb_libpgquery::PGRowExpr * rowexpr, duckdb_libpgquery::PGList * groupsets,
        duckdb_libpgquery::PGList * targetList);

    duckdb_libpgquery::PGList * reorderGroupList(duckdb_libpgquery::PGList * grouplist);

    duckdb_libpgquery::PGList * findListTargetlistEntries(
        PGParseState * pstate, duckdb_libpgquery::PGNode * node,
        duckdb_libpgquery::PGList ** tlist, bool in_grpext, bool ignore_in_grpext,
        PGParseExprKind exprKind, bool useSQL99);

    duckdb_libpgquery::PGList * transformGroupClause(
        PGParseState * pstate, duckdb_libpgquery::PGList * grouplist,
        duckdb_libpgquery::PGList ** targetlist, duckdb_libpgquery::PGList * sortClause,
        PGParseExprKind exprKind, bool useSQL99);

    duckdb_libpgquery::PGList *
    transformDistinctOnClause(PGParseState *pstate, duckdb_libpgquery::PGList *distinctlist,
						  duckdb_libpgquery::PGList **targetlist,
                          duckdb_libpgquery::PGList *sortClause);

    duckdb_libpgquery::PGNode * transformFrameOffset(
        PGParseState * pstate, int frameOptions,
        duckdb_libpgquery::PGNode * clause,
        duckdb_libpgquery::PGList * orderClause,
        duckdb_libpgquery::PGList * targetlist, bool isFollowing, int location);

    duckdb_libpgquery::PGTargetEntry * getTargetBySortGroupRef(Index ref, duckdb_libpgquery::PGList * tl);

    duckdb_libpgquery::PGNode *
    transformLimitClause(PGParseState *pstate, duckdb_libpgquery::PGNode *clause,
					 PGParseExprKind exprKind, const char *constructName);

    duckdb_libpgquery::PGList *
    transformWindowDefinitions(PGParseState *pstate,
						   duckdb_libpgquery::PGList *windowdefs,
						   duckdb_libpgquery::PGList **targetlist);

    duckdb_libpgquery::PGNode *
    flatten_grouping_sets(duckdb_libpgquery::PGNode *expr, bool toplevel, bool *hasGroupingSets);

    Index
    transformGroupClauseExpr(duckdb_libpgquery::PGList **flatresult,
                        duckdb_libpgquery::PGBitmapset *seen_local,
						PGParseState *pstate, duckdb_libpgquery::PGNode *gexpr,
						duckdb_libpgquery::PGList **targetlist,
                        duckdb_libpgquery::PGList *sortClause,
						PGParseExprKind exprKind, bool useSQL99, bool toplevel);

    duckdb_libpgquery::PGList *
    transformGroupClauseList(duckdb_libpgquery::PGList **flatresult,
						 PGParseState *pstate, duckdb_libpgquery::PGList *list,
						 duckdb_libpgquery::PGList **targetlist,
                         duckdb_libpgquery::PGList *sortClause,
						 PGParseExprKind exprKind, bool useSQL99, bool toplevel);

    duckdb_libpgquery::PGNode *
    transformGroupingSet(duckdb_libpgquery::PGList **flatresult,
					 PGParseState *pstate, duckdb_libpgquery::PGGroupingSet *gset,
					 duckdb_libpgquery::PGList **targetlist, duckdb_libpgquery::PGList *sortClause,
					 PGParseExprKind exprKind, bool useSQL99, bool toplevel);

    duckdb_libpgquery::PGWindowClause *
    findWindowClause(duckdb_libpgquery::PGList *wclist, const char *name);

    int
    get_matching_location(int sortgroupref, duckdb_libpgquery::PGList *sortgrouprefs,
                    duckdb_libpgquery::PGList *exprs);

    void
    checkExprIsVarFree(PGParseState *pstate, duckdb_libpgquery::PGNode *n, const char *constructName);

    duckdb_libpgquery::PGSortGroupClause * make_group_clause(duckdb_libpgquery::PGTargetEntry * tle,
        duckdb_libpgquery::PGList * targetlist, Oid eqop, Oid sortop, bool nulls_first, bool hashable);

    duckdb_libpgquery::PGList * create_group_clause(duckdb_libpgquery::PGList * tlist_group,
        duckdb_libpgquery::PGList * targetlist, duckdb_libpgquery::PGList * sortClause,
        duckdb_libpgquery::PGList ** tlist_remainder);

    duckdb_libpgquery::PGList * transformDistinctToGroupBy(PGParseState * pstate,
        duckdb_libpgquery::PGList ** targetlist, duckdb_libpgquery::PGList ** sortClause,
        duckdb_libpgquery::PGList ** groupClause);

    void processExtendedGrouping(PGParseState * pstate, duckdb_libpgquery::PGNode * havingQual,
        duckdb_libpgquery::PGList * windowClause, duckdb_libpgquery::PGList * targetlist);
};

}
