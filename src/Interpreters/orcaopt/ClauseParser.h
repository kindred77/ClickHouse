#pragma once

#include <common/parser_common.hpp>

#include <Interpreters/orcaopt/Parser.h>

namespace DB
{

// class RelationParser;
// class SelectParser;
// class CoerceParser;
// class ExprParser;
// class TargetParser;
// class OperParser;
// class NodeParser;
// class TypeParser;
// class OperProvider;
// class FunctionProvider;

// using RelationParserPtr = std::shared_ptr<RelationParser>;
// using SelectParserPtr = std::shared_ptr<SelectParser>;
// using CoerceParserPtr = std::shared_ptr<CoerceParser>;
// using ExprParserPtr = std::shared_ptr<ExprParser>;
// using TargetParserPtr = std::shared_ptr<TargetParser>;
// using OperParserPtr = std::shared_ptr<OperParser>;
// using NodeParserPtr = std::shared_ptr<NodeParser>;
// using TypeParserPtr = std::shared_ptr<TypeParser>;
//using OperProviderPtr = std::shared_ptr<OperProvider>;
//using FunctionProviderPtr = std::shared_ptr<FunctionProvider>;

// class Context;
// using ContextPtr = std::shared_ptr<const Context>;

class ClauseParser : public Parser
{
private:
    // RelationParserPtr relation_parser;
    // SelectParserPtr select_parser;
    // CoerceParserPtr coerce_parser;
    // ExprParserPtr expr_parser;
    // TargetParserPtr target_parser;
    // OperParserPtr oper_parser;
    // NodeParserPtr node_parser;
    // TypeParserPtr type_parser;
    // ContextPtr context;
public:
	//explicit ClauseParser(const ContextPtr& context_);
    
    static void
    transformFromClause(duckdb_libpgquery::PGParseState *pstate, duckdb_libpgquery::PGList *frmList);

    static duckdb_libpgquery::PGNode *
    transformWhereClause(duckdb_libpgquery::PGParseState *pstate, duckdb_libpgquery::PGNode *clause,
					 duckdb_libpgquery::PGParseExprKind exprKind, const char *constructName);
    
    static duckdb_libpgquery::PGTargetEntry *
    findTargetlistEntrySQL99(duckdb_libpgquery::PGParseState *pstate, duckdb_libpgquery::PGNode *node, duckdb_libpgquery::PGList **tlist,
						 duckdb_libpgquery::PGParseExprKind exprKind);

    static duckdb_libpgquery::PGTargetEntry *
    findTargetlistEntrySQL92(duckdb_libpgquery::PGParseState *pstate, duckdb_libpgquery::PGNode *node, duckdb_libpgquery::PGList **tlist,
						 duckdb_libpgquery::PGParseExprKind exprKind);

    static duckdb_libpgquery::PGList *
    addTargetToSortList(duckdb_libpgquery::PGParseState * pstate, duckdb_libpgquery::PGTargetEntry * tle,
        duckdb_libpgquery::PGList * sortlist, duckdb_libpgquery::PGList * targetlist,
        duckdb_libpgquery::PGSortBy * sortby, bool resolveUnknown);

    static duckdb_libpgquery::PGList * transformSortClause(
        duckdb_libpgquery::PGParseState * pstate,
        duckdb_libpgquery::PGList * orderlist,
        duckdb_libpgquery::PGList ** targetlist,
        duckdb_libpgquery::PGParseExprKind exprKind, bool resolveUnknown, bool useSQL99);

    static duckdb_libpgquery::PGRangeTblEntry * transformCTEReference(duckdb_libpgquery::PGParseState * pstate,
        duckdb_libpgquery::PGRangeVar * r, duckdb_libpgquery::PGCommonTableExpr * cte, duckdb_libpgquery::PGIndex levelsup);

    static duckdb_libpgquery::PGRangeTblEntry * transformRangeFunction(duckdb_libpgquery::PGParseState * pstate, duckdb_libpgquery::PGRangeFunction * r);

    static duckdb_libpgquery::PGNode *
    transformFromClauseItem(duckdb_libpgquery::PGParseState *pstate, duckdb_libpgquery::PGNode *n,
						duckdb_libpgquery::PGRangeTblEntry **top_rte, int *top_rti,
						duckdb_libpgquery::PGList **namespace_ptr);

    static duckdb_libpgquery::PGRangeTblEntry *
    transformTableEntry(duckdb_libpgquery::PGParseState *pstate, duckdb_libpgquery::PGRangeVar *r);

    static duckdb_libpgquery::PGParseNamespaceItem *
    makeNamespaceItem(duckdb_libpgquery::PGRangeTblEntry *rte, bool rel_visible, bool cols_visible,
				  bool lateral_only, bool lateral_ok);

    static duckdb_libpgquery::PGRangeTblEntry *
    transformRangeSubselect(duckdb_libpgquery::PGParseState *pstate, duckdb_libpgquery::PGRangeSubselect *r);

    static void
    setNamespaceLateralState(duckdb_libpgquery::PGList *namespace_ptr,
                    bool lateral_only, bool lateral_ok);

    static duckdb_libpgquery::PGNode *
    buildMergedJoinVar(duckdb_libpgquery::PGParseState *pstate, duckdb_libpgquery::PGJoinType jointype,
				   duckdb_libpgquery::PGVar *l_colvar, duckdb_libpgquery::PGVar *r_colvar);

    static duckdb_libpgquery::PGNode *
    transformJoinUsingClause(duckdb_libpgquery::PGParseState *pstate,
						 duckdb_libpgquery::PGRangeTblEntry *leftRTE, duckdb_libpgquery::PGRangeTblEntry *rightRTE,
						 duckdb_libpgquery::PGList *leftVars, duckdb_libpgquery::PGList *rightVars);
    
    static duckdb_libpgquery::PGNode *
    transformJoinOnClause(duckdb_libpgquery::PGParseState *pstate, duckdb_libpgquery::PGJoinExpr *j, duckdb_libpgquery::PGList *namespace_ptr);

    static void
    extractRemainingColumns(duckdb_libpgquery::PGList *common_colnames,
						duckdb_libpgquery::PGList *src_colnames, duckdb_libpgquery::PGList *src_colvars,
						duckdb_libpgquery::PGList **res_colnames, duckdb_libpgquery::PGList **res_colvars);

    static void
    setNamespaceColumnVisibility(duckdb_libpgquery::PGList *namespace_ptr, bool cols_visible);

    static void
    checkTargetlistEntrySQL92(duckdb_libpgquery::PGParseState *pstate, duckdb_libpgquery::PGTargetEntry *tle,
						  duckdb_libpgquery::PGParseExprKind exprKind);

    static bool
    targetIsInSortList(duckdb_libpgquery::PGTargetEntry *tle, duckdb_libpgquery::PGOid sortop, duckdb_libpgquery::PGList *sortList);

    static duckdb_libpgquery::PGIndex
    assignSortGroupRef(duckdb_libpgquery::PGTargetEntry *tle, duckdb_libpgquery::PGList *tlist);

    static duckdb_libpgquery::PGList *
    addTargetToGroupList(duckdb_libpgquery::PGParseState *pstate, duckdb_libpgquery::PGTargetEntry *tle,
					 duckdb_libpgquery::PGList *grouplist,
                     duckdb_libpgquery::PGList *targetlist, int location);

    static duckdb_libpgquery::PGList *
    transformDistinctClause(duckdb_libpgquery::PGParseState *pstate,
						duckdb_libpgquery::PGList **targetlist,
                        duckdb_libpgquery::PGList *sortClause, bool is_agg);

    static void freeGroupList(duckdb_libpgquery::PGList * grouplist);

    static duckdb_libpgquery::PGList * transformRowExprToGroupClauses(duckdb_libpgquery::PGParseState * pstate,
        duckdb_libpgquery::PGRowExpr * rowexpr, duckdb_libpgquery::PGList * groupsets,
        duckdb_libpgquery::PGList * targetList);

    static duckdb_libpgquery::PGList * reorderGroupList(duckdb_libpgquery::PGList * grouplist);

    static duckdb_libpgquery::PGList * findListTargetlistEntries(
        duckdb_libpgquery::PGParseState * pstate, duckdb_libpgquery::PGNode * node,
        duckdb_libpgquery::PGList ** tlist, bool in_grpext, bool ignore_in_grpext,
        duckdb_libpgquery::PGParseExprKind exprKind, bool useSQL99);

    static duckdb_libpgquery::PGList * transformGroupClause(
        duckdb_libpgquery::PGParseState * pstate, duckdb_libpgquery::PGList * grouplist,
        duckdb_libpgquery::PGList ** targetlist, duckdb_libpgquery::PGList * sortClause,
        duckdb_libpgquery::PGParseExprKind exprKind, bool useSQL99);

    static duckdb_libpgquery::PGList *
    transformDistinctOnClause(duckdb_libpgquery::PGParseState *pstate, duckdb_libpgquery::PGList *distinctlist,
						  duckdb_libpgquery::PGList **targetlist,
                          duckdb_libpgquery::PGList *sortClause);

    static duckdb_libpgquery::PGNode * transformFrameOffset(
        duckdb_libpgquery::PGParseState * pstate, int frameOptions,
        duckdb_libpgquery::PGNode * clause,
        duckdb_libpgquery::PGList * orderClause,
        duckdb_libpgquery::PGList * targetlist, bool isFollowing, int location);

    static duckdb_libpgquery::PGTargetEntry * getTargetBySortGroupRef(duckdb_libpgquery::PGIndex ref, duckdb_libpgquery::PGList * tl);

    static duckdb_libpgquery::PGNode *
    transformLimitClause(duckdb_libpgquery::PGParseState *pstate, duckdb_libpgquery::PGNode *clause,
					 duckdb_libpgquery::PGParseExprKind exprKind, const char *constructName);

    static duckdb_libpgquery::PGList *
    transformWindowDefinitions(duckdb_libpgquery::PGParseState *pstate,
						   duckdb_libpgquery::PGList *windowdefs,
						   duckdb_libpgquery::PGList **targetlist);

    static duckdb_libpgquery::PGNode *
    flatten_grouping_sets(duckdb_libpgquery::PGNode *expr, bool toplevel, bool *hasGroupingSets);

    static duckdb_libpgquery::PGIndex
    transformGroupClauseExpr(duckdb_libpgquery::PGList **flatresult,
                        duckdb_libpgquery::PGBitmapset *seen_local,
						duckdb_libpgquery::PGParseState *pstate, duckdb_libpgquery::PGNode *gexpr,
						duckdb_libpgquery::PGList **targetlist,
                        duckdb_libpgquery::PGList *sortClause,
						duckdb_libpgquery::PGParseExprKind exprKind, bool useSQL99, bool toplevel);

    static duckdb_libpgquery::PGList *
    transformGroupClauseList(duckdb_libpgquery::PGList **flatresult,
						 duckdb_libpgquery::PGParseState *pstate, duckdb_libpgquery::PGList *list,
						 duckdb_libpgquery::PGList **targetlist,
                         duckdb_libpgquery::PGList *sortClause,
						 duckdb_libpgquery::PGParseExprKind exprKind, bool useSQL99, bool toplevel);

    static duckdb_libpgquery::PGNode *
    transformGroupingSet(duckdb_libpgquery::PGList **flatresult,
					 duckdb_libpgquery::PGParseState *pstate, duckdb_libpgquery::PGGroupingSet *gset,
					 duckdb_libpgquery::PGList **targetlist, duckdb_libpgquery::PGList *sortClause,
					 duckdb_libpgquery::PGParseExprKind exprKind, bool useSQL99, bool toplevel);

    static duckdb_libpgquery::PGWindowClause *
    findWindowClause(duckdb_libpgquery::PGList *wclist, const char *name);

    static int
    get_matching_location(int sortgroupref, duckdb_libpgquery::PGList *sortgrouprefs,
                    duckdb_libpgquery::PGList *exprs);

    static void
    checkExprIsVarFree(duckdb_libpgquery::PGParseState *pstate, duckdb_libpgquery::PGNode *n, const char *constructName);

    static duckdb_libpgquery::PGSortGroupClause * make_group_clause(duckdb_libpgquery::PGTargetEntry * tle,
        duckdb_libpgquery::PGList * targetlist, duckdb_libpgquery::PGOid eqop, duckdb_libpgquery::PGOid sortop, bool nulls_first, bool hashable);

    static duckdb_libpgquery::PGList * create_group_clause(duckdb_libpgquery::PGList * tlist_group,
        duckdb_libpgquery::PGList * targetlist, duckdb_libpgquery::PGList * sortClause,
        duckdb_libpgquery::PGList ** tlist_remainder);

    static duckdb_libpgquery::PGList * transformDistinctToGroupBy(duckdb_libpgquery::PGParseState * pstate,
        duckdb_libpgquery::PGList ** targetlist, duckdb_libpgquery::PGList ** sortClause,
        duckdb_libpgquery::PGList ** groupClause);

    static void processExtendedGrouping(duckdb_libpgquery::PGParseState * pstate, duckdb_libpgquery::PGNode * havingQual,
        duckdb_libpgquery::PGList * windowClause, duckdb_libpgquery::PGList * targetlist);
};

}
