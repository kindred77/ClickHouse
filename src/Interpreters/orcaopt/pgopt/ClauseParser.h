#pragma once

#include <parser_common.h>

#include <RelationParser.h>
#include <SelectParser.h>
#include <ExprParser.h>

namespace DB
{

class ClauseParser
{
private:
    RelationParser relation_parser;
    SelectParser select_parser;
    CoerceParser coerce_parser;
    ExprParser expr_parser;
    TargetParser target_parser;
public:
	explicit ClauseParser();
    void transformFromClause(PGParseState *pstate, duckdb_libpgquery::PGList *frmList);

    duckdb_libpgquery::PGNode *
    transformFromClauseItem(PGParseState *pstate, duckdb_libpgquery::PGNode *n,
        duckdb_libpgquery::PGRangeTblEntry **top_rte, int *top_rti,
        duckdb_libpgquery::PGList **namespace);
    
    duckdb_libpgquery::PGRangeTblEntry *
    transformCTEReference(PGParseState *pstate, duckdb_libpgquery::PGRangeVar *r,
					  duckdb_libpgquery::PGCommonTableExpr *cte, Index levelsup);
    
    duckdb_libpgquery::PGRangeTblEntry *
    transformRangeSubselect(PGParseState *pstate, duckdb_libpgquery::PGRangeSubselect *r);
    
    void setNamespaceLateralState(duckdb_libpgquery::PGList *namespace, bool lateral_only, bool lateral_ok);

    duckdb_libpgquery::PGRangeTblEntry *
    transformTableEntry(PGParseState *pstate, duckdb_libpgquery::PGRangeVar *r);

    bool interpretInhOption(InhOption inhOpt);

    PGParseNamespaceItem * makeNamespaceItem(duckdb_libpgquery::PGRangeTblEntry *rte,
        bool rel_visible = true, bool cols_visible = true,
		bool lateral_only = false, bool lateral_ok = true);
    
    duckdb_libpgquery::PGNode *
    buildMergedJoinVar(PGParseState *pstate, duckdb_libpgquery::PGJoinType jointype,
				   duckdb_libpgquery::PGVar *l_colvar, duckdb_libpgquery::PGVar *r_colvar);
    
    duckdb_libpgquery::PGNode *
    transformJoinUsingClause(PGParseState *pstate,
						 duckdb_libpgquery::PGRangeTblEntry *leftRTE, duckdb_libpgquery::PGRangeTblEntry *rightRTE,
						 duckdb_libpgquery::PGList *leftVars, duckdb_libpgquery::PGList *rightVars);
    
    duckdb_libpgquery::PGNode *
    transformJoinOnClause(PGParseState *pstate, duckdb_libpgquery::PGJoinExpr *j, duckdb_libpgquery::PGList *namespace);

    duckdb_libpgquery::PGNode *
    transformWhereClause(PGParseState *pstate, duckdb_libpgquery::PGNode *clause,
					 PGParseExprKind exprKind, const char *constructName);
    
    duckdb_libpgquery::PGList *
    transformSortClause(PGParseState *pstate,
					duckdb_libpgquery::PGList *orderlist,
					duckdb_libpgquery::PGList **targetlist,
					PGParseExprKind exprKind,
					bool resolveUnknown,
					bool useSQL99);

    duckdb_libpgquery::PGTargetEntry *
    findTargetlistEntrySQL99(PGParseState *pstate, duckdb_libpgquery::PGNode *node, duckdb_libpgquery::PGList **tlist,
						 PGParseExprKind exprKind);
    
    duckdb_libpgquery::PGTargetEntry *
    findTargetlistEntrySQL92(PGParseState *pstate, duckdb_libpgquery::PGNode *node, duckdb_libpgquery::PGList **tlist,
						 PGParseExprKind exprKind);
    
    void checkTargetlistEntrySQL92(PGParseState *pstate, duckdb_libpgquery::PGTargetEntry *tle,
						  PGParseExprKind exprKind);
    
    duckdb_libpgquery::PGList *
    addTargetToSortList(PGParseState *pstate, duckdb_libpgquery::PGTargetEntry *tle,
					duckdb_libpgquery::PGList *sortlist, duckdb_libpgquery::PGList *targetlist, duckdb_libpgquery::PGSortBy *sortby,
					bool resolveUnknown);
    
    bool targetIsInSortList(duckdb_libpgquery::PGTargetEntry *tle, Oid sortop, duckdb_libpgquery::PGList *sortgroupList);

    Index assignSortGroupRef(duckdb_libpgquery::PGTargetEntry *tle, duckdb_libpgquery::PGList *tlist);

    duckdb_libpgquery::PGList *
    transformGroupClause(PGParseState *pstate, duckdb_libpgquery::PGList *grouplist,
					 duckdb_libpgquery::PGList **targetlist, duckdb_libpgquery::PGList *sortClause,
					 PGParseExprKind exprKind, bool useSQL99);
    
    duckdb_libpgquery::PGList *
    findListTargetlistEntries(PGParseState *pstate, duckdb_libpgquery::PGNode *node,
									   duckdb_libpgquery::PGList **tlist, bool in_grpext,
									   bool ignore_in_grpext,
									   PGParseExprKind exprKind,
                                       bool useSQL99);

    duckdb_libpgquery::PGList *
    create_group_clause(duckdb_libpgquery::PGList *tlist_group, duckdb_libpgquery::PGList *targetlist,
					duckdb_libpgquery::PGList *sortClause, duckdb_libpgquery::PGList **tlist_remainder);
    
    duckdb_libpgquery::PGSortGroupClause *
    make_group_clause(duckdb_libpgquery::PGTargetEntry *tle, duckdb_libpgquery::PGList *targetlist,
				  Oid eqop, Oid sortop, bool nulls_first, bool hashable);
    
    duckdb_libpgquery::PGList *
    reorderGroupList(duckdb_libpgquery::PGList *grouplist);

    duckdb_libpgquery::PGList *
    transformRowExprToGroupClauses(PGParseState *pstate, duckdb_libpgquery::PGRowExpr *rowexpr,
							   duckdb_libpgquery::PGList *groupsets, duckdb_libpgquery::PGList *targetList);
    
    duckdb_libpgquery::PGList *
    transformScatterClause(PGParseState *pstate,
					   duckdb_libpgquery::PGList *scatterlist,
					   duckdb_libpgquery::PGList **targetlist);
    
    duckdb_libpgquery::PGList *
    transformDistinctToGroupBy(PGParseState *pstate, duckdb_libpgquery::PGList **targetlist,
							duckdb_libpgquery::PGList **sortClause, duckdb_libpgquery::PGList **groupClause);
    
    duckdb_libpgquery::PGList *
    transformDistinctClause(PGParseState *pstate,
						duckdb_libpgquery::PGList **targetlist, duckdb_libpgquery::PGList *sortClause, bool is_agg);

    duckdb_libpgquery::PGList *
    addTargetToGroupList(PGParseState *pstate, duckdb_libpgquery::PGTargetEntry *tle,
					 duckdb_libpgquery::PGList *grouplist, duckdb_libpgquery::PGList *targetlist, int location,
					 bool resolveUnknown);
    
    duckdb_libpgquery::PGList *
    transformDistinctOnClause(PGParseState *pstate, duckdb_libpgquery::PGList *distinctlist,
						  duckdb_libpgquery::PGList **targetlist, duckdb_libpgquery::PGList *sortClause);
    
    duckdb_libpgquery::PGNode *
    transformLimitClause(PGParseState *pstate, duckdb_libpgquery::PGNode *clause,
					 PGParseExprKind exprKind, const char *constructName);

    duckdb_libpgquery::PGList *
    transformWindowDefinitions(PGParseState *pstate,
						   duckdb_libpgquery::PGList *windowdefs,
						   duckdb_libpgquery::PGList **targetlist);
    
    duckdb_libpgquery::PGNode *
    transformFrameOffset(PGParseState *pstate, int frameOptions, duckdb_libpgquery::PGNode *clause,
					 duckdb_libpgquery::PGList *orderClause, duckdb_libpgquery::PGList *targetlist, bool isFollowing,
					 int location);
};

}