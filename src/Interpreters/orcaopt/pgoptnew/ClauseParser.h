#pragma once

#include <Interpreters/orcaopt/pgoptnew/parser_common.h>
#include <Interpreters/orcaopt/pgoptnew/RelationParser.h>
#include <Interpreters/orcaopt/pgoptnew/SelectParser.h>
#include <Interpreters/orcaopt/pgoptnew/ExprParser.h>
#include <Interpreters/orcaopt/pgoptnew/CollationParser.h>
#include <Interpreters/orcaopt/pgoptnew/OperParser.h>
#include <Interpreters/orcaopt/pgoptnew/NodeParser.h>
//#include <Interpreters/orcaopt/pgoptnew/ScalarOperatorProvider.h>


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
    CollationParser collation_parser;
    OperParser oper_parser;
    NodeParser node_parser;
    std::shared_ptr<ScalarOperatorProvider> scalar_operator_provider = nullptr;
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
    addTargetToSortList(PGParseState *pstate, duckdb_libpgquery::PGTargetEntry *tle,
					duckdb_libpgquery::PGList *sortlist, duckdb_libpgquery::PGList *targetlist, duckdb_libpgquery::PGSortBy *sortby);

    duckdb_libpgquery::PGList *
    transformSortClause(PGParseState *pstate,
					duckdb_libpgquery::PGList *orderlist,
					duckdb_libpgquery::PGList **targetlist,
					PGParseExprKind exprKind,
					bool useSQL99);
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
};

}