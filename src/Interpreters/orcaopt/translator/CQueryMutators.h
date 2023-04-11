#pragma once

//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CQueryMutators.h
//
//	@doc:
//		Class providing methods for translating a GPDB Query object into a
//		DXL Tree
//
//	@test:
//
//
//---------------------------------------------------------------------------
#include <Interpreters/orcaopt/parser_common.h>

#include "gpos/base.h"

#include "Interpreters/orcaopt/translator/CMappingVarColId.h"
#include "Interpreters/orcaopt/translator/CTranslatorScalarToDXL.h"
#include "Interpreters/orcaopt/translator/CTranslatorUtils.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/md/IMDType.h"

// fwd declarations
namespace gpopt
{
class CMDAccessor;
}

// struct Query;
// struct RangeTblEntry;
// struct Const;
// struct List;


namespace gpdxl
{
//---------------------------------------------------------------------------
//	@class:
//		CQueryMutators
//
//	@doc:
//		Class providing methods for translating a GPDB Query object into a
//      DXL Tree.
//
//---------------------------------------------------------------------------
class CQueryMutators
{
	typedef duckdb_libpgquery::PGNode *(*MutatorWalkerFn)();
	typedef BOOL (*ExprWalkerFn)();

	typedef struct SContextGrpbyPlMutator
	{
	public:
		// memory pool
		CMemoryPool *m_mp;

		// MD accessor to get the function name
		CMDAccessor *m_mda;

		// original query
		duckdb_libpgquery::PGQuery *m_query;

		// the new target list of the group by or window (lower) query
		duckdb_libpgquery::PGList *m_lower_table_tlist;

		// the current query level
		ULONG m_current_query_level;

		// indicate the levels up of the aggregate we are mutating
		ULONG m_agg_levels_up;

		// indicate whether we are mutating the argument of an aggregate
		BOOL m_is_mutating_agg_arg;

		// ctor
		SContextGrpbyPlMutator(CMemoryPool *mp, CMDAccessor *mda, duckdb_libpgquery::PGQuery *query,
							   duckdb_libpgquery::PGList *derived_table_tlist)
			: m_mp(mp),
			  m_mda(mda),
			  m_query(query),
			  m_lower_table_tlist(derived_table_tlist),
			  m_current_query_level(0),
			  m_agg_levels_up(gpos::ulong_max),
			  m_is_mutating_agg_arg(false)
		{
		}

		// dtor
		~SContextGrpbyPlMutator()
		{
		}

	} CContextGrpbyPlMutator;

	typedef struct SContextIncLevelsupMutator
	{
	public:
		// the current query level
		ULONG m_current_query_level;

		// fix target list entry of the top level
		BOOL m_should_fix_top_level_target_list;

		// ctor
		SContextIncLevelsupMutator(ULONG current_query_level,
								   BOOL should_fix_top_level_target_list)
			: m_current_query_level(current_query_level),
			  m_should_fix_top_level_target_list(
				  should_fix_top_level_target_list)
		{
		}

		// dtor
		~SContextIncLevelsupMutator()
		{
		}

	} CContextIncLevelsupMutator;

	// context for walker that iterates over the expression in the target entry
	typedef struct SContextTLWalker
	{
	public:
		// list of target list entries in the query
		duckdb_libpgquery::PGList *m_target_entries;

		// list of grouping clauses
		duckdb_libpgquery::PGList *m_group_clause;

		// ctor
		SContextTLWalker(duckdb_libpgquery::PGList *target_entries, duckdb_libpgquery::PGList *group_clause)
			: m_target_entries(target_entries), m_group_clause(group_clause)
		{
		}

		// dtor
		~SContextTLWalker()
		{
		}

	} CContextTLWalker;

public:
	// fall back during since the target list refers to a attribute which algebrizer at this point cannot resolve
	static BOOL ShouldFallback(duckdb_libpgquery::PGNode *node, SContextTLWalker *context);

	// check if the project list contains expressions on aggregates thereby needing normalization
	static BOOL NeedsProjListNormalization(const duckdb_libpgquery::PGQuery *query);

	// normalize query
	static duckdb_libpgquery::PGQuery *NormalizeQuery(CMemoryPool *mp, CMDAccessor *md_accessor,
								 const duckdb_libpgquery::PGQuery *query, ULONG query_level);

	// check if the project list contains expressions on window operators thereby needing normalization
	static BOOL NeedsProjListWindowNormalization(const duckdb_libpgquery::PGQuery *query);

	// flatten expressions in window operation project list
	static duckdb_libpgquery::PGQuery *NormalizeWindowProjList(CMemoryPool *mp,
										  CMDAccessor *md_accessor,
										  const duckdb_libpgquery::PGQuery *original_query);

	// traverse the project list to extract all window functions in an arbitrarily complex project element
	static duckdb_libpgquery::PGNode *RunWindowProjListMutator(duckdb_libpgquery::PGNode *node,
										  SContextGrpbyPlMutator *context);

	// flatten expressions in project list
	static duckdb_libpgquery::PGQuery *NormalizeGroupByProjList(CMemoryPool *mp,
										   CMDAccessor *md_accessor,
										   const duckdb_libpgquery::PGQuery *query);

	// make a copy of the aggref (minus the arguments)
	static duckdb_libpgquery::PGAggref *FlatCopyAggref(duckdb_libpgquery::PGAggref *aggref);

	// create a new entry in the derived table and return its corresponding var
	static duckdb_libpgquery::PGVar *MakeVarInDerivedTable(duckdb_libpgquery::PGNode *node,
									  SContextGrpbyPlMutator *context);

	// check if a matching node exists in the list of target entries
	static duckdb_libpgquery::PGNode *FindNodeInGroupByTargetList(duckdb_libpgquery::PGNode *node,
											 SContextGrpbyPlMutator *context);

	// increment the levels up of outer references
	static duckdb_libpgquery::PGVar *IncrLevelsUpIfOuterRef(duckdb_libpgquery::PGVar *var);

	// pull up having clause into a select
	static duckdb_libpgquery::PGQuery *NormalizeHaving(CMemoryPool *mp, CMDAccessor *md_accessor,
								  const duckdb_libpgquery::PGQuery *query);

	// traverse the expression and fix the levels up of any outer reference
	static Node *RunIncrLevelsUpMutator(duckdb_libpgquery::PGNode *node,
										SContextIncLevelsupMutator *context);

	// traverse the expression and fix the levels up of any CTE
	static BOOL RunFixCTELevelsUpWalker(duckdb_libpgquery::PGNode *node,
										SContextIncLevelsupMutator *context);

	// mutate the grouping columns, fix levels up when necessary
	static duckdb_libpgquery::PGNode *RunGroupingColMutator(duckdb_libpgquery::PGNode *node,
									   SContextGrpbyPlMutator *context);

	// fix the level up of grouping columns when necessary
	static duckdb_libpgquery::PGNode *FixGroupingCols(duckdb_libpgquery::PGNode *node, duckdb_libpgquery::PGTargetEntry *original,
								 SContextGrpbyPlMutator *context);

	// return a target entry for the aggregate expression
	static duckdb_libpgquery::PGTargetEntry *GetTargetEntryForAggExpr(CMemoryPool *mp,
												 CMDAccessor *md_accessor,
												 duckdb_libpgquery::PGNode *node, ULONG attno);

	// traverse the having qual to extract all aggregate functions,
	// fix correlated vars and return the modified having qual
	static duckdb_libpgquery::PGNode *RunExtractAggregatesMutator(duckdb_libpgquery::PGNode *node,
											 SContextGrpbyPlMutator *context);

	// for a given an TE in the derived table, create a new TE to be added to the top level query
	static duckdb_libpgquery::PGTargetEntry *MakeTopLevelTargetEntry(duckdb_libpgquery::PGTargetEntry *target_entry,
												ULONG attno);

	// return the column name of the target entry
	static CHAR *GetTargetEntryColName(duckdb_libpgquery::PGTargetEntry *target_entry, duckdb_libpgquery::PGQuery *query);

	// make the input query into a derived table and return a new root query
	static duckdb_libpgquery::PGQuery *ConvertToDerivedTable(const duckdb_libpgquery::PGQuery *original_query,
										BOOL should_fix_target_list,
										BOOL should_fix_having_qual);

	// eliminate distinct clause
	static duckdb_libpgquery::PGQuery *EliminateDistinctClause(const duckdb_libpgquery::PGQuery *query);

	// reassign the sorting clause from the derived table to the new top-level query
	static void ReassignSortClause(duckdb_libpgquery::PGQuery *top_level_query,
								   duckdb_libpgquery::PGQuery *derive_table_query);
};
}  // namespace gpdxl

//EOF
