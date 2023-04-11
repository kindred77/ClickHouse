#pragma once

//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CTranslatorQueryToDXL.h
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

#include "Interpreters/orcaopt/translator/CContextQueryToDXL.h"
#include "Interpreters/orcaopt/translator/CMappingVarColId.h"
#include "Interpreters/orcaopt/translator/CTranslatorScalarToDXL.h"
#include "Interpreters/orcaopt/translator/CTranslatorUtils.h"
#include "naucrates/dxl/operators/CDXLNode.h"

// fwd declarations
namespace gpopt
{
class CMDAccessor;
}

// struct Query;
// struct RangeTblEntry;
// struct Const;
// struct List;
// struct CommonTableExpr;

namespace gpdxl
{
using namespace gpos;
using namespace gpopt;

typedef CHashMap<ULONG, BOOL, gpos::HashValue<ULONG>, gpos::Equals<ULONG>,
				 CleanupDelete<ULONG>, CleanupDelete<BOOL> >
	UlongBoolHashMap;

typedef CHashMapIter<INT, ULONG, gpos::HashValue<INT>, gpos::Equals<INT>,
					 CleanupDelete<INT>, CleanupDelete<ULONG> >
	IntUlongHashmapIter;

//---------------------------------------------------------------------------
//	@class:
//		CTranslatorQueryToDXL
//
//	@doc:
//		Class providing methods for translating a GPDB Query object into a
//      DXL Tree.
//
//---------------------------------------------------------------------------
class CTranslatorQueryToDXL
{
	friend class CTranslatorScalarToDXL;

	// shorthand for functions for translating DXL nodes to GPDB expressions
	typedef CDXLNode *(CTranslatorQueryToDXL::*DXLNodeToLogicalFunc)(
		const duckdb_libpgquery::PGRangeTblEntry *rte, ULONG rti, ULONG current_query_level);

	// mapping RTEKind to WCHARs
	struct SRTENameElem
	{
		duckdb_libpgquery::PGRTEKind m_rtekind;
		const WCHAR *m_rte_name;
	};

	// pair of RTEKind and its translators
	struct SRTETranslator
	{
		duckdb_libpgquery::PGRTEKind m_rtekind;
		DXLNodeToLogicalFunc dxlnode_to_logical_funct;
	};

	// mapping CmdType to WCHARs
	struct SCmdNameElem
	{
		duckdb_libpgquery::PGCmdType m_cmd_type;
		const WCHAR *m_cmd_name;
	};

	// pair of unsupported node tag and feature name
	struct SUnsupportedFeature
	{
		duckdb_libpgquery::PGNodeTag node_tag;
		const WCHAR *m_feature_name;
	};

private:
	// context for the whole query
	CContextQueryToDXL *m_context;

	// memory pool
	CMemoryPool *m_mp;

	// source system id
	CSystemId m_sysid;

	// meta data accessor
	CMDAccessor *m_md_accessor;

	// scalar translator used to convert scalar operation into DXL.
	CTranslatorScalarToDXL *m_scalar_translator;

	// holds the var to col id information mapping
	CMappingVarColId *m_var_to_colid_map;

	// query being translated
	duckdb_libpgquery::PGQuery *m_query;

	// absolute level of query being translated
	ULONG m_query_level;

	// top query is a DML
	BOOL m_is_top_query_dml;

	// this is a CTAS query
	BOOL m_is_ctas_query;

	// hash map that maintains the list of CTEs defined at a particular query level
	HMUlCTEListEntry *m_query_level_to_cte_map;

	// query output columns
	CDXLNodeArray *m_dxl_query_output_cols;

	// list of CTE producers
	CDXLNodeArray *m_dxl_cte_producers;

	// CTE producer IDs defined at the current query level
	UlongBoolHashMap *m_cteid_at_current_query_level_map;

	//ctor
	// private constructor, called from the public factory function QueryToDXLInstance
	CTranslatorQueryToDXL(
		CContextQueryToDXL *context, CMDAccessor *md_accessor,
		const CMappingVarColId *var_colid_mapping, duckdb_libpgquery::PGQuery *query,
		ULONG query_level, BOOL is_top_query_dml,
		HMUlCTEListEntry *
			query_level_to_cte_map	// hash map between query level -> list of CTEs defined at that level
	);

	// private copy ctor
	CTranslatorQueryToDXL(const CTranslatorQueryToDXL &);

	// check for unsupported node types, throws an exception if an unsupported
	// node is found
	void CheckUnsupportedNodeTypes(duckdb_libpgquery::PGQuery *query);

	// check for SIRV functions in the targetlist without a FROM clause and
	// throw an exception when found
	void CheckSirvFuncsWithoutFromClause(duckdb_libpgquery::PGQuery *query);

	// check for SIRV functions in the tree rooted at the given node
	BOOL HasSirvFunctions(duckdb_libpgquery::PGNode *node) const;

	// translate FromExpr (in the GPDB query) into a CDXLLogicalJoin or CDXLLogicalGet
	CDXLNode *TranslateFromExprToDXL(duckdb_libpgquery::PGFromExpr *from_expr);

	// translate set operations
	CDXLNode *TranslateSetOpToDXL(duckdb_libpgquery::PGNode *setop_node, duckdb_libpgquery::PGList *target_list,
								  IntToUlongMap *output_attno_to_colid_mapping);

	// create the set operation given its children, input and output columns
	CDXLNode *CreateDXLSetOpFromColumns(EdxlSetOpType setop_type,
										duckdb_libpgquery::PGList *output_target_list,
										ULongPtrArray *output_colids,
										ULongPtr2dArray *input_colids,
										CDXLNodeArray *children_dxlnodes,
										BOOL is_cast_across_input,
										BOOL keep_res_junked) const;

	// check if the set operation need to cast any of its input columns
	BOOL SetOpNeedsCast(duckdb_libpgquery::PGList *target_list, IMdIdArray *input_col_mdids) const;
	// translate a window operator
	CDXLNode *TranslateWindowToDXL(
		CDXLNode *child_dxlnode, duckdb_libpgquery::PGList *target_list, duckdb_libpgquery::PGList *window_clause,
		duckdb_libpgquery::PGList *sort_clause, IntToUlongMap *sort_col_attno_to_colid_mapping,
		IntToUlongMap *output_attno_to_colid_mapping);

	// translate window spec
	CDXLWindowSpecArray *TranslateWindowSpecToDXL(
		duckdb_libpgquery::PGList *window_clause, IntToUlongMap *sort_col_attno_to_colid_mapping,
		CDXLNode *project_list_dxlnode_node);

	// update window spec positions of LEAD/LAG functions
	void UpdateLeadLagWinSpecPos(
		CDXLNode *project_list_dxlnode,
		CDXLWindowSpecArray *window_specs_dxlnode) const;

	// manufacture window frame for lead/lag functions
	CDXLWindowFrame *CreateWindowFramForLeadLag(BOOL is_lead_func,
												CDXLNode *dxl_offset) const;

	// translate the child of a set operation
	CDXLNode *TranslateSetOpChild(Node *child_node, ULongPtrArray *pdrgpul,
								  IMdIdArray *input_col_mdids,
								  duckdb_libpgquery::PGList *target_list);

	// return a dummy const table get
	CDXLNode *DXLDummyConstTableGet() const;

	// translate an Expr into CDXLNode
	CDXLNode *TranslateExprToDXL(duckdb_libpgquery::PGExpr *expr);

	// translate the JoinExpr (inside FromExpr) into a CDXLLogicalJoin node
	CDXLNode *TranslateJoinExprInFromToDXL(duckdb_libpgquery::PGJoinExpr *join_expr);

	// construct a group by node for a set of grouping columns
	CDXLNode *CreateSimpleGroupBy(
		duckdb_libpgquery::PGList *target_list, duckdb_libpgquery::PGList *group_clause, CBitSet *bitset, BOOL has_aggs,
		BOOL has_grouping_sets,	 // is this GB part of a GS query
		CDXLNode *child_dxlnode,
		IntToUlongMap *phmiulSortGrpColsColId,	// mapping sortgroupref -> ColId
		IntToUlongMap
			*child_attno_colid_mapping,	 // mapping attno->colid in child node
		IntToUlongMap *
			output_attno_to_colid_mapping  // mapping attno -> ColId for output columns
	);

	// check if the argument of a DQA has already being used by another DQA
	static BOOL IsDuplicateDqaArg(duckdb_libpgquery::PGList *dqa_list, duckdb_libpgquery::PGAggref *aggref);

	// translate a query with grouping sets
	CDXLNode *TranslateGroupingSets(
		duckdb_libpgquery::PGFromExpr *from_expr, duckdb_libpgquery::PGList *target_list, duckdb_libpgquery::PGList *group_clause,
		BOOL has_aggs, IntToUlongMap *phmiulSortGrpColsColId,
		IntToUlongMap *output_attno_to_colid_mapping);

	// expand the grouping sets into a union all operator
	CDXLNode *CreateDXLUnionAllForGroupingSets(
		duckdb_libpgquery::PGFromExpr *from_expr, duckdb_libpgquery::PGList *target_list, duckdb_libpgquery::PGList *group_clause,
		BOOL has_aggs, CBitSetArray *pdrgpbsGroupingSets,
		IntToUlongMap *phmiulSortGrpColsColId,
		IntToUlongMap *output_attno_to_colid_mapping,
		UlongToUlongMap *
			grpcol_index_to_colid_mapping  // mapping pos->unique grouping columns for grouping func arguments
	);

	// construct a project node with NULL values for columns not included in the grouping set
	CDXLNode *CreateDXLProjectNullsForGroupingSets(
		duckdb_libpgquery::PGList *target_list, CDXLNode *child_dxlnode, CBitSet *bitset,
		IntToUlongMap *sort_grouping_col_mapping,
		IntToUlongMap *output_attno_to_colid_mapping,
		UlongToUlongMap *grpcol_index_to_colid_mapping) const;

	// construct a project node with appropriate values for the grouping funcs in the given target list
	CDXLNode *CreateDXLProjectGroupingFuncs(
		duckdb_libpgquery::PGList *target_list, CDXLNode *child_dxlnode, CBitSet *bitset,
		IntToUlongMap *output_attno_to_colid_mapping,
		UlongToUlongMap *grpcol_index_to_colid_mapping,
		IntToUlongMap *sort_grpref_to_colid_mapping) const;

	// add sorting and grouping column into the hash map
	void AddSortingGroupingColumn(duckdb_libpgquery::PGTargetEntry *target_entry,
								  IntToUlongMap *phmiulSortGrpColsColId,
								  ULONG colid) const;

	// translate the list of sorting columns
	CDXLNodeArray *TranslateSortColumsToDXL(
		duckdb_libpgquery::PGList *sort_clause, IntToUlongMap *col_attno_colid_mapping) const;

	// translate the list of partition-by column identifiers
	ULongPtrArray *TranslatePartColumns(
		duckdb_libpgquery::PGList *sort_clause, IntToUlongMap *col_attno_colid_mapping) const;

	CDXLNode *TranslateLimitToDXLGroupBy(
		duckdb_libpgquery::PGList *plsortcl,			  // list of sort clauses
		duckdb_libpgquery::PGNode *limit_count,		  // query node representing the limit count
		duckdb_libpgquery::PGNode *limit_offset_node,  // query node representing the limit offset
		CDXLNode *dxlnode,		  // the dxl node representing the subtree
		IntToUlongMap *
			grpcols_to_colid_mapping  // the mapping between the position in the TargetList to the ColId
	);

	// throws an exception when RTE kind not yet supported
	void UnsupportedRTEKind(duckdb_libpgquery::PGRTEKind rtekind) const;

	// translate an entry of the from clause (this can either be FromExpr or JoinExpr)
	CDXLNode *TranslateFromClauseToDXL(duckdb_libpgquery::PGNode *node);

	// translate the target list entries of the query into a logical project
	CDXLNode *TranslateTargetListToDXLProject(
		duckdb_libpgquery::PGList *target_list, CDXLNode *child_dxlnode,
		IntToUlongMap *group_col_to_colid_mapping,
		IntToUlongMap *output_attno_to_colid_mapping, duckdb_libpgquery::PGList *group_clause,
		BOOL is_aggref_expanded = false);

	// translate a target list entry or a join alias entry into a project element
	CDXLNode *TranslateExprToDXLProject(duckdb_libpgquery::PGExpr *expr, const CHAR *alias_name,
										BOOL insist_new_colids = false);

	// translate a CTE into a DXL logical CTE operator
	CDXLNode *TranslateCTEToDXL(const duckdb_libpgquery::PGRangeTblEntry *rte, ULONG rti,
								ULONG current_query_level);

	// translate a base table range table entry into a logical get
	CDXLNode *TranslateRTEToDXLLogicalGet(const duckdb_libpgquery::PGRangeTblEntry *rte, ULONG rti,
										  ULONG	 //current_query_level
	);

	void NoteDistributionPolicyOpclasses(const duckdb_libpgquery::PGRangeTblEntry *rte);

	// generate a DXL node from column values, where each column value is
	// either a datum or scalar expression represented as a project element.
	CDXLNode *TranslateColumnValuesToDXL(
		CDXLDatumArray *dxl_datum_array,
		CDXLColDescrArray *dxl_column_descriptors,
		CDXLNodeArray *dxl_project_elements) const;

	// translate a value scan range table entry
	CDXLNode *TranslateValueScanRTEToDXL(const duckdb_libpgquery::PGRangeTblEntry *rte, ULONG rti,
										 ULONG	//current_query_level
	);

	// create a dxl node from a array of datums and project elements
	CDXLNode *TranslateTVFToDXL(const duckdb_libpgquery::PGRangeTblEntry *rte, ULONG rti,
								ULONG  //current_query_level
	);

	// translate a derived table into a DXL logical operator
	CDXLNode *TranslateDerivedTablesToDXL(const duckdb_libpgquery::PGRangeTblEntry *rte, ULONG rti,
										  ULONG current_query_level);

	// create a DXL node representing the scalar constant "true"
	CDXLNode *CreateDXLConstValueTrue();

	// store mapping attno->colid
	void StoreAttnoColIdMapping(IntToUlongMap *attno_to_colid_mapping,
								INT attno, ULONG colid) const;

	// construct an array of output columns
	CDXLNodeArray *CreateDXLOutputCols(
		duckdb_libpgquery::PGList *target_list, IntToUlongMap *attno_to_colid_mapping) const;

	// check for support command types, throws an exception when command type not yet supported
	void CheckSupportedCmdType(duckdb_libpgquery::PGQuery *query);

	// check for supported range table entries, throws an exception when something is not yet supported
	void CheckRangeTable(duckdb_libpgquery::PGQuery *query);

	// translate a select-project-join expression into DXL
	CDXLNode *TranslateSelectProjectJoinToDXL(
		duckdb_libpgquery::PGList *target_list, duckdb_libpgquery::PGFromExpr *from_expr,
		IntToUlongMap *sort_group_attno_to_colid_mapping,
		IntToUlongMap *output_attno_to_colid_mapping, duckdb_libpgquery::PGList *group_clause);

	// translate a select-project-join expression into DXL and keep variables appearing
	// in aggregates and grouping columns in the output column map
	CDXLNode *TranslateSelectProjectJoinForGrpSetsToDXL(
		duckdb_libpgquery::PGList *target_list, duckdb_libpgquery::PGFromExpr *from_expr,
		IntToUlongMap *sort_group_attno_to_colid_mapping,
		IntToUlongMap *output_attno_to_colid_mapping, duckdb_libpgquery::PGList *group_clause);

	// helper to check if OID is included in given array of OIDs
	static BOOL OIDFound(Oid oid, const Oid oids[], ULONG size);

	// check if given operator is lead() window function
	static BOOL IsLeadWindowFunc(CDXLOperator *dxlop);

	// check if given operator is lag() window function
	static BOOL IsLagWindowFunc(CDXLOperator *dxlop);

	// translate an insert query
	CDXLNode *TranslateInsertQueryToDXL();

	// translate a delete query
	CDXLNode *TranslateDeleteQueryToDXL();

	// translate an update query
	CDXLNode *TranslateUpdateQueryToDXL();

	// translate a CTAS query
	CDXLNode *TranslateCTASToDXL();
	// translate CTAS storage options
	CDXLCtasStorageOptions::CDXLCtasOptionArray *GetDXLCtasOptionArray(
		List *options, IMDRelation::Erelstoragetype *storage_type);

	// extract storage option value from defelem
	CWStringDynamic *ExtractStorageOptionStr(duckdb_libpgquery::PGDefElem *def_elem);

	// return resno -> colId mapping of columns to be updated
	IntToUlongMap *UpdatedColumnMapping();

	// obtain the ids of the ctid and segmentid columns for the target
	// table of a DML query
	void GetCtidAndSegmentId(ULONG *ctid, ULONG *segment_id);

	// obtain the column id for the tuple oid column of the target table
	// of a DML statement
	ULONG GetTupleOidColId();

	// translate a grouping func expression
	CDXLNode *TranslateGroupingFuncToDXL(
		const duckdb_libpgquery::PGExpr *expr, CBitSet *bitset,
		UlongToUlongMap *grpcol_index_to_colid_mapping) const;

	// construct a list of CTE producers from the query's CTE list
	void ConstructCTEProducerList(duckdb_libpgquery::PGList *cte_list, ULONG query_level);

	// construct a stack of CTE anchors for each CTE producer in the given array
	void ConstructCTEAnchors(CDXLNodeArray *dxlnodes,
							 CDXLNode **dxl_cte_anchor_top,
							 CDXLNode **dxl_cte_anchor_bottom);

	// generate an array of new column ids of the given size
	ULongPtrArray *GenerateColIds(CMemoryPool *mp, ULONG size) const;

	// extract an array of colids from the given column mapping
	ULongPtrArray *ExtractColIds(CMemoryPool *mp,
								 IntToUlongMap *attno_to_colid_mapping) const;

	// construct a new mapping based on the given one by replacing the colid in the "From" list
	// with the colid at the same position in the "To" list
	IntToUlongMap *RemapColIds(CMemoryPool *mp,
							   IntToUlongMap *attno_to_colid_mapping,
							   ULongPtrArray *from_list_colids,
							   ULongPtrArray *to_list_colids) const;

	// true iff this query or one of its ancestors is a DML query
	BOOL IsDMLQuery();

public:
	// dtor
	~CTranslatorQueryToDXL();

	// query object
	const duckdb_libpgquery::PGQuery *
	Pquery() const
	{
		return m_query;
	}

	// does query have distributed tables
	BOOL
	HasDistributedTables() const
	{
		return m_context->m_has_distributed_tables;
	}

	// does query have distributed tables
	DistributionHashOpsKind
	GetDistributionHashOpsKind() const
	{
		return m_context->m_distribution_hashops;
	}

	// main translation routine for Query -> DXL tree
	CDXLNode *TranslateSelectQueryToDXL();

	// main driver
	CDXLNode *TranslateQueryToDXL();

	// return the list of output columns
	CDXLNodeArray *GetQueryOutputCols() const;

	// return the list of CTEs
	CDXLNodeArray *GetCTEs() const;

	// factory function
	static CTranslatorQueryToDXL *QueryToDXLInstance(CMemoryPool *mp,
													 CMDAccessor *md_accessor,
													 duckdb_libpgquery::PGQuery *query);
};
}  // namespace gpdxl

//EOF
