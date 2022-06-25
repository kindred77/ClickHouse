#pragma once

#include "gpos/base.h"

#include <Parsers/ASTTablesInSelectQuery.h>

namespace DB
{

enum EPlStmtPhysicalOpType
{
	EpspotTblScan,
	EpspotHashjoin,
	EpspotNLJoin,
	EpspotMergeJoin,
	EpspotMotion,
	EpspotLimit,
	EpspotAgg,
	EpspotWindow,
	EpspotSort,
	EpspotSubqueryScan,
	EpspotAppend,
	EpspotResult,
	EpspotMaterialize,
	EpspotSharedScan,
	EpspotIndexScan,
	EpspotIndexOnlyScan,
	EpspotNone
};

class MappingVarColId
{
private:
	// memory pool
	gpos::CMemoryPool *m_mp;

	// hash map structure to store gpdb att -> opt col information
	typedef CHashMap<GPDBAttInfo, GPDBAttOptCol, HashGPDBAttInfo,
					 EqualGPDBAttInfo, CleanupRelease, CleanupRelease>
		GPDBAttOptColHashMap;

	// iterator
	typedef CHashMapIter<GPDBAttInfo, GPDBAttOptCol, HashGPDBAttInfo,
						 EqualGPDBAttInfo, CleanupRelease, CleanupRelease>
		GPDBAttOptColHashMapIter;

	// map from gpdb att to optimizer col
	GPDBAttOptColHashMap *m_gpdb_att_opt_col_mapping;

	// hash map structure to store gpdb att -> opt col information
	typedef CHashMap<CKDBAttInfo, CKDBAttOptCol, HashCKDBAttInfo,
					 EqualGCKBAttInfo, CleanupRelease, CleanupRelease>
		CKDBAttOptColHashMap;

	// iterator
	typedef CHashMapIter<CKDBAttInfo, CKDBAttOptCol, HashCKDBAttInfo,
						 EqualCKDBAttInfo, CleanupRelease, CleanupRelease>
		CKDBAttOptColHashMapIter;

	// map from gpdb att to optimizer col
	CKDBAttOptColHashMap *m_ckdb_att_opt_col_mapping;
	std::vector<CKDBAttOptColHashMap> m_ckdb_arr;

	bool searchFor(
		const ULONG current_query_level,
		const ASTPtr var_exp,
		ULONG & abs_query_level,
		std::shared_ptr<CKDBAttInfo> & find_key,
		std::shared_ptr<CKDBAttOptCol> & find_value
	);

	// insert mapping entry
	void Insert(ULONG, ULONG, INT, ULONG, gpos::CWStringBase *str);

	void Insert(ULONG, String * database_name, 
		String * table_name, ULONG,
		gpos::CWStringBase *str, OID type_oid, int typemod, int attno);

	// no copy constructor
	MappingVarColId(const MappingVarColId &);

	// helper function to access mapping
	const GPDBAttOptCol *GetGPDBAttOptColMapping(
		ULONG current_query_level, const Var *var,
		EPlStmtPhysicalOpType plstmt_physical_op_type) const;

	const GPDBAttOptCol *GetCKDBAttOptColMapping(
		ULONG current_query_level, const ASTPtr *var_exp
		/*EPlStmtPhysicalOpType plstmt_physical_op_type*/) const;

public:
	// ctor
	explicit MappingVarColId(gpos::CMemoryPool *);

	// dtor
	virtual ~MappingVarColId()
	{
		m_gpdb_att_opt_col_mapping->Release();
	}

	// given a gpdb attribute, return a column name in optimizer world
	virtual const gpos::CWStringBase *GetOptColName(
		ULONG current_query_level, const Var *var,
		EPlStmtPhysicalOpType plstmt_physical_op_type) const;

	virtual const gpos::CWStringBase *GetOptColName(
		ULONG current_query_level, const ASTPtr var_exp,
		EPlStmtPhysicalOpType plstmt_physical_op_type) const;

	// given a gpdb attribute, return column id
	virtual ULONG GetColId(ULONG current_query_level, const Var *var,
						   EPlStmtPhysicalOpType plstmt_physical_op_type) const;

	virtual ULONG GetColId(ULONG current_query_level, const ASTPtr *var_exp,
						   EPlStmtPhysicalOpType plstmt_physical_op_type) const;

	bool GetColInfo(
		ULONG current_query_level, const ASTPtr *var_exp,
		//EPlStmtPhysicalOpType plstmt_physical_op_type,
		gpos::CWStringBase & col_name,
		ULONG & col_id,
		Oid & type_oid,
		int & typemod,
		int & attno
	);

	// load up mapping information from an index
	void LoadIndexColumns(ULONG query_level, ULONG RTE_index,
						  const IMDIndex *index,
						  const CDXLTableDescr *table_descr);

	// load up mapping information from table descriptor
	void LoadTblColumns(ULONG query_level, ULONG RTE_index,
						const CDXLTableDescr *table_descr);

	// load up mapping information from table descriptor
	void LoadTblColumns(ULONG query_level, const ASTTableExpression * table_expression,
						const CDXLTableDescr *table_descr);

	// load up column id mapping information from the array of column descriptors
	void LoadColumns(ULONG query_level, ULONG RTE_index,
					 const CDXLColDescrArray *column_descrs);

	// load up mapping information from derived table columns
	void LoadDerivedTblColumns(ULONG query_level, ULONG RTE_index,
							   const CDXLNodeArray *derived_columns_dxl,
							   List *target_list);

	// load information from CTE columns
	void LoadCTEColumns(ULONG query_level, ULONG RTE_index,
						const ULongPtrArray *pdrgpulCTE, List *target_list);

	// load up mapping information from scalar projection list
	void LoadProjectElements(ULONG query_level, ULONG RTE_index,
							 const CDXLNode *project_list_dxlnode);

	// load up mapping information from list of column names
	void Load(ULONG query_level, ULONG RTE_index, CIdGenerator *id_generator,
			  List *col_names);

	// create a deep copy
	MappingVarColId *CopyMapColId(gpos::CMemoryPool *mp) const;

	// create a deep copy
	MappingVarColId *CopyMapColId(ULONG query_level) const;

	// create a copy of the mapping replacing old col ids with new ones
	MappingVarColId *CopyRemapColId(gpos::CMemoryPool *mp, ULongPtrArray *old_colids,
									 ULongPtrArray *new_colids) const;
};

}