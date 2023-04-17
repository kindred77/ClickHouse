#pragma once

#include <Interpreters/orcaopt/parser_common.h>

//#define MakeNode(_type_) ((_type_ *) NewNode(sizeof(_type_), T_##_type_))

#define ForEachWithCount(cell, list, counter)                          \
	for ((cell) = ListHead(list), (counter) = 0; (cell) != NULL; \
		 (cell) = lnext(cell), ++(counter))

namespace gpos
{
	typedef uint32_t ULONG;
};

namespace gpdxl
{

duckdb_libpgquery::PGListCell *
ListHead(duckdb_libpgquery::PGList *l);

duckdb_libpgquery::PGList *
LAppendInt(duckdb_libpgquery::PGList *list, int iDatum);

int
FindNodes(duckdb_libpgquery::PGNode *node, duckdb_libpgquery::PGList *nodeTags);

uint32
ListLength(duckdb_libpgquery::PGList *l);

void *
ListNth(duckdb_libpgquery::PGList *list, int n);

duckdb_libpgquery::PGList *
LAppend(duckdb_libpgquery::PGList *list, void *datum);

void
ListFree(duckdb_libpgquery::PGList *list);

void *
CopyObject(void *from);

void
GPDBFree(void *ptr);

// bool
// IndexExists(Oid oid);

bool
TypeExists(Oid oid);

bool
RelationExists(Oid oid);

bool
OperatorExists(Oid oid);

bool
AggregateExists(Oid oid);

bool
FunctionExists(Oid oid);

std::string
NodeToString(duckdb_libpgquery::PGNode *obj);

duckdb_libpgquery::PGNode *
StringToNode(std::string& str);

duckdb_libpgquery::PGExpr *
TransformArrayConstToArrayExpr(duckdb_libpgquery::PGConst *c);

int32
ExprTypeMod(duckdb_libpgquery::PGNode *expr);

Oid
ExprType(duckdb_libpgquery::PGNode *expr);

bool
Equals(void *p1, void *p2);

duckdb_libpgquery::PGNode *
MakeBoolConst(bool value, bool isnull);

int
CheckCollation(duckdb_libpgquery::PGNode *node);

duckdb_libpgquery::PGList *
ExtractNodesExpression(duckdb_libpgquery::PGNode *node, int node_tag,
							 bool descend_into_subqueries);

duckdb_libpgquery::PGTargetEntry *
FindFirstMatchingMemberInTargetList(duckdb_libpgquery::PGNode *node, duckdb_libpgquery::PGList *targetlist);

bool
WalkExpressionTree(duckdb_libpgquery::PGNode *node, bool (*walker)(duckdb_libpgquery::PGNode *node, void *context), void *context);

PGLogicalIndexType
GetLogicalIndexType(Oid index_oid);

bool
RelPartIsNone(Oid relid);

bool
IsLeafPartition(Oid oid);

bool
RelPartIsRoot(Oid relid);

PGRelationPtr
GetRelation(Oid rel_oid);

void
CloseRelation(PGRelationPtr rel);

duckdb_libpgquery::PGList *
GetRelationIndexes(PGRelationPtr relation);

PGLogicalIndexes *
GetLogicalPartIndexes(Oid oid);

Oid
GetOpclassFamily(Oid opclass);

Oid
GetHashProcInOpfamily(Oid opfamily, Oid typid);

Oid
IsLegacyCdbHashFunction(Oid funcid);

Oid
GetDefaultDistributionOpclassForType(Oid typid);

duckdb_libpgquery::PGList *
ListConcat(duckdb_libpgquery::PGList *list1, duckdb_libpgquery::PGList *list2);

int
GetIntFromValue(duckdb_libpgquery::PGNode *node);

duckdb_libpgquery::PGList *
GetCheckConstraintOids(Oid rel_oid);

bool
RelPartIsInterior(Oid relid);

duckdb_libpgquery::PGList *
GetPartitionAttrs(Oid oid);

bool
HasSubclassSlow(Oid rel_oid);

bool
IsMultilevelPartitionUniform(Oid root_oid);

bool
IsChildPartDistributionMismatched(PGRelationPtr rel);

gpos::ULONG
CountLeafPartTables(Oid rel_oid);

bool
HasExternalPartition(Oid oid);

PGExtTableEntry *
GetExternalTableEntry(Oid rel_oid);

PGStatisticPtr
GetAttStats(Oid relid, PGAttrNumber attnum);

duckdb_libpgquery::PGNode *
GetTypeDefault(Oid typid);

duckdb_libpgquery::PGList *
GetExternalPartitions(Oid oid);

Oid
GetAggregate(const char *agg, Oid type_oid, int nargs);

bool
IsAppendOnlyPartitionTable(Oid root_oid);

duckdb_libpgquery::PGNode *
MutateQueryOrExpressionTree(duckdb_libpgquery::PGNode * node,
	duckdb_libpgquery::PGNode * (*mutator)(duckdb_libpgquery::PGNode *node, void *context),
	void * context, int flags);

duckdb_libpgquery::PGQuery *
MutateQueryTree(duckdb_libpgquery::PGQuery *query,
	duckdb_libpgquery::PGNode *(*mutator)(duckdb_libpgquery::PGNode *node, void *context),
	void *context, int flags);

duckdb_libpgquery::PGNode *
MutateExpressionTree(duckdb_libpgquery::PGNode *node,
	duckdb_libpgquery::PGNode *(*mutator)(duckdb_libpgquery::PGNode *node, void *context), void *context);

bool
WalkQueryOrExpressionTree(duckdb_libpgquery::PGNode *node, bool (*walker)(duckdb_libpgquery::PGNode *node, void *context), void *context,
								int flags);

duckdb_libpgquery::PGTargetEntry *
MakeTargetEntry(duckdb_libpgquery::PGExpr *expr, PGAttrNumber resno, char *resname, bool resjunk);

duckdb_libpgquery::PGVar *
MakeVar(PGIndex varno, PGAttrNumber varattno, Oid vartype, int32 vartypmod,
			  PGIndex varlevelsup);

Oid
TypeCollation(Oid type);

duckdb_libpgquery::PGQuery *
FlattenJoinAliasVar(duckdb_libpgquery::PGQuery *query, gpos::ULONG query_level);

int GetGPSegmentCount(void);

PGPolicyPtr
GetDistributionPolicy(PGRelationPtr rel);

duckdb_libpgquery::PGNode *
GetRelationPartContraints(Oid rel_oid, duckdb_libpgquery::PGList **default_levels);

Oid GetInverseOp(Oid opno);

bool IsOpHashJoinable(Oid opno, Oid inputtype);

bool IsOpMergeJoinable(Oid opno, Oid inputtype);

bool IsCompositeType(Oid typid);

bool IsTextRelatedType(Oid typid);

PGTypePtr LookupTypeCache(Oid type_id, int flags);

Oid GetTypeRelid(Oid typid);

Oid GetArrayType(Oid typid);

Oid GetDefaultDistributionOpfamilyForType(Oid typid);

Oid GetLegacyCdbHashOpclassForBaseType(Oid typid);

char * GetOpName(Oid opno);

void GetOpInputTypes(Oid opno, Oid *lefttype, Oid *righttype);

unsigned int GetComparisonType(Oid op_oid);

Oid GetOpFunc(Oid opno);

Oid GetFuncRetType(Oid funcid);

Oid GetCommutatorOp(Oid opno);

bool IsOpStrict(Oid opno);

bool IsOpNDVPreserving(Oid opno);

Oid GetCompatibleHashOpFamily(Oid opno);

Oid GetCompatibleLegacyHashOpFamily(Oid opno);

char FuncStability(Oid funcid);

char FuncDataAccess(Oid funcid);

char FuncExecLocation(Oid funcid);

bool GetFuncRetset(Oid funcid);

bool FuncStrict(Oid funcid);

bool IsFuncNDVPreserving(Oid funcid);

bool IsFuncAllowedForPartitionSelection(Oid funcid);

char * GetFuncName(Oid funcid);

duckdb_libpgquery::PGList * GetFuncOutputArgTypes(Oid funcid);

bool IsOrderedAgg(Oid aggid);

bool IsAggPartialCapable(Oid aggid);

char * GetTypeName(Oid typid);

Oid GetAggIntermediateResultType(Oid aggid);

double CdbEstimatePartitionedNumTuples(PGRelationPtr rel);

bool GetAttrStatsSlot(PGAttStatsSlot& sslot,
	PGStatisticPtr statstuple, int reqkind, Oid reqop, int flags);

void FreeAttrStatsSlot(PGAttStatsSlotPtr sslot);

bool GetCastFunc(Oid src_oid, Oid dest_oid, bool *is_binary_coercible,
				  Oid *cast_fn_oid, PGCoercionPathType *pathtype);

Oid GetComparisonOperator(Oid left_oid, Oid right_oid, unsigned int cmpt);

void GetOrderedPartKeysAndKinds(Oid oid, duckdb_libpgquery::PGList **pkeys, duckdb_libpgquery::PGList **pkinds);

duckdb_libpgquery::PGList * GetRelationKeys(Oid relid);

size_t DatumSize(Datum value, bool type_by_val, int iTypLen);

bool BoolFromDatum(Datum d);

int16 Int16FromDatum(Datum d);

int32 Int32FromDatum(Datum d);

int64 Int64FromDatum(Datum d);

bool NumericIsNan(Numeric num);

}
