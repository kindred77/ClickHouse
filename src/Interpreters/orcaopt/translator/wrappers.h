#pragma once

#include <common/parser_common.hpp>

//#define MakeNode(_type_) ((_type_ *) NewNode(sizeof(_type_), T_##_type_))

#define ForEachWithCount(cell, list, counter)                          \
	for ((cell) = ListHead(list), (counter) = 0; (cell) != NULL; \
		 (cell) = lnext(cell), ++(counter))

// #define ListMake1Int(x1) LPrependInt(x1, NIL)

#define LInitial(l) lfirst(ListHead(l))
#define LInitialOID(l) lfirst_oid(ListHead(l))

#define GpdbEreport(xerrcode, severitylevel, xerrmsg, xerrhint)       \
	GpdbEreportImpl(xerrcode, severitylevel, xerrmsg, xerrhint, \
						  __FILE__, __LINE__, __func__)

namespace gpos
{
	typedef uint32_t ULONG;
};

namespace gpdxl
{

// duckdb_libpgquery::PGList *LPrependInt(int datum, duckdb_libpgquery::PGList *list);

duckdb_libpgquery::PGListCell *
ListHead(duckdb_libpgquery::PGList *l);

duckdb_libpgquery::PGList *
LAppendInt(duckdb_libpgquery::PGList *list, int iDatum);

int
FindNodes(duckdb_libpgquery::PGNode *node, const std::vector<duckdb_libpgquery::PGNodeTag>& nodeTags);

duckdb_libpgquery::uint32
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
TypeExists(duckdb_libpgquery::PGOid oid);

bool
RelationExists(duckdb_libpgquery::PGOid oid);

bool
OperatorExists(duckdb_libpgquery::PGOid oid);

bool
AggregateExists(duckdb_libpgquery::PGOid oid);

bool
FunctionExists(duckdb_libpgquery::PGOid oid);

std::string
NodeToString(duckdb_libpgquery::PGNode *obj);

duckdb_libpgquery::PGNode *
StringToNode(std::string& str);

duckdb_libpgquery::PGExpr *
TransformArrayConstToArrayExpr(duckdb_libpgquery::PGConst *c);

duckdb_libpgquery::int32
ExprTypeMod(duckdb_libpgquery::PGNode *expr);

duckdb_libpgquery::PGOid
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

duckdb_libpgquery::PGLogicalIndexType
GetLogicalIndexType(duckdb_libpgquery::PGOid index_oid);

bool
RelPartIsNone(duckdb_libpgquery::PGOid relid);

bool
IsLeafPartition(duckdb_libpgquery::PGOid oid);

bool
RelPartIsRoot(duckdb_libpgquery::PGOid relid);

duckdb_libpgquery::PGNode* GetLeafPartContraints(duckdb_libpgquery::PGOid rel_oid, duckdb_libpgquery::PGList **default_levels);

// duckdb_libpgquery::PGRelationPtr
// GetRelation(duckdb_libpgquery::PGOid rel_oid);

// void
// CloseRelation(duckdb_libpgquery::PGRelationPtr rel);

duckdb_libpgquery::PGList *
GetRelationIndexes(duckdb_libpgquery::PGRelationPtr relation);

duckdb_libpgquery::PGLogicalIndexes *
GetLogicalPartIndexes(duckdb_libpgquery::PGOid oid);

duckdb_libpgquery::PGOid
GetOpclassFamily(duckdb_libpgquery::PGOid opclass);

duckdb_libpgquery::PGOid
GetHashProcInOpfamily(duckdb_libpgquery::PGOid opfamily, duckdb_libpgquery::PGOid typid);

duckdb_libpgquery::PGOid
IsLegacyCdbHashFunction(duckdb_libpgquery::PGOid funcid);

duckdb_libpgquery::PGOid
GetDefaultDistributionOpclassForType(duckdb_libpgquery::PGOid typid);

duckdb_libpgquery::PGList *
ListConcat(duckdb_libpgquery::PGList *list1, duckdb_libpgquery::PGList *list2);

int
GetIntFromValue(duckdb_libpgquery::PGNode *node);

duckdb_libpgquery::PGList *
GetCheckConstraintOids(duckdb_libpgquery::PGOid rel_oid);

bool
RelPartIsInterior(duckdb_libpgquery::PGOid relid);

duckdb_libpgquery::PGList *
GetPartitionAttrs(duckdb_libpgquery::PGOid oid);

bool
HasSubclassSlow(duckdb_libpgquery::PGOid rel_oid);

bool
IsMultilevelPartitionUniform(duckdb_libpgquery::PGOid root_oid);

bool
IsChildPartDistributionMismatched(duckdb_libpgquery::PGRelationPtr rel);

gpos::ULONG
CountLeafPartTables(duckdb_libpgquery::PGOid rel_oid);

bool
HasExternalPartition(duckdb_libpgquery::PGOid oid);

duckdb_libpgquery::PGExtTableEntry *
GetExternalTableEntry(duckdb_libpgquery::PGOid rel_oid);

duckdb_libpgquery::PGStatisticPtr
GetAttStats(duckdb_libpgquery::PGOid relid, duckdb_libpgquery::PGAttrNumber attnum);

duckdb_libpgquery::PGNode *
GetTypeDefault(duckdb_libpgquery::PGOid typid);

duckdb_libpgquery::PGList *
GetExternalPartitions(duckdb_libpgquery::PGOid oid);

duckdb_libpgquery::PGOid
GetAggregate(const char *agg, duckdb_libpgquery::PGOid type_oid, int nargs);

bool
IsAppendOnlyPartitionTable(duckdb_libpgquery::PGOid root_oid);

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
MakeTargetEntry(duckdb_libpgquery::PGExpr *expr, duckdb_libpgquery::PGAttrNumber resno, char *resname, bool resjunk);

duckdb_libpgquery::PGVar *
MakeVar(duckdb_libpgquery::PGIndex varno, duckdb_libpgquery::PGAttrNumber varattno, duckdb_libpgquery::PGOid vartype, duckdb_libpgquery::int32 vartypmod,
			  duckdb_libpgquery::PGIndex varlevelsup);

duckdb_libpgquery::PGValue* MakeStringValue(char *str);

duckdb_libpgquery::PGOid
TypeCollation(duckdb_libpgquery::PGOid type);

duckdb_libpgquery::PGQuery *
FlattenJoinAliasVar(duckdb_libpgquery::PGQuery *query, gpos::ULONG query_level);

int GetGPSegmentCount(void);

duckdb_libpgquery::PGPolicyPtr
GetDistributionPolicy(duckdb_libpgquery::PGRelationPtr rel);

duckdb_libpgquery::PGNode *
GetRelationPartContraints(duckdb_libpgquery::PGOid rel_oid, duckdb_libpgquery::PGList **default_levels);

duckdb_libpgquery::PGOid GetInverseOp(duckdb_libpgquery::PGOid opno);

bool IsOpHashJoinable(duckdb_libpgquery::PGOid opno, duckdb_libpgquery::PGOid inputtype);

bool IsOpMergeJoinable(duckdb_libpgquery::PGOid opno, duckdb_libpgquery::PGOid inputtype);

bool IsCompositeType(duckdb_libpgquery::PGOid typid);

bool IsTextRelatedType(duckdb_libpgquery::PGOid typid);

// duckdb_libpgquery::PGTypePtr LookupTypeCache(duckdb_libpgquery::PGOid type_id/* , int flags */);

duckdb_libpgquery::PGOid GetTypeRelid(duckdb_libpgquery::PGOid typid);

duckdb_libpgquery::PGOid GetArrayType(duckdb_libpgquery::PGOid typid);

duckdb_libpgquery::PGOid GetDefaultDistributionOpfamilyForType(duckdb_libpgquery::PGOid typid);

duckdb_libpgquery::PGOid GetLegacyCdbHashOpclassForBaseType(duckdb_libpgquery::PGOid typid);

char * GetOpName(duckdb_libpgquery::PGOid opno);

void GetOpInputTypes(duckdb_libpgquery::PGOid opno, duckdb_libpgquery::PGOid *lefttype, duckdb_libpgquery::PGOid *righttype);

unsigned int WrapperGetComparisonType(duckdb_libpgquery::PGOid op_oid);

duckdb_libpgquery::PGOid GetOpFunc(duckdb_libpgquery::PGOid opno);

duckdb_libpgquery::PGOid GetFuncRetType(duckdb_libpgquery::PGOid funcid);

duckdb_libpgquery::PGOid GetCommutatorOp(duckdb_libpgquery::PGOid opno);

bool IsOpStrict(duckdb_libpgquery::PGOid opno);

bool IsOpNDVPreserving(duckdb_libpgquery::PGOid opno);

duckdb_libpgquery::PGOid GetCompatibleHashOpFamily(duckdb_libpgquery::PGOid opno);

duckdb_libpgquery::PGOid GetCompatibleLegacyHashOpFamily(duckdb_libpgquery::PGOid opno);

char FuncStability(duckdb_libpgquery::PGOid funcid);

char FuncDataAccess(duckdb_libpgquery::PGOid funcid);

char FuncExecLocation(duckdb_libpgquery::PGOid funcid);

bool GetFuncRetset(duckdb_libpgquery::PGOid funcid);

bool FuncStrict(duckdb_libpgquery::PGOid funcid);

bool IsFuncNDVPreserving(duckdb_libpgquery::PGOid funcid);

bool IsFuncAllowedForPartitionSelection(duckdb_libpgquery::PGOid funcid);

// char * GetFuncName(duckdb_libpgquery::PGOid funcid);

duckdb_libpgquery::PGList * GetFuncOutputArgTypes(duckdb_libpgquery::PGOid funcid);

bool IsOrderedAgg(duckdb_libpgquery::PGOid aggid);

bool IsAggPartialCapable(duckdb_libpgquery::PGOid aggid);

// char * WrapperGetTypeName(duckdb_libpgquery::PGOid typid);

duckdb_libpgquery::PGOid GetAggIntermediateResultType(duckdb_libpgquery::PGOid aggid);

double CdbEstimatePartitionedNumTuples(duckdb_libpgquery::PGRelationPtr rel);

bool GetAttrStatsSlot(duckdb_libpgquery::PGAttStatsSlotPtr& sslot,
	duckdb_libpgquery::PGStatisticPtr statstuple, int reqkind, duckdb_libpgquery::PGOid reqop, int flags);

void FreeAttrStatsSlot(duckdb_libpgquery::PGAttStatsSlotPtr sslot);

bool GetCastFunc(duckdb_libpgquery::PGOid src_oid, duckdb_libpgquery::PGOid dest_oid, bool *is_binary_coercible,
				  duckdb_libpgquery::PGOid *cast_fn_oid, duckdb_libpgquery::PGCoercionPathType *pathtype);

duckdb_libpgquery::PGOid GetComparisonOperator(duckdb_libpgquery::PGOid left_oid, duckdb_libpgquery::PGOid right_oid, unsigned int cmpt);

void GetOrderedPartKeysAndKinds(duckdb_libpgquery::PGOid oid, duckdb_libpgquery::PGList **pkeys, duckdb_libpgquery::PGList **pkinds);

duckdb_libpgquery::PGList * GetRelationKeys(duckdb_libpgquery::PGOid relid);

size_t DatumSize(duckdb_libpgquery::PGDatum value, bool type_by_val, int iTypLen);

bool BoolFromDatum(duckdb_libpgquery::PGDatum d);

duckdb_libpgquery::int16 Int16FromDatum(duckdb_libpgquery::PGDatum d);

duckdb_libpgquery::int32 Int32FromDatum(duckdb_libpgquery::PGDatum d);

duckdb_libpgquery::int64 Int64FromDatum(duckdb_libpgquery::PGDatum d);

duckdb_libpgquery::PGOid OidFromDatum(duckdb_libpgquery::PGDatum d);

bool NumericIsNan(duckdb_libpgquery::PGNumeric* num);

duckdb_libpgquery::float4 Float4FromDatum(duckdb_libpgquery::PGDatum d);

duckdb_libpgquery::float8 Float8FromDatum(duckdb_libpgquery::PGDatum d);

double NumericToDoubleNoOverflow(duckdb_libpgquery::PGNumeric* num);

double ConvertTimeValueToScalar(duckdb_libpgquery::PGDatum datum, duckdb_libpgquery::PGOid typid);

double ConvertNetworkToScalar(duckdb_libpgquery::PGDatum datum, duckdb_libpgquery::PGOid typid);

void * PointerFromDatum(duckdb_libpgquery::PGDatum d);

duckdb_libpgquery::uint32 UUIDHash(duckdb_libpgquery::PGDatum d);

duckdb_libpgquery::uint32 HashBpChar(duckdb_libpgquery::PGDatum d);

duckdb_libpgquery::uint32 HashChar(duckdb_libpgquery::PGDatum d);

duckdb_libpgquery::uint32 HashName(duckdb_libpgquery::PGDatum d);

duckdb_libpgquery::uint32 HashText(duckdb_libpgquery::PGDatum d);

duckdb_libpgquery::PGList * GetFuncArgTypes(duckdb_libpgquery::PGOid funcid);

bool ResolvePolymorphicArgType(int numargs, duckdb_libpgquery::PGOid *argtypes, char *argmodes,
								duckdb_libpgquery::PGFuncExpr *call_expr);

duckdb_libpgquery::PGNode *
CoerceToCommonType(duckdb_libpgquery::PGParseState *pstate, duckdb_libpgquery::PGNode *node, duckdb_libpgquery::PGOid target_type,
						 const char *context);

duckdb_libpgquery::PGList *
FindMatchingMembersInTargetList(duckdb_libpgquery::PGNode *node, duckdb_libpgquery::PGList *targetlist);

void * GPDBAlloc(size_t size);

void CheckRTPermissions(duckdb_libpgquery::PGList *rtable);

void GpdbEreportImpl(int xerrcode, int severitylevel, const char *xerrmsg,
					 const char *xerrhint, const char *filename, int lineno,
					 const char *funcname);

duckdb_libpgquery::PGList *GetIndexOpFamilies(duckdb_libpgquery::PGOid index_oid);

duckdb_libpgquery::PGList *GetOpFamiliesForScOp(duckdb_libpgquery::PGOid opno);

}
