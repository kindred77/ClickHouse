#include <Interpreters/orcaopt/translator/wrappers.h>

#include <common/parser_common.hpp>

#include "gpos/base.h"
#include "gpos/error/CAutoExceptionStack.h"
#include "gpos/error/CException.h"

#include "naucrates/exception.h"

using namespace duckdb_libpgquery;

// #define GP_WRAP_START                                            \
// 	sigjmp_buf local_sigjmp_buf;                                 \
// 	{                                                            \
// 		CAutoExceptionStack aes((void **) &PG_exception_stack,   \
// 								(void **) &error_context_stack); \
// 		if (0 == sigsetjmp(local_sigjmp_buf, 0))                 \
// 		{                                                        \
// 			aes.SetLocalJmp(&local_sigjmp_buf)

// #define GP_WRAP_END                                        \
// 	}                                                      \
// 	else                                                   \
// 	{                                                      \
// 		GPOS_RAISE(gpdxl::ExmaGPDB, gpdxl::ExmiGPDBError); \
// 	}                                                      \
// 	}

// PGList *LPrependInt(int datum, PGList *list)
// {
// 	return NULL;
// };
namespace gpdxl
{

PGListCell *
ListHead(PGList *l)
{
	return list_head(l);
};

PGList *
LAppendInt(PGList *list, int iDatum)
{
	return lappend_int(list, iDatum);
};

int
FindNodes(PGNode *node, const std::vector<PGNodeTag>& nodeTags)
{
	pg_find_nodes_context context;
	Assert(NULL != node);
	context.nodeTags = nodeTags;
	context.foundNode = -1;
	pg_find_nodes_walker(node, &context);

	return context.foundNode;
};

uint32
ListLength(PGList *l)
{
    return list_length(l);
};

void *
ListNth(PGList *list, int n)
{
	return list_nth(list, n);
};

PGList *
LAppend(PGList *list, void *datum)
{
	return lappend(list, datum);
};

void
ListFree(PGList *list)
{
	list_free(list);
};

void *
CopyObject(void *from)
{
	return copyObject(from);
};

void
GPDBFree(void *ptr)
{
	pfree(ptr);
	return;
};

// bool
// IndexExists(PGOid oid)
// {
// 	return false;
// };

bool
TypeExists(PGOid oid)
{
	return false;
};

bool
RelationExists(PGOid oid)
{
	return false;
};

bool
OperatorExists(PGOid oid)
{
	return false;
};

bool
AggregateExists(PGOid oid)
{
	return false;
};

bool
FunctionExists(PGOid oid)
{
	return false;
};

std::string
NodeToString(PGNode *obj)
{
	return "";
};

PGNode *
StringToNode(std::string& str)
{
	return NULL;
};

PGExpr *
TransformArrayConstToArrayExpr(PGConst *c)
{
	return NULL;
};

int32
ExprTypeMod(PGNode *expr)
{
	return 0;
};

PGOid
ExprType(PGNode *expr)
{
	return 0;
};

bool
Equals(void *p1, void *p2)
{
	return false;
};

PGNode *
MakeBoolConst(bool value, bool isnull)
{
	return NULL;
};

int
CheckCollation(PGNode *node)
{
	return 0;
};

PGList *
ExtractNodesExpression(PGNode *node, int node_tag,
							 bool descend_into_subqueries)
{
	return NULL;
};

PGTargetEntry *
FindFirstMatchingMemberInTargetList(PGNode *node, PGList *targetlist)
{
	return NULL;
};

bool
WalkExpressionTree(PGNode *node, bool (*walker)(PGNode *node, void *context), void *context)
{
	return pg_expression_tree_walker(node, walker, context);
};

PGLogicalIndexType
GetLogicalIndexType(PGOid index_oid)
{
	return PGINDTYPE_BTREE;
};

bool
RelPartIsNone(PGOid relid)
{
	return false;
};

bool
IsLeafPartition(PGOid oid)
{
	return false;
};

bool
RelPartIsRoot(PGOid relid)
{
	return false;
};

PGNode* GetLeafPartContraints(PGOid rel_oid, PGList **default_levels)
{
	return NULL;
};

PGRelationPtr
GetRelation(PGOid rel_oid)
{
	return NULL;
};

void
CloseRelation(PGRelationPtr rel)
{

};

PGList *
GetRelationIndexes(PGRelationPtr relation)
{
	return NULL;
};

PGLogicalIndexes *
GetLogicalPartIndexes(PGOid oid)
{
	return NULL;
};

PGOid
GetOpclassFamily(PGOid opclass)
{
	return 0;
};

PGOid
GetHashProcInOpfamily(PGOid opfamily, PGOid typid)
{
	return 0;
};

PGOid
IsLegacyCdbHashFunction(PGOid funcid)
{
	return 0;
};

PGOid
GetDefaultDistributionOpclassForType(PGOid typid)
{
	return 0;
};

PGList *
ListConcat(PGList *list1, PGList *list2)
{
	return NULL;
};

int
GetIntFromValue(PGNode *node)
{
	return 0;
};

PGList *
GetCheckConstraintOids(PGOid rel_oid)
{
	return NULL;
};

bool
RelPartIsInterior(PGOid relid)
{
	return false;
};

PGList *
GetPartitionAttrs(PGOid oid)
{
	return NULL;
};

bool
HasSubclassSlow(PGOid rel_oid)
{
	return false;
};

bool
IsMultilevelPartitionUniform(PGOid root_oid)
{
	return false;
};

bool
IsChildPartDistributionMismatched(PGRelationPtr rel)
{
	return false;
};

gpos::ULONG
CountLeafPartTables(PGOid rel_oid)
{
	return 0;
};

bool
HasExternalPartition(PGOid oid)
{
	return false;
};

PGExtTableEntry *
GetExternalTableEntry(PGOid rel_oid)
{
	return NULL;
}

PGStatisticPtr
GetAttStats(PGOid relid, PGAttrNumber attnum)
{
	return NULL;
};

PGNode *
GetTypeDefault(PGOid typid)
{
	return NULL;
};

PGList *
GetExternalPartitions(PGOid oid)
{
	return NULL;
};

PGOid
GetAggregate(const char *agg, PGOid type_oid, int nargs)
{
	return 0;
};

bool
IsAppendOnlyPartitionTable(PGOid root_oid)
{
	return false;
};

PGNode *
MutateQueryOrExpressionTree(PGNode * node,
	PGNode * (*mutator)(PGNode *node, void *context), void * context, int flags)
{
	return pg_query_or_expression_tree_mutator(node, mutator, context, flags);
};

PGQuery *
MutateQueryTree(PGQuery *query,
	PGNode *(*mutator)(PGNode *node, void *context),
	void *context, int flags)
{
	return pg_query_tree_mutator(query, mutator, context, flags);
};

PGNode *
MutateExpressionTree(PGNode *node,
	PGNode *(*mutator)(PGNode *node, void *context), void *context)
{
	return pg_expression_tree_mutator(node, mutator, context);
};

bool
WalkQueryOrExpressionTree(PGNode *node, bool (*walker)(PGNode *node, void *context), void *context, int flags)
{
	return pg_query_or_expression_tree_walker(node, walker, context, flags);
};

PGTargetEntry *
MakeTargetEntry(PGExpr *expr, PGAttrNumber resno, char *resname, bool resjunk)
{
	return makeTargetEntry(expr, resno, resname, resjunk);
};

PGOid
TypeCollation(PGOid type)
{
    PGOid collation = InvalidOid;
    // if (type_is_collatable(type))
    // {
    //     collation = DEFAULT_COLLATION_OID;
    // }
    return collation;
};

PGVar *
MakeVar(PGIndex varno, PGAttrNumber varattno, PGOid vartype, int32 vartypmod,
			  PGIndex varlevelsup)
{
    PGOid collation = TypeCollation(vartype);
    return makeVar(varno, varattno, vartype, vartypmod, collation, varlevelsup);
};

PGValue* MakeStringValue(char *str)
{
	return NULL;
};

PGQuery *
FlattenJoinAliasVar(PGQuery *query, gpos::ULONG query_level)
{
	return pg_flatten_join_alias_var_optimizer(query, query_level);
};

int GetGPSegmentCount(void)
{
	return 1;
};

PGPolicyPtr
GetDistributionPolicy(PGRelationPtr rel)
{
	return rel->rd_cdbpolicy;
};

PGNode *
GetRelationPartContraints(PGOid rel_oid, PGList **default_levels)
{
	return NULL;
};

PGOid
GetInverseOp(PGOid opno)
{
	return 0;
};

bool IsOpHashJoinable(PGOid opno, PGOid inputtype)
{
	return false;
};

bool IsOpMergeJoinable(PGOid opno, PGOid inputtype)
{
	return false;
};

bool IsCompositeType(PGOid typid)
{
	return false;
};

bool IsTextRelatedType(PGOid typid)
{
	return false;
};

PGTypePtr LookupTypeCache(PGOid type_id/* , int flags */)
{
	return NULL;
};

PGOid GetTypeRelid(PGOid typid)
{
	return 0;
};

PGOid GetArrayType(PGOid typid)
{
	return 0;
};

PGOid GetDefaultDistributionOpfamilyForType(PGOid typid)
{
	return 0;
};

PGOid GetLegacyCdbHashOpclassForBaseType(PGOid typid)
{
	return 0;
};

char * GetOpName(PGOid opno)
{
	return NULL;
};

void GetOpInputTypes(PGOid opno, PGOid *lefttype, PGOid *righttype)
{
	return;
};

unsigned int WrapperGetComparisonType(PGOid op_oid)
{
	return 0;
};

PGOid GetOpFunc(PGOid opno)
{
	return 0;
};

PGOid GetFuncRetType(PGOid funcid)
{
	return 0;
};

PGOid GetCommutatorOp(PGOid opno)
{
	return 0;
};

bool IsOpStrict(PGOid opno)
{
	return false;
};

bool IsOpNDVPreserving(PGOid opno)
{
	return false;
};

PGOid GetCompatibleHashOpFamily(PGOid opno)
{
	return 0;
};

PGOid GetCompatibleLegacyHashOpFamily(PGOid opno)
{
	return 0;
};

char FuncStability(PGOid funcid)
{
	return '\0';
};

char FuncDataAccess(PGOid funcid)
{
	return '\0';
};

char FuncExecLocation(PGOid funcid)
{
	return '\0';
};

bool GetFuncRetset(PGOid funcid)
{
	return false;
};

bool FuncStrict(PGOid funcid)
{
	return false;
};

bool IsFuncNDVPreserving(PGOid funcid)
{
	return false;
};

bool IsFuncAllowedForPartitionSelection(PGOid funcid)
{
	return false;
};

char * GetFuncName(PGOid funcid)
{
	return NULL;
}

PGList * GetFuncOutputArgTypes(PGOid funcid)
{
	return NULL;
};

bool IsOrderedAgg(PGOid aggid)
{
	return false;
};

bool IsAggPartialCapable(PGOid aggid)
{
	return false;
};

char * WrapperGetTypeName(PGOid typid)
{
	return NULL;
};

PGOid GetAggIntermediateResultType(PGOid aggid)
{
	return 0;
};

double CdbEstimatePartitionedNumTuples(PGRelationPtr rel)
{
	return 0.0;
};

bool GetAttrStatsSlot(PGAttStatsSlotPtr& sslot,
	PGStatisticPtr statstuple, int reqkind, PGOid reqop, int flags)
{
	return false;
};

void FreeAttrStatsSlot(PGAttStatsSlotPtr sslot)
{
	return;
};

bool GetCastFunc(PGOid src_oid, PGOid dest_oid, bool *is_binary_coercible,
				  PGOid *cast_fn_oid, PGCoercionPathType *pathtype)
{
	return false;
};

PGOid GetComparisonOperator(PGOid left_oid, PGOid right_oid, unsigned int cmpt)
{
	return 0;
};

void GetOrderedPartKeysAndKinds(PGOid oid, PGList **pkeys, PGList **pkinds)
{
	return;
};

duckdb_libpgquery::PGList * GetRelationKeys(PGOid relid)
{
	return NULL;
};

size_t DatumSize(Datum value, bool type_by_val, int iTypLen)
{
	return 0;
};

bool BoolFromDatum(Datum d)
{
	return false;
};

int16 Int16FromDatum(Datum d)
{
	return 0;
};

int32 Int32FromDatum(Datum d)
{
	return 0;
};

int64 Int64FromDatum(Datum d)
{
	return 0;
};

bool NumericIsNan(PGNumeric* num)
{
	return PG_NUMERIC_IS_NAN(num);
};

float4 Float4FromDatum(PGDatum d)
{
	return 0.0;
};

float8 Float8FromDatum(PGDatum d)
{
	return 0.0;
};

PGOid OidFromDatum(PGDatum d)
{
	return 0;
};

double NumericToDoubleNoOverflow(PGNumeric* num)
{
	return 0.0;
};

double ConvertTimeValueToScalar(PGDatum datum, PGOid typid)
{
	return 0.0;
};

double ConvertNetworkToScalar(PGDatum datum, PGOid typid)
{
	return 0.0;
};

void * PointerFromDatum(PGDatum d)
{
	return NULL;
};

uint32 UUIDHash(PGDatum d)
{
	return 0;
};

uint32 HashBpChar(PGDatum d)
{
	return 0;
};

uint32 HashChar(PGDatum d)
{
	return 0;
};

uint32 HashName(PGDatum d)
{
	return 0;
};

uint32 HashText(PGDatum d)
{
	return 0;
};

PGList * GetFuncArgTypes(PGOid funcid)
{
	return NULL;
};

bool ResolvePolymorphicArgType(int numargs, PGOid *argtypes, char *argmodes,
								PGFuncExpr *call_expr)
{
	return false;
};

PGNode *
CoerceToCommonType(PGParseState *pstate, PGNode *node, PGOid target_type,
						 const char *context)
{
	return NULL;
};

PGList *
FindMatchingMembersInTargetList(PGNode *node, PGList *targetlist)
{
	return NULL;
};

void * GPDBAlloc(size_t size)
{
	return NULL;
};

void CheckRTPermissions(PGList *rtable)
{
	return;
};

void GpdbEreportImpl(int xerrcode, int severitylevel, const char *xerrmsg,
					 const char *xerrhint, const char *filename, int lineno,
					 const char *funcname)
{
	// if (errstart(severitylevel, filename, lineno, funcname, TEXTDOMAIN))
	// 		errfinish(errcode(xerrcode), errmsg("%s", xerrmsg),
	// 				  xerrhint ? errhint("%s", xerrhint) : 0);
};

PGList *GetIndexOpFamilies(PGOid index_oid)
{
	return NULL;
};

PGList *GetOpFamiliesForScOp(PGOid opno)
{
	return NULL;
};

}
