#include <Interpreters/orcaopt/translator/wrappers.h>

#include <Interpreters/orcaopt/walkers.h>

#include "gpos/base.h"
#include "gpos/error/CAutoExceptionStack.h"
#include "gpos/error/CException.h"

#include "naucrates/exception.h"

#ifdef __clang__
#pragma clang diagnostic ignored "-Wunused-parameter"
#else
#pragma GCC diagnostic ignored "-Wunused-parameter"
#endif

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
// IndexExists(Oid oid)
// {
// 	return false;
// };

bool
TypeExists(Oid oid)
{
	return false;
};

bool
RelationExists(Oid oid)
{
	return false;
};

bool
OperatorExists(Oid oid)
{
	return false;
};

bool
AggregateExists(Oid oid)
{
	return false;
};

bool
FunctionExists(Oid oid)
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

Oid
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
GetLogicalIndexType(Oid index_oid)
{
	return PGINDTYPE_BTREE;
};

bool
RelPartIsNone(Oid relid)
{
	return false;
};

bool
IsLeafPartition(Oid oid)
{
	return false;
};

bool
RelPartIsRoot(Oid relid)
{
	return false;
};

PGNode* GetLeafPartContraints(Oid rel_oid, PGList **default_levels)
{
	return NULL;
};

PGRelationPtr
GetRelation(Oid rel_oid)
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
GetLogicalPartIndexes(Oid oid)
{
	return NULL;
};

Oid
GetOpclassFamily(Oid opclass)
{
	return 0;
};

Oid
GetHashProcInOpfamily(Oid opfamily, Oid typid)
{
	return 0;
};

Oid
IsLegacyCdbHashFunction(Oid funcid)
{
	return 0;
};

Oid
GetDefaultDistributionOpclassForType(Oid typid)
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
GetCheckConstraintOids(Oid rel_oid)
{
	return NULL;
};

bool
RelPartIsInterior(Oid relid)
{
	return false;
};

PGList *
GetPartitionAttrs(Oid oid)
{
	return NULL;
};

bool
HasSubclassSlow(Oid rel_oid)
{
	return false;
};

bool
IsMultilevelPartitionUniform(Oid root_oid)
{
	return false;
};

bool
IsChildPartDistributionMismatched(PGRelationPtr rel)
{
	return false;
};

gpos::ULONG
CountLeafPartTables(Oid rel_oid)
{
	return 0;
};

bool
HasExternalPartition(Oid oid)
{
	return false;
};

PGExtTableEntry *
GetExternalTableEntry(Oid rel_oid)
{
	return NULL;
}

PGStatisticPtr
GetAttStats(Oid relid, PGAttrNumber attnum)
{
	return NULL;
};

PGNode *
GetTypeDefault(Oid typid)
{
	return NULL;
};

PGList *
GetExternalPartitions(Oid oid)
{
	return NULL;
};

Oid
GetAggregate(const char *agg, Oid type_oid, int nargs)
{
	return 0;
};

bool
IsAppendOnlyPartitionTable(Oid root_oid)
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

Oid
TypeCollation(Oid type)
{
    Oid collation = InvalidOid;
    // if (type_is_collatable(type))
    // {
    //     collation = DEFAULT_COLLATION_OID;
    // }
    return collation;
};

PGVar *
MakeVar(PGIndex varno, PGAttrNumber varattno, Oid vartype, int32 vartypmod,
			  PGIndex varlevelsup)
{
    Oid collation = TypeCollation(vartype);
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
GetRelationPartContraints(Oid rel_oid, PGList **default_levels)
{
	return NULL;
};

Oid
GetInverseOp(Oid opno)
{
	return 0;
};

bool IsOpHashJoinable(Oid opno, Oid inputtype)
{
	return false;
};

bool IsOpMergeJoinable(Oid opno, Oid inputtype)
{
	return false;
};

bool IsCompositeType(Oid typid)
{
	return false;
};

bool IsTextRelatedType(Oid typid)
{
	return false;
};

PGTypePtr LookupTypeCache(Oid type_id/* , int flags */)
{
	return NULL;
};

Oid GetTypeRelid(Oid typid)
{
	return 0;
};

Oid GetArrayType(Oid typid)
{
	return 0;
};

Oid GetDefaultDistributionOpfamilyForType(Oid typid)
{
	return 0;
};

Oid GetLegacyCdbHashOpclassForBaseType(Oid typid)
{
	return 0;
};

char * GetOpName(Oid opno)
{
	return NULL;
};

void GetOpInputTypes(Oid opno, Oid *lefttype, Oid *righttype)
{
	return;
};

unsigned int WrapperGetComparisonType(Oid op_oid)
{
	return 0;
};

Oid GetOpFunc(Oid opno)
{
	return 0;
};

Oid GetFuncRetType(Oid funcid)
{
	return 0;
};

Oid GetCommutatorOp(Oid opno)
{
	return 0;
};

bool IsOpStrict(Oid opno)
{
	return false;
};

bool IsOpNDVPreserving(Oid opno)
{
	return false;
};

Oid GetCompatibleHashOpFamily(Oid opno)
{
	return 0;
};

Oid GetCompatibleLegacyHashOpFamily(Oid opno)
{
	return 0;
};

char FuncStability(Oid funcid)
{
	return '\0';
};

char FuncDataAccess(Oid funcid)
{
	return '\0';
};

char FuncExecLocation(Oid funcid)
{
	return '\0';
};

bool GetFuncRetset(Oid funcid)
{
	return false;
};

bool FuncStrict(Oid funcid)
{
	return false;
};

bool IsFuncNDVPreserving(Oid funcid)
{
	return false;
};

bool IsFuncAllowedForPartitionSelection(Oid funcid)
{
	return false;
};

char * GetFuncName(Oid funcid)
{
	return NULL;
}

PGList * GetFuncOutputArgTypes(Oid funcid)
{
	return NULL;
};

bool IsOrderedAgg(Oid aggid)
{
	return false;
};

bool IsAggPartialCapable(Oid aggid)
{
	return false;
};

char * WrapperGetTypeName(Oid typid)
{
	return NULL;
};

Oid GetAggIntermediateResultType(Oid aggid)
{
	return 0;
};

double CdbEstimatePartitionedNumTuples(PGRelationPtr rel)
{
	return 0.0;
};

bool GetAttrStatsSlot(PGAttStatsSlotPtr& sslot,
	PGStatisticPtr statstuple, int reqkind, Oid reqop, int flags)
{
	return false;
};

void FreeAttrStatsSlot(PGAttStatsSlotPtr sslot)
{
	return;
};

bool GetCastFunc(Oid src_oid, Oid dest_oid, bool *is_binary_coercible,
				  Oid *cast_fn_oid, PGCoercionPathType *pathtype)
{
	return false;
};

Oid GetComparisonOperator(Oid left_oid, Oid right_oid, unsigned int cmpt)
{
	return 0;
};

void GetOrderedPartKeysAndKinds(Oid oid, PGList **pkeys, PGList **pkinds)
{
	return;
};

duckdb_libpgquery::PGList * GetRelationKeys(Oid relid)
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

float4 Float4FromDatum(Datum d)
{
	return 0.0;
};

float8 Float8FromDatum(Datum d)
{
	return 0.0;
};

Oid OidFromDatum(Datum d)
{
	return 0;
};

double NumericToDoubleNoOverflow(PGNumeric* num)
{
	return 0.0;
};

double ConvertTimeValueToScalar(Datum datum, Oid typid)
{
	return 0.0;
};

double ConvertNetworkToScalar(Datum datum, Oid typid)
{
	return 0.0;
};

void * PointerFromDatum(Datum d)
{
	return NULL;
};

uint32 UUIDHash(Datum d)
{
	return 0;
};

uint32 HashBpChar(Datum d)
{
	return 0;
};

uint32 HashChar(Datum d)
{
	return 0;
};

uint32 HashName(Datum d)
{
	return 0;
};

uint32 HashText(Datum d)
{
	return 0;
};

PGList * GetFuncArgTypes(Oid funcid)
{
	return NULL;
};

bool ResolvePolymorphicArgType(int numargs, Oid *argtypes, char *argmodes,
								PGFuncExpr *call_expr)
{
	return false;
};

PGNode *
CoerceToCommonType(PGParseState *pstate, PGNode *node, Oid target_type,
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

PGList *GetIndexOpFamilies(Oid index_oid)
{
	return NULL;
};

PGList *GetOpFamiliesForScOp(Oid opno)
{
	return NULL;
};
