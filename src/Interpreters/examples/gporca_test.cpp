#include <iostream>
#include <Interpreters/orcaopt/SelectParser.h>
#include <Interpreters/orcaopt/provider/RelationProvider.h>
#include <Interpreters/Context.h>
#include <postgres_parser.hpp>
#include <Interpreters/orcaopt/translator/CTranslatorQueryToDXL.h>
#include <Interpreters/orcaopt/translator/CTranslatorRelcacheToDXL.h>
#include <gpos/common/CAutoP.h>
#include <gpos/memory/CAutoMemoryPool.h>
#include <gpos/task/CWorkerPoolManager.h>
#include <gpos/_api.h>
#include <gpopt/mdcache/CMDCache.h>
#include <gpopt/init.h>
#include <gpopt/search/CSearchStage.h>
#include <gpopt/engine/CEnumeratorConfig.h>
#include <gpopt/engine/CCTEConfig.h>
#include <gpopt/engine/CHint.h>
#include <gpopt/optimizer/COptimizer.h>
#include <gpopt/optimizer/COptimizerConfig.h>
#include <gpopt/base/CWindowOids.h>
#include <gpdbcost/CCostModelParamsGPDB.h>
#include <gpdbcost/CCostModelGPDB.h>
#include <naucrates/init.h>
#include <naucrates/dxl/CDXLUtils.h>
#include <naucrates/dxl/parser/CParseHandlerDXL.h>
#include <naucrates/md/IMDProvider.h>

using namespace duckdb_libpgquery;
using namespace gpmd;
using namespace gpdxl;
using namespace gpopt;
using namespace gpdbcost;

#define GPOPT_ERROR_BUFFER_SIZE 10 * 1024 * 1024
#define AUTO_MEM_POOL(amp) CAutoMemoryPool amp(CAutoMemoryPool::ElcExc)

int optimizer_mdcache_size = 10240;
bool optimizer_metadata_caching = false;
int optimizer_segments = 2;

class CMDProviderRelcache : public IMDProvider
{
private:
	// memory pool
	CMemoryPool *m_mp;

	// private copy ctor
	CMDProviderRelcache(const CMDProviderRelcache &);

public:
	// ctor/dtor
	explicit CMDProviderRelcache(CMemoryPool *mp);

	~CMDProviderRelcache()
	{
	}

	// returns the DXL string of the requested metadata object
	virtual CWStringBase *GetMDObjDXLStr(CMemoryPool *mp,
										 CMDAccessor *md_accessor,
										 IMDId *md_id) const;

	// return the mdid for the requested type
	virtual IMDId *
	MDId(CMemoryPool *mp, CSystemId sysid, IMDType::ETypeInfo type_info) const
	{
		return GetGPDBTypeMdid(mp, sysid, type_info);
	}
};

CMDProviderRelcache::CMDProviderRelcache(CMemoryPool *mp) : m_mp(mp)
{
	GPOS_ASSERT(NULL != m_mp);
};

CWStringBase *
CMDProviderRelcache::GetMDObjDXLStr(CMemoryPool *mp, CMDAccessor *md_accessor,
									IMDId *md_id) const
{
	IMDCacheObject *md_obj =
		CTranslatorRelcacheToDXL::RetrieveObject(mp, md_accessor, md_id);
	GPOS_ASSERT(NULL != md_obj);
	CWStringDynamic *str = CDXLUtils::SerializeMDObj(
		m_mp, md_obj, true /*fSerializeHeaders*/, false /*findent*/);
	// cleanup DXL object
	md_obj->Release();
	return str;
};

CSearchStageArray *
LoadSearchStrategy(CMemoryPool *mp, char *path)
{
	CSearchStageArray *search_strategy_arr = NULL;
	CParseHandlerDXL *dxl_parse_handler = NULL;

	GPOS_TRY
	{
		if (NULL != path)
		{
			std::cout << "LoadSearchStrategy--00" << std::endl;
			dxl_parse_handler =
				CDXLUtils::GetParseHandlerForDXLFile(mp, path, NULL);
			std::cout << "LoadSearchStrategy--11" << std::endl;
			if (NULL != dxl_parse_handler)
			{
				elog(DEBUG2, "\n[OPT]: Using search strategy in (%s)", path);

				search_strategy_arr = dxl_parse_handler->GetSearchStageArray();
				search_strategy_arr->AddRef();
			}
		}
	}
	GPOS_CATCH_EX(ex)
	{
		if (GPOS_MATCH_EX(ex, gpdxl::ExmaGPDB, gpdxl::ExmiGPDBError))
		{
			GPOS_RETHROW(ex);
		}
		elog(DEBUG2, "\n[OPT]: Using default search strategy");
		GPOS_RESET_EX;
	}
	GPOS_CATCH_END;

	GPOS_DELETE(dxl_parse_handler);

	return search_strategy_arr;
};

double optimizer_nestloop_factor = 1.0;
double optimizer_sort_factor = 1.0;

void SetCostModelParams(ICostModel *cost_model)
{
	GPOS_ASSERT(NULL != cost_model);

	if (optimizer_nestloop_factor > 1.0)
	{
		// change NLJ cost factor
		ICostModelParams::SCostParam *cost_param =
			cost_model->GetCostModelParams()->PcpLookup(
				CCostModelParamsGPDB::EcpNLJFactor);
		CDouble nlj_factor(optimizer_nestloop_factor);
		cost_model->GetCostModelParams()->SetParam(
			cost_param->Id(), nlj_factor, nlj_factor - 0.5, nlj_factor + 0.5);
	}

	if (optimizer_sort_factor > 1.0 || optimizer_sort_factor < 1.0)
	{
		// change sort cost factor
		ICostModelParams::SCostParam *cost_param =
			cost_model->GetCostModelParams()->PcpLookup(
				CCostModelParamsGPDB::EcpSortTupWidthCostUnit);

		CDouble sort_factor(optimizer_sort_factor);
		cost_model->GetCostModelParams()->SetParam(
			cost_param->Id(), cost_param->Get() * optimizer_sort_factor,
			cost_param->GetLowerBoundVal() * optimizer_sort_factor,
			cost_param->GetUpperBoundVal() * optimizer_sort_factor);
	}
};

ICostModel *
GetCostModel(CMemoryPool *mp, ULONG num_segments)
{
	ICostModel *cost_model = GPOS_NEW(mp) CCostModelGPDB(mp, num_segments);

	SetCostModelParams(cost_model);

	return cost_model;
};

int optimizer_plan_id = 1;
int optimizer_samples_number =1;
double optimizer_cost_threshold = 1.0;
double optimizer_damping_factor_filter = 1.0;
double optimizer_damping_factor_join = 1.0;
double optimizer_damping_factor_groupby = 1.0;
int optimizer_cte_inlining_bound =1;
int optimizer_join_arity_for_associativity_commutativity = 1;
int optimizer_array_expansion_threshold =1;
int optimizer_join_order_threshold = 1;
int optimizer_penalize_broadcast_threshold = 1;
int optimizer_push_group_by_below_setop_threshold = 1;
int optimizer_xform_bind_threshold = 1;

#define F_WINDOW_ROW_NUMBER 3100
#define F_WINDOW_RANK 3101


COptimizerConfig *
CreateOptimizerConfig(CMemoryPool *mp, ICostModel *cost_model)
{
	// get chosen plan number, cost threshold
	ULLONG plan_id = (ULLONG) optimizer_plan_id;
	ULLONG num_samples = (ULLONG) optimizer_samples_number;
	DOUBLE cost_threshold = (DOUBLE) optimizer_cost_threshold;

	DOUBLE damping_factor_filter = (DOUBLE) optimizer_damping_factor_filter;
	DOUBLE damping_factor_join = (DOUBLE) optimizer_damping_factor_join;
	DOUBLE damping_factor_groupby = (DOUBLE) optimizer_damping_factor_groupby;

	ULONG cte_inlining_cutoff = (ULONG) optimizer_cte_inlining_bound;
	ULONG join_arity_for_associativity_commutativity =
		(ULONG) optimizer_join_arity_for_associativity_commutativity;
	ULONG array_expansion_threshold =
		(ULONG) optimizer_array_expansion_threshold;
	ULONG join_order_threshold = (ULONG) optimizer_join_order_threshold;
	ULONG broadcast_threshold = (ULONG) optimizer_penalize_broadcast_threshold;
	ULONG push_group_by_below_setop_threshold =
		(ULONG) optimizer_push_group_by_below_setop_threshold;
	ULONG xform_bind_threshold = (ULONG) optimizer_xform_bind_threshold;

	return GPOS_NEW(mp) COptimizerConfig(
		GPOS_NEW(mp)
			CEnumeratorConfig(mp, plan_id, num_samples, cost_threshold),
		GPOS_NEW(mp)
			CStatisticsConfig(mp, damping_factor_filter, damping_factor_join,
							  damping_factor_groupby, MAX_STATS_BUCKETS),
		GPOS_NEW(mp) CCTEConfig(cte_inlining_cutoff), cost_model,
		GPOS_NEW(mp)
			CHint(gpos::int_max /* optimizer_parts_to_force_sort_on_insert */,
				  join_arity_for_associativity_commutativity,
				  array_expansion_threshold, join_order_threshold,
				  broadcast_threshold,
				  false, /* don't create Assert nodes for constraints, we'll
								      * enforce them ourselves in the executor */
				  push_group_by_below_setop_threshold, xform_bind_threshold),
		GPOS_NEW(mp) CWindowOids(OID(F_WINDOW_ROW_NUMBER), OID(F_WINDOW_RANK)));
};

void * OptimizeTask(void *ptr)
{
    CSystemId default_sysid(IMDId::EmdidGPDB, GPOS_WSZ_STR_LENGTH("GPDB"));

    GPOS_ASSERT(NULL != ptr);
    AUTO_MEM_POOL(amp);
	CMemoryPool *mp = amp.Pmp();

    bool reset_mdcache = true;//gpdb::MDCacheNeedsReset();

	// initialize metadata cache, or purge if needed, or change size if requested
	if (!CMDCache::FInitialized())
	{
		CMDCache::Init();
		CMDCache::SetCacheQuota(optimizer_mdcache_size * 1024L);
	}
	else if (reset_mdcache)
	{
		CMDCache::Reset();
		CMDCache::SetCacheQuota(optimizer_mdcache_size * 1024L);
	}
	else if (CMDCache::ULLGetCacheQuota() !=
			 (ULLONG) optimizer_mdcache_size * 1024L)
	{
		CMDCache::SetCacheQuota(optimizer_mdcache_size * 1024L);
	}
	
    // load search strategy
	// CSearchStageArray *search_strategy_arr =
	// 	LoadSearchStrategy(mp, "default");

	//CBitSet *trace_flags = NULL;
	//CBitSet *enabled_trace_flags = NULL;
	//CBitSet *disabled_trace_flags = NULL;
	CDXLNode *plan_dxl = NULL;

	IMdIdArray *col_stats = NULL;
	MdidHashSet *rel_stats = NULL;

    GPOS_TRY
	{
		// set trace flags
		// trace_flags = CConfigParamMapping::PackConfigParamInBitset(
		// 	mp, CXform::ExfSentinel);
		// SetTraceflags(mp, trace_flags, &enabled_trace_flags,
		// 			  &disabled_trace_flags);

		// set up relcache MD provider
		CMDProviderRelcache *relcache_provider =
			GPOS_NEW(mp) CMDProviderRelcache(mp);

		{
			// scope for MD accessor
			CMDAccessor mda(mp, CMDCache::Pcache(), default_sysid,
							relcache_provider);

			ULONG num_segments = 2;//gpdb::GetGPSegmentCount();
			ULONG num_segments_for_costing = optimizer_segments;
			if (0 == num_segments_for_costing)
			{
				num_segments_for_costing = num_segments;
			}
			CAutoP<CTranslatorQueryToDXL> query_to_dxl_translator;
			query_to_dxl_translator = CTranslatorQueryToDXL::QueryToDXLInstance(
				mp, &mda, (PGQuery *) ptr);

			ICostModel *cost_model = GetCostModel(mp, num_segments_for_costing);
			COptimizerConfig *optimizer_config =
				CreateOptimizerConfig(mp, cost_model);

			// CConstExprEvaluatorProxy expr_eval_proxy(mp, &mda);
			// IConstExprEvaluator *expr_evaluator =
			// 	GPOS_NEW(mp) CConstExprEvaluatorDXL(mp, &mda, &expr_eval_proxy);

			CDXLNode *query_dxl =
				query_to_dxl_translator->TranslateQueryToDXL();
			CDXLNodeArray *query_output_dxlnode_array =
				query_to_dxl_translator->GetQueryOutputCols();
			CDXLNodeArray *cte_dxlnode_array =
				query_to_dxl_translator->GetCTEs();
			GPOS_ASSERT(NULL != query_output_dxlnode_array);

            CWStringDynamic str(mp);
	        COstreamString oss(&str);

            CDXLUtils::SerializeQuery(mp, oss, query_dxl, query_output_dxlnode_array, cte_dxlnode_array, true, true);
			std::wcout << std::wstring(str.GetBuffer()) << std::endl;


			// BOOL is_master_only =
			// 	!optimizer_enable_motions ||
			// 	(!optimizer_enable_motions_masteronly_queries &&
			// 	 !query_to_dxl_translator->HasDistributedTables());
			// // See NoteDistributionPolicyOpclasses() in src/backend/gpopt/translate/CTranslatorQueryToDXL.cpp
			// BOOL use_legacy_opfamilies =
			// 	(query_to_dxl_translator->GetDistributionHashOpsKind() ==
			// 	 DistrUseLegacyHashOps);
			// CAutoTraceFlag atf1(EopttraceDisableMotions, is_master_only);
			// CAutoTraceFlag atf2(EopttraceUseLegacyOpfamilies,
			// 					use_legacy_opfamilies);

			int gp_command_count = 1;

			plan_dxl = COptimizer::PdxlnOptimize(
				mp, &mda, query_dxl, query_output_dxlnode_array,
				cte_dxlnode_array, NULL, num_segments, 1985,
				gp_command_count, NULL, optimizer_config);

			// plan_dxl = COptimizer::PdxlnOptimize(
			// 	mp, &mda, query_dxl, query_output_dxlnode_array,
			// 	cte_dxlnode_array, expr_evaluator, num_segments, gp_session_id,
			// 	gp_command_count, search_strategy_arr, optimizer_config);

			// if (opt_ctxt->m_should_serialize_plan_dxl)
			// {
			// 	// serialize DXL to xml
			// 	CWStringDynamic plan_str(mp);
			// 	COstreamString oss(&plan_str);
			// 	CDXLUtils::SerializePlan(
			// 		mp, oss, plan_dxl,
			// 		optimizer_config->GetEnumeratorCfg()->GetPlanId(),
			// 		optimizer_config->GetEnumeratorCfg()->GetPlanSpaceSize(),
			// 		true /*serialize_header_footer*/, true /*indentation*/);
			// 	opt_ctxt->m_plan_dxl =
			// 		CreateMultiByteCharStringFromWCString(plan_str.GetBuffer());
			// }

			// // translate DXL->PlStmt only when needed
			// if (opt_ctxt->m_should_generate_plan_stmt)
			// {
			// 	// always use opt_ctxt->m_query->can_set_tag as the query_to_dxl_translator->Pquery() is a mutated Query object
			// 	// that may not have the correct can_set_tag
			// 	opt_ctxt->m_plan_stmt =
			// 		(PlannedStmt *) gpdb::CopyObject(ConvertToPlanStmtFromDXL(
			// 			mp, &mda, plan_dxl, opt_ctxt->m_query->canSetTag,
			// 			query_to_dxl_translator->GetDistributionHashOpsKind()));
			// }

			// CStatisticsConfig *stats_conf = optimizer_config->GetStatsConf();
			// col_stats = GPOS_NEW(mp) IMdIdArray(mp);
			// stats_conf->CollectMissingStatsColumns(col_stats);

			// rel_stats = GPOS_NEW(mp) MdidHashSet(mp);
			// PrintMissingStatsWarning(mp, &mda, col_stats, rel_stats);

			// rel_stats->Release();
			// col_stats->Release();

			//expr_evaluator->Release();
			query_dxl->Release();
			//optimizer_config->Release();
			plan_dxl->Release();
		}
	}
	GPOS_CATCH_EX(ex)
	{
		// ResetTraceflags(enabled_trace_flags, disabled_trace_flags);
		CRefCount::SafeRelease(rel_stats);
		CRefCount::SafeRelease(col_stats);
		// CRefCount::SafeRelease(enabled_trace_flags);
		// CRefCount::SafeRelease(disabled_trace_flags);
		// CRefCount::SafeRelease(trace_flags);
		CRefCount::SafeRelease(plan_dxl);
		CMDCache::Shutdown();

		// IErrorContext *errctxt = CTask::Self()->GetErrCtxt();

		// opt_ctxt->m_should_error_out = ShouldErrorOut(ex);
		// opt_ctxt->m_is_unexpected_failure = IsLoggableFailure(ex);
		// opt_ctxt->m_error_msg =
		// 	CreateMultiByteCharStringFromWCString(errctxt->GetErrorMsg());

		GPOS_RETHROW(ex);
	}
	GPOS_CATCH_END;

	// cleanup
	// ResetTraceflags(enabled_trace_flags, disabled_trace_flags);
	// CRefCount::SafeRelease(enabled_trace_flags);
	// CRefCount::SafeRelease(disabled_trace_flags);
	// CRefCount::SafeRelease(trace_flags);
	if (!optimizer_metadata_caching)
	{
		CMDCache::Shutdown();
	}

	return NULL;
};

//CMemoryPool *pmpXerces = NULL;

//CMemoryPool *pmpDXL = NULL;

void Execute(void *(*func)(void *), void *func_arg)
{
    Assert(func);

	CHAR *err_buf = (CHAR *) palloc(GPOPT_ERROR_BUFFER_SIZE);
	err_buf[0] = '\0';

	// initialize DXL support
	InitDXL();

	bool abort_flag = false;

	CAutoMemoryPool amp(CAutoMemoryPool::ElcNone);

	gpos_exec_params params;
	params.func = func;
	params.arg = func_arg;
	params.stack_start = &params;
	params.error_buffer = err_buf;
	params.error_buffer_size = GPOPT_ERROR_BUFFER_SIZE;
	params.abort_requested = &abort_flag;

	// execute task and send log message to server log
	GPOS_TRY
	{
		(void) gpos_exec(&params);
	}
	GPOS_CATCH_EX(ex)
	{
		//LogExceptionMessageAndDelete(err_buf, ex.SeverityLevel());
		GPOS_RETHROW(ex);
	}
	GPOS_CATCH_END;
	//LogExceptionMessageAndDelete(err_buf);
};

bool
IsAbortRequested(void)
{
	// No GP_WRAP_START/END needed here. We just check these global flags,
	// it cannot throw an ereport().
	return false;
}

void InitGPOPT()
{
	// if (optimizer_use_gpdb_allocators)
	// {
	// 	CMemoryPoolPallocManager::Init();
	// }

	struct gpos_init_params params = {IsAbortRequested};

	gpos_init(&params);
	gpdxl_init();
	gpopt_init();
};

void optimize2(PGQuery * query)
{
    Execute(&OptimizeTask, query);
};

// void optimize(PGQuery * query)
// {
//     // InitDXL();

//     // if (CMemoryPoolManager::Init() != GPOS_OK)
//     // {
//     //     std::cout << "can not init memory pool manager!" << std::endl;
//     //     return;
//     // }
//     // if (CWorkerPoolManager::Init() != GPOS_OK)
//     // {
//     //     std::cout << "can not init worker pool manager!" << std::endl;
//     //     return;
//     // }
//     // auto mem_pool_mgr = CMemoryPoolManager::GetMemoryPoolMgr();
//     // if (!mem_pool_mgr)
//     // {
//     //     std::cout << "can not get memory pool manager!" << std::endl;
//     //     return;
//     // }

//     // if (CCacheFactory::Init() != GPOS_OK)
//     // {
//     //     std::cout << "can not init CCacheFactory!" << std::endl;
//     //     return;
//     // }

//     //CWorker worker{1024, NULL};

//     // create memory pool
// 	CAutoMemoryPool amp;
// 	CMemoryPool *mp = amp.Pmp();

//     // set up relcache MD provider
// 	CMDProviderRelcache *relcache_provider =
// 		GPOS_NEW(mp) CMDProviderRelcache(mp);
    
//     CSystemId default_sysid(IMDId::EmdidGPDB, GPOS_WSZ_STR_LENGTH("GPDB"));

//     CMDCache::Init();
//     CMDAccessor mda(mp, CMDCache::Pcache(), default_sysid,
// 		relcache_provider);

// 	//CAutoP<CTranslatorQueryToDXL> query_to_dxl_translator = 
//     //    CTranslatorQueryToDXL::QueryToDXLInstance(mp, &mda, query);
//     auto query_to_dxl_translator = 
//         CTranslatorQueryToDXL::QueryToDXLInstance(mp, &mda, query);
    
//     CDXLNode *query_dxl =
// 		query_to_dxl_translator->TranslateQueryToDXL();
// 	CDXLNodeArray *query_output_dxlnode_array =
// 		query_to_dxl_translator->GetQueryOutputCols();
// 	CDXLNodeArray *cte_dxlnode_array =
// 		query_to_dxl_translator->GetCTEs();
    
//     CWStringDynamic str(mp);
// 	COstreamString oss(&str);

//     CDXLUtils::SerializeQuery(mp, oss, query_dxl, query_output_dxlnode_array, cte_dxlnode_array, true, true);

//     GPOS_TRACE(str.GetBuffer());

// }

int main(int argc, char ** argv)
{
    std::string query_str = "select * from test.test;";
    duckdb::PostgresParser::SetPreserveIdentifierCase(false);

    auto shared_context = DB::Context::createShared();
    auto global_context = DB::Context::createGlobal(shared_context.get());
    //DB::RelationProvider::Init(std::const_pointer_cast<const DB::Context>(global_context));
    auto const_context = std::const_pointer_cast<const DB::Context>(global_context);
    DB::DatabaseCatalog::init(global_context);
    DB::RelationProvider::Init(const_context);
    DB::RelationProvider::mockTestData();

    duckdb::PostgresParser parser;
    parser.Parse(query_str);
    if (!parser.success || !parser.parse_tree)
    {
        std::cout << "Failed!" << std::endl;
        return -1;
    }

    InitGPOPT();
    
    for (auto entry = parser.parse_tree->head; entry != nullptr; entry = entry->next)
    {
        auto query_node = (PGNode *)entry->data.ptr_value;
        if (query_node->type == T_PGRawStmt)
        {
            auto raw_stmt = (PGRawStmt *)query_node;
            auto ps_stat = std::make_shared<PGParseState>();
            auto query = DB::SelectParser::transformStmt(ps_stat.get(), raw_stmt->stmt);
            optimize2(query);
        }
        else
        {
            std::cout << "Unknown statement!" << std::endl;
        }
    }
    return 0;
}
