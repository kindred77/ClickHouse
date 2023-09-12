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
#include <naucrates/init.h>
#include <naucrates/dxl/CDXLUtils.h>
#include <naucrates/md/IMDProvider.h>

using namespace duckdb_libpgquery;
using namespace gpmd;
using namespace gpdxl;
using namespace gpopt;

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
	// 	LoadSearchStrategy(mp, optimizer_search_strategy_path);

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

			// ICostModel *cost_model = GetCostModel(mp, num_segments_for_costing);
			// COptimizerConfig *optimizer_config =
			// 	CreateOptimizerConfig(mp, cost_model);
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

            GPOS_TRACE(str.GetBuffer());


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
			//plan_dxl->Release();
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
            std::cout << "---nodeTag: " << nodeTag(raw_stmt->stmt) << std::endl;
            auto ps_stat = std::make_shared<PGParseState>();
            auto query = DB::SelectParser::transformStmt(ps_stat.get(), raw_stmt->stmt);
            std::cout << "----type: " << query->commandType << "---nodeTag: " << nodeTag(raw_stmt->stmt) << std::endl;
            optimize2(query);
        }
        else
        {
            std::cout << "Unknown statement!" << std::endl;
        }
    }
    return 0;
}
