#include <iostream>
#include <Interpreters/orcaopt/SelectParser.h>
#include <Interpreters/orcaopt/provider/RelationProvider.h>
#include <Interpreters/Context.h>
#include <postgres_parser.hpp>
#include <Interpreters/orcaopt/translator/CTranslatorQueryToDXL.h>
#include <Interpreters/orcaopt/translator/CTranslatorRelcacheToDXL.h>
#include <gpos/common/CAutoP.h>
#include <gpos/memory/CAutoMemoryPool.h>
#include <gpopt/mdcache/CMDCache.h>
#include <naucrates/dxl/CDXLUtils.h>
#include <naucrates/md/IMDProvider.h>

using namespace duckdb_libpgquery;
using namespace gpmd;
using namespace gpdxl;
using namespace gpopt;

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

void optimize(PGQuery * query)
{
    if (CMemoryPoolManager::Init() != GPOS_OK)
    {
        std::cout << "can not init memory pool manager!" << std::endl;
        return;
    }
    auto mem_pool_mgr = CMemoryPoolManager::GetMemoryPoolMgr();
    if (!mem_pool_mgr)
    {
        std::cout << "can not get memory pool manager!" << std::endl;
        return;
    }

    // create memory pool
	CAutoMemoryPool amp;
	CMemoryPool *mp = amp.Pmp();

    // set up relcache MD provider
	CMDProviderRelcache *relcache_provider =
		GPOS_NEW(mp) CMDProviderRelcache(mp);
    
    CSystemId default_sysid(IMDId::EmdidGPDB, GPOS_WSZ_STR_LENGTH("GPDB"));

    CMDAccessor mda(mp, CMDCache::Pcache(), default_sysid,
		relcache_provider);

	//CAutoP<CTranslatorQueryToDXL> query_to_dxl_translator = 
    //    CTranslatorQueryToDXL::QueryToDXLInstance(mp, &mda, query);
    auto query_to_dxl_translator = 
        CTranslatorQueryToDXL::QueryToDXLInstance(mp, &mda, query);
    
    CDXLNode *query_dxl =
		query_to_dxl_translator->TranslateQueryToDXL();
	CDXLNodeArray *query_output_dxlnode_array =
		query_to_dxl_translator->GetQueryOutputCols();
	CDXLNodeArray *cte_dxlnode_array =
		query_to_dxl_translator->GetCTEs();
    
    CWStringDynamic str(mp);
	COstreamString oss(&str);

    CDXLUtils::SerializeQuery(mp, oss, query_dxl, query_output_dxlnode_array, cte_dxlnode_array, true, true);

    GPOS_TRACE(str.GetBuffer());

}

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
            optimize(query);
        }
        else
        {
            std::cout << "Unknown statement!" << std::endl;
        }
    }
    return 0;
}
