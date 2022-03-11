#pragma once

#include <gpopt/mdcache/CMDAccessor.h>

#include <naucrates/dxl/CDXLUtils.h>

#include <Parsers/IAST_fwd.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>

#include <Interpreters/orcaopt/ContextQueryToDXL.h>

namespace Poco
{
class Logger;
}

namespace DB
{

class gpdxl::CDXLNode;

using ASTsArr = std::vector<ASTs>;

class TranslatorScalarToDXL {

private:
    gpopt::CMDAccessor * metadata_accessor;
    ASTPtr select_query;
    CMemoryPool * memory_pool;
    ContextQueryToDXL * context;
    Poco::Logger * log;

public:
    TranslatorScalarToDXL();

    TranslatorScalarToDXL(
    	ContextQueryToDXL * context_,
        gpopt::CMDAccessor * metadata_accessor_,
        ASTPtr select_query_);

    virtual ~TranslatorScalarToDXL();

    gpdxl::CDXLNode * translateExprToDXL(ASTPtr expr);

    static TranslatorScalarToDXL *ScalarToDXLInstance(CMemoryPool * memory_pool_,
            gpdxl::CMDAccessor * md_accessor_,
            ASTPtr query);
};

}
