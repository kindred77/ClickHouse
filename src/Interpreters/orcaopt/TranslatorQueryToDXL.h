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

class TranslatorQueryToDXL {

private:
    gpopt::CMDAccessor * metadata_accessor;
    ASTPtr select_query;
    CMemoryPool * memory_pool;
    ContextQueryToDXL * context;
    Poco::Logger * log;
    ASTsArr * splitWithCommaJoin(const ASTTablesInSelectQuery & tables_in_select);
public:
    TranslatorQueryToDXL();

    TranslatorQueryToDXL(
    	ContextQueryToDXL * context_,
        gpopt::CMDAccessor * metadata_accessor_,
        ASTPtr select_query_);

    virtual ~TranslatorQueryToDXL();

    gpdxl::CDXLNode * translateSimpleSelectToDXL();

    gpdxl::CDXLNode * translateExprToDXL(ASTPtr expr);

    gpdxl::CDXLNode * translateTableExpressionToDXL(const ASTTableExpression * table_expression);
    gpdxl::CDXLNode * translateTablesInSelectQueryElementToDXL(
            const gpdxl::CDXLNode * previous_node,
            const ASTTablesInSelectQueryElement * table_ele_in_select);
    gpdxl::CDXLNode * translateFromAndWhereToDXL(
            const ASTTablesInSelectQuery * tables_in_select);


    static TranslatorQueryToDXL *QueryToDXLInstance(CMemoryPool * memory_pool_,
            gpdxl::CMDAccessor * md_accessor_,
            ASTPtr query);
};

}
