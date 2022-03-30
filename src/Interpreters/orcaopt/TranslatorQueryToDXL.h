#pragma once

#include <gpopt/mdcache/CMDAccessor.h>

#include <naucrates/dxl/CDXLUtils.h>

#include <Parsers/IAST_fwd.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>

#include <Interpreters/orcaopt/ContextQueryToDXL.h>
#include <Interpreters/orcaopt/MappingVarColId.h>
#include <Interpreters/orcaopt/TranslatorScalarToDXL.h>

namespace Poco
{
class Logger;
}

namespace DB
{

class gpdxl::CDXLNode;

using ASTsArr = std::vector<ASTs>;

class TranslatorQueryToDXL
{

private:
    gpopt::CMDAccessor * metadata_accessor;
    ASTPtr select_query;
    CMemoryPool * memory_pool;
    ContextQueryToDXL * context;
    Poco::Logger * log;
    // scalar translator used to convert scalar operation into DXL.
	TranslatorScalarToDXL *m_scalar_translator;

	// holds the var to col id information mapping
	MappingVarColId *m_var_to_colid_map;
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
            const ASTTablesInSelectQuery * tables_in_select,
            const ASTPtr where_expression);


    static TranslatorQueryToDXL *QueryToDXLInstance(CMemoryPool * memory_pool_,
            gpdxl::CMDAccessor * md_accessor_,
            ASTPtr query);
};

}
