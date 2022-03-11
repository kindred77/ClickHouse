/*
 * TranslatorQueryToDXL.cpp
 *
 *  Created on: Jan 11, 2022
 *      Author: kindred
 */

#include <Interpreters/orcaopt/TranslatorScalarToDXL.h>

#include <naucrates/dxl/operators/CDXLLogicalJoin.h>
#include <naucrates/dxl/operators/CDXLLogicalGet.h>
#include <naucrates/dxl/operators/CDXLOperator.h>
#include <naucrates/dxl/operators/CDXLScalarBoolExpr.h>

#include <Parsers/ASTFunction.h>

#include <common/logger_useful.h>

namespace DB
{

TranslatorScalarToDXL *
TranslatorScalarToDXL::ScalarToDXLInstance(CMemoryPool * memory_pool_,
        gpdxl::CMDAccessor * md_accessor_,
        ASTPtr query)
{
    ContextQueryToDXL *context = GPOS_NEW(memory_pool_) ContextQueryToDXL(memory_pool_);

    return GPOS_NEW(context->memory_pool)
            TranslatorQueryToDXL(context, md_accessor_,
                              //NULL,    // var_colid_mapping,
                              query
                              //0,    // query_level
                              //false,  // is_top_query_dml
                              //NULL      // query_level_to_cte_map
        );
}

TranslatorScalarToDXL::TranslatorScalarToDXL()
        : log(&Poco::Logger::get("TranslatorScalarToDXL"))
{
    LOG_TRACE(log, "----0000----");
}

TranslatorScalarToDXL::TranslatorScalarToDXL(
        ContextQueryToDXL * context_,
        gpopt::CMDAccessor * metadata_accessor_,
        ASTPtr select_query_)
    : context(std::move(context_))
    , memory_pool(context->memory_pool)
    , metadata_accessor(std::move(metadata_accessor_))
    , select_query(std::move(select_query_))
    , log(&Poco::Logger::get("TranslatorScalarToDXL"))
{
    LOG_TRACE(log, "----111----");
}

gpdxl::CDXLNode *
TranslatorScalarToDXL::translateAndOrNotExprToDXL(ASTPtr expr)
{
    if (auto & func = expr->as<ASTFunction &>())
    {
        const auto * list = func.arguments->as<ASTExpressionList>();
        if (func.name == "and" || func.name == "or")
        {
            if (list->children.size() < 2)
            {
                throw Exception("Boolean Expression (OR / AND): Incorrect Number of Children.", ErrorCodes::SYNTAX_ERROR);
            }
        }
        else if(func.name == "not")
        {
            if (list->children.size() != 1)
            {
                throw Exception("Boolean Expression (NOT): Incorrect Number of Children .", ErrorCodes::SYNTAX_ERROR);
            }
        }

        gpdxl::CDXLNode *dxlnode = GPOS_NEW(memory_pool)
                gpdxl::CDXLNode(memory_pool, GPOS_NEW(memory_pool) gpdxl::CDXLScalarBoolExpr(memory_pool, type));
        for (auto const * exp : list->children)
        {
            gpdxl::CDXLNode child_node = translateExprToDXL(exp);
            dxlnode->AddChild(child_node);
        }
    }
}

gpdxl::CDXLNode *
TranslatorScalarToDXL::translateOpExprToDXL(const ASTPtr expr)
{

}

gpdxl::CDXLNode *
TranslatorScalarToDXL::translateExprToDXL(ASTPtr expr)
{
    static const STranslatorElem translators[] = {
            {T_OpExpr, &TranslatorScalarToDXL::translateOpExprToDXL},
            {T_BoolExpr, &TranslatorScalarToDXL::translateAndOrNotExprToDXL},
    };
}

TranslatorScalarToDXL::~TranslatorScalarToDXL()
{

}

}

