/*
 * TranslatorQueryToDXL.cpp
 *
 *  Created on: Jan 11, 2022
 *      Author: kindred
 */

#include <Interpreters/orcaopt/TranslatorQueryToDXL.h>

#include <naucrates/dxl/operators/CDXLLogicalJoin.h>
#include <naucrates/dxl/operators/CDXLLogicalGet.h>
#include <naucrates/dxl/operators/CDXLOperator.h>

#include <common/logger_useful.h>

namespace DB
{

TranslatorQueryToDXL *
TranslatorQueryToDXL::QueryToDXLInstance(CMemoryPool * memory_pool_,
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

TranslatorQueryToDXL::TranslatorQueryToDXL()
        : log(&Poco::Logger::get("TranslatorQueryToDXL"))
{
    LOG_TRACE(log, "----0000----");
}

TranslatorQueryToDXL::TranslatorQueryToDXL(
        ContextQueryToDXL * context_,
        gpopt::CMDAccessor * metadata_accessor_,
        ASTPtr select_query_)
    : context(std::move(context_))
    , memory_pool(context->memory_pool)
    , metadata_accessor(std::move(metadata_accessor_))
    , select_query(std::move(select_query_))
    , log(&Poco::Logger::get("TranslatorQueryToDXL"))
{
    LOG_TRACE(log, "----111----");
}

gpdxl::CDXLNode *
TranslatorQueryToDXL::translateSimpleSelectToDXL()
{

}

ASTsArr *
TranslatorQueryToDXL::splitWithCommaJoin(const ASTTablesInSelectQuery & tables_in_select)
{
    for (const auto & child : tables_in_select.children)
    {
        if (const auto * tables_element = child->as<ASTTablesInSelectQueryElement>())
        {
            if ( auto table_join = tables_element->table_join
                    && table_join->kind == ASTTableJoin::Kind::Comma)
            {
                is_have_comma_join = true;
                break;

            }
        }
    }
}

gpdxl::CDXLNode *
TranslatorQueryToDXL::translateExprToDXL(ASTPtr expr)
{

}

gpdxl::CDXLNode *
TranslatorQueryToDXL::translateTableExpressionToDXL(const ASTTableExpression * table_expression)
{
    gpdxl::CDXLNode * result = nullptr;
    if (table_expression->database_and_table_name)
    {
        result = GPOS_NEW(memory_pool) gpdxl::CDXLLogicalGet(memory_pool, dxl_table_descr);
    }
    else if (table_expression->subquery)
    {

    }
    else if (table_expression->table_function)
    {

    }
}

gpdxl::CDXLNode *
TranslatorQueryToDXL::translateTablesInSelectQueryElementToDXL(
        const gpdxl::CDXLNode * previous_node,
        const ASTTablesInSelectQueryElement * table_ele_in_select)
{
    gpdxl::CDXLNode * table_expression_node = translateTableExpressionToDXL(table_ele_in_select->table_expression);
    if (const auto table_join = table_ele_in_select->table_join
            && table_join->kind != ASTTableJoin::Kind::Comma)
    {
        gpdxl::EdxlJoinType join_type = gpdxl::EdxljtInner;
        switch (table_join->kind)
        {
        case ASTTableJoin::Kind::Left:
            join_type = gpdxl::EdxljtLeft;
            break;
        case ASTTableJoin::Kind::Right:
            join_type = gpdxl::EdxljtRight;
            break;
        case ASTTableJoin::Kind::Full:
            join_type = gpdxl::EdxljtFull;
            break;
        }
        gpdxl::CDXLNode * join_node = GPOS_NEW(memory_pool) gpdxl::CDXLNode(memory_pool,
                GPOS_NEW(memory_pool) gpdxl::CDXLLogicalJoin(memory_pool, join_type));
        if (previous_node) join_node->AddChild(previous_node);
        join_node->AddChild(table_expression_node);
        return join_node;
    }
    return table_expression_node;
}

gpdxl::CDXLNode *
TranslatorQueryToDXL::translateFromAndWhereToDXL(const ASTTablesInSelectQuery * tables_in_select)
{
    gpdxl::CDXLNode * comma_join_nodes = nullptr;
    gpdxl::CDXLNode * normal_join_node = nullptr;
    for (const auto & child : tables_in_select->children)
    {
        if (const auto * tables_element = child->as<ASTTablesInSelectQueryElement>())
        {
            if (auto table_join = tables_element->table_join
                    && table_join->kind == ASTTableJoin::Kind::Comma)
            {
                if (!normal_join_node) throw Exception("No table element before comma.", ErrorCodes::SYNTAX_ERROR);
                if (!comma_join_nodes) comma_join_nodes =
                        GPOS_NEW(memory_pool) gpdxl::CDXLNode(memory_pool,
                                GPOS_NEW(memory_pool) gpdxl::CDXLLogicalJoin(memory_pool, gpdxl::EdxljtInner));
                comma_join_nodes->AddChild(normal_join_node);
            }
            normal_join_node = translateTablesInSelectQueryElementToDXL(normal_join_node, tables_element);
        }
    }

    if (comma_join_nodes)
    {
        if (normal_join_node)
        {
            comma_join_nodes->AddChild(normal_join_node);
        }
    }
    //if (normal_join_node)
}

TranslatorQueryToDXL::~TranslatorQueryToDXL()
{

}

}

