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
        const MappingVarColId *var_colid_mapping,
        ASTPtr select_query_,
        ULONG query_level_)
    : context(std::move(context_))
    , memory_pool(context->memory_pool)
    , metadata_accessor(std::move(metadata_accessor_))
    , m_query_level(query_level_)
    , select_query(std::move(select_query_))
    , log(&Poco::Logger::get("TranslatorQueryToDXL"))
{
    LOG_TRACE(log, "----111----");

    if (var_colid_mapping)
		m_var_to_colid_map = var_colid_mapping->CopyMapColId(memory_pool);
	else
		m_var_to_colid_map = GPOS_NEW(memory_pool) CMappingVarColId(memory_pool);

    m_scalar_translator = GPOS_NEW(memory_pool)
		TranslatorScalarToDXL(context, metadata_accessor, m_query_level);
							   //m_query_level_to_cte_map, m_dxl_cte_producers);
}

gpdxl::CDXLNode *
TranslatorQueryToDXL::translateSimpleSelectToDXL()
{

}

gpdxl::CDXLNode *
TranslatorQueryToDXL::translateExprToDXL(ASTPtr expr)
{
    gpdxl::CDXLNode *scalar_dxlnode =
		m_scalar_translator->translateExprToDXL(expr, m_var_to_colid_map);
	GPOS_ASSERT(NULL != scalar_dxlnode);

	return scalar_dxlnode;
}

gpdxl::CDXLTableDescr *
getTableDescr(CMemoryPool *mp, CMDAccessor *md_accessor,
				CIdGenerator *id_generator,
				const ASTPtr database_and_table_name,
				BOOL *is_distributed_table	// output
)
{
    auto pair = StorageProvider::getPairByDBAndTableName(database_and_table_name);
    if (!pair)
    {
        throw Exception("Can not find table.", ErrorCodes::SYNTAX_ERROR);
    }
    const OID rel_oid = pair.first;

    gpmd::CMDIdGPDB *mdid = GPOS_NEW(mp) gpmd::CMDIdGPDB(rel_oid);

	const gpmd::IMDRelation *rel = md_accessor->RetrieveRel(mdid);

	// look up table name
	const CWStringConst *tablename = rel->Mdname().GetMDName();
	gpmd::CMDName *table_mdname = GPOS_NEW(mp) gpmd::CMDName(mp, tablename);

	gpdxl::CDXLTableDescr *table_descr =
		GPOS_NEW(mp) gpdxl::CDXLTableDescr(mp, mdid, table_mdname, rte->checkAsUser);

	const ULONG len = rel->ColumnCount();

    // add columns from md cache relation object to table descriptor
	for (ULONG ul = 0; ul < len; ul++)
	{
		const gpmd::IMDColumn *md_col = rel->GetMdCol(ul);
		if (md_col->IsDropped())
		{
			continue;
		}

		gpmd::CMDName *col = GPOS_NEW(mp) gpmd::CMDName(mp, md_col->Mdname().GetMDName());
		gpmd::CMDIdGPDB *col_type = gpmd::CMDIdGPDB::CastMdid(md_col->MdidType());
		col_type->AddRef();

		// create a column descriptor for the column
		gpmd::CDXLColDescr *dxl_col_descr = GPOS_NEW(mp)
			CDXLColDescr(mp, col, id_generator->next_id(), md_col->AttrNum(),
						 col_type, md_col->TypeModifier(), /* type_modifier */
						 false,							   /* fColDropped */
						 md_col->Length());
		table_descr->AddColumnDescr(dxl_col_descr);
	}

    return table_descr;
}

gpdxl::CDXLNode *
TranslatorQueryToDXL::translateTableExpressionToDXL(
    const ASTTableExpression * table_expression
)
{
    gpdxl::CDXLNode * node = nullptr;
    if (table_expression->database_and_table_name)
    {
        gpdxl::CDXLTableDescr *dxl_table_descr = getTableDescr(
		    memory_pool, m_md_accessor, m_context->m_colid_counter, 
            table_expression->database_and_table_name,
		    &m_context->m_has_distributed_tables);
        node = GPOS_NEW(memory_pool) gpdxl::CDXLLogicalGet(memory_pool, dxl_table_descr);

        gpdxl::CDXLNode *dxl_node = GPOS_NEW(m_mp) gpdxl::CDXLNode(memory_pool, node);
	    // make note of new columns from base relation
	    m_var_to_colid_map->LoadTblColumns(m_query_level,
            table_expression,
			dxl_table_descr
        );

        return dxl_node;
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
TranslatorQueryToDXL::translateFromAndWhereToDXL(
    const ASTTablesInSelectQuery * tables_in_select,
    const ASTPtr where_expression)
{
    gpdxl::CDXLNode * result = nullptr;

    gpdxl::CDXLNode * comma_join_nodes = nullptr;
    gpdxl::CDXLNode * normal_join_node = nullptr;
    //TODO for normal join mapping
    MappingVarColId * var_to_colid_map = nullptr;
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
                //TODO check the var col mapping here
                var_to_colid_map;
            }
            normal_join_node = translateTablesInSelectQueryElementToDXL(normal_join_node, tables_element, var_to_colid_map);
        }
    }

    gpdxl::CDXLNode *condition_dxlnode = nullptr;
    if (where_expression)
    {
        condition_dxlnode = translateExprToDXL(where_expression);
    }

    //the where expression is a part of comma join
    if (comma_join_nodes)
    {
        comma_join_nodes->AddChild(normal_join_node);
        if (!condition_dxlnode)
		{
			// A cross join (the scalar condition is true)
			condition_dxlnode = CreateDXLConstValueTrue();
		}
        comma_join_nodes->AddChild(condition_dxlnode);

        result = comma_join_nodes;
    }
    else
    {
        if (!normal_join_node)
        {
            normal_join_node = DXLDummyConstTableGet();
        }
        gpdxl::CDXLNode *select_dxlnode = GPOS_NEW(m_mp)
            CDXLNode(m_mp, GPOS_NEW(m_mp) CDXLLogicalSelect(m_mp));
        select_dxlnode->AddChild(condition_dxlnode);
        select_dxlnode->AddChild(normal_join_node);
        result = select_dxlnode;
    }

    return result;
}

TranslatorQueryToDXL::~TranslatorQueryToDXL()
{

}

}

