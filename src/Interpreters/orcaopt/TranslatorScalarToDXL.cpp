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

gpdxl::EdxlBoolExprType
TranslatorScalarToDXL::EdxlbooltypeFromGPDBBoolType(
	BoolExprType boolexprtype) const
{
	static ULONG mapping[][2] = {
		{NOT_EXPR, Edxlnot},
		{AND_EXPR, Edxland},
		{OR_EXPR, Edxlor},
	};

	EdxlBoolExprType type = EdxlBoolExprTypeSentinel;

	const ULONG arity = GPOS_ARRAY_SIZE(mapping);
	for (ULONG ul = 0; ul < arity; ul++)
	{
		ULONG *elem = mapping[ul];
		if ((ULONG) boolexprtype == elem[0])
		{
			type = (EdxlBoolExprType) elem[1];
			break;
		}
	}

	GPOS_ASSERT(EdxlBoolExprTypeSentinel != type && "Invalid bool expr type");

	return type;
}

gpdxl::CDXLNode *
TranslatorScalarToDXL::translateAndOrNotExprToDXL(ASTPtr expr)
{
    gpdxl::CDXLNode * dxlnode = nullptr;
    if (auto & func = expr->as<ASTFunction &>())
    {
        const auto * list = func.arguments->as<ASTExpressionList>();
        gpdxl::EdxlBoolExprType type = gpdxl::EdxlBoolExprTypeSentinel;
        if (func.name == "and" || func.name == "or")
        {
            if (list->children.size() < 2)
            {
                throw Exception("Boolean Expression (OR / AND): Incorrect Number of Children.", ErrorCodes::SYNTAX_ERROR);
            }

            if (func.name == "and")
            {
                type = gpdxl::Edxland;
            }
            else
            {
                type = gpdxl::Edxlor;
            }
        }
        else if(func.name == "not")
        {
            if (list->children.size() != 1)
            {
                throw Exception("Boolean Expression (NOT): Incorrect Number of Children .", ErrorCodes::SYNTAX_ERROR);
            }
            type = gpdxl::Edxlnot;
        }

        dxlnode = GPOS_NEW(memory_pool)gpdxl::CDXLNode(memory_pool,
            GPOS_NEW(memory_pool) gpdxl::CDXLScalarBoolExpr(memory_pool, type));
        for (auto const * exp : list->children)
        {
            gpdxl::CDXLNode child_node = translateExprToDXL(exp);
            dxlnode->AddChild(child_node);
        }
    }

    return dxlnode;
}

gpdxl::CDXLNode *
TranslatorScalarToDXL::translateOpExprToDXL(const ASTPtr expr)
{
    if (auto & func = expr->as<ASTFunction &>())
    {
        //+
        if (func.name == "plus")
        {
            if (auto * args = func.arguments->as<ASTExpressionList *>())
            {
                for (auto arg : args->children)
                {
                    gpdxl::CDXLNode * arg_dxl_node = translateExprToDXL(arg);
                }
            }
        }
        //-
        else if (func.name == "minus")
        {

        }
        //*
        else if (func.name == "multiply")
        {

        }
        ///
        else if (func.name == "divide")
        {

        }
        //%
        else if (func.name == "modulo")
        {

        }
        //= or ==
        else if (func.name == "equals")
        {

        }
        //!= or <>
        else if (func.name == "notEquals")
        {

        }
    }
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

