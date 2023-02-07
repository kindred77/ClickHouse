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
        gpos::ULONG m_query_level_)
    : context(std::move(context_))
    , memory_pool(context->memory_pool)
    , metadata_accessor(std::move(metadata_accessor_))
    , m_query_level(m_query_level_)
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
TranslatorScalarToDXL::translateVarToDXL(ASTPtr expr,
    const MappingVarColId *var_colid_mapping,
    gpmd::CMDIdGPDB & mid_type_ret)
{
    if (var->varattno == 0)
	{
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature,
				   GPOS_WSZ_LIT("Whole-row variable"));
	}

    CWStringBase col_name;
    ULONG col_id;
    Oid type_oid;
    int typemod;
    int attno;
    if (!var_colid_mapping->GetColInfo(m_query_level, expr,// m_op_type,
            col_name, col_id, type_oid, typemod, attno))
    {
        GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLError,
				   GPOS_WSZ_LIT("No variable"));
    }
	// column name
	//const CWStringBase *str =
		//var_colid_mapping->GetOptColName(m_query_level, expr, m_op_type);

	// column id
	//ULONG id;

	if (attno != 0 || EpspotIndexScan == m_op_type ||
		EpspotIndexOnlyScan == m_op_type)
	{
		//id = var_colid_mapping->GetColId(m_query_level, expr, m_op_type);
	}
	else
	{
		if (m_context == NULL)
			GPOS_RAISE(
				gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature,
				GPOS_WSZ_LIT(
					"Var with no existing mapping in a stand-alone context"));
		col_id = m_context->m_colid_counter->next_id();
	}
	CMDName *mdname = GPOS_NEW(m_mp) CMDName(m_mp, &col_name);

	// create a column reference for the given var
	CDXLColRef *dxl_colref = GPOS_NEW(m_mp)
		CDXLColRef(m_mp, mdname, col_id, GPOS_NEW(m_mp) CMDIdGPDB(type_oid),
				   typemod);

	// create the scalar ident operator
	CDXLScalarIdent *scalar_ident =
		GPOS_NEW(m_mp) CDXLScalarIdent(m_mp, dxl_colref);

	// create the DXL node holding the scalar ident operator
	CDXLNode *dxlnode = GPOS_NEW(m_mp) CDXLNode(m_mp, scalar_ident);

    mid_type_ret = type_oid;

    return dxlnode;
}

BYTE*
TranslatorScalarToDXL::extractByteArrayFromDatum(CMemoryPool *mp,
												 //const IMDType *md_type,
                                                 BOOL is_null,
												 const ASTLiteral* literal)
{
    BYTE * bytes = NULL;
    if (is_null)
    {
        return bytes;
    }
    size_t size;
    switch(literal->value.getType())
    {
    case Types::Float64:
        size = sizeof(literal->value.get<Float64>());
        bytes = GPOS_NEW_ARRAY(mp, BYTE, size);
        clib::Memcpy(bytes, &(literal->value.get<Float64>()), size);
        break;
    case Types::Int64:
        size = sizeof(literal->value.get<Int64>());
        bytes = GPOS_NEW_ARRAY(mp, BYTE, size);
        clib::Memcpy(bytes, &(literal->value.get<Int64>()), size);
        break;
    case Types::UInt64:
        size = sizeof(literal->value.get<UInt64>());
        bytes = GPOS_NEW_ARRAY(mp, BYTE, size);
        clib::Memcpy(bytes, &(literal->value.get<UInt64>()), size);
        break;
    case Types::String:
        size = sizeof(literal->value.get<String>());
        bytes = GPOS_NEW_ARRAY(mp, BYTE, size);
        clib::Memcpy(bytes, &(literal->value.get<String>()), size);
        break;
    default:
        GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLError,
				   GPOS_WSZ_LIT("Unsupported field type."));
    }

    return bytes;
}

gpos::CDouble
TranslatorScalarToDXL::extractDoubleValueFromDatum(//CMemoryPool *mp,
												   const BOOL is_null,
                                                   //BYTE *bytes,
												   const ASTLiteral* literal)
{
    GPOS_ASSERT(literal->value.getType() == Types::Float64);

    double d = 0;

	if (is_null)
	{
		return CDouble(d);
	}

    return CDouble(literal->value.get<Float64>());
}

gpos::LINT
TranslatorScalarToDXL::extractLintValueFromDatum(const BOOL is_null,
                                        const ASTLiteral* literal)
{
    GPOS_ASSERT(literal->value.getType() == Types::Int64
        || literal->value.getType() == Types::UInt64
        || literal->value.getType() == Types::String);

    LINT lint_value = 0;
	if (is_null)
	{
		return lint_value;
	}

    switch(literal->value.getType())
    {
    case Types::Int64:
        return literal->value.get<Int64>();
    case Types::UInt64:
        return (LINT)(literal->value.get<UInt64>());
    //TODO hash string
    case Types::String:
        return lint_value;
    default:
        GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLError,
				   GPOS_WSZ_LIT("Unsupported field type."));
    }
}

gpos::ULONG
TranslatorScalarToDXL::getDatumLength(BOOL is_null, const ASTLiteral* literal)
{
    size_t size;
    if (is_null)
    {
        return 0;
    }
    switch(literal->value.getType())
    {
    case Types::Float64:
        size = sizeof(literal->value.get<Float64>());
        break;
    case Types::Int64:
        size = sizeof(literal->value.get<Int64>());
        break;
    case Types::UInt64:
        size = sizeof(literal->value.get<UInt64>());
        break;
    case Types::String:
        size = sizeof(literal->value.get<String>());
        break;
    default:
        GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLError,
				   GPOS_WSZ_LIT("Unsupported field type."));
    }

    return (gpos::ULONG)size;
}

gpdxl::CDXLDatum *
TranslatorScalarToDXL::translateGenericDatumToDXL(CMemoryPool *mp,
												   const IMDType *md_type,
												   INT type_modifier,
												   BOOL is_null,
												   const ASTLiteral* datum)
{
	CMDIdGPDB *mdid_old = CMDIdGPDB::CastMdid(md_type->MDId());
	CMDIdGPDB *mdid = GPOS_NEW(mp) CMDIdGPDB(*mdid_old);

	//BYTE *bytes = extractByteArrayFromDatum(mp, md_type, is_null, len, datum);
    BYTE *bytes = extractByteArrayFromDatum(mp, is_null, datum);
	gpos::ULONG length = getDatumLength(is_null, datum);

	CDouble double_value(0);
	if (CMDTypeGenericGPDB::HasByte2DoubleMapping(mdid))
	{
		double_value = extractDoubleValueFromDatum(is_null, datum);
	}

	LINT lint_value = 0;
	if (CMDTypeGenericGPDB::HasByte2IntMapping(md_type))
	{
		lint_value = extractLintValueFromDatum(is_null, datum);
	}

	return CMDTypeGenericGPDB::CreateDXLDatumVal(
		mp, mdid, md_type, type_modifier, is_null, bytes, length, lint_value,
		double_value);
}

gpdxl::CDXLNode * 
TranslatorScalarToDXL::translateConstToDXL(
        const ASTPtr *expr,
        const CMappingVarColId *  // var_colid_mapping
    )
{
    GPOS_ASSERT(IsA(expr, ASTLiteral));
    auto * literal = typeid_cast<ASTLiteral>(expr);
	//const Const *constant = (Const *) expr;

    //CMDIdGPDB *mdid = GPOS_NEW(mp) CMDIdGPDB(constant->consttype);
	//const IMDType *md_type = mda->RetrieveType(mdid);
	//mdid->Release();
    const IMDType *md_type = TypeProvider::getType(literal->value.getType());

	// translate gpdb datum into a DXL datum
    CDXLDatum *datum_dxl = nullptr;

    switch (md_type->GetDatumType())
    {
        case IMDType::EtiInt2:
        {
            GPOS_ASSERT(md_type->IsPassedByValue());
            CMDIdGPDB *mdid_old = CMDIdGPDB::CastMdid(md_type->MDId());
            CMDIdGPDB *mdid = GPOS_NEW(mp) CMDIdGPDB(*mdid_old);

            datum_dxl = GPOS_NEW(mp)
                CDXLDatumInt2(mp, mdid, literal->value.isNull(), literal->value.get<Int16>());
            break;
        }
        case IMDType::EtiInt4:
        {
            GPOS_ASSERT(md_type->IsPassedByValue());
            CMDIdGPDB *mdid_old = CMDIdGPDB::CastMdid(md_type->MDId());
            CMDIdGPDB *mdid = GPOS_NEW(mp) CMDIdGPDB(*mdid_old);

            datum_dxl = GPOS_NEW(mp)
                CDXLDatumInt4(mp, mdid, literal->value.isNull(), literal->value.get<Int32>());
            break;
        }
        case IMDType::EtiInt8:
        {
            GPOS_ASSERT(md_type->IsPassedByValue());
            CMDIdGPDB *mdid_old = CMDIdGPDB::CastMdid(md_type->MDId());
            CMDIdGPDB *mdid = GPOS_NEW(mp) CMDIdGPDB(*mdid_old);

            datum_dxl = GPOS_NEW(mp)
                CDXLDatumInt8(mp, mdid, literal->value.isNull(), literal->value.get<Int64>());
            break;
        }
        case IMDType::EtiBool:
        {
            GPOS_ASSERT(md_type->IsPassedByValue());
            CMDIdGPDB *mdid_old = CMDIdGPDB::CastMdid(md_type->MDId());
            CMDIdGPDB *mdid = GPOS_NEW(mp) CMDIdGPDB(*mdid_old);

            datum_dxl = GPOS_NEW(mp)
                CDXLDatumBool(mp, mdid, literal->value.isNull(), literal->value.get<bool>());
            break;
        }
        // case IMDType::EtiOid:
        // {
        //     GPOS_ASSERT(md_type->IsPassedByValue());
        //     CMDIdGPDB *mdid_old = CMDIdGPDB::CastMdid(md_type->MDId());
        //     CMDIdGPDB *mdid = GPOS_NEW(mp) CMDIdGPDB(*mdid_old);

        //     datum_dxl = GPOS_NEW(mp)
        //         CDXLDatumBool(mp, mdid, literal->value.isNull(), gpdb::OidFromDatum(datum));
        //     break;
        // }
        default:
        {
            datum_dxl = translateGenericDatumToDXL(mp, md_type, -1, literal->value.isNull(),
										  literal);
        }
    }

    gpdxl::CDXLNode *dxlnode =
		GPOS_NEW(m_mp) CDXLNode(m_mp, GPOS_NEW(m_mp) CDXLScalarConstValue(
										  m_mp, datum_dxl));

	return dxlnode;
}

gpdxl::CDXLNode *
TranslatorScalarToDXL::translateAndOrNotExprToDXL(ASTPtr expr,
    const MappingVarColId *var_colid_mapping，
    gpmd::CMDIdGPDB & mid_type_ret)
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
            gpmd::CMDIdGPDB mid_type_ret_child;
            gpdxl::CDXLNode child_node = translateExprToDXL(exp, var_colid_mapping, mid_type_ret_child);
            dxlnode->AddChild(child_node);
        }
    }

    return dxlnode;
}

gpdxl::CDXLNode *
TranslatorScalarToDXL::translateOpExprToDXL(const ASTPtr expr,
    const MappingVarColId *var_colid_mapping,
    gpmd::CMDIdGPDB & mid_type_ret)
{
    if (auto & func = expr->as<ASTFunction &>())
    {
        gpdxl::CDXLScalarComp *dxlop = nullptr;
        std::vector<gpdxl::CDXLNode *> arg_dxl_nodes;
        std::vector<OID> arg_type_oids;
        //comparing operator
        if (func.name == "equals")
        {
            if (auto * args = func.arguments->as<ASTExpressionList *>())
            {
                GPOS_ASSERT(args->children.size()==2);
                dxlop = GPOS_NEW(memory_pool) CDXLScalarComp(
		            memory_pool, gpos::CMDIdGPDB(OID(1)), GPOS_NEW(memory_pool) CWStringConst(L"=");

                for (auto arg : args->children)
                {
                    gpmd::CMDIdGPDB type_ret;
                    gpdxl::CDXLNode * arg_dxl_node = translateExprToDXL(arg, var_colid_mapping, type_ret);
                    GPOS_ASSERT(nullptr != arg_dxl_node);
                    arg_dxl_nodes.push_back(arg_dxl_node);
                    arg_type_oids.push_back(type_ret.Oid());
                }
            }
            
        }
        else if (func.name == "notEquals")
        {
            if (auto * args = func.arguments->as<ASTExpressionList *>())
            {
                GPOS_ASSERT(args->children.size()==2);
                dxlop = GPOS_NEW(memory_pool) CDXLScalarComp(
		            memory_pool, gpos::CMDIdGPDB(OID(2)), GPOS_NEW(memory_pool) CWStringConst(L"!="));

                for (auto arg : args->children)
                {
                    gpmd::CMDIdGPDB type_ret;
                    gpdxl::CDXLNode * arg_dxl_node = translateExprToDXL(arg, var_colid_mapping, type_ret);
                    GPOS_ASSERT(nullptr != arg_dxl_node);
                    arg_dxl_nodes.push_back(arg_dxl_node);
                    arg_type_oids.push_back(type_ret.Oid());
                }
            }
        }
        //+
        else if (func.name == "plus")
        {
            if (auto * args = func.arguments->as<ASTExpressionList *>())
            {
                for (auto arg : args->children)
                {
                    gpmd::CMDIdGPDB type_ret;
                    gpdxl::CDXLNode * arg_dxl_node = translateExprToDXL(arg, var_colid_mapping, type_ret);
                    GPOS_ASSERT(nullptr != arg_dxl_node);
                    arg_dxl_nodes.push_back(arg_dxl_node);
                    arg_type_oids.push_back(type_ret.Oid());
                }

                //get the operator according arguments
                auto val_pair = scalar_op_provider->
                    getScalarOperatorByNameAndArgTypes("+", arg_type_oids[0], arg_type_oids[1]);

                dxlop = GPOS_NEW(memory_pool)
		            CDXLScalarOpExpr(memory_pool, gpos::CMDIdGPDB(val_pair.first),
                        /*return_type_mdid*/ val_pair.second->GetResultTypeMdid(),
						GPOS_NEW(memory_pool) CWStringConst(L"+"));
            }
        }
        //-
        else if (func.name == "minus")
        {
            if (auto * args = func.arguments->as<ASTExpressionList *>())
            {
                
            }
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

        mid_type_ret = dxlop->GetReturnTypeMdId();
        gpdxl::CDXLNode * dxlnode = GPOS_NEW(memory_pool) CDXLNode(memory_pool, dxlop);
        for (auto arg_node : arg_dxl_nodes)
        {
            dxlnode->AddChild(arg_node);
        }

        return dxlnode;
    }
    else 
    {
        throw Exception("The operator is not a function.", ErrorCodes::SYNTAX_ERROR);
    }
}

gpdxl::CDXLNode *
TranslatorScalarToDXL::translateExprToDXL(
    ASTPtr expr,
    const MappingVarColId *var_colid_mapping,
    gpmd::CMDIdGPDB & mid_type_ret)
{
    static const STranslatorElem translators[] = {
        {T_OpExpr, &TranslatorScalarToDXL::translateOpExprToDXL},
        {T_BoolExpr, &TranslatorScalarToDXL::translateAndOrNotExprToDXL},
        {T_BoolExpr, &TranslatorScalarToDXL::translateVarToDXL},
    };

    
}

TranslatorScalarToDXL::~TranslatorScalarToDXL()
{

}

}

