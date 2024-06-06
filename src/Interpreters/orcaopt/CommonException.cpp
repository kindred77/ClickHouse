#include <Interpreters/orcaopt/CommonException.h>
#include <gpos/error/CMessage.h>
#include <gpos/error/CMessageRepository.h>

using namespace gpos;

namespace DB
{

bool CommonException::is_init = false;

GPOS_RESULT CommonException::EresExceptionInit(CMemoryPool *mp)
{
    CMessage rgmsg[ExmiSentinel] = {
        //provider
		CMessage(CException(ExmaProcProvider, ExmiNoProcFound),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN(
					 "Can not find proc, oid: %lld"),
				 1, GPOS_WSZ_WSZLEN("Can not find proc by the given oid.")),
		
		CMessage(CException(ExmaRelationProvider, ExmiUnknownType),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN(
					 "Unknow type: %s."),
				 1, GPOS_WSZ_WSZLEN("unknow type.")),

		CMessage(CException(ExmaRelationProvider, ExmiRelationProviderNotInit),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN(
					 "RelationProvider not inited, call Init() first."),
				 0, GPOS_WSZ_WSZLEN("relation provider not inited.")),
		
		CMessage(CException(ExmaRelationProvider, ExmiRelationNotFound),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN(
					 "Relation not found: %d."),
				 1, GPOS_WSZ_WSZLEN("relation not found.")),
		
		CMessage(CException(ExmaRelationProvider, ExmiLockModeNotSupported),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN(
					 "Lock mode not supported: %u."),
				 1, GPOS_WSZ_WSZLEN("Lock mode not supported.")),
		
		CMessage(CException(ExmaRelationProvider, ExmiInvalidAttNum),
				 CException::ExsevWarning,
				 GPOS_WSZ_WSZLEN(
					 "Invalid attnum %d for rangetable entry %s."),
				 2, GPOS_WSZ_WSZLEN("Invalid attnum for rangetable entry.")),
		
		CMessage(CException(ExmaRelationProvider, ExmiWrongObjectType),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN(
					 "Wrong object type: %s."),
				 1, GPOS_WSZ_WSZLEN("Wrong object type.")),
		
		CMessage(CException(ExmaRelationProvider, ExmiTypeNotFound),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN(
					 "Type not found: %d."),
				 1, GPOS_WSZ_WSZLEN("Type not found.")),
		
		CMessage(CException(ExmaRelationProvider, ExmiInvalidTypeForComposite),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN(
					 "Invalid typrelid for composite type: %d."),
				 1, GPOS_WSZ_WSZLEN("Invalid typrelid for composite type.")),
		
		CMessage(CException(ExmaRelationProvider, ExmiRecordTypeNotRegister),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN(
					 "Record type has not been registered."),
				 0, GPOS_WSZ_WSZLEN("Record type has not been registered.")),
		
		CMessage(CException(ExmaRelationProvider, ExmiTypeIsShell),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN(
					 "Type %s is only a shell."),
				 1, GPOS_WSZ_WSZLEN("type is a shell.")),
		
		CMessage(CException(ExmaRelationProvider, ExmiNoAvailableOutputFunction),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN(
					 "No output function available for type %s."),
				 1, GPOS_WSZ_WSZLEN("No output function available for type.")),
		
		CMessage(CException(ExmaRelationProvider, ExmiRangeTypeNotSupported),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN(
					 "Range type not supported yet: %u."),
				 1, GPOS_WSZ_WSZLEN("Range type not supported yet.")),
		
		CMessage(CException(ExmaRelationProvider, ExmiProArgModesNot1DCharArr),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN(
					 "proargmodes is not a 1-D char array."),
				 0, GPOS_WSZ_WSZLEN("proargmodes is not a 1-D char array.")),
		
		CMessage(CException(ExmaRelationProvider, ExmiNotAnyArray),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN(
					 "Argument declared \"anyarray\" is not an array but type %s."),
				 1, GPOS_WSZ_WSZLEN("Argument declared \"anyarray\" is not an array.")),
		
		CMessage(CException(ExmaRelationProvider, ExmiNotFoundArrayType),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN(
					 "Could not find array type for data type %s."),
				 1, GPOS_WSZ_WSZLEN("Could not find array type.")),
		
		CMessage(CException(ExmaRelationProvider, ExmiCouldNotDeterminePolymorphicType),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN(
					 "Could not determine polymorphic type because context isn't polymorphic."),
				 0, GPOS_WSZ_WSZLEN("could not determine polymorphic type because context isn't polymorphic.")),
		
		CMessage(CException(ExmaRelationProvider, ExmiCouldNotDetermineActualResultTypeForFunc),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN(
					 "Could not determine actual result type for function \"%s\" declared to return type %s."),
				 2, GPOS_WSZ_WSZLEN("Could not determine actual result type for function.")),
		
		CMessage(CException(ExmaRelationProvider, ExmiSetOfRecordTypeNotSupported),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN(
					 "Return set of record type not supported yet: %u."),
				 1, GPOS_WSZ_WSZLEN("Return set of record type not supported yet.")),
        
        //parser
        CMessage(CException(ExmaExprParser, ExmiUnrecognizedAExprKind),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN(
					 "Unrecognized A_Expr kind: %d"),
				 1, GPOS_WSZ_WSZLEN("Unrecognized A_Expr kind.")),
		
		CMessage(CException(ExmaFuncParser, ExmiComplexProjNotSupported),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN(
					 "Complex projection do not supported yet!"),
				 0, GPOS_WSZ_WSZLEN("Complex projection do not supported yet")),
		
		CMessage(CException(ExmaFuncParser, ExmiFuncTooManyArgs),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN(
					 "Cannot pass more than %d argument to a function."),
				 1, GPOS_WSZ_WSZLEN("too many argements")),
		
		CMessage(CException(ExmaFuncParser, ExmiNoFuncFound),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN(
					 "Lookup failed for function %u."),
				 1, GPOS_WSZ_WSZLEN("Lookup failed for function")),

		CMessage(CException(ExmaFuncParser, ExmiNotImplementYet),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN(
					 "Not implemented yet."),
				 0, GPOS_WSZ_WSZLEN("Not implemented yet")),
		
		CMessage(CException(ExmaFuncParser, ExmiIncorrectNumOfArgsToHypotheticalSetAgg),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN(
					 "Incorrect number of arguments to hypothetical-set aggregate."),
				 0, GPOS_WSZ_WSZLEN("incorrect number of arguments to hypothetical-set aggregate")),
		
		CMessage(CException(ExmaFuncParser, ExmiHypotheticalSetAggInconsistentDeclArgTypes),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN(
					 "Hypothetical-set aggregate has inconsistent declared argument types."),
				 0, GPOS_WSZ_WSZLEN("hypothetical-set aggregate has inconsistent declared argument types")),
	
		CMessage(CException(ExmaFuncParser, ExmiArgNameUsedMoreThanOnce),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN(
					 "Argument name \"%s\" used more than once."),
				 1, GPOS_WSZ_WSZLEN("argument name used more than once")),
		
		CMessage(CException(ExmaFuncParser, ExmiPosArgCannotFollowNamedArg),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN(
					 "Positional argument cannot follow named argument."),
				 0, GPOS_WSZ_WSZLEN("positional argument cannot follow named argument")),
		
		CMessage(CException(ExmaFuncParser, ExmiButIsNotAggFunc),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN(
					 "%s(*) specified, but %s is not an aggregate function."),
				 2, GPOS_WSZ_WSZLEN("but is not an aggregate function")),
		
		CMessage(CException(ExmaFuncParser, ExmiDistinctSpecifiedButIsNotAggFunc),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN(
					 "DISTINCT specified, but %s is not an aggregate function."),
				 1, GPOS_WSZ_WSZLEN("DISTINCT specified, but is not an aggregate function")),
		
		CMessage(CException(ExmaFuncParser, ExmiWithGroupSpecifiedButIsNotAggFunc),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN(
					 "WITHIN GROUP specified, but %s is not an aggregate function."),
				 1, GPOS_WSZ_WSZLEN("WITHIN GROUP specified, but is not an aggregate function")),
		
		CMessage(CException(ExmaFuncParser, ExmiOrderBySpecifiedButIsNotAggFunc),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN(
					 "ORDER BY specified, but %s is not an aggregate function."),
				 1, GPOS_WSZ_WSZLEN("ORDER BY specified, but is not an aggregate function")),

		CMessage(CException(ExmaFuncParser, ExmiFilterSpecifiedButIsNotAggFunc),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN(
					 "FILTER specified, but %s is not an aggregate function."),
				 1, GPOS_WSZ_WSZLEN("FILTER specified, but is not an aggregate function")),
		
		CMessage(CException(ExmaFuncParser, ExmiOverSpecifiedButIsNotAggOrWinFunc),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN(
					 "OVER specified, but %s is not a window function nor an aggregate function."),
				 1, GPOS_WSZ_WSZLEN("OVER specified, but is not a window function nor an aggregate function")),
		
		CMessage(CException(ExmaFuncParser, ExmiNoAggFound),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN(
					 "Lookup failed for aggregate %u."),
				 1, GPOS_WSZ_WSZLEN("Lookup failed for aggregate")),
		
		CMessage(CException(ExmaFuncParser, ExmiWithGroupIsRequiredForOrderSetAgg),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN(
					 "WITHIN GROUP is required for ordered-set aggregate %s."),
				 1, GPOS_WSZ_WSZLEN("WITHIN GROUP is required for ordered-set aggregate.")),
		
		CMessage(CException(ExmaFuncParser, ExmiOverIsNotSupportedForOrderSetAgg),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN(
					 "OVER is not supported for ordered-set aggregate %s."),
				 1, GPOS_WSZ_WSZLEN("OVER is not supported for ordered-set aggregate.")),
		
		CMessage(CException(ExmaFuncParser, ExmiFuncNotFound),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN(
					 "Function %s does not exist."),
				 1, GPOS_WSZ_WSZLEN("Function does not exist.")),
		
		CMessage(CException(ExmaFuncParser, ExmiCannotHaveWithinGroupInNoneOrderSetAgg),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN(
					 "%s is not an ordered-set aggregate, so it cannot have WITHIN GROUP."),
				 1, GPOS_WSZ_WSZLEN("Not an ordered-set aggregate, so it cannot have WITHIN GROUP.")),

		CMessage(CException(ExmaFuncParser, ExmiWinFuncRequiresOver),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN(
					 "Window function %s requires an OVER clause."),
				 1, GPOS_WSZ_WSZLEN("window function requires an OVER clause.")),

		CMessage(CException(ExmaFuncParser, ExmiWinFuncCannotHaveWithinGroup),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN(
					 "Window function %s cannot have WITHIN GROUP."),
				 1, GPOS_WSZ_WSZLEN("window function cannot have WITHIN GROUP.")),

		CMessage(CException(ExmaFuncParser, ExmiFuncIsNotUnique),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN(
					 "Function %s is not unique, Could not choose a best candidate function, You might need to add explicit type casts."),
				 1, GPOS_WSZ_WSZLEN("Function is not unique.")),

		CMessage(CException(ExmaFuncParser, ExmiFuncNotFoundNoAggFunc),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN(
					 "Function %s does not exist, No aggregate function matches the given name and argument types, Perhaps you misplaced ORDER BY; ORDER BY must appear after all regular arguments of the aggregate."),
				 1, GPOS_WSZ_WSZLEN("Function does not exist.")),

		CMessage(CException(ExmaFuncParser, ExmiFuncNotFoundNoMatchingNameAndArgTypes),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN(
					 "Function %s does not exist, No function matches the given name and argument types, You might need to add explicit type casts."),
				 1, GPOS_WSZ_WSZLEN("Function does not exist.")),
		
		CMessage(CException(ExmaFuncParser, ExmiCannotFindArrayTypeForType),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN(
					 "Could not find array type for data type %s."),
				 1, GPOS_WSZ_WSZLEN("could not find array type for data type.")),
		
		CMessage(CException(ExmaFuncParser, ExmiVARIADICMustBeAnArray),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN(
					 "VARIADIC argument must be an array."),
				 0, GPOS_WSZ_WSZLEN("VARIADIC argument must be an array.")),
		
		CMessage(CException(ExmaFuncParser, ExmiAggCannotReturnSets),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN(
					 "Aggregates cannot return sets."),
				 0, GPOS_WSZ_WSZLEN("Aggregates cannot return sets.")),
		
		CMessage(CException(ExmaFuncParser, ExmiAggCannotUseNamedArg),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN(
					 "Aggregates cannot use named arguments."),
				 0, GPOS_WSZ_WSZLEN("Aggregates cannot use named arguments.")),
		
		CMessage(CException(ExmaFuncParser, ExmiDistinctIsNotImplForWinFuncs),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN(
					 "DISTINCT is not implemented for window functions."),
				 0, GPOS_WSZ_WSZLEN("DISTINCT is not implemented for window functions.")),
		
		CMessage(CException(ExmaFuncParser, ExmiDistinctIsOnlySupportForSingleArgWinAggs),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN(
					 "DISTINCT is supported only for single-argument window aggregates."),
				 0, GPOS_WSZ_WSZLEN("DISTINCT is supported only for single-argument window aggregates.")),
		
		CMessage(CException(ExmaFuncParser, ExmiOrderByIsNotImplForWinFuncs),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN(
					 "Aggregate ORDER BY is not implemented for window functions."),
				 0, GPOS_WSZ_WSZLEN("aggregate ORDER BY is not implemented for window functions.")),
		
		CMessage(CException(ExmaFuncParser, ExmiFilterIsNotImplForNoneAggWinFuncs),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN(
					 "FILTER is not implemented for non-aggregate window functions."),
				 0, GPOS_WSZ_WSZLEN("FILTER is not implemented for non-aggregate window functions.")),
		
		CMessage(CException(ExmaFuncParser, ExmiWinFuncsCannotReturnSets),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN(
					 "Window functions cannot return sets."),
				 0, GPOS_WSZ_WSZLEN("window functions cannot return sets.")),
	};

	GPOS_RESULT eres = GPOS_FAILED;

	GPOS_TRY
	{
		// copy exception array into heap
		CMessage *rgpmsg[ExmiSentinel];
		CMessageRepository *pmr = CMessageRepository::GetMessageRepository();

		for (ULONG i = 0; i < GPOS_ARRAY_SIZE(rgpmsg); i++)
		{
			rgpmsg[i] = GPOS_NEW(mp) CMessage(rgmsg[i]);
			pmr->AddMessage(ElocEnUS_Utf8, rgpmsg[i]);
		}

		eres = GPOS_OK;
	}
	GPOS_CATCH_EX(ex)
	{
		return GPOS_FAILED;
	}

	GPOS_CATCH_END;

	return eres;
}

void CommonException::Init(CMemoryPool *mp)
{
    EresExceptionInit(mp);
}

}
