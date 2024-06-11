#pragma once

#include <gpos/memory/CMemoryPool.h>

namespace DB
{

enum ExMajor
{
    //provider
	ExmaProvider = 10000,

    //parser
	ExmaParser,

	ExmaSentinel
};

// minor exception types
enum ExMinor
{
    //provider
	ExmiNoProcFound,
	ExmiUnknownType,
	ExmiRelationProviderNotInit,
	ExmiRelationNotFound,
	ExmiLockModeNotSupported,
	ExmiInvalidAttNum,
	ExmiWrongObjectType,
	ExmiTypeNotFound,
	ExmiInvalidTypeForComposite,
	ExmiRecordTypeNotRegister,
	ExmiTypeIsShell,
	ExmiNoAvailableOutputFunction,
	ExmiRangeTypeNotSupported,
	ExmiProArgModesNot1DCharArr,
	ExmiNotAnyArray,
	ExmiNotFoundArrayType,
	ExmiCouldNotDeterminePolymorphicType,
	ExmiCouldNotDetermineActualResultTypeForFunc,
	ExmiSetOfRecordTypeNotSupported,

    //parser
	ExmiUnrecognizedAExprKind,
	ExmiComplexProjNotSupported,
	ExmiFuncTooManyArgs,
	ExmiNoFuncFound,
	ExmiNotImplementYet,
	ExmiIncorrectNumOfArgsToHypotheticalSetAgg,
	ExmiHypotheticalSetAggInconsistentDeclArgTypes,
	ExmiArgNameUsedMoreThanOnce,
	ExmiPosArgCannotFollowNamedArg,
	ExmiButIsNotAggFunc,
	ExmiDistinctSpecifiedButIsNotAggFunc,
	ExmiWithGroupSpecifiedButIsNotAggFunc,
	ExmiOrderBySpecifiedButIsNotAggFunc,
	ExmiFilterSpecifiedButIsNotAggFunc,
	ExmiOverSpecifiedButIsNotAggOrWinFunc,
	ExmiNoAggFound,
	ExmiWithGroupIsRequiredForOrderSetAgg,
	ExmiOverIsNotSupportedForOrderSetAgg,
	ExmiFuncNotFound,
	ExmiCannotHaveWithinGroupInNoneOrderSetAgg,
	ExmiWinFuncRequiresOver,
	ExmiWinFuncCannotHaveWithinGroup,
	ExmiFuncIsNotUnique,
	ExmiFuncNotFoundNoAggFunc,
	ExmiFuncNotFoundNoMatchingNameAndArgTypes,
	ExmiCannotFindArrayTypeForType,
	ExmiVARIADICMustBeAnArray,
	ExmiAggCannotReturnSets,
	ExmiAggCannotUseNamedArg,
	ExmiDistinctIsNotImplForWinFuncs,
	ExmiDistinctIsOnlySupportForSingleArgWinAggs,
	ExmiOrderByIsNotImplForWinFuncs,
	ExmiFilterIsNotImplForNoneAggWinFuncs,
	ExmiWinFuncsCannotReturnSets,
	ExmiSetReturningFuncIsNotAllowedIn,

	ExmiSentinel
};

class CommonException
{
private:
    static bool is_init;
    static gpos::GPOS_RESULT EresExceptionInit(gpos::CMemoryPool *mp);
public:
    static void Init(gpos::CMemoryPool *mp);
};

}
