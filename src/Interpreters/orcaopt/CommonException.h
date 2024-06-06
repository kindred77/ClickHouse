#pragma once

#include <gpos/memory/CMemoryPool.h>

namespace DB
{

enum ExMajor
{
    //provider
	ExmaProcProvider = 10000,
	ExmaRelationProvider,
	ExmaTypeProvider,

    //parser
	ExmaExprParser,

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
