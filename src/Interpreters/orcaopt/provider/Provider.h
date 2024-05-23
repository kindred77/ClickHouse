#pragma once

#include <gpos/memory/CMemoryPool.h>

namespace DB
{

enum ExMajor
{
	ExmaProcProvider = 10000,

	ExmaSentinel
};

// minor exception types
enum ExMinor
{
	ExmiNoProcFound,


	ExmiSentinel
};

class Provider
{
private:
    static bool is_init;
    static gpos::GPOS_RESULT EresExceptionInit(gpos::CMemoryPool *mp);
public:
    static void Init(gpos::CMemoryPool *mp);
};

}
