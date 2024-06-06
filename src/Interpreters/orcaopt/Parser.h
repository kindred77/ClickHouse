#pragma once

#include <gpos/memory/CMemoryPool.h>

#include <Interpreters/orcaopt/provider/Provider.h>

namespace DB
{

class Parser
{
private:
    static bool is_init;
public:
    static void Init(gpos::CMemoryPool *mp);
};

}
