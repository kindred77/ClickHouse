#pragma once

#include <gpos/memory/CMemoryPool.h>

namespace DB
{

class Provider
{
private:
    static bool is_init;
public:
    static void Init(gpos::CMemoryPool *mp);
};

}
