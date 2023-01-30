#pragma once

#include <Interpreters/orcaopt/pgopt_hawq/parser_common.h>

namespace DB
{

typedef HeapTuple Type;

class TypeParser
{
private:

public:
	explicit TypeParser();

    Oid typeidTypeRelid(Oid type_id);
};

}