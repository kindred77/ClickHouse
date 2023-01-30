#pragma once

#include <Interpreters/orcaopt/pgopt_hawq/parser_common.h>

namespace DB
{

class FuncParser
{
private:

public:
	explicit FuncParser();

    bool typeInheritsFrom(Oid subclassTypeId, Oid superclassTypeId);
};

}