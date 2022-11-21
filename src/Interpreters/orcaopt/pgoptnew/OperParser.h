#pragma once

#include <Interpreters/orcaopt/pgoptnew/parser_common.h>
#include <Interpreters/orcaopt/pgoptnew/FuncParser.h>
#include <Interpreters/orcaopt/pgoptnew/NodeParser.h>

namespace DB
{

class OperParser
{
private:
    FuncParser func_parser;
	NodeParser node_parser;
public:
	explicit OperParser();

    
};

}