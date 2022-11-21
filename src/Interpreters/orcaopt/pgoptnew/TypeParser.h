#pragma once

#include <Interpreters/orcaopt/pgoptnew/parser_common.h>
#include <Interpreters/orcaopt/pgoptnew/RelationParser.h>
#include <Interpreters/orcaopt/pgoptnew/NodeParser.h>

namespace DB
{

typedef HeapTuple Type;

class TypeParser
{
private:
    RelationParser relation_parser;
    NodeParser node_parser;
public:
	explicit TypeParser();

};

}