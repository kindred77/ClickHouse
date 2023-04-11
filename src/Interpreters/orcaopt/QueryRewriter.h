#pragma once

#include <Interpreters/orcaopt/parser_common.h>

namespace DB
{

class Context;
using ContextPtr = std::shared_ptr<const Context>;

class QueryRewriter
{
public:
    explicit QueryRewriter(const ContextPtr& context_);

    duckdb_libpgquery::PGList * rewrite(duckdb_libpgquery::PGQuery *parsetree);
};

};
