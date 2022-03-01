/*
 * TranslatorQueryToDXL.cpp
 *
 *  Created on: Jan 11, 2022
 *      Author: kindred
 */

#include "TranslatorQueryToDXL.h"
#include <common/logger_useful.h>

namespace DB
{

TranslatorQueryToDXL::TranslatorQueryToDXL()
        : log(&Poco::Logger::get("TranslatorQueryToDXL"))
{
    LOG_TRACE(log, "----0000----");
}

TranslatorQueryToDXL::TranslatorQueryToDXL(
    gpopt::CMDAccessor * metadata_accessor_,
    ASTPtr select_query_)
    : metadata_accessor(std::move(metadata_accessor_))
    , select_query(select_query_)
    , log(&Poco::Logger::get("TranslatorQueryToDXL"))
{
    LOG_TRACE(log, "----111----");
}

TranslatorQueryToDXL::~TranslatorQueryToDXL()
{

}

}

