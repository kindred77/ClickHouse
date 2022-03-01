/*
 * TranslatorQueryToDXL.h
 *
 *  Created on: Jan 11, 2022
 *      Author: kindred
 */

#ifndef SRC_INTERPRETERS_ORCAOPT_TRANSLATORQUERYTODXL_H_
#define SRC_INTERPRETERS_ORCAOPT_TRANSLATORQUERYTODXL_H_

#include <naucrates/dxl/CDXLUtils.h>
#include <Parsers/ASTSelectQuery.h>

namespace Poco
{
class Logger;
}

namespace DB
{
class CDXLNode;

class TranslatorQueryToDXL {

public:
    TranslatorQueryToDXL();

    TranslatorQueryToDXL(
        gpopt::CMDAccessor * metadata_accessor_,
        ASTPtr select_query_);

    virtual ~TranslatorQueryToDXL();

    CDXLNode * translateSimpleSelectToDXL();

private:
    gpopt::CMDAccessor * metadata_accessor;
    ASTPtr select_query;

    Poco::Logger * log;
};
}

#endif /* SRC_INTERPRETERS_ORCAOPT_TRANSLATORQUERYTODXL_H_ */
