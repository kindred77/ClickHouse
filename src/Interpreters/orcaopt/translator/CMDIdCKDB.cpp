#include <Interpreters/orcaopt/translator/CMDIdCKDB.h>

namespace gpmd
{
using namespace gpos;

CMDIdCKDB::CMDIdCKDB(const ECKDBMDOIdType oid_type, OID oid) : CMDIdGPDB(oid)
{
    mdoid_type = oid_type;
};

}
