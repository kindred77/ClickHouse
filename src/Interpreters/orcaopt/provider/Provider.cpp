#include <Interpreters/orcaopt/provider/Provider.h>
#include <gpos/error/CMessage.h>
#include <gpos/error/CMessageRepository.h>

using namespace gpos;

namespace DB
{

bool Provider::is_init = false;

GPOS_RESULT Provider::EresExceptionInit(CMemoryPool *mp)
{
    CMessage rgmsg[ExmiSentinel] = {
		CMessage(CException(ExmaProcProvider, ExmiNoProcFound),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN(
					 "Can not find proc %lld"),
				 1, GPOS_WSZ_WSZLEN("Can not find proc by the given oid.")),
	};

	GPOS_RESULT eres = GPOS_FAILED;

	GPOS_TRY
	{
		// copy exception array into heap
		CMessage *rgpmsg[ExmiSentinel];
		CMessageRepository *pmr = CMessageRepository::GetMessageRepository();

		for (ULONG i = 0; i < GPOS_ARRAY_SIZE(rgpmsg); i++)
		{
			rgpmsg[i] = GPOS_NEW(mp) CMessage(rgmsg[i]);
			pmr->AddMessage(ElocEnUS_Utf8, rgpmsg[i]);
		}

		eres = GPOS_OK;
	}
	GPOS_CATCH_EX(ex)
	{
		return GPOS_FAILED;
	}

	GPOS_CATCH_END;

	return eres;
}

void Provider::Init(CMemoryPool *mp)
{
    EresExceptionInit(mp);
}

}
