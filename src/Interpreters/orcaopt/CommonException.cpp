#include <Interpreters/orcaopt/CommonException.h>
#include <gpos/error/CMessage.h>
#include <gpos/error/CMessageRepository.h>

using namespace gpos;

namespace DB
{

bool CommonException::is_init = false;

GPOS_RESULT CommonException::EresExceptionInit(CMemoryPool *mp)
{
    CMessage rgmsg[ExmiSentinel] = {
        //provider
		CMessage(CException(ExmaProcProvider, ExmiNoProcFound),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN(
					 "Can not find proc, oid: %lld"),
				 1, GPOS_WSZ_WSZLEN("Can not find proc by the given oid.")),
		
		CMessage(CException(ExmaRelationProvider, ExmiUnknownType),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN(
					 "Unknow type: %s."),
				 1, GPOS_WSZ_WSZLEN("unknow type.")),

		CMessage(CException(ExmaRelationProvider, ExmiRelationProviderNotInit),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN(
					 "RelationProvider not inited, call Init() first."),
				 0, GPOS_WSZ_WSZLEN("relation provider not inited.")),
		
		CMessage(CException(ExmaRelationProvider, ExmiRelationNotFound),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN(
					 "Relation not found: %d."),
				 1, GPOS_WSZ_WSZLEN("relation not found.")),
		
		CMessage(CException(ExmaRelationProvider, ExmiLockModeNotSupported),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN(
					 "Lock mode not supported: %u."),
				 1, GPOS_WSZ_WSZLEN("Lock mode not supported.")),
		
		CMessage(CException(ExmaRelationProvider, ExmiInvalidAttNum),
				 CException::ExsevWarning,
				 GPOS_WSZ_WSZLEN(
					 "Invalid attnum %d for rangetable entry %s."),
				 2, GPOS_WSZ_WSZLEN("Invalid attnum for rangetable entry.")),
		
		CMessage(CException(ExmaRelationProvider, ExmiWrongObjectType),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN(
					 "Wrong object type: %s."),
				 1, GPOS_WSZ_WSZLEN("Wrong object type.")),
		
		CMessage(CException(ExmaRelationProvider, ExmiTypeNotFound),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN(
					 "Type not found: %d."),
				 1, GPOS_WSZ_WSZLEN("Type not found.")),
		
		CMessage(CException(ExmaRelationProvider, ExmiInvalidTypeForComposite),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN(
					 "Invalid typrelid for composite type: %d."),
				 1, GPOS_WSZ_WSZLEN("Invalid typrelid for composite type.")),
		
		CMessage(CException(ExmaRelationProvider, ExmiRecordTypeNotRegister),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN(
					 "Record type has not been registered."),
				 0, GPOS_WSZ_WSZLEN("Record type has not been registered.")),
		
		CMessage(CException(ExmaRelationProvider, ExmiTypeIsShell),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN(
					 "Type %s is only a shell."),
				 1, GPOS_WSZ_WSZLEN("type is a shell.")),
		
		CMessage(CException(ExmaRelationProvider, ExmiNoAvailableOutputFunction),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN(
					 "No output function available for type %s."),
				 1, GPOS_WSZ_WSZLEN("No output function available for type.")),
		
		CMessage(CException(ExmaRelationProvider, ExmiRangeTypeNotSupported),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN(
					 "Range type not supported yet: %u."),
				 1, GPOS_WSZ_WSZLEN("Range type not supported yet.")),
		
		CMessage(CException(ExmaRelationProvider, ExmiProArgModesNot1DCharArr),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN(
					 "proargmodes is not a 1-D char array."),
				 0, GPOS_WSZ_WSZLEN("proargmodes is not a 1-D char array.")),
		
		CMessage(CException(ExmaRelationProvider, ExmiNotAnyArray),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN(
					 "Argument declared \"anyarray\" is not an array but type %s."),
				 1, GPOS_WSZ_WSZLEN("Argument declared \"anyarray\" is not an array.")),
		
		CMessage(CException(ExmaRelationProvider, ExmiNotFoundArrayType),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN(
					 "Could not find array type for data type %s."),
				 1, GPOS_WSZ_WSZLEN("Could not find array type.")),
		
		CMessage(CException(ExmaRelationProvider, ExmiCouldNotDeterminePolymorphicType),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN(
					 "Could not determine polymorphic type because context isn't polymorphic."),
				 0, GPOS_WSZ_WSZLEN("could not determine polymorphic type because context isn't polymorphic.")),
		
		CMessage(CException(ExmaRelationProvider, ExmiCouldNotDetermineActualResultTypeForFunc),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN(
					 "Could not determine actual result type for function \"%s\" declared to return type %s."),
				 2, GPOS_WSZ_WSZLEN("Could not determine actual result type for function.")),
		
		CMessage(CException(ExmaRelationProvider, ExmiSetOfRecordTypeNotSupported),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN(
					 "Return set of record type not supported yet: %u."),
				 1, GPOS_WSZ_WSZLEN("Return set of record type not supported yet.")),
        
        //parser
        CMessage(CException(ExmaExprParser, ExmiUnrecognizedAExprKind),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN(
					 "Unrecognized A_Expr kind: %d"),
				 1, GPOS_WSZ_WSZLEN("Unrecognized A_Expr kind.")),
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

void CommonException::Init(CMemoryPool *mp)
{
    EresExceptionInit(mp);
}

}
