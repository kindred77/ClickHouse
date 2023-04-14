#include <Interpreters/orcaopt/translator/wrappers.h>

#include "gpos/base.h"
#include "gpos/error/CAutoExceptionStack.h"
#include "gpos/error/CException.h"

#include "naucrates/exception.h"

using namespace duckdb_libpgquery;

// #define GP_WRAP_START                                            \
// 	sigjmp_buf local_sigjmp_buf;                                 \
// 	{                                                            \
// 		CAutoExceptionStack aes((void **) &PG_exception_stack,   \
// 								(void **) &error_context_stack); \
// 		if (0 == sigsetjmp(local_sigjmp_buf, 0))                 \
// 		{                                                        \
// 			aes.SetLocalJmp(&local_sigjmp_buf)

// #define GP_WRAP_END                                        \
// 	}                                                      \
// 	else                                                   \
// 	{                                                      \
// 		GPOS_RAISE(gpdxl::ExmaGPDB, gpdxl::ExmiGPDBError); \
// 	}                                                      \
// 	}

uint32
ListLength(PGList *l)
{
    return list_length(l);
};

void *
ListNth(PGList *list, int n)
{
	return list_nth(list, n);
};

PGList *
LAppend(PGList *list, void *datum)
{
	return lappend(list, datum);
};

void
ListFree(PGList *list)
{
	list_free(list);
};
