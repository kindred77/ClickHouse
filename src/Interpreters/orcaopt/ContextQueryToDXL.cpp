#include <Interpreters/orcaopt/ContextQueryToDXL.h>

ContextQueryToDXL::ContextQueryToDXL(CMemoryPool *memory_pool_)
	: memory_pool(memory_pool_),
	  has_distributed_tables(false),
	  distribution_hashops(DistrHashOpsNotDeterminedYet)
{
	// map that stores gpdb att to optimizer col mapping
	colid_counter = GPOS_NEW(memory_pool) CIdGenerator(GPDXL_COL_ID_START);
	cte_id_counter = GPOS_NEW(memory_pool) CIdGenerator(GPDXL_CTE_ID_START);
}

ContextQueryToDXL::~ContextQueryToDXL()
{
	GPOS_DELETE(colid_counter);
	GPOS_DELETE(cte_id_counter);
}
