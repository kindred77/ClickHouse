#pragma once

#include "gpos/base.h"

#include "naucrates/dxl/CIdGenerator.h"
#include "naucrates/dxl/operators/CDXLNode.h"

namespace DB
{

enum DistributionHashOpsKind
{
	DistrHashOpsNotDeterminedYet,
	DistrUseDefaultHashOps,
	DistrUseLegacyHashOps
};

class ContextQueryToDXL
{
	friend class TranslatorQueryToDXL;

private:
	// memory pool
	CMemoryPool * memory_pool;

	// counter for generating unique column ids
	CIdGenerator * colid_counter;

	// counter for generating unique CTE ids
	CIdGenerator * cte_id_counter;

	// does the query have any distributed tables?
	BOOL has_distributed_tables;

	// What operator classes are used in the distribution keys?
	DistributionHashOpsKind distribution_hashops;

public:
	// ctor
	ContextQueryToDXL(CMemoryPool *mp);

	// dtor
	~ContextQueryToDXL();
};

}
