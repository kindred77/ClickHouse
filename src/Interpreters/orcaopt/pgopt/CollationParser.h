#pragma once

#include <parser_common.h>

namespace DB
{

class CollationParser
{
private:
    

public:
	explicit CollationParser();

    void
    assign_expr_collations(PGParseState *pstate, duckdb_libpgquery::PGNode *expr);

    bool
    assign_collations_walker(duckdb_libpgquery::PGNode *node, assign_collations_context *context);

    void
    assign_aggregate_collations(duckdb_libpgquery::PGAggref *aggref,
							assign_collations_context *loccontext);
    
    void
    assign_ordered_set_collations(duckdb_libpgquery::PGAggref *aggref,
							  assign_collations_context *loccontext);
    
    void
    assign_hypothetical_collations(duckdb_libpgquery::PGAggref *aggref,
							   assign_collations_context *loccontext);
    
    void
    merge_collation_state(Oid collation,
					  CollateStrength strength,
					  int location,
					  Oid collation2,
					  int location2,
					  assign_collations_context *context);

    //typedef bool (CollationParser::*walker_func) (duckdb_libpgquery::PGNode *node, assign_collations_context *context);
    
    //bool
    // expression_tree_walker(duckdb_libpgquery::PGNode *node,
	// 				   walker_func walker,
	// 				   void *context);
};

}