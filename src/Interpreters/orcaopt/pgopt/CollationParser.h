#pragma once

#include <parser_common.h>

namespace DB
{

/*
 * Collation strength (the SQL standard calls this "derivation").  Order is
 * chosen to allow comparisons to work usefully.  Note: the standard doesn't
 * seem to distinguish between NONE and CONFLICT.
 */
typedef enum
{
	COLLATE_NONE,				/* expression is of a noncollatable datatype */
	COLLATE_IMPLICIT,			/* collation was derived implicitly */
	COLLATE_CONFLICT,			/* we had a conflict of implicit collations */
	COLLATE_EXPLICIT			/* collation was derived explicitly */
} CollateStrength;

typedef struct
{
	PGParseState *pstate;			/* parse state (for error reporting) */
	Oid			collation;		/* OID of current collation, if any */
	CollateStrength strength;	/* strength of current collation choice */
	int			location;		/* location of expr that set collation */
	/* Remaining fields are only valid when strength == COLLATE_CONFLICT */
	Oid			collation2;		/* OID of conflicting collation */
	int			location2;		/* location of expr that set collation2 */
} assign_collations_context;

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

    typedef bool (CollationParser::*walker_func) (duckdb_libpgquery::PGNode *node, assign_collations_context *context);
    
    bool
    expression_tree_walker(duckdb_libpgquery::PGNode *node,
					   walker_func walker,
					   void *context);
};

}