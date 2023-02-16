#pragma once

#include <Interpreters/orcaopt/parser_common.h>
#include <Interpreters/orcaopt/walkers.h>

namespace DB
{

typedef struct
{
    int sublevels_up;
} checkExprHasAggs_context;

typedef struct
{
    int sublevels_up;
} checkHasWindFuncs_context;

static bool checkExprHasAggs_walker(duckdb_libpgquery::PGNode * node, checkExprHasAggs_context * context)
{
	using duckdb_libpgquery::PGAggref;
	using duckdb_libpgquery::PGQuery;
    using duckdb_libpgquery::PGNode;
    using duckdb_libpgquery::T_PGAggref;
    using duckdb_libpgquery::T_PGQuery;
    if (node == NULL)
        return false;
    if (IsA(node, PGAggref))
    {
        if (((PGAggref *)node)->agglevelsup == (PGIndex)context->sublevels_up)
            return true; /* abort the tree traversal and return true */
        /* else fall through to examine argument */
    }

	//TODO kindred
    // if (IsA(node, PercentileExpr))
    // {
    //     /* PercentileExpr is always levelsup == 0 */
    //     if (context->sublevels_up == 0)
    //         return true;
    // }

    if (IsA(node, PGQuery))
    {
        /* Recurse into subselects */
        bool result;

        context->sublevels_up++;
        result = pg_query_tree_walker((PGQuery *)node, (walker_func)checkExprHasAggs_walker, (void *)context, 0);
        context->sublevels_up--;
        return result;
    }
    return pg_expression_tree_walker(node, (walker_func)checkExprHasAggs_walker, (void *)context);
};

bool checkExprHasAggs(duckdb_libpgquery::PGNode * node)
{
    checkExprHasAggs_context context;

    context.sublevels_up = 0;

    /*
	 * Must be prepared to start with a Query or a bare expression tree; if
	 * it's a Query, we don't want to increment sublevels_up.
	 */
    return pg_query_or_expression_tree_walker(node, (walker_func)checkExprHasAggs_walker, (void *)&context, 0);
};

bool checkExprHasWindFuncs_walker(duckdb_libpgquery::PGNode * node, checkHasWindFuncs_context * context)
{
	using duckdb_libpgquery::PGWindowDef;
	using duckdb_libpgquery::PGQuery;
	using duckdb_libpgquery::PGSortBy;
    using duckdb_libpgquery::PGNode;
    using duckdb_libpgquery::T_PGAggref;
    using duckdb_libpgquery::T_PGQuery;
    using duckdb_libpgquery::T_PGWindowDef;
    using duckdb_libpgquery::T_PGSortBy;
    using duckdb_libpgquery::PGAExpr;
    using duckdb_libpgquery::PGColumnRef;
    using duckdb_libpgquery::PGAConst;
    using duckdb_libpgquery::PGTypeCast;
    using duckdb_libpgquery::T_PGAExpr;
    using duckdb_libpgquery::T_PGColumnRef;
    using duckdb_libpgquery::T_PGAConst;
    using duckdb_libpgquery::T_PGTypeCast;
    if (node == NULL)
        return false;
    if (IsA(node, PGWindowDef))
    {
        //if (((PGWindowDef *)node)->winlevelsup == context->sublevels_up)
            //return true; /* abort the tree traversal and return true */
        /* else fall through to examine argument */
    }
    else if (IsA(node, PGSortBy))
    {
        PGSortBy * s = (PGSortBy *)node;
        return checkExprHasWindFuncs_walker(s->node, context);
    }
    // else if (IsA(node, WindowFrame))
    // {
    //     WindowFrame * f = (WindowFrame *)node;
    //     if (checkExprHasWindFuncs_walker((Node *)f->trail, context))
    //         return true;
    //     if (checkExprHasWindFuncs_walker((Node *)f->lead, context))
    //         return true;
    // }
    // else if (IsA(node, WindowFrameEdge))
    // {
    //     WindowFrameEdge * e = (WindowFrameEdge *)node;

    //     return checkExprHasWindFuncs_walker(e->val, context);
    // }
    else if (IsA(node, PGQuery))
    {
        /* Recurse into subselects */
        bool result;

        context->sublevels_up++;
        result = pg_query_tree_walker((PGQuery *)node, (walker_func)checkExprHasWindFuncs_walker, (void *)context, 0);
        context->sublevels_up--;
        return result;
    }
    else if (IsA(node, PGAExpr))
    {
        /* could be seen inside an untransformed window clause */
        return false;
    }
    else if (IsA(node, PGColumnRef))
    {
        /* could be seen inside an untransformed window clause */
        return false;
    }

    else if (IsA(node, PGAConst))
    {
        /* could be seen inside an untransformed window clause */
        return false;
    }

    else if (IsA(node, PGTypeCast))
    {
        /* could be seen inside an untransformed window clause */
        return false;
    }

    return pg_expression_tree_walker(node, (walker_func)checkExprHasWindFuncs_walker, (void *)context);
};

bool checkExprHasWindFuncs(duckdb_libpgquery::PGNode * node)
{
    checkHasWindFuncs_context context;
    context.sublevels_up = 0;

    /*
	 * Must be prepared to start with a Query or a bare expression tree; if
	 * it's a Query, we don't want to increment sublevels_up.
	 */
    return pg_query_or_expression_tree_walker(node, (walker_func)checkExprHasWindFuncs_walker, (void *)&context, 0);
};

typedef struct
{
    int min_varlevel;
} find_minimum_var_level_context;

/*
 * find_minimum_var_level
 *	  Recursively scan a clause to find the lowest variable level it
 *	  contains --- for example, zero is returned if there are any local
 *	  variables, one if there are no local variables but there are
 *	  one-level-up outer references, etc.  Subqueries are scanned to see
 *	  if they possess relevant outer references.  (But any local variables
 *	  within subqueries are not relevant.)
 *
 *	  -1 is returned if the clause has no variables at all.
 *
 * Will recurse into sublinks.	Also, may be invoked directly on a Query.
 */
static bool find_minimum_var_level_cbVar(duckdb_libpgquery::PGVar * var, void * context, int sublevelsup)
{
    find_minimum_var_level_context * ctx = (find_minimum_var_level_context *)context;
    int varlevelsup = var->varlevelsup;

    /* convert levelsup to frame of reference of original query */
    varlevelsup -= sublevelsup;
    /* ignore local vars of subqueries */
    if (varlevelsup >= 0)
    {
        if (ctx->min_varlevel < 0 || ctx->min_varlevel > varlevelsup)
        {
            ctx->min_varlevel = varlevelsup;

            /*
			 * As soon as we find a local variable, we can abort the tree
			 * traversal, since min_varlevel is then certainly 0.
			 */
            if (varlevelsup == 0)
                return true;
        }
    }
    return false;
};

static bool find_minimum_var_level_cbAggref(duckdb_libpgquery::PGAggref * aggref, void * context, int sublevelsup)
{
    /*
	 * An Aggref must be treated like a Var of its level.  Normally we'd get
	 * the same result from looking at the Vars in the aggregate's argument,
	 * but this fails in the case of a Var-less aggregate call (COUNT(*)).
	 */
    find_minimum_var_level_context * ctx = (find_minimum_var_level_context *)context;
    int agglevelsup = aggref->agglevelsup;

    /* convert levelsup to frame of reference of original query */
    agglevelsup -= sublevelsup;
    /* ignore local aggs of subqueries */
    if (agglevelsup >= 0)
    {
        if (ctx->min_varlevel < 0 || ctx->min_varlevel > agglevelsup)
        {
            ctx->min_varlevel = agglevelsup;

            /*
			 * As soon as we find a local aggregate, we can abort the tree
			 * traversal, since min_varlevel is then certainly 0.
			 */
            if (agglevelsup == 0)
                return true;
        }
    }

    /* visit aggregate's args */
    return cdb_walk_vars((duckdb_libpgquery::PGNode *)aggref->args, find_minimum_var_level_cbVar, find_minimum_var_level_cbAggref, NULL, ctx, sublevelsup);
};

int find_minimum_var_level(duckdb_libpgquery::PGNode * node)
{
    find_minimum_var_level_context context;

    context.min_varlevel = -1; /* signifies nothing found yet */

    cdb_walk_vars(node, find_minimum_var_level_cbVar, find_minimum_var_level_cbAggref, NULL, &context, 0);

    return context.min_varlevel;
};

class ExprParser;
using ExprParserPtr = std::unique_ptr<ExprParser>;

class AggParser
{
private:
    ExprParserPtr expr_parser_ptr;
public:
	explicit AggParser();

    void transformWindowSpec(PGParseState * pstate, duckdb_libpgquery::PGWindowDef * spec);

    void transformWindowSpecExprs(PGParseState * pstate);

    //void check_call(PGParseState * pstate, duckdb_libpgquery::PGNode * call);

    void transformWindowFuncCall(PGParseState * pstate, duckdb_libpgquery::PGWindowFunc * wfunc, duckdb_libpgquery::PGWindowDef * windef);
};

}
