#pragma once

#include <Interpreters/orcaopt/pgopt_hawq/parser_common.h>
#include <Interpreters/orcaopt/pgopt_hawq/walkers.h>

#include <Interpreters/orcaopt/pgopt_hawq/ExprParser.h>

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

	//TODO
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

class AggParser
{
private:
    ExprParser expr_parser;
public:
	explicit AggParser();

    void transformWindowSpec(PGParseState * pstate, duckdb_libpgquery::PGWindowDef * spec);

    void transformWindowSpecExprs(PGParseState * pstate);

    void check_call(PGParseState * pstate, duckdb_libpgquery::PGNode * call);

    void transformWindowFuncCall(PGParseState * pstate, duckdb_libpgquery::PGWindowDef * wind);
};

}
