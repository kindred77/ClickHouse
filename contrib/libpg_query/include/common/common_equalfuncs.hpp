#pragma once

#include "common/common_def.hpp"
#include "common/common_datum.hpp"

namespace duckdb_libpgquery {

extern bool pg_equal(const void * a, const void * b);

/*
 * Macros to simplify comparison of different kinds of fields.  Use these
 * wherever possible to reduce the chance for silly typos.  Note that these
 * hard-wire the convention that the local variables in an Equal routine are
 * named 'a' and 'b'.
 */

/* Compare a simple scalar field (int, float, bool, enum, etc) */
#define COMPARE_SCALAR_FIELD(fldname) \
    do \
    { \
        if (a->fldname != b->fldname) \
            return false; \
    } while (0)

/* Compare a field that is a pointer to some kind of Node or Node tree */
#define COMPARE_NODE_FIELD(fldname) \
    do \
    { \
        if (!pg_equal(a->fldname, b->fldname)) \
            return false; \
    } while (0)

/* Compare a field that is a pointer to a Bitmapset */
#define COMPARE_BITMAPSET_FIELD(fldname) \
    do \
    { \
        if (!bms_equal(a->fldname, b->fldname)) \
            return false; \
    } while (0)

/* Compare a field that is a pointer to a C string, or perhaps NULL */
#define COMPARE_STRING_FIELD(fldname) \
    do \
    { \
        if (!equalstr(a->fldname, b->fldname)) \
            return false; \
    } while (0)

/* Macro for comparing string fields that might be NULL */
#define equalstr(a, b) (((a) != NULL && (b) != NULL) ? (strcmp(a, b) == 0) : (a) == (b))

/* Compare a field that is a pointer to a simple palloc'd object of size sz */
#define COMPARE_POINTER_FIELD(fldname, sz) \
    do \
    { \
        if (memcmp(a->fldname, b->fldname, (sz)) != 0) \
            return false; \
    } while (0)

/*
 * Compare a field that is a varlena datum to the other.
 * Note the result will be false if one is toasted and the other is untoasted.
 * It depends on the context if we can say those are equal or not.
 */
#define COMPARE_VARLENA_FIELD(fldname, len) \
    do \
    { \
        if (a->fldname != b->fldname) \
        { \
            if (a->fldname == NULL || b->fldname == NULL) \
                return false; \
            if (!datumIsEqual(PointerGetDatum(a->fldname), PointerGetDatum(b->fldname), false, len)) \
                return false; \
        } \
    } while (0)

/* Compare a parse location field (this is a no-op, per note above) */
#define COMPARE_LOCATION_FIELD(fldname) ((void)0)

/* Compare a CoercionForm field (also a no-op, per comment in primnodes.h) */
#define COMPARE_COERCIONFORM_FIELD(fldname) ((void)0)


/*
 *	Stuff from primnodes.h
 */

extern bool _equalAlias(const void * a_arg, const void * b_arg);

extern bool _equalRangeVar(const void * a_arg, const void * b_arg);

/*
 * Records information about the target of a CTAS (SELECT ... INTO).
 */
extern bool _equalIntoClause(const void * a_arg, const void * b_arg);

/*
 * We don't need an _equalExpr because Expr is an abstract supertype which
 * should never actually get instantiated.  Also, since it has no common
 * fields except NodeTag, there's no need for a helper routine to factor
 * out comparing the common fields...
 */

extern bool _equalVar(const void * a_arg, const void * b_arg);

extern bool _equalConst(const void * a_arg, const void * b_arg);

extern bool _equalParam(const void * a_arg, const void * b_arg);

extern bool _equalAggref(const void * a_arg, const void * b_arg);

extern bool _equalWindowFunc(const void * a_arg, const void * b_arg);

extern bool _equalArrayRef(const void * a_arg, const void * b_arg);

extern bool _equalFuncExpr(const void * a_arg, const void * b_arg);

extern bool _equalNamedArgExpr(const void * a_arg, const void * b_arg);

extern bool _equalOpExpr(const void * a_arg, const void * b_arg);

extern bool _equalDistinctExpr(const void * a_arg, const void * b_arg);

extern bool _equalNullIfExpr(const void * a_arg, const void * b_arg);

extern bool _equalScalarArrayOpExpr(const void * a_arg, const void * b_arg);

extern bool _equalBoolExpr(const void * a_arg, const void * b_arg);

extern bool _equalSubLink(const void * a_arg, const void * b_arg);

extern bool _equalSubPlan(const void * a_arg, const void * b_arg);

// static bool _equalAlternativeSubPlan(const void * a_arg, const void * b_arg)
// {
//     const duckdb_libpgquery::PGAlternativeSubPlan * a = (const duckdb_libpgquery::PGAlternativeSubPlan *)a_arg;
//     const duckdb_libpgquery::PGAlternativeSubPlan * b = (const duckdb_libpgquery::PGAlternativeSubPlan *)b_arg;

//     COMPARE_NODE_FIELD(subplans);

//     return true;
// }

extern bool _equalFieldSelect(const void * a_arg, const void * b_arg);

// static bool _equalFieldStore(const void * a_arg, const void * b_arg)
// {
//     const duckdb_libpgquery::PGFieldStore * a = (const duckdb_libpgquery::PGFieldStore *)a_arg;
//     const duckdb_libpgquery::PGFieldStore * b = (const duckdb_libpgquery::PGFieldStore *)b_arg;

//     COMPARE_NODE_FIELD(arg);
//     COMPARE_NODE_FIELD(newvals);
//     COMPARE_NODE_FIELD(fieldnums);
//     COMPARE_SCALAR_FIELD(resulttype);

//     return true;
// }

// static bool _equalRelabelType(const duckdb_libpgquery::PGRelabelType * a, const duckdb_libpgquery::PGRelabelType * b)
// {
//     COMPARE_NODE_FIELD(arg);
//     COMPARE_SCALAR_FIELD(resulttype);
//     COMPARE_SCALAR_FIELD(resulttypmod);
//     COMPARE_SCALAR_FIELD(resultcollid);
//     COMPARE_COERCIONFORM_FIELD(relabelformat);
//     COMPARE_LOCATION_FIELD(location);

//     return true;
// }

// static bool _equalCoerceViaIO(const duckdb_libpgquery::PGCoerceViaIO * a, const duckdb_libpgquery::PGCoerceViaIO * b)
// {
//     COMPARE_NODE_FIELD(arg);
//     COMPARE_SCALAR_FIELD(resulttype);
//     COMPARE_SCALAR_FIELD(resultcollid);
//     COMPARE_COERCIONFORM_FIELD(coerceformat);
//     COMPARE_LOCATION_FIELD(location);

//     return true;
// }

// static bool _equalArrayCoerceExpr(const duckdb_libpgquery::PGArrayCoerceExpr * a, const duckdb_libpgquery::PGArrayCoerceExpr * b)
// {
//     COMPARE_NODE_FIELD(arg);
//     COMPARE_SCALAR_FIELD(elemfuncid);
//     COMPARE_SCALAR_FIELD(resulttype);
//     COMPARE_SCALAR_FIELD(resulttypmod);
//     COMPARE_SCALAR_FIELD(resultcollid);
//     COMPARE_SCALAR_FIELD(isExplicit);
//     COMPARE_COERCIONFORM_FIELD(coerceformat);
//     COMPARE_LOCATION_FIELD(location);

//     return true;
// }

// static bool _equalConvertRowtypeExpr(const duckdb_libpgquery::PGConvertRowtypeExpr * a, const duckdb_libpgquery::PGConvertRowtypeExpr * b)
// {
//     COMPARE_NODE_FIELD(arg);
//     COMPARE_SCALAR_FIELD(resulttype);
//     COMPARE_COERCIONFORM_FIELD(convertformat);
//     COMPARE_LOCATION_FIELD(location);

//     return true;
// }

// static bool _equalCollateExpr(const duckdb_libpgquery::PGCollateExpr * a, const duckdb_libpgquery::PGCollateExpr * b)
// {
//     COMPARE_NODE_FIELD(arg);
//     COMPARE_SCALAR_FIELD(collOid);
//     COMPARE_LOCATION_FIELD(location);

//     return true;
// }

extern bool _equalCaseExpr(const void * a_arg, const void * b_arg);

extern bool _equalCaseWhen(const void * a_arg, const void * b_arg);

extern bool _equalCaseTestExpr(const void * a_arg, const void * b_arg);

extern bool _equalArrayExpr(const void * a_arg, const void * b_arg);

extern bool _equalRowExpr(const void * a_arg, const void * b_arg);

extern bool _equalRowCompareExpr(const void * a_arg, const void * b_arg);

extern bool _equalCoalesceExpr(const void * a_arg, const void * b_arg);

extern bool _equalMinMaxExpr(const void * a_arg, const void * b_arg);

// static bool _equalXmlExpr(const PGXmlExpr * a, const PGXmlExpr * b)
// {
//     COMPARE_SCALAR_FIELD(op);
//     COMPARE_STRING_FIELD(name);
//     COMPARE_NODE_FIELD(named_args);
//     COMPARE_NODE_FIELD(arg_names);
//     COMPARE_NODE_FIELD(args);
//     COMPARE_SCALAR_FIELD(xmloption);
//     COMPARE_SCALAR_FIELD(type);
//     COMPARE_SCALAR_FIELD(typmod);
//     COMPARE_LOCATION_FIELD(location);

//     return true;
// }

extern bool _equalNullTest(const void * a_arg, const void * b_arg);

extern bool _equalBooleanTest(const void * a_arg, const void * b_arg);

extern bool _equalCoerceToDomain(const void * a_arg, const void * b_arg);

extern bool _equalCoerceToDomainValue(const void * a_arg, const void * b_arg);

// static bool _equalSetToDefault(const duckdb_libpgquery::PGSetToDefault * a, const duckdb_libpgquery::PGSetToDefault * b)
// {
//     COMPARE_SCALAR_FIELD(typeId);
//     COMPARE_SCALAR_FIELD(typeMod);
//     COMPARE_SCALAR_FIELD(collation);
//     COMPARE_LOCATION_FIELD(location);

//     return true;
// }

extern bool _equalCurrentOfExpr(const void * a_arg, const void * b_arg);

extern bool _equalTargetEntry(const void * a_arg, const void * b_arg);

extern bool _equalRangeTblRef(const void * a_arg, const void * b_arg);

extern bool _equalJoinExpr(const void * a_arg, const void * b_arg);

extern bool _equalFromExpr(const void * a_arg, const void * b_arg);

// static bool _equalFlow(const PGFlow * a, const PGFlow * b)
// {
//     COMPARE_SCALAR_FIELD(flotype);
//     COMPARE_SCALAR_FIELD(req_move);
//     COMPARE_SCALAR_FIELD(locustype);
//     COMPARE_SCALAR_FIELD(segindex);
//     COMPARE_SCALAR_FIELD(numsegments);
//     COMPARE_NODE_FIELD(hashExprs);
//     COMPARE_NODE_FIELD(hashOpfamilies);

//     return true;
// }


// /*
//  * Stuff from relation.h
//  */

// static bool _equalPathKey(const PGPathKey * a, const PGPathKey * b)
// {
//     /* We assume pointer equality is sufficient to compare the eclasses */
//     COMPARE_SCALAR_FIELD(pk_eclass);
//     COMPARE_SCALAR_FIELD(pk_opfamily);
//     COMPARE_SCALAR_FIELD(pk_strategy);
//     COMPARE_SCALAR_FIELD(pk_nulls_first);

//     return true;
// }

// static bool _equalRestrictInfo(const PGRestrictInfo * a, const PGRestrictInfo * b)
// {
//     COMPARE_NODE_FIELD(clause);
//     COMPARE_SCALAR_FIELD(is_pushed_down);
//     COMPARE_SCALAR_FIELD(outerjoin_delayed);
//     COMPARE_BITMAPSET_FIELD(required_relids);
//     COMPARE_BITMAPSET_FIELD(outer_relids);
//     COMPARE_BITMAPSET_FIELD(nullable_relids);

//     /*
// 	 * We ignore all the remaining fields, since they may not be set yet, and
// 	 * should be derivable from the clause anyway.
// 	 */

//     return true;
// }

// static bool _equalPlaceHolderVar(const PGPlaceHolderVar * a, const PGPlaceHolderVar * b)
// {
//     /*
// 	 * We intentionally do not compare phexpr.  Two PlaceHolderVars with the
// 	 * same ID and levelsup should be considered equal even if the contained
// 	 * expressions have managed to mutate to different states.  This will
// 	 * happen during final plan construction when there are nested PHVs, since
// 	 * the inner PHV will get replaced by a Param in some copies of the outer
// 	 * PHV.  Another way in which it can happen is that initplan sublinks
// 	 * could get replaced by differently-numbered Params when sublink folding
// 	 * is done.  (The end result of such a situation would be some
// 	 * unreferenced initplans, which is annoying but not really a problem.) On
// 	 * the same reasoning, there is no need to examine phrels.
// 	 *
// 	 * COMPARE_NODE_FIELD(phexpr);
// 	 *
// 	 * COMPARE_BITMAPSET_FIELD(phrels);
// 	 */
//     COMPARE_SCALAR_FIELD(phid);
//     COMPARE_SCALAR_FIELD(phlevelsup);

//     return true;
// }

// static bool _equalSpecialJoinInfo(const SpecialJoinInfo * a, const SpecialJoinInfo * b)
// {
//     COMPARE_BITMAPSET_FIELD(min_lefthand);
//     COMPARE_BITMAPSET_FIELD(min_righthand);
//     COMPARE_BITMAPSET_FIELD(syn_lefthand);
//     COMPARE_BITMAPSET_FIELD(syn_righthand);
//     COMPARE_SCALAR_FIELD(jointype);
//     COMPARE_SCALAR_FIELD(lhs_strict);
//     COMPARE_SCALAR_FIELD(delay_upper_joins);
//     COMPARE_NODE_FIELD(join_quals);

//     return true;
// }

// static bool _equalLateralJoinInfo(const LateralJoinInfo * a, const LateralJoinInfo * b)
// {
//     COMPARE_BITMAPSET_FIELD(lateral_lhs);
//     COMPARE_BITMAPSET_FIELD(lateral_rhs);

//     return true;
// }

// static bool _equalAppendRelInfo(const AppendRelInfo * a, const AppendRelInfo * b)
// {
//     COMPARE_SCALAR_FIELD(parent_relid);
//     COMPARE_SCALAR_FIELD(child_relid);
//     COMPARE_SCALAR_FIELD(parent_reltype);
//     COMPARE_SCALAR_FIELD(child_reltype);
//     COMPARE_NODE_FIELD(translated_vars);
//     COMPARE_SCALAR_FIELD(parent_reloid);

//     return true;
// }

// static bool _equalPlaceHolderInfo(const PGPlaceHolderInfo * a, const PGPlaceHolderInfo * b)
// {
//     COMPARE_SCALAR_FIELD(phid);
//     COMPARE_NODE_FIELD(ph_var); /* should be redundant */
//     COMPARE_BITMAPSET_FIELD(ph_eval_at);
//     COMPARE_BITMAPSET_FIELD(ph_lateral);
//     COMPARE_BITMAPSET_FIELD(ph_needed);
//     COMPARE_SCALAR_FIELD(ph_width);

//     return true;
// }


/*
 * Stuff from parsenodes.h
 */

extern bool _equalQuery(const void * a_arg, const void * b_arg);

extern bool _equalInsertStmt(const void * a_arg, const void * b_arg);

extern bool _equalDeleteStmt(const void * a_arg, const void * b_arg);

extern bool _equalUpdateStmt(const void * a_arg, const void * b_arg);

extern bool _equalSelectStmt(const void * a_arg, const void * b_arg);

// static bool _equalSetOperationStmt(const PGSetOperationStmt * a, const PGSetOperationStmt * b)
// {
//     COMPARE_SCALAR_FIELD(op);
//     COMPARE_SCALAR_FIELD(all);
//     COMPARE_NODE_FIELD(larg);
//     COMPARE_NODE_FIELD(rarg);
//     COMPARE_NODE_FIELD(colTypes);
//     COMPARE_NODE_FIELD(colTypmods);
//     COMPARE_NODE_FIELD(colCollations);
//     COMPARE_NODE_FIELD(groupClauses);

//     return true;
// }

// static bool _equalAlterTableStmt(const PGAlterTableStmt * a, const PGAlterTableStmt * b)
// {
//     COMPARE_NODE_FIELD(relation);
//     COMPARE_NODE_FIELD(cmds);
//     COMPARE_SCALAR_FIELD(relkind);
//     COMPARE_SCALAR_FIELD(missing_ok);

//     /* No need to compare AT workspace fields.  */

//     return true;
// }

// static bool _equalAlterTableCmd(const PGAlterTableCmd * a, const PGAlterTableCmd * b)
// {
//     COMPARE_SCALAR_FIELD(subtype);
//     COMPARE_STRING_FIELD(name);
//     COMPARE_NODE_FIELD(def);
//     COMPARE_SCALAR_FIELD(behavior);
//     COMPARE_SCALAR_FIELD(part_expanded);

//     /* No need to compare AT workspace field, partoids.  */
//     COMPARE_SCALAR_FIELD(missing_ok);

//     return true;
// }

// static bool _equalSetDistributionCmd(const SetDistributionCmd * a, const SetDistributionCmd * b)
// {
//     COMPARE_SCALAR_FIELD(backendId);
//     COMPARE_NODE_FIELD(relids);

//     return true;
// }

// static bool _equalInheritPartitionCmd(const InheritPartitionCmd * a, const InheritPartitionCmd * b)
// {
//     COMPARE_NODE_FIELD(parent);

//     return true;
// }

// static bool _equalAlterPartitionCmd(const AlterPartitionCmd * a, const AlterPartitionCmd * b)
// {
//     COMPARE_NODE_FIELD(partid);
//     COMPARE_NODE_FIELD(arg1);
//     COMPARE_NODE_FIELD(arg2);

//     return true;
// }

// static bool _equalAlterPartitionId(const AlterPartitionId * a, const AlterPartitionId * b)
// {
//     COMPARE_SCALAR_FIELD(idtype);
//     COMPARE_NODE_FIELD(partiddef);

//     return true;
// }

// static bool _equalAlterDomainStmt(const AlterDomainStmt * a, const AlterDomainStmt * b)
// {
//     COMPARE_SCALAR_FIELD(subtype);
//     COMPARE_NODE_FIELD(typeName);
//     COMPARE_STRING_FIELD(name);
//     COMPARE_NODE_FIELD(def);
//     COMPARE_SCALAR_FIELD(behavior);
//     COMPARE_SCALAR_FIELD(missing_ok);

//     return true;
// }

// static bool _equalGrantStmt(const GrantStmt * a, const GrantStmt * b)
// {
//     COMPARE_SCALAR_FIELD(is_grant);
//     COMPARE_SCALAR_FIELD(targtype);
//     COMPARE_SCALAR_FIELD(objtype);
//     COMPARE_NODE_FIELD(objects);
//     COMPARE_NODE_FIELD(privileges);
//     COMPARE_NODE_FIELD(grantees);
//     COMPARE_SCALAR_FIELD(grant_option);
//     COMPARE_SCALAR_FIELD(behavior);

//     return true;
// }

// static bool _equalPrivGrantee(const PrivGrantee * a, const PrivGrantee * b)
// {
//     COMPARE_STRING_FIELD(rolname);

//     return true;
// }

// static bool _equalFuncWithArgs(const FuncWithArgs * a, const FuncWithArgs * b)
// {
//     COMPARE_NODE_FIELD(funcname);
//     COMPARE_NODE_FIELD(funcargs);

//     return true;
// }

// static bool _equalAccessPriv(const AccessPriv * a, const AccessPriv * b)
// {
//     COMPARE_STRING_FIELD(priv_name);
//     COMPARE_NODE_FIELD(cols);

//     return true;
// }

// static bool _equalGrantRoleStmt(const GrantRoleStmt * a, const GrantRoleStmt * b)
// {
//     COMPARE_NODE_FIELD(granted_roles);
//     COMPARE_NODE_FIELD(grantee_roles);
//     COMPARE_SCALAR_FIELD(is_grant);
//     COMPARE_SCALAR_FIELD(admin_opt);
//     COMPARE_STRING_FIELD(grantor);
//     COMPARE_SCALAR_FIELD(behavior);

//     return true;
// }

// static bool _equalAlterDefaultPrivilegesStmt(const AlterDefaultPrivilegesStmt * a, const AlterDefaultPrivilegesStmt * b)
// {
//     COMPARE_NODE_FIELD(options);
//     COMPARE_NODE_FIELD(action);

//     return true;
// }

// static bool _equalDeclareCursorStmt(const DeclareCursorStmt * a, const DeclareCursorStmt * b)
// {
//     COMPARE_STRING_FIELD(portalname);
//     COMPARE_SCALAR_FIELD(options);
//     COMPARE_NODE_FIELD(query);

//     return true;
// }

// static bool _equalClosePortalStmt(const ClosePortalStmt * a, const ClosePortalStmt * b)
// {
//     COMPARE_STRING_FIELD(portalname);

//     return true;
// }

// static bool _equalClusterStmt(const ClusterStmt * a, const ClusterStmt * b)
// {
//     COMPARE_NODE_FIELD(relation);
//     COMPARE_STRING_FIELD(indexname);
//     COMPARE_SCALAR_FIELD(verbose);

//     return true;
// }

// static bool _equalSingleRowErrorDesc(const SingleRowErrorDesc * a, const SingleRowErrorDesc * b)
// {
//     COMPARE_SCALAR_FIELD(rejectlimit);
//     COMPARE_SCALAR_FIELD(is_limit_in_rows);
//     COMPARE_SCALAR_FIELD(into_file);
//     COMPARE_SCALAR_FIELD(log_errors_type);

//     return true;
// }

// static bool _equalCopyStmt(const CopyStmt * a, const CopyStmt * b)
// {
//     COMPARE_NODE_FIELD(relation);
//     COMPARE_NODE_FIELD(query);
//     COMPARE_NODE_FIELD(attlist);
//     COMPARE_SCALAR_FIELD(is_from);
//     COMPARE_SCALAR_FIELD(is_program);
//     COMPARE_SCALAR_FIELD(skip_ext_partition);
//     COMPARE_STRING_FIELD(filename);
//     COMPARE_NODE_FIELD(options);
//     COMPARE_NODE_FIELD(sreh);

//     return true;
// }

// static bool _equalCreateStmt(const void * a_arg, const void * b_arg)
// {
//     const duckdb_libpgquery::PGCreateStmt * a = (const duckdb_libpgquery::PGCreateStmt *)a_arg;
//     const duckdb_libpgquery::PGCreateStmt * b = (const duckdb_libpgquery::PGCreateStmt *)b_arg;

//     COMPARE_NODE_FIELD(relation);
//     COMPARE_NODE_FIELD(tableElts);
//     COMPARE_NODE_FIELD(inhRelations);
//     //COMPARE_NODE_FIELD(inhOids);
//     //COMPARE_SCALAR_FIELD(parentOidCount);
//     COMPARE_NODE_FIELD(ofTypename);
//     COMPARE_NODE_FIELD(constraints);
//     COMPARE_NODE_FIELD(options);
//     COMPARE_SCALAR_FIELD(oncommit);
//     COMPARE_STRING_FIELD(tablespacename);
//     //COMPARE_SCALAR_FIELD(if_not_exists);

//     //COMPARE_NODE_FIELD(distributedBy);
//     //COMPARE_SCALAR_FIELD(relKind);
//     //COMPARE_SCALAR_FIELD(relStorage);
//     /* deferredStmts omitted */
//     //COMPARE_SCALAR_FIELD(is_part_child);
//     //COMPARE_SCALAR_FIELD(is_add_part);
//     //COMPARE_SCALAR_FIELD(is_split_part);
//     //COMPARE_SCALAR_FIELD(ownerid);
//     //COMPARE_SCALAR_FIELD(buildAoBlkdir);
//     //COMPARE_NODE_FIELD(attr_encodings);
//     //COMPARE_SCALAR_FIELD(isCtas);

//     return true;
// }

// static bool _equalColumnReferenceStorageDirective(const ColumnReferenceStorageDirective * a, const ColumnReferenceStorageDirective * b)
// {
//     COMPARE_STRING_FIELD(column);
//     COMPARE_SCALAR_FIELD(deflt);
//     COMPARE_NODE_FIELD(encoding);

//     return true;
// }

// static bool _equalPartitionRangeItem(const PartitionRangeItem * a, const PartitionRangeItem * b)
// {
//     COMPARE_NODE_FIELD(partRangeVal);
//     COMPARE_SCALAR_FIELD(partedge);
//     COMPARE_SCALAR_FIELD(everycount);

//     return true;
// }

// static bool _equalExtTableTypeDesc(const ExtTableTypeDesc * a, const ExtTableTypeDesc * b)
// {
//     COMPARE_SCALAR_FIELD(exttabletype);
//     COMPARE_NODE_FIELD(location_list);
//     COMPARE_NODE_FIELD(on_clause);
//     COMPARE_STRING_FIELD(command_string);

//     return true;
// }

// static bool _equalCreateExternalStmt(const CreateExternalStmt * a, const CreateExternalStmt * b)
// {
//     COMPARE_NODE_FIELD(relation);
//     COMPARE_NODE_FIELD(tableElts);
//     COMPARE_NODE_FIELD(exttypedesc);
//     COMPARE_STRING_FIELD(format);
//     COMPARE_NODE_FIELD(formatOpts);
//     COMPARE_SCALAR_FIELD(isweb);
//     COMPARE_SCALAR_FIELD(iswritable);
//     COMPARE_NODE_FIELD(sreh);
//     COMPARE_NODE_FIELD(extOptions);
//     COMPARE_NODE_FIELD(encoding);
//     COMPARE_NODE_FIELD(distributedBy);

//     return true;
// }

// static bool _equalTableLikeClause(const TableLikeClause * a, const TableLikeClause * b)
// {
//     COMPARE_NODE_FIELD(relation);
//     COMPARE_SCALAR_FIELD(options);

//     return true;
// }

// static bool _equalDefineStmt(const DefineStmt * a, const DefineStmt * b)
// {
//     COMPARE_SCALAR_FIELD(kind);
//     COMPARE_SCALAR_FIELD(oldstyle);
//     COMPARE_NODE_FIELD(defnames);
//     COMPARE_NODE_FIELD(args);
//     COMPARE_NODE_FIELD(definition);
//     COMPARE_SCALAR_FIELD(trusted); /* CDB */

//     return true;
// }

// static bool _equalDropStmt(const DropStmt * a, const DropStmt * b)
// {
//     COMPARE_NODE_FIELD(objects);
//     COMPARE_NODE_FIELD(arguments);
//     COMPARE_SCALAR_FIELD(removeType);
//     COMPARE_SCALAR_FIELD(behavior);
//     COMPARE_SCALAR_FIELD(missing_ok);
//     COMPARE_SCALAR_FIELD(concurrent);

//     return true;
// }

// static bool _equalTruncateStmt(const TruncateStmt * a, const TruncateStmt * b)
// {
//     COMPARE_NODE_FIELD(relations);
//     COMPARE_SCALAR_FIELD(restart_seqs);
//     COMPARE_SCALAR_FIELD(behavior);

//     return true;
// }

// static bool _equalCommentStmt(const CommentStmt * a, const CommentStmt * b)
// {
//     COMPARE_SCALAR_FIELD(objtype);
//     COMPARE_NODE_FIELD(objname);
//     COMPARE_NODE_FIELD(objargs);
//     COMPARE_STRING_FIELD(comment);

//     return true;
// }

// static bool _equalSecLabelStmt(const SecLabelStmt * a, const SecLabelStmt * b)
// {
//     COMPARE_SCALAR_FIELD(objtype);
//     COMPARE_NODE_FIELD(objname);
//     COMPARE_NODE_FIELD(objargs);
//     COMPARE_STRING_FIELD(provider);
//     COMPARE_STRING_FIELD(label);

//     return true;
// }

// static bool _equalFetchStmt(const FetchStmt * a, const FetchStmt * b)
// {
//     COMPARE_SCALAR_FIELD(direction);
//     COMPARE_SCALAR_FIELD(howMany);
//     COMPARE_STRING_FIELD(portalname);
//     COMPARE_SCALAR_FIELD(ismove);

//     return true;
// }

// static bool _equalRetrieveStmt(const RetrieveStmt * a, const RetrieveStmt * b)
// {
//     COMPARE_STRING_FIELD(endpoint_name);
//     COMPARE_SCALAR_FIELD(count);
//     COMPARE_SCALAR_FIELD(is_all);

//     return true;
// }

// static bool _equalIndexStmt(const IndexStmt * a, const IndexStmt * b)
// {
//     COMPARE_STRING_FIELD(idxname);
//     COMPARE_NODE_FIELD(relation);
//     COMPARE_STRING_FIELD(accessMethod);
//     COMPARE_STRING_FIELD(tableSpace);
//     COMPARE_NODE_FIELD(indexParams);
//     COMPARE_NODE_FIELD(options);
//     COMPARE_NODE_FIELD(whereClause);
//     COMPARE_NODE_FIELD(excludeOpNames);
//     COMPARE_STRING_FIELD(idxcomment);
//     COMPARE_SCALAR_FIELD(indexOid);
//     COMPARE_SCALAR_FIELD(oldNode);
//     COMPARE_SCALAR_FIELD(is_part_child);
//     COMPARE_SCALAR_FIELD(unique);
//     COMPARE_SCALAR_FIELD(primary);
//     COMPARE_SCALAR_FIELD(isconstraint);
//     COMPARE_SCALAR_FIELD(deferrable);
//     COMPARE_SCALAR_FIELD(initdeferred);
//     COMPARE_SCALAR_FIELD(concurrent);
//     COMPARE_SCALAR_FIELD(is_split_part);
//     COMPARE_SCALAR_FIELD(parentIndexId);
//     COMPARE_SCALAR_FIELD(parentConstraintId);

//     return true;
// }

// static bool _equalCreateFunctionStmt(const CreateFunctionStmt * a, const CreateFunctionStmt * b)
// {
//     COMPARE_SCALAR_FIELD(replace);
//     COMPARE_NODE_FIELD(funcname);
//     COMPARE_NODE_FIELD(parameters);
//     COMPARE_NODE_FIELD(returnType);
//     COMPARE_NODE_FIELD(options);
//     COMPARE_NODE_FIELD(withClause);

//     return true;
// }

// static bool _equalFunctionParameter(const FunctionParameter * a, const FunctionParameter * b)
// {
//     COMPARE_STRING_FIELD(name);
//     COMPARE_NODE_FIELD(argType);
//     COMPARE_SCALAR_FIELD(mode);
//     COMPARE_NODE_FIELD(defexpr);

//     return true;
// }

// static bool _equalAlterFunctionStmt(const AlterFunctionStmt * a, const AlterFunctionStmt * b)
// {
//     COMPARE_NODE_FIELD(func);
//     COMPARE_NODE_FIELD(actions);

//     return true;
// }

// static bool _equalDoStmt(const DoStmt * a, const DoStmt * b)
// {
//     COMPARE_NODE_FIELD(args);

//     return true;
// }

// static bool _equalRenameStmt(const RenameStmt * a, const RenameStmt * b)
// {
//     COMPARE_SCALAR_FIELD(renameType);
//     COMPARE_SCALAR_FIELD(relationType);
//     COMPARE_NODE_FIELD(relation);
//     COMPARE_SCALAR_FIELD(objid);
//     COMPARE_NODE_FIELD(object);
//     COMPARE_NODE_FIELD(objarg);
//     COMPARE_STRING_FIELD(subname);
//     COMPARE_STRING_FIELD(newname);
//     COMPARE_SCALAR_FIELD(behavior);
//     COMPARE_SCALAR_FIELD(missing_ok);

//     return true;
// }

// static bool _equalAlterObjectSchemaStmt(const AlterObjectSchemaStmt * a, const AlterObjectSchemaStmt * b)
// {
//     COMPARE_SCALAR_FIELD(objectType);
//     COMPARE_NODE_FIELD(relation);
//     COMPARE_NODE_FIELD(object);
//     COMPARE_NODE_FIELD(objarg);
//     COMPARE_STRING_FIELD(newschema);
//     COMPARE_SCALAR_FIELD(missing_ok);

//     return true;
// }

// static bool _equalAlterOwnerStmt(const AlterOwnerStmt * a, const AlterOwnerStmt * b)
// {
//     COMPARE_SCALAR_FIELD(objectType);
//     COMPARE_NODE_FIELD(relation);
//     COMPARE_NODE_FIELD(object);
//     COMPARE_NODE_FIELD(objarg);
//     COMPARE_STRING_FIELD(newowner);

//     return true;
// }

// static bool _equalRuleStmt(const RuleStmt * a, const RuleStmt * b)
// {
//     COMPARE_NODE_FIELD(relation);
//     COMPARE_STRING_FIELD(rulename);
//     COMPARE_NODE_FIELD(whereClause);
//     COMPARE_SCALAR_FIELD(event);
//     COMPARE_SCALAR_FIELD(instead);
//     COMPARE_NODE_FIELD(actions);
//     COMPARE_SCALAR_FIELD(replace);

//     return true;
// }

// static bool _equalNotifyStmt(const NotifyStmt * a, const NotifyStmt * b)
// {
//     COMPARE_STRING_FIELD(conditionname);
//     COMPARE_STRING_FIELD(payload);

//     return true;
// }

// static bool _equalListenStmt(const ListenStmt * a, const ListenStmt * b)
// {
//     COMPARE_STRING_FIELD(conditionname);

//     return true;
// }

// static bool _equalUnlistenStmt(const UnlistenStmt * a, const UnlistenStmt * b)
// {
//     COMPARE_STRING_FIELD(conditionname);

//     return true;
// }

// static bool _equalTransactionStmt(const TransactionStmt * a, const TransactionStmt * b)
// {
//     COMPARE_SCALAR_FIELD(kind);
//     COMPARE_NODE_FIELD(options);
//     COMPARE_STRING_FIELD(gid);

//     return true;
// }

// static bool _equalCompositeTypeStmt(const CompositeTypeStmt * a, const CompositeTypeStmt * b)
// {
//     COMPARE_NODE_FIELD(typevar);
//     COMPARE_NODE_FIELD(coldeflist);

//     return true;
// }

// static bool _equalCreateEnumStmt(const CreateEnumStmt * a, const CreateEnumStmt * b)
// {
//     COMPARE_NODE_FIELD(typeName);
//     COMPARE_NODE_FIELD(vals);

//     return true;
// }

// static bool _equalCreateRangeStmt(const CreateRangeStmt * a, const CreateRangeStmt * b)
// {
//     COMPARE_NODE_FIELD(typeName);
//     COMPARE_NODE_FIELD(params);

//     return true;
// }

// static bool _equalAlterEnumStmt(const AlterEnumStmt * a, const AlterEnumStmt * b)
// {
//     COMPARE_NODE_FIELD(typeName);
//     COMPARE_STRING_FIELD(newVal);
//     COMPARE_STRING_FIELD(newValNeighbor);
//     COMPARE_SCALAR_FIELD(newValIsAfter);
//     COMPARE_SCALAR_FIELD(skipIfExists);

//     return true;
// }

// static bool _equalViewStmt(const ViewStmt * a, const ViewStmt * b)
// {
//     COMPARE_NODE_FIELD(view);
//     COMPARE_NODE_FIELD(aliases);
//     COMPARE_NODE_FIELD(query);
//     COMPARE_SCALAR_FIELD(replace);
//     COMPARE_NODE_FIELD(options);
//     COMPARE_SCALAR_FIELD(withCheckOption);

//     return true;
// }

// static bool _equalLoadStmt(const LoadStmt * a, const LoadStmt * b)
// {
//     COMPARE_STRING_FIELD(filename);

//     return true;
// }

// static bool _equalCreateDomainStmt(const CreateDomainStmt * a, const CreateDomainStmt * b)
// {
//     COMPARE_NODE_FIELD(domainname);
//     COMPARE_NODE_FIELD(typeName);
//     COMPARE_NODE_FIELD(collClause);
//     COMPARE_NODE_FIELD(constraints);

//     return true;
// }

// static bool _equalCreateOpClassStmt(const CreateOpClassStmt * a, const CreateOpClassStmt * b)
// {
//     COMPARE_NODE_FIELD(opclassname);
//     COMPARE_NODE_FIELD(opfamilyname);
//     COMPARE_STRING_FIELD(amname);
//     COMPARE_NODE_FIELD(datatype);
//     COMPARE_NODE_FIELD(items);
//     COMPARE_SCALAR_FIELD(isDefault);

//     return true;
// }

// static bool _equalCreateOpClassItem(const CreateOpClassItem * a, const CreateOpClassItem * b)
// {
//     COMPARE_SCALAR_FIELD(itemtype);
//     COMPARE_NODE_FIELD(name);
//     COMPARE_NODE_FIELD(args);
//     COMPARE_SCALAR_FIELD(number);
//     COMPARE_NODE_FIELD(order_family);
//     COMPARE_NODE_FIELD(class_args);
//     COMPARE_NODE_FIELD(storedtype);

//     return true;
// }

// static bool _equalCreateOpFamilyStmt(const CreateOpFamilyStmt * a, const CreateOpFamilyStmt * b)
// {
//     COMPARE_NODE_FIELD(opfamilyname);
//     COMPARE_STRING_FIELD(amname);

//     return true;
// }

// static bool _equalAlterOpFamilyStmt(const AlterOpFamilyStmt * a, const AlterOpFamilyStmt * b)
// {
//     COMPARE_NODE_FIELD(opfamilyname);
//     COMPARE_STRING_FIELD(amname);
//     COMPARE_SCALAR_FIELD(isDrop);
//     COMPARE_NODE_FIELD(items);

//     return true;
// }

// static bool _equalCreatedbStmt(const CreatedbStmt * a, const CreatedbStmt * b)
// {
//     COMPARE_STRING_FIELD(dbname);
//     COMPARE_NODE_FIELD(options);
//     return true;
// }

// static bool _equalAlterDatabaseStmt(const AlterDatabaseStmt * a, const AlterDatabaseStmt * b)
// {
//     COMPARE_STRING_FIELD(dbname);
//     COMPARE_NODE_FIELD(options);

//     return true;
// }


// static bool _equalAlterDatabaseSetStmt(const AlterDatabaseSetStmt * a, const AlterDatabaseSetStmt * b)
// {
//     COMPARE_STRING_FIELD(dbname);
//     COMPARE_NODE_FIELD(setstmt);

//     return true;
// }

// static bool _equalDropdbStmt(const DropdbStmt * a, const DropdbStmt * b)
// {
//     COMPARE_STRING_FIELD(dbname);
//     COMPARE_SCALAR_FIELD(missing_ok);

//     return true;
// }

// static bool _equalVacuumStmt(const VacuumStmt * a, const VacuumStmt * b)
// {
//     COMPARE_SCALAR_FIELD(options);
//     COMPARE_SCALAR_FIELD(freeze_min_age);
//     COMPARE_SCALAR_FIELD(freeze_table_age);
//     COMPARE_SCALAR_FIELD(multixact_freeze_min_age);
//     COMPARE_SCALAR_FIELD(multixact_freeze_table_age);
//     COMPARE_NODE_FIELD(relation);
//     COMPARE_NODE_FIELD(va_cols);
//     COMPARE_NODE_FIELD(expanded_relids);

//     return true;
// }

// static bool _equalExplainStmt(const ExplainStmt * a, const ExplainStmt * b)
// {
//     COMPARE_NODE_FIELD(query);
//     COMPARE_NODE_FIELD(options);

//     return true;
// }

// static bool _equalCreateTableAsStmt(const CreateTableAsStmt * a, const CreateTableAsStmt * b)
// {
//     COMPARE_NODE_FIELD(query);
//     COMPARE_NODE_FIELD(into);
//     COMPARE_SCALAR_FIELD(relkind);
//     COMPARE_SCALAR_FIELD(is_select_into);

//     return true;
// }

// static bool _equalRefreshMatViewStmt(const RefreshMatViewStmt * a, const RefreshMatViewStmt * b)
// {
//     COMPARE_SCALAR_FIELD(concurrent);
//     COMPARE_SCALAR_FIELD(skipData);
//     COMPARE_NODE_FIELD(relation);

//     return true;
// }

// static bool _equalReplicaIdentityStmt(const ReplicaIdentityStmt * a, const ReplicaIdentityStmt * b)
// {
//     COMPARE_SCALAR_FIELD(identity_type);
//     COMPARE_STRING_FIELD(name);

//     return true;
// }

// static bool _equalAlterSystemStmt(const AlterSystemStmt * a, const AlterSystemStmt * b)
// {
//     COMPARE_NODE_FIELD(setstmt);

//     return true;
// }


// static bool _equalCreateSeqStmt(const CreateSeqStmt * a, const CreateSeqStmt * b)
// {
//     COMPARE_NODE_FIELD(sequence);
//     COMPARE_NODE_FIELD(options);
//     COMPARE_SCALAR_FIELD(ownerId);

//     return true;
// }

// static bool _equalAlterSeqStmt(const AlterSeqStmt * a, const AlterSeqStmt * b)
// {
//     COMPARE_NODE_FIELD(sequence);
//     COMPARE_NODE_FIELD(options);
//     COMPARE_SCALAR_FIELD(missing_ok);

//     return true;
// }

// static bool _equalVariableSetStmt(const VariableSetStmt * a, const VariableSetStmt * b)
// {
//     COMPARE_SCALAR_FIELD(kind);
//     COMPARE_STRING_FIELD(name);
//     COMPARE_NODE_FIELD(args);
//     COMPARE_SCALAR_FIELD(is_local);

//     return true;
// }

// static bool _equalVariableShowStmt(const VariableShowStmt * a, const VariableShowStmt * b)
// {
//     COMPARE_STRING_FIELD(name);

//     return true;
// }

// static bool _equalDiscardStmt(const DiscardStmt * a, const DiscardStmt * b)
// {
//     COMPARE_SCALAR_FIELD(target);

//     return true;
// }

// static bool _equalCreateTableSpaceStmt(const CreateTableSpaceStmt * a, const CreateTableSpaceStmt * b)
// {
//     COMPARE_STRING_FIELD(tablespacename);
//     COMPARE_STRING_FIELD(owner);
//     COMPARE_STRING_FIELD(location);
//     COMPARE_NODE_FIELD(options);

//     return true;
// }

// static bool _equalDropTableSpaceStmt(const DropTableSpaceStmt * a, const DropTableSpaceStmt * b)
// {
//     COMPARE_STRING_FIELD(tablespacename);
//     COMPARE_SCALAR_FIELD(missing_ok);

//     return true;
// }

// static bool _equalAlterTableSpaceOptionsStmt(const AlterTableSpaceOptionsStmt * a, const AlterTableSpaceOptionsStmt * b)
// {
//     COMPARE_STRING_FIELD(tablespacename);
//     COMPARE_NODE_FIELD(options);
//     COMPARE_SCALAR_FIELD(isReset);

//     return true;
// }

// static bool _equalAlterTableMoveAllStmt(const AlterTableMoveAllStmt * a, const AlterTableMoveAllStmt * b)
// {
//     COMPARE_STRING_FIELD(orig_tablespacename);
//     COMPARE_SCALAR_FIELD(objtype);
//     COMPARE_NODE_FIELD(roles);
//     COMPARE_STRING_FIELD(new_tablespacename);
//     COMPARE_SCALAR_FIELD(nowait);

//     return true;
// }

// static bool _equalCreateExtensionStmt(const CreateExtensionStmt * a, const CreateExtensionStmt * b)
// {
//     COMPARE_STRING_FIELD(extname);
//     COMPARE_SCALAR_FIELD(if_not_exists);
//     COMPARE_NODE_FIELD(options);
//     COMPARE_SCALAR_FIELD(create_ext_state);

//     return true;
// }

// static bool _equalAlterExtensionStmt(const AlterExtensionStmt * a, const AlterExtensionStmt * b)
// {
//     COMPARE_STRING_FIELD(extname);
//     COMPARE_NODE_FIELD(options);
//     COMPARE_SCALAR_FIELD(update_ext_state);

//     return true;
// }

// static bool _equalAlterExtensionContentsStmt(const AlterExtensionContentsStmt * a, const AlterExtensionContentsStmt * b)
// {
//     COMPARE_STRING_FIELD(extname);
//     COMPARE_SCALAR_FIELD(action);
//     COMPARE_SCALAR_FIELD(objtype);
//     COMPARE_NODE_FIELD(objname);
//     COMPARE_NODE_FIELD(objargs);

//     return true;
// }

// static bool _equalCreateFdwStmt(const CreateFdwStmt * a, const CreateFdwStmt * b)
// {
//     COMPARE_STRING_FIELD(fdwname);
//     COMPARE_NODE_FIELD(func_options);
//     COMPARE_NODE_FIELD(options);

//     return true;
// }

// static bool _equalAlterFdwStmt(const AlterFdwStmt * a, const AlterFdwStmt * b)
// {
//     COMPARE_STRING_FIELD(fdwname);
//     COMPARE_NODE_FIELD(func_options);
//     COMPARE_NODE_FIELD(options);

//     return true;
// }

// static bool _equalCreateForeignServerStmt(const CreateForeignServerStmt * a, const CreateForeignServerStmt * b)
// {
//     COMPARE_STRING_FIELD(servername);
//     COMPARE_STRING_FIELD(servertype);
//     COMPARE_STRING_FIELD(version);
//     COMPARE_STRING_FIELD(fdwname);
//     COMPARE_NODE_FIELD(options);

//     return true;
// }

// static bool _equalAlterForeignServerStmt(const AlterForeignServerStmt * a, const AlterForeignServerStmt * b)
// {
//     COMPARE_STRING_FIELD(servername);
//     COMPARE_STRING_FIELD(version);
//     COMPARE_NODE_FIELD(options);
//     COMPARE_SCALAR_FIELD(has_version);

//     return true;
// }

// static bool _equalCreateUserMappingStmt(const CreateUserMappingStmt * a, const CreateUserMappingStmt * b)
// {
//     COMPARE_STRING_FIELD(username);
//     COMPARE_STRING_FIELD(servername);
//     COMPARE_NODE_FIELD(options);

//     return true;
// }

// static bool _equalAlterUserMappingStmt(const AlterUserMappingStmt * a, const AlterUserMappingStmt * b)
// {
//     COMPARE_STRING_FIELD(username);
//     COMPARE_STRING_FIELD(servername);
//     COMPARE_NODE_FIELD(options);

//     return true;
// }

// static bool _equalDropUserMappingStmt(const DropUserMappingStmt * a, const DropUserMappingStmt * b)
// {
//     COMPARE_STRING_FIELD(username);
//     COMPARE_STRING_FIELD(servername);
//     COMPARE_SCALAR_FIELD(missing_ok);

//     return true;
// }

// static bool _equalCreateForeignTableStmt(const CreateForeignTableStmt * a, const CreateForeignTableStmt * b)
// {
//     if (!_equalCreateStmt(&a->base, &b->base))
//         return false;

//     COMPARE_STRING_FIELD(servername);
//     COMPARE_NODE_FIELD(options);

//     return true;
// }

// static bool _equalCreateTrigStmt(const CreateTrigStmt * a, const CreateTrigStmt * b)
// {
//     COMPARE_STRING_FIELD(trigname);
//     COMPARE_NODE_FIELD(relation);
//     COMPARE_NODE_FIELD(funcname);
//     COMPARE_NODE_FIELD(args);
//     COMPARE_SCALAR_FIELD(row);
//     COMPARE_SCALAR_FIELD(timing);
//     COMPARE_SCALAR_FIELD(events);
//     COMPARE_NODE_FIELD(columns);
//     COMPARE_NODE_FIELD(whenClause);
//     COMPARE_SCALAR_FIELD(isconstraint);
//     COMPARE_SCALAR_FIELD(deferrable);
//     COMPARE_SCALAR_FIELD(initdeferred);
//     COMPARE_NODE_FIELD(constrrel);

//     return true;
// }

// static bool _equalCreateEventTrigStmt(const CreateEventTrigStmt * a, const CreateEventTrigStmt * b)
// {
//     COMPARE_STRING_FIELD(trigname);
//     COMPARE_STRING_FIELD(eventname);
//     COMPARE_NODE_FIELD(funcname);
//     COMPARE_NODE_FIELD(whenclause);

//     return true;
// }

// static bool _equalAlterEventTrigStmt(const AlterEventTrigStmt * a, const AlterEventTrigStmt * b)
// {
//     COMPARE_STRING_FIELD(trigname);
//     COMPARE_SCALAR_FIELD(tgenabled);

//     return true;
// }

// static bool _equalCreatePLangStmt(const CreatePLangStmt * a, const CreatePLangStmt * b)
// {
//     COMPARE_SCALAR_FIELD(replace);
//     COMPARE_STRING_FIELD(plname);
//     COMPARE_NODE_FIELD(plhandler);
//     COMPARE_NODE_FIELD(plinline);
//     COMPARE_NODE_FIELD(plvalidator);
//     COMPARE_SCALAR_FIELD(pltrusted);

//     return true;
// }

// static bool _equalCreateRoleStmt(const CreateRoleStmt * a, const CreateRoleStmt * b)
// {
//     COMPARE_SCALAR_FIELD(stmt_type);
//     COMPARE_STRING_FIELD(role);
//     COMPARE_NODE_FIELD(options);

//     return true;
// }

// static bool _equalDenyLoginInterval(const DenyLoginInterval * a, const DenyLoginInterval * b)
// {
//     COMPARE_NODE_FIELD(start);
//     COMPARE_NODE_FIELD(end);

//     return true;
// }

// static bool _equalDenyLoginPoint(const DenyLoginPoint * a, const DenyLoginPoint * b)
// {
//     COMPARE_NODE_FIELD(day);
//     COMPARE_NODE_FIELD(time);

//     return true;
// }

// static bool _equalAlterRoleStmt(const AlterRoleStmt * a, const AlterRoleStmt * b)
// {
//     COMPARE_STRING_FIELD(role);
//     COMPARE_NODE_FIELD(options);
//     COMPARE_SCALAR_FIELD(action);

//     return true;
// }

// static bool _equalAlterRoleSetStmt(const AlterRoleSetStmt * a, const AlterRoleSetStmt * b)
// {
//     COMPARE_STRING_FIELD(role);
//     COMPARE_STRING_FIELD(database);
//     COMPARE_NODE_FIELD(setstmt);

//     return true;
// }

// static bool _equalDropRoleStmt(const DropRoleStmt * a, const DropRoleStmt * b)
// {
//     COMPARE_NODE_FIELD(roles);
//     COMPARE_SCALAR_FIELD(missing_ok);

//     return true;
// }

// static bool _equalLockStmt(const LockStmt * a, const LockStmt * b)
// {
//     COMPARE_NODE_FIELD(relations);
//     COMPARE_SCALAR_FIELD(mode);
//     COMPARE_SCALAR_FIELD(nowait);
//     COMPARE_SCALAR_FIELD(masteronly);

//     return true;
// }

// static bool _equalConstraintsSetStmt(const ConstraintsSetStmt * a, const ConstraintsSetStmt * b)
// {
//     COMPARE_NODE_FIELD(constraints);
//     COMPARE_SCALAR_FIELD(deferred);

//     return true;
// }

// static bool _equalReindexStmt(const ReindexStmt * a, const ReindexStmt * b)
// {
//     COMPARE_SCALAR_FIELD(kind);
//     COMPARE_NODE_FIELD(relation);
//     COMPARE_STRING_FIELD(name);
//     COMPARE_SCALAR_FIELD(do_system);
//     COMPARE_SCALAR_FIELD(do_user);
//     COMPARE_SCALAR_FIELD(relid);

//     return true;
// }

// static bool _equalCreateSchemaStmt(const CreateSchemaStmt * a, const CreateSchemaStmt * b)
// {
//     COMPARE_STRING_FIELD(schemaname);
//     COMPARE_STRING_FIELD(authid);
//     COMPARE_NODE_FIELD(schemaElts);
//     COMPARE_SCALAR_FIELD(if_not_exists);
//     COMPARE_SCALAR_FIELD(istemp);

//     return true;
// }

// static bool _equalCreateConversionStmt(const CreateConversionStmt * a, const CreateConversionStmt * b)
// {
//     COMPARE_NODE_FIELD(conversion_name);
//     COMPARE_STRING_FIELD(for_encoding_name);
//     COMPARE_STRING_FIELD(to_encoding_name);
//     COMPARE_NODE_FIELD(func_name);
//     COMPARE_SCALAR_FIELD(def);

//     return true;
// }

// static bool _equalCreateCastStmt(const CreateCastStmt * a, const CreateCastStmt * b)
// {
//     COMPARE_NODE_FIELD(sourcetype);
//     COMPARE_NODE_FIELD(targettype);
//     COMPARE_NODE_FIELD(func);
//     COMPARE_SCALAR_FIELD(context);
//     COMPARE_SCALAR_FIELD(inout);

//     return true;
// }

// static bool _equalPrepareStmt(const PrepareStmt * a, const PrepareStmt * b)
// {
//     COMPARE_STRING_FIELD(name);
//     COMPARE_NODE_FIELD(argtypes);
//     COMPARE_NODE_FIELD(query);

//     return true;
// }

// static bool _equalExecuteStmt(const ExecuteStmt * a, const ExecuteStmt * b)
// {
//     COMPARE_STRING_FIELD(name);
//     COMPARE_NODE_FIELD(params);

//     return true;
// }

// static bool _equalDeallocateStmt(const DeallocateStmt * a, const DeallocateStmt * b)
// {
//     COMPARE_STRING_FIELD(name);

//     return true;
// }

// static bool _equalDropOwnedStmt(const DropOwnedStmt * a, const DropOwnedStmt * b)
// {
//     COMPARE_NODE_FIELD(roles);
//     COMPARE_SCALAR_FIELD(behavior);

//     return true;
// }


// static bool _equalCreateQueueStmt(const CreateQueueStmt * a, const CreateQueueStmt * b)
// {
//     COMPARE_STRING_FIELD(queue);
//     COMPARE_NODE_FIELD(options);
//     return true;
// }

// static bool _equalAlterQueueStmt(const AlterQueueStmt * a, const AlterQueueStmt * b)
// {
//     COMPARE_STRING_FIELD(queue);
//     COMPARE_NODE_FIELD(options);
//     return true;
// }

// static bool _equalDropQueueStmt(const DropQueueStmt * a, const DropQueueStmt * b)
// {
//     COMPARE_STRING_FIELD(queue);
//     return true;
// }

// static bool _equalCreateResourceGroupStmt(const CreateResourceGroupStmt * a, const CreateResourceGroupStmt * b)
// {
//     COMPARE_STRING_FIELD(name);
//     COMPARE_NODE_FIELD(options);
//     return true;
// }

// static bool _equalDropResourceGroupStmt(const DropResourceGroupStmt * a, const DropResourceGroupStmt * b)
// {
//     COMPARE_STRING_FIELD(name);
//     return true;
// }

// static bool _equalAlterResourceGroupStmt(const AlterResourceGroupStmt * a, const AlterResourceGroupStmt * b)
// {
//     COMPARE_STRING_FIELD(name);
//     COMPARE_NODE_FIELD(options);
//     return true;
// }

// /*
//  * stuff from parsenodes.h
//  */

// static bool _equalReassignOwnedStmt(const ReassignOwnedStmt * a, const ReassignOwnedStmt * b)
// {
//     COMPARE_NODE_FIELD(roles);
//     COMPARE_STRING_FIELD(newrole);

//     return true;
// }

// static bool _equalAlterTSDictionaryStmt(const AlterTSDictionaryStmt * a, const AlterTSDictionaryStmt * b)
// {
//     COMPARE_NODE_FIELD(dictname);
//     COMPARE_NODE_FIELD(options);

//     return true;
// }

// static bool _equalAlterTSConfigurationStmt(const AlterTSConfigurationStmt * a, const AlterTSConfigurationStmt * b)
// {
//     COMPARE_NODE_FIELD(cfgname);
//     COMPARE_NODE_FIELD(tokentype);
//     COMPARE_NODE_FIELD(dicts);
//     COMPARE_SCALAR_FIELD(override);
//     COMPARE_SCALAR_FIELD(replace);
//     COMPARE_SCALAR_FIELD(missing_ok);

//     return true;
// }

extern bool _equalAExpr(const void * a_arg, const void * b_arg);

extern bool _equalColumnRef(const void * a_arg, const void * b_arg);

extern bool _equalParamRef(const void * a_arg, const void * b_arg);

extern bool _equalAConst(const void * a_arg, const void * b_arg);

extern bool _equalFuncCall(const void * a_arg, const void * b_arg);

extern bool _equalAStar(const void * a_arg, const void * b_arg);

extern bool _equalAIndices(const void * a_arg, const void * b_arg);

extern bool _equalA_Indirection(const void * a_arg, const void * b_arg);

extern bool _equalA_ArrayExpr(const void * a_arg, const void * b_arg);

extern bool _equalResTarget(const void * a_arg, const void * b_arg);

extern bool _equalTypeName(const void * a_arg, const void * b_arg);

extern bool _equalTypeCast(const void * a_arg, const void * b_arg);

// static bool _equalCollateClause(const duckdb_libpgquery::PGCollateClause * a, const duckdb_libpgquery::PGCollateClause * b)
// {
//     COMPARE_NODE_FIELD(arg);
//     COMPARE_NODE_FIELD(collname);
//     COMPARE_LOCATION_FIELD(location);

//     return true;
// }

extern bool _equalSortBy(const void * a_arg, const void * b_arg);

extern bool _equalWindowDef(const void * a_arg, const void * b_arg);

extern bool _equalRangeSubselect(const void * a_arg, const void * b_arg);

extern bool _equalRangeFunction(const void * a_arg, const void * b_arg);

// static bool _equalIndexElem(const duckdb_libpgquery::PGIndexElem * a, const duckdb_libpgquery::PGIndexElem * b)
// {
//     COMPARE_STRING_FIELD(name);
//     COMPARE_NODE_FIELD(expr);
//     COMPARE_STRING_FIELD(indexcolname);
//     COMPARE_NODE_FIELD(collation);
//     COMPARE_NODE_FIELD(opclass);
//     COMPARE_SCALAR_FIELD(ordering);
//     COMPARE_SCALAR_FIELD(nulls_ordering);

//     return true;
// }

extern bool _equalColumnDef(const void * a_arg, const void * b_arg);

// static bool _equalConstraint(const Constraint * a, const Constraint * b)
// {
//     COMPARE_SCALAR_FIELD(contype);
//     COMPARE_STRING_FIELD(conname);
//     COMPARE_SCALAR_FIELD(deferrable);
//     COMPARE_SCALAR_FIELD(initdeferred);
//     COMPARE_LOCATION_FIELD(location);
//     COMPARE_SCALAR_FIELD(is_no_inherit);
//     COMPARE_NODE_FIELD(raw_expr);
//     COMPARE_STRING_FIELD(cooked_expr);
//     COMPARE_NODE_FIELD(keys);
//     COMPARE_NODE_FIELD(exclusions);
//     COMPARE_NODE_FIELD(options);
//     COMPARE_STRING_FIELD(indexname);
//     COMPARE_STRING_FIELD(indexspace);
//     COMPARE_STRING_FIELD(access_method);
//     COMPARE_NODE_FIELD(where_clause);
//     COMPARE_NODE_FIELD(pktable);
//     COMPARE_NODE_FIELD(fk_attrs);
//     COMPARE_NODE_FIELD(pk_attrs);
//     COMPARE_SCALAR_FIELD(fk_matchtype);
//     COMPARE_SCALAR_FIELD(fk_upd_action);
//     COMPARE_SCALAR_FIELD(fk_del_action);
//     COMPARE_NODE_FIELD(old_conpfeqop);
//     COMPARE_SCALAR_FIELD(old_pktable_oid);
//     COMPARE_SCALAR_FIELD(skip_validation);
//     COMPARE_SCALAR_FIELD(initially_valid);

//     COMPARE_SCALAR_FIELD(trig1Oid);
//     COMPARE_SCALAR_FIELD(trig2Oid);
//     COMPARE_SCALAR_FIELD(trig3Oid);
//     COMPARE_SCALAR_FIELD(trig4Oid);

//     return true;
// }

// static bool _equalDefElem(const DefElem * a, const DefElem * b)
// {
//     COMPARE_STRING_FIELD(defnamespace);
//     COMPARE_STRING_FIELD(defname);
//     COMPARE_NODE_FIELD(arg);
//     COMPARE_SCALAR_FIELD(defaction);

//     return true;
// }

extern bool _equalLockingClause(const void * a_arg, const void * b_arg);

extern bool _equalRangeTblEntry(const void * a_arg, const void * b_arg);

extern bool _equalRangeTblFunction(const void * a_arg, const void * b_arg);

// static bool _equalWithCheckOption(const WithCheckOption * a, const WithCheckOption * b)
// {
//     COMPARE_STRING_FIELD(viewname);
//     COMPARE_NODE_FIELD(qual);
//     COMPARE_SCALAR_FIELD(cascaded);

//     return true;
// }

extern bool _equalSortGroupClause(const void * a_arg, const void * b_arg);

// static bool _equalGroupingClause(const PGGroupingClause * a, const PGGroupingClause * b)
// {
//     COMPARE_SCALAR_FIELD(groupType);
//     COMPARE_NODE_FIELD(groupsets);

//     return true;
// }

extern bool _equalGroupingFunc(const void * a_arg, const void * b_arg);

// static bool _equalGrouping(const Grouping * a __attribute__((unused)), const Grouping * b __attribute__((unused)))

// {
//     return true;
// }

// static bool _equalGroupId(const GroupId * a __attribute__((unused)), const GroupId * b __attribute__((unused)))
// {
//     return true;
// }

extern bool _equalWindowClause(const void * a_arg, const void * b_arg);

// static bool _equalRowMarkClause(const RowMarkClause * a, const RowMarkClause * b)
// {
//     COMPARE_SCALAR_FIELD(rti);
//     COMPARE_SCALAR_FIELD(strength);
//     COMPARE_SCALAR_FIELD(noWait);
//     COMPARE_SCALAR_FIELD(pushedDown);

//     return true;
// }

extern bool _equalWithClause(const void * a_arg, const void * b_arg);

extern bool _equalCommonTableExpr(const void * a_arg, const void * b_arg);

// static bool _equalTableValueExpr(const TableValueExpr * a, const TableValueExpr * b)
// {
//     COMPARE_NODE_FIELD(subquery);

//     return true;
// }

// static bool _equalAlterTypeStmt(const AlterTypeStmt * a, const AlterTypeStmt * b)
// {
//     COMPARE_NODE_FIELD(typeName);
//     COMPARE_NODE_FIELD(encoding);

//     return true;
// }

// static bool _equalDistributedBy(const DistributedBy * a, const DistributedBy * b)
// {
//     COMPARE_SCALAR_FIELD(ptype);
//     COMPARE_SCALAR_FIELD(numsegments);
//     COMPARE_NODE_FIELD(keyCols);

//     return true;
// }

// static bool _equalPartitionRule(const PartitionRule * a, const PartitionRule * b)
// {
//     COMPARE_SCALAR_FIELD(parchildrelid);

//     return true;
// }

// static bool _equalXmlSerialize(const XmlSerialize * a, const XmlSerialize * b)
// {
//     COMPARE_SCALAR_FIELD(xmloption);
//     COMPARE_NODE_FIELD(expr);
//     COMPARE_NODE_FIELD(typeName);
//     COMPARE_LOCATION_FIELD(location);

//     return true;
// }

/*
 * Stuff from pg_list.h
 */

extern bool _equalList(const void * a_arg, const void * b_arg);

/*
 * Stuff from value.h
 */

extern bool _equalValue(const void * a_arg, const void * b_arg);

/*
 * Compare two TupleDesc structures for logical equality
 *
 * Note: we deliberately do not check the attrelid and tdtypmod fields.
 * This allows typcache.c to use this routine to see if a cached record type
 * matches a requested type, and is harmless for relcache.c's uses.
 * We don't compare tdrefcount, either.
 */
extern bool equalTupleDescs(PGTupleDescPtr tupdesc1, PGTupleDescPtr tupdesc2, bool strict);

}
