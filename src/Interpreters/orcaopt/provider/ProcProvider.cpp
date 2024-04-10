#include <Interpreters/orcaopt/provider/ProcProvider.h>

#include <Interpreters/Context.h>

using namespace duckdb_libpgquery;

#define NEW_PROC(PROCVARNM, OID, PRONAME, PRONAMESPACE, PROOWNER, PROLANG, PROCOST, PROROWS, PROVARIADIC, PROTRANSFORM, PROISAGG, PROISWINDOW, PROSECDEF, PROLEAKPROOF, PROISSTRICT, PRORETSET, PROVOLATILE, PRONARGS, PRONARGDEFAULTS, PRORETTYPE, ...) \
    std::pair<PGOid, PGProcPtr> ProcProvider::PROC_##PROCVARNM = {PGOid(OID), \
        std::make_shared<Form_pg_proc>(Form_pg_proc{ \
            .oid = PGOid(OID), \
            /*proname*/ .proname = PRONAME, \
            /*pronamespace*/ .pronamespace = PGOid(PRONAMESPACE), \
            /*proowner*/ .proowner = PGOid(PROOWNER), \
            /*prolang*/ .prolang = PGOid(PROLANG), \
            /*procost*/ .procost = PROCOST, \
            /*prorows*/ .prorows = PROROWS, \
            /*provariadic*/ .provariadic = PGOid(PROVARIADIC), \
            /*protransform*/ .protransform = PGOid(PROTRANSFORM), \
            /*proisagg*/ .proisagg = PROISAGG, \
            /*proiswindow*/ .proiswindow = PROISWINDOW, \
            /*prosecdef*/ .prosecdef = PROSECDEF, \
            /*proleakproof*/ .proleakproof = PROLEAKPROOF, \
            /*proisstrict*/ .proisstrict = PROISSTRICT, \
            /*proretset*/ .proretset = PRORETSET, \
            /*provolatile*/ .provolatile = PROVOLATILE, \
            /*pronargs*/ .pronargs = PRONARGS, \
            /*pronargdefaults*/ .pronargdefaults = PRONARGDEFAULTS, \
            /*prorettype*/ .prorettype = PGOid(PRORETTYPE), \
            /*proargtypes*/ .proargtypes = __VA_ARGS__})};

namespace DB
{

NEW_PROC(INT2PL, 176, "plus", 1, 1, 12, 1, 0.0, 0, 0, false, false, false, false, true, false, 'i', 2, 0, 21, {PGOid(21), PGOid(21)})

// std::pair<PGOid, PGProcPtr> ProcProvider::PROC_INT2PL = std::pair<PGOid, PGProcPtr>(
//     PGOid(176),
//     std::make_shared<Form_pg_proc>(Form_pg_proc{
//         .oid = PGOid(176),
//         /*proname*/ .proname = "plus",
//         /*pronamespace*/ .pronamespace = PGOid(1),
//         /*proowner*/ .proowner = PGOid(1),
//         /*prolang*/ .prolang = PGOid(12),
//         /*procost*/ .procost = 1,
//         /*prorows*/ .prorows = 0.0,
//         /*provariadic*/ .provariadic = PGOid(0),
//         /*protransform*/ .protransform = PGOid(0),
//         /*proisagg*/ .proisagg = false,
//         /*proiswindow*/ .proiswindow = false,
//         /*prosecdef*/ .prosecdef = false,
//         /*proleakproof*/ .proleakproof = false,
//         /*proisstrict*/ .proisstrict = true,
//         /*proretset*/ .proretset = false,
//         /*provolatile*/ .provolatile = 'i',
//         /*pronargs*/ .pronargs = 2,
//         /*pronargdefaults*/ .pronargdefaults = 0,
//         /*prorettype*/ .prorettype = PGOid(21),
//         /*proargtypes*/ .proargtypes = {PGOid(21), PGOid(21)}}));

std::pair<PGOid, PGProcPtr> ProcProvider::PROC_INT4PL = std::pair<PGOid, PGProcPtr>(
    PGOid(177),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = PGOid(177),
        /*proname*/ .proname = "plus",
        /*pronamespace*/ .pronamespace = PGOid(1),
        /*proowner*/ .proowner = PGOid(1),
        /*prolang*/ .prolang = PGOid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = PGOid(0),
        /*protransform*/ .protransform = PGOid(0),
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 2,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = PGOid(23),
        /*proargtypes*/ .proargtypes = {PGOid(23), PGOid(23)}}));

std::pair<PGOid, PGProcPtr> ProcProvider::PROC_INT24PL = std::pair<PGOid, PGProcPtr>(
    PGOid(178),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = PGOid(178),
        /*proname*/ .proname = "plus",
        /*pronamespace*/ .pronamespace = PGOid(1),
        /*proowner*/ .proowner = PGOid(1),
        /*prolang*/ .prolang = PGOid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = PGOid(0),
        /*protransform*/ .protransform = PGOid(0),
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 2,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = PGOid(23),
        /*proargtypes*/ .proargtypes = {PGOid(21), PGOid(23)}}));

std::pair<PGOid, PGProcPtr> ProcProvider::PROC_INT42PL = std::pair<PGOid, PGProcPtr>(
    PGOid(179),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = PGOid(179),
        /*proname*/ .proname = "plus",
        /*pronamespace*/ .pronamespace = PGOid(1),
        /*proowner*/ .proowner = PGOid(1),
        /*prolang*/ .prolang = PGOid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = PGOid(0),
        /*protransform*/ .protransform = PGOid(0),
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 2,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = PGOid(23),
        /*proargtypes*/ .proargtypes = {PGOid(23), PGOid(21)}}));

std::pair<PGOid, PGProcPtr> ProcProvider::PROC_INT2MI = std::pair<PGOid, PGProcPtr>(
    PGOid(180),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = PGOid(180),
        /*proname*/ .proname = "minus",
        /*pronamespace*/ .pronamespace = PGOid(1),
        /*proowner*/ .proowner = PGOid(1),
        /*prolang*/ .prolang = PGOid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = PGOid(0),
        /*protransform*/ .protransform = PGOid(0),
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 2,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = PGOid(21),
        /*proargtypes*/ .proargtypes = {PGOid(21), PGOid(21)}}));

std::pair<PGOid, PGProcPtr> ProcProvider::PROC_INT4MI = std::pair<PGOid, PGProcPtr>(
    PGOid(181),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = PGOid(181),
        /*proname*/ .proname = "minus",
        /*pronamespace*/ .pronamespace = PGOid(1),
        /*proowner*/ .proowner = PGOid(1),
        /*prolang*/ .prolang = PGOid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = PGOid(0),
        /*protransform*/ .protransform = PGOid(0),
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 2,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = PGOid(23),
        /*proargtypes*/ .proargtypes = {PGOid(23), PGOid(23)}}));

std::pair<PGOid, PGProcPtr> ProcProvider::PROC_INT24MI = std::pair<PGOid, PGProcPtr>(
    PGOid(182),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = PGOid(182),
        /*proname*/ .proname = "minus",
        /*pronamespace*/ .pronamespace = PGOid(1),
        /*proowner*/ .proowner = PGOid(1),
        /*prolang*/ .prolang = PGOid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = PGOid(0),
        /*protransform*/ .protransform = PGOid(0),
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 2,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = PGOid(23),
        /*proargtypes*/ .proargtypes = {PGOid(21), PGOid(23)}}));

std::pair<PGOid, PGProcPtr> ProcProvider::PROC_INT42MI = std::pair<PGOid, PGProcPtr>(
    PGOid(183),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = PGOid(183),
        /*proname*/ .proname = "minus",
        /*pronamespace*/ .pronamespace = PGOid(1),
        /*proowner*/ .proowner = PGOid(1),
        /*prolang*/ .prolang = PGOid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = PGOid(0),
        /*protransform*/ .protransform = PGOid(0),
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 2,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = PGOid(23),
        /*proargtypes*/ .proargtypes = {PGOid(23), PGOid(21)}}));
std::pair<PGOid, PGProcPtr> ProcProvider::PROC_INT2MUL = std::pair<PGOid, PGProcPtr>(
    PGOid(152),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = PGOid(152),
        /*proname*/ .proname = "multiply",
        /*pronamespace*/ .pronamespace = PGOid(1),
        /*proowner*/ .proowner = PGOid(1),
        /*prolang*/ .prolang = PGOid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = PGOid(0),
        /*protransform*/ .protransform = PGOid(0),
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 2,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = PGOid(21),
        /*proargtypes*/ .proargtypes = {PGOid(21), PGOid(21)}}));

std::pair<PGOid, PGProcPtr> ProcProvider::PROC_INT4MUL = std::pair<PGOid, PGProcPtr>(
    PGOid(141),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = PGOid(141),
        /*proname*/ .proname = "multiply",
        /*pronamespace*/ .pronamespace = PGOid(1),
        /*proowner*/ .proowner = PGOid(1),
        /*prolang*/ .prolang = PGOid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = PGOid(0),
        /*protransform*/ .protransform = PGOid(0),
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 2,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = PGOid(23),
        /*proargtypes*/ .proargtypes = {PGOid(23), PGOid(23)}}));

std::pair<PGOid, PGProcPtr> ProcProvider::PROC_INT24MUL = std::pair<PGOid, PGProcPtr>(
    PGOid(170),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = PGOid(170),
        /*proname*/ .proname = "multiply",
        /*pronamespace*/ .pronamespace = PGOid(1),
        /*proowner*/ .proowner = PGOid(1),
        /*prolang*/ .prolang = PGOid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = PGOid(0),
        /*protransform*/ .protransform = PGOid(0),
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 2,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = PGOid(23),
        /*proargtypes*/ .proargtypes = {PGOid(21), PGOid(23)}}));

std::pair<PGOid, PGProcPtr> ProcProvider::PROC_INT42MUL = std::pair<PGOid, PGProcPtr>(
    PGOid(171),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = PGOid(171),
        /*proname*/ .proname = "multiply",
        /*pronamespace*/ .pronamespace = PGOid(1),
        /*proowner*/ .proowner = PGOid(1),
        /*prolang*/ .prolang = PGOid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = PGOid(0),
        /*protransform*/ .protransform = PGOid(0),
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 2,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = PGOid(23),
        /*proargtypes*/ .proargtypes = {PGOid(23), PGOid(21)}}));

std::pair<PGOid, PGProcPtr> ProcProvider::PROC_INT2DIV = std::pair<PGOid, PGProcPtr>(
    PGOid(153),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = PGOid(153),
        /*proname*/ .proname = "devide",
        /*pronamespace*/ .pronamespace = PGOid(1),
        /*proowner*/ .proowner = PGOid(1),
        /*prolang*/ .prolang = PGOid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = PGOid(0),
        /*protransform*/ .protransform = PGOid(0),
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 2,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = PGOid(21),
        /*proargtypes*/ .proargtypes = {PGOid(21), PGOid(21)}}));

std::pair<PGOid, PGProcPtr> ProcProvider::PROC_INT4DIV = std::pair<PGOid, PGProcPtr>(
    PGOid(154),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = PGOid(154),
        /*proname*/ .proname = "devide",
        /*pronamespace*/ .pronamespace = PGOid(1),
        /*proowner*/ .proowner = PGOid(1),
        /*prolang*/ .prolang = PGOid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = PGOid(0),
        /*protransform*/ .protransform = PGOid(0),
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 2,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = PGOid(23),
        /*proargtypes*/ .proargtypes = {PGOid(23), PGOid(23)}}));

std::pair<PGOid, PGProcPtr> ProcProvider::PROC_INT24DIV = std::pair<PGOid, PGProcPtr>(
    PGOid(172),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = PGOid(172),
        /*proname*/ .proname = "devide",
        /*pronamespace*/ .pronamespace = PGOid(1),
        /*proowner*/ .proowner = PGOid(1),
        /*prolang*/ .prolang = PGOid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = PGOid(0),
        /*protransform*/ .protransform = PGOid(0),
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 2,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = PGOid(23),
        /*proargtypes*/ .proargtypes = {PGOid(21), PGOid(23)}}));

std::pair<PGOid, PGProcPtr> ProcProvider::PROC_INT42DIV = std::pair<PGOid, PGProcPtr>(
    PGOid(173),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = PGOid(173),
        /*proname*/ .proname = "devide",
        /*pronamespace*/ .pronamespace = PGOid(1),
        /*proowner*/ .proowner = PGOid(1),
        /*prolang*/ .prolang = PGOid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = PGOid(0),
        /*protransform*/ .protransform = PGOid(0),
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 2,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = PGOid(23),
        /*proargtypes*/ .proargtypes = {PGOid(23), PGOid(21)}}));

std::pair<PGOid, PGProcPtr> ProcProvider::PROC_INT64TOINT16 = std::pair<PGOid, PGProcPtr>(
    PGOid(714),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = PGOid(714),
        /*proname*/ .proname = "toInt16",
        /*pronamespace*/ .pronamespace = PGOid(1),
        /*proowner*/ .proowner = PGOid(1),
        /*prolang*/ .prolang = PGOid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = PGOid(0),
        /*protransform*/ .protransform = PGOid(0),
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 1,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = PGOid(21),
        /*proargtypes*/ .proargtypes = {PGOid(20)}}));

std::pair<PGOid, PGProcPtr> ProcProvider::PROC_INT64TOINT32 = std::pair<PGOid, PGProcPtr>(
    PGOid(480),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = PGOid(480),
        /*proname*/ .proname = "toInt32",
        /*pronamespace*/ .pronamespace = PGOid(1),
        /*proowner*/ .proowner = PGOid(1),
        /*prolang*/ .prolang = PGOid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = PGOid(0),
        /*protransform*/ .protransform = PGOid(0),
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 1,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = PGOid(23),
        /*proargtypes*/ .proargtypes = {PGOid(20)}}));

std::pair<PGOid, PGProcPtr> ProcProvider::PROC_INT64TOFLOAT32 = std::pair<PGOid, PGProcPtr>(
    PGOid(652),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = PGOid(652),
        /*proname*/ .proname = "toFloat32",
        /*pronamespace*/ .pronamespace = PGOid(1),
        /*proowner*/ .proowner = PGOid(1),
        /*prolang*/ .prolang = PGOid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = PGOid(0),
        /*protransform*/ .protransform = PGOid(0),
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 1,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = PGOid(700),
        /*proargtypes*/ .proargtypes = {PGOid(20)}}));

std::pair<PGOid, PGProcPtr> ProcProvider::PROC_INT64TOFLOAT64 = std::pair<PGOid, PGProcPtr>(
    PGOid(482),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = PGOid(482),
        /*proname*/ .proname = "toFloat64",
        /*pronamespace*/ .pronamespace = PGOid(1),
        /*proowner*/ .proowner = PGOid(1),
        /*prolang*/ .prolang = PGOid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = PGOid(0),
        /*protransform*/ .protransform = PGOid(0),
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 1,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = PGOid(701),
        /*proargtypes*/ .proargtypes = {PGOid(20)}}));

std::pair<PGOid, PGProcPtr> ProcProvider::PROC_INT16TOINT64 = std::pair<PGOid, PGProcPtr>(
    PGOid(754),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = PGOid(754),
        /*proname*/ .proname = "toInt64",
        /*pronamespace*/ .pronamespace = PGOid(1),
        /*proowner*/ .proowner = PGOid(1),
        /*prolang*/ .prolang = PGOid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = PGOid(0),
        /*protransform*/ .protransform = PGOid(0),
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 1,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = PGOid(20),
        /*proargtypes*/ .proargtypes = {PGOid(21)}}));

std::pair<PGOid, PGProcPtr> ProcProvider::PROC_INT16TOINT32 = std::pair<PGOid, PGProcPtr>(
    PGOid(313),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = PGOid(313),
        /*proname*/ .proname = "toInt32",
        /*pronamespace*/ .pronamespace = PGOid(1),
        /*proowner*/ .proowner = PGOid(1),
        /*prolang*/ .prolang = PGOid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = PGOid(0),
        /*protransform*/ .protransform = PGOid(0),
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 1,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = PGOid(23),
        /*proargtypes*/ .proargtypes = {PGOid(21)}}));

std::pair<PGOid, PGProcPtr> ProcProvider::PROC_INT16TOFLOAT32 = std::pair<PGOid, PGProcPtr>(
    PGOid(236),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = PGOid(236),
        /*proname*/ .proname = "toFloat32",
        /*pronamespace*/ .pronamespace = PGOid(1),
        /*proowner*/ .proowner = PGOid(1),
        /*prolang*/ .prolang = PGOid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = PGOid(0),
        /*protransform*/ .protransform = PGOid(0),
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 1,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = PGOid(700),
        /*proargtypes*/ .proargtypes = {PGOid(21)}}));

std::pair<PGOid, PGProcPtr> ProcProvider::PROC_INT16TOFLOAT64 = std::pair<PGOid, PGProcPtr>(
    PGOid(235),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = PGOid(235),
        /*proname*/ .proname = "toFloat64",
        /*pronamespace*/ .pronamespace = PGOid(1),
        /*proowner*/ .proowner = PGOid(1),
        /*prolang*/ .prolang = PGOid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = PGOid(0),
        /*protransform*/ .protransform = PGOid(0),
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 1,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = PGOid(701),
        /*proargtypes*/ .proargtypes = {PGOid(21)}}));

std::pair<PGOid, PGProcPtr> ProcProvider::PROC_BOOLTOINT32 = std::pair<PGOid, PGProcPtr>(
    PGOid(2558),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = PGOid(2558),
        /*proname*/ .proname = "toInt32",
        /*pronamespace*/ .pronamespace = PGOid(1),
        /*proowner*/ .proowner = PGOid(1),
        /*prolang*/ .prolang = PGOid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = PGOid(0),
        /*protransform*/ .protransform = PGOid(0),
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 1,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = PGOid(23),
        /*proargtypes*/ .proargtypes = {PGOid(16)}}));

std::pair<PGOid, PGProcPtr> ProcProvider::PROC_BOOLTOSTRING = std::pair<PGOid, PGProcPtr>(
    PGOid(2971),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = PGOid(2971),
        /*proname*/ .proname = "toString",
        /*pronamespace*/ .pronamespace = PGOid(1),
        /*proowner*/ .proowner = PGOid(1),
        /*prolang*/ .prolang = PGOid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = PGOid(0),
        /*protransform*/ .protransform = PGOid(0),
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 1,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = PGOid(25),
        /*proargtypes*/ .proargtypes = {PGOid(16)}}));

// std::pair<PGOid, PGProcPtr> ProcProvider::PROC_BOOLTOFIXEDSTRING = std::pair<PGOid, PGProcPtr>(
//     PGOid(2971),
//     std::make_shared<Form_pg_proc>(Form_pg_proc{
//         .oid = PGOid(2971),
//         /*proname*/ .proname = "toFixedString",
//         /*pronamespace*/ .pronamespace = PGOid(1),
//         /*proowner*/ .proowner = PGOid(1),
//         /*prolang*/ .prolang = PGOid(12),
//         /*procost*/ .procost = 1,
//         /*prorows*/ .prorows = 0.0,
//         /*provariadic*/ .provariadic = PGOid(0),
//         /*protransform*/ .protransform = PGOid(0),
//         /*proisagg*/ .proisagg = false,
//         /*proiswindow*/ .proiswindow = false,
//         /*prosecdef*/ .prosecdef = false,
//         /*proleakproof*/ .proleakproof = false,
//         /*proisstrict*/ .proisstrict = true,
//         /*proretset*/ .proretset = false,
//         /*provolatile*/ .provolatile = 'i',
//         /*pronargs*/ .pronargs = 1,
//         /*pronargdefaults*/ .pronargdefaults = 0,
//         /*prorettype*/ .prorettype = PGOid(25),
//         /*proargtypes*/ .proargtypes = {PGOid(16)}}));

std::pair<PGOid, PGProcPtr> ProcProvider::PROC_FLOAT32TOINT64 = std::pair<PGOid, PGProcPtr>(
    PGOid(653),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = PGOid(653),
        /*proname*/ .proname = "toInt64",
        /*pronamespace*/ .pronamespace = PGOid(1),
        /*proowner*/ .proowner = PGOid(1),
        /*prolang*/ .prolang = PGOid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = PGOid(0),
        /*protransform*/ .protransform = PGOid(0),
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 1,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = PGOid(20),
        /*proargtypes*/ .proargtypes = {PGOid(700)}}));

std::pair<PGOid, PGProcPtr> ProcProvider::PROC_FLOAT32TOINT16 = std::pair<PGOid, PGProcPtr>(
    PGOid(238),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = PGOid(238),
        /*proname*/ .proname = "toInt16",
        /*pronamespace*/ .pronamespace = PGOid(1),
        /*proowner*/ .proowner = PGOid(1),
        /*prolang*/ .prolang = PGOid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = PGOid(0),
        /*protransform*/ .protransform = PGOid(0),
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 1,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = PGOid(21),
        /*proargtypes*/ .proargtypes = {PGOid(700)}}));

std::pair<PGOid, PGProcPtr> ProcProvider::PROC_FLOAT32TOINT32 = std::pair<PGOid, PGProcPtr>(
    PGOid(319),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = PGOid(319),
        /*proname*/ .proname = "toInt32",
        /*pronamespace*/ .pronamespace = PGOid(1),
        /*proowner*/ .proowner = PGOid(1),
        /*prolang*/ .prolang = PGOid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = PGOid(0),
        /*protransform*/ .protransform = PGOid(0),
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 1,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = PGOid(23),
        /*proargtypes*/ .proargtypes = {PGOid(700)}}));

std::pair<PGOid, PGProcPtr> ProcProvider::PROC_FLOAT32TOFLOAT64 = std::pair<PGOid, PGProcPtr>(
    PGOid(311),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = PGOid(311),
        /*proname*/ .proname = "toFloat64",
        /*pronamespace*/ .pronamespace = PGOid(1),
        /*proowner*/ .proowner = PGOid(1),
        /*prolang*/ .prolang = PGOid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = PGOid(0),
        /*protransform*/ .protransform = PGOid(0),
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 1,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = PGOid(701),
        /*proargtypes*/ .proargtypes = {PGOid(700)}}));

std::pair<PGOid, PGProcPtr> ProcProvider::PROC_FLOAT64TOINT64 = std::pair<PGOid, PGProcPtr>(
    PGOid(483),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = PGOid(483),
        /*proname*/ .proname = "toInt64",
        /*pronamespace*/ .pronamespace = PGOid(1),
        /*proowner*/ .proowner = PGOid(1),
        /*prolang*/ .prolang = PGOid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = PGOid(0),
        /*protransform*/ .protransform = PGOid(0),
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 1,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = PGOid(20),
        /*proargtypes*/ .proargtypes = {PGOid(701)}}));

std::pair<PGOid, PGProcPtr> ProcProvider::PROC_FLOAT64TOINT16 = std::pair<PGOid, PGProcPtr>(
    PGOid(237),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = PGOid(237),
        /*proname*/ .proname = "toInt16",
        /*pronamespace*/ .pronamespace = PGOid(1),
        /*proowner*/ .proowner = PGOid(1),
        /*prolang*/ .prolang = PGOid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = PGOid(0),
        /*protransform*/ .protransform = PGOid(0),
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 1,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = PGOid(21),
        /*proargtypes*/ .proargtypes = {PGOid(701)}}));

std::pair<PGOid, PGProcPtr> ProcProvider::PROC_FLOAT64TOINT32 = std::pair<PGOid, PGProcPtr>(
    PGOid(317),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = PGOid(317),
        /*proname*/ .proname = "toInt32",
        /*pronamespace*/ .pronamespace = PGOid(1),
        /*proowner*/ .proowner = PGOid(1),
        /*prolang*/ .prolang = PGOid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = PGOid(0),
        /*protransform*/ .protransform = PGOid(0),
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 1,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = PGOid(23),
        /*proargtypes*/ .proargtypes = {PGOid(701)}}));

std::pair<PGOid, PGProcPtr> ProcProvider::PROC_FLOAT64TOFLOAT32 = std::pair<PGOid, PGProcPtr>(
    PGOid(312),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = PGOid(312),
        /*proname*/ .proname = "toFloat32",
        /*pronamespace*/ .pronamespace = PGOid(1),
        /*proowner*/ .proowner = PGOid(1),
        /*prolang*/ .prolang = PGOid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = PGOid(0),
        /*protransform*/ .protransform = PGOid(0),
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 1,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = PGOid(700),
        /*proargtypes*/ .proargtypes = {PGOid(701)}}));

std::pair<PGOid, PGProcPtr> ProcProvider::PROC_INT32TOINT64 = std::pair<PGOid, PGProcPtr>(
    PGOid(481),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = PGOid(481),
        /*proname*/ .proname = "toInt64",
        /*pronamespace*/ .pronamespace = PGOid(1),
        /*proowner*/ .proowner = PGOid(1),
        /*prolang*/ .prolang = PGOid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = PGOid(0),
        /*protransform*/ .protransform = PGOid(0),
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 1,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = PGOid(20),
        /*proargtypes*/ .proargtypes = {PGOid(23)}}));

std::pair<PGOid, PGProcPtr> ProcProvider::PROC_INT32TOINT16 = std::pair<PGOid, PGProcPtr>(
    PGOid(314),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = PGOid(314),
        /*proname*/ .proname = "toInt16",
        /*pronamespace*/ .pronamespace = PGOid(1),
        /*proowner*/ .proowner = PGOid(1),
        /*prolang*/ .prolang = PGOid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = PGOid(0),
        /*protransform*/ .protransform = PGOid(0),
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 1,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = PGOid(21),
        /*proargtypes*/ .proargtypes = {PGOid(23)}}));

std::pair<PGOid, PGProcPtr> ProcProvider::PROC_INT32TOFLOAT32 = std::pair<PGOid, PGProcPtr>(
    PGOid(318),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = PGOid(318),
        /*proname*/ .proname = "toFloat32",
        /*pronamespace*/ .pronamespace = PGOid(1),
        /*proowner*/ .proowner = PGOid(1),
        /*prolang*/ .prolang = PGOid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = PGOid(0),
        /*protransform*/ .protransform = PGOid(0),
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 1,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = PGOid(700),
        /*proargtypes*/ .proargtypes = {PGOid(23)}}));

std::pair<PGOid, PGProcPtr> ProcProvider::PROC_INT32TOFLOAT64 = std::pair<PGOid, PGProcPtr>(
    PGOid(316),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = PGOid(316),
        /*proname*/ .proname = "toFloat64",
        /*pronamespace*/ .pronamespace = PGOid(1),
        /*proowner*/ .proowner = PGOid(1),
        /*prolang*/ .prolang = PGOid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = PGOid(0),
        /*protransform*/ .protransform = PGOid(0),
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 1,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = PGOid(701),
        /*proargtypes*/ .proargtypes = {PGOid(23)}}));

// std::pair<PGOid, PGProcPtr> ProcProvider::PROC_DATETODATETIME = std::pair<PGOid, PGProcPtr>(
//     PGOid(2024),
//     std::make_shared<Form_pg_proc>(Form_pg_proc{
//         .oid = PGOid(2024),
//         /*proname*/ .proname = "toDateTime",
//         /*pronamespace*/ .pronamespace = PGOid(1),
//         /*proowner*/ .proowner = PGOid(1),
//         /*prolang*/ .prolang = PGOid(12),
//         /*procost*/ .procost = 1,
//         /*prorows*/ .prorows = 0.0,
//         /*provariadic*/ .provariadic = PGOid(0),
//         /*protransform*/ .protransform = PGOid(0),
//         /*proisagg*/ .proisagg = false,
//         /*proiswindow*/ .proiswindow = false,
//         /*prosecdef*/ .prosecdef = false,
//         /*proleakproof*/ .proleakproof = false,
//         /*proisstrict*/ .proisstrict = true,
//         /*proretset*/ .proretset = false,
//         /*provolatile*/ .provolatile = 'i',
//         /*pronargs*/ .pronargs = 1,
//         /*pronargdefaults*/ .pronargdefaults = 0,
//         /*prorettype*/ .prorettype = PGOid(1114),
//         /*proargtypes*/ .proargtypes = {PGOid(1082)}}));

// std::pair<PGOid, PGProcPtr> ProcProvider::PROC_DATETODATETIME64 = std::pair<PGOid, PGProcPtr>(
//     PGOid(1174),
//     std::make_shared<Form_pg_proc>(Form_pg_proc{
//         .oid = PGOid(1174),
//         /*proname*/ .proname = "toDateTime64",
//         /*pronamespace*/ .pronamespace = PGOid(1),
//         /*proowner*/ .proowner = PGOid(1),
//         /*prolang*/ .prolang = PGOid(12),
//         /*procost*/ .procost = 1,
//         /*prorows*/ .prorows = 0.0,
//         /*provariadic*/ .provariadic = PGOid(0),
//         /*protransform*/ .protransform = PGOid(0),
//         /*proisagg*/ .proisagg = false,
//         /*proiswindow*/ .proiswindow = false,
//         /*prosecdef*/ .prosecdef = false,
//         /*proleakproof*/ .proleakproof = false,
//         /*proisstrict*/ .proisstrict = true,
//         /*proretset*/ .proretset = false,
//         /*provolatile*/ .provolatile = 'i',
//         /*pronargs*/ .pronargs = 1,
//         /*pronargdefaults*/ .pronargdefaults = 0,
//         /*prorettype*/ .prorettype = PGOid(1184),
//         /*proargtypes*/ .proargtypes = {PGOid(1082)}}));

std::pair<PGOid, PGProcPtr> ProcProvider::PROC_DECIMAL64TOINT64 = std::pair<PGOid, PGProcPtr>(
    PGOid(1779),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = PGOid(1779),
        /*proname*/ .proname = "toInt64",
        /*pronamespace*/ .pronamespace = PGOid(1),
        /*proowner*/ .proowner = PGOid(1),
        /*prolang*/ .prolang = PGOid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = PGOid(0),
        /*protransform*/ .protransform = PGOid(0),
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 1,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = PGOid(20),
        /*proargtypes*/ .proargtypes = {PGOid(1700)}}));

std::pair<PGOid, PGProcPtr> ProcProvider::PROC_DECIMAL64TOINT16 = std::pair<PGOid, PGProcPtr>(
    PGOid(1783),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = PGOid(1783),
        /*proname*/ .proname = "toInt16",
        /*pronamespace*/ .pronamespace = PGOid(1),
        /*proowner*/ .proowner = PGOid(1),
        /*prolang*/ .prolang = PGOid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = PGOid(0),
        /*protransform*/ .protransform = PGOid(0),
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 1,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = PGOid(21),
        /*proargtypes*/ .proargtypes = {PGOid(1700)}}));

std::pair<PGOid, PGProcPtr> ProcProvider::PROC_DECIMAL64TOINT32 = std::pair<PGOid, PGProcPtr>(
    PGOid(1744),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = PGOid(1744),
        /*proname*/ .proname = "toInt32",
        /*pronamespace*/ .pronamespace = PGOid(1),
        /*proowner*/ .proowner = PGOid(1),
        /*prolang*/ .prolang = PGOid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = PGOid(0),
        /*protransform*/ .protransform = PGOid(0),
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 1,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = PGOid(23),
        /*proargtypes*/ .proargtypes = {PGOid(1700)}}));

std::pair<PGOid, PGProcPtr> ProcProvider::PROC_DECIMAL64TOFLOAT64 = std::pair<PGOid, PGProcPtr>(
    PGOid(1746),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = PGOid(1746),
        /*proname*/ .proname = "toFloat64",
        /*pronamespace*/ .pronamespace = PGOid(1),
        /*proowner*/ .proowner = PGOid(1),
        /*prolang*/ .prolang = PGOid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = PGOid(0),
        /*protransform*/ .protransform = PGOid(0),
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 1,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = PGOid(701),
        /*proargtypes*/ .proargtypes = {PGOid(1700)}}));

std::pair<PGOid, PGProcPtr> ProcProvider::PROC_COUNTANY = std::pair<PGOid, PGProcPtr>(
    PGOid(2147),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = PGOid(2147),
        /*proname*/ .proname = "count",
        /*pronamespace*/ .pronamespace = PGOid(1),
        /*proowner*/ .proowner = PGOid(1),
        /*prolang*/ .prolang = PGOid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = PGOid(0),
        /*protransform*/ .protransform = PGOid(0),
        /*proisagg*/ .proisagg = true,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = false,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 1,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = PGOid(20),
        /*proargtypes*/ .proargtypes = {PGOid(2276)}}));

std::pair<PGOid, PGProcPtr> ProcProvider::PROC_COUNTSTAR = std::pair<PGOid, PGProcPtr>(
    PGOid(2803),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = PGOid(2803),
        /*proname*/ .proname = "count",
        /*pronamespace*/ .pronamespace = PGOid(1),
        /*proowner*/ .proowner = PGOid(1),
        /*prolang*/ .prolang = PGOid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = InvalidOid,
        /*protransform*/ .protransform = InvalidOid,
        /*proisagg*/ .proisagg = true,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = false,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 0,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = PGOid(20),
        /*proargtypes*/ .proargtypes = {}}));

std::pair<PGOid, PGProcPtr> ProcProvider::PROC_INT64INCANY = std::pair<PGOid, PGProcPtr>(
    PGOid(2804),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = PGOid(2804),
        /*proname*/ .proname = "int8inc_any",
        /*pronamespace*/ .pronamespace = PGOid(1),
        /*proowner*/ .proowner = PGOid(1),
        /*prolang*/ .prolang = PGOid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = InvalidOid,
        /*protransform*/ .protransform = InvalidOid,
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 2,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = PGOid(20),
        /*proargtypes*/ .proargtypes = {PGOid(20), PGOid(2276)}}));

std::pair<PGOid, PGProcPtr> ProcProvider::PROC_INT64INC = std::pair<PGOid, PGProcPtr>(
    PGOid(1219),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = PGOid(1219),
        /*proname*/ .proname = "int8inc",
        /*pronamespace*/ .pronamespace = PGOid(1),
        /*proowner*/ .proowner = PGOid(1),
        /*prolang*/ .prolang = PGOid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = InvalidOid,
        /*protransform*/ .protransform = InvalidOid,
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 1,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = PGOid(20),
        /*proargtypes*/ .proargtypes = {PGOid(20)}}));

std::pair<PGOid, PGProcPtr> ProcProvider::PROC_INT64PL = std::pair<PGOid, PGProcPtr>(
    PGOid(463),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = PGOid(463),
        /*proname*/ .proname = "int8pl",
        /*pronamespace*/ .pronamespace = PGOid(1),
        /*proowner*/ .proowner = PGOid(1),
        /*prolang*/ .prolang = PGOid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = InvalidOid,
        /*protransform*/ .protransform = InvalidOid,
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 2,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = PGOid(20),
        /*proargtypes*/ .proargtypes = {PGOid(20), PGOid(20)}}));

std::pair<PGOid, PGProcPtr> ProcProvider::PROC_INT64DEC = std::pair<PGOid, PGProcPtr>(
    PGOid(3546),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = PGOid(3546),
        /*proname*/ .proname = "int8dec",
        /*pronamespace*/ .pronamespace = PGOid(1),
        /*proowner*/ .proowner = PGOid(1),
        /*prolang*/ .prolang = PGOid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = InvalidOid,
        /*protransform*/ .protransform = InvalidOid,
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 1,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = PGOid(20),
        /*proargtypes*/ .proargtypes = {PGOid(20)}}));

std::pair<PGOid, PGProcPtr> ProcProvider::PROC_INT64DECANY = std::pair<PGOid, PGProcPtr>(
    PGOid(3547),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = PGOid(3547),
        /*proname*/ .proname = "int8dec_any",
        /*pronamespace*/ .pronamespace = PGOid(1),
        /*proowner*/ .proowner = PGOid(1),
        /*prolang*/ .prolang = PGOid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = InvalidOid,
        /*protransform*/ .protransform = InvalidOid,
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 2,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = PGOid(20),
        /*proargtypes*/ .proargtypes = {PGOid(20), PGOid(2276)}}));

std::pair<PGOid, PGProcPtr> ProcProvider::PROC_TEXTLT = std::pair<PGOid, PGProcPtr>(
    PGOid(740),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = PGOid(740),
        /*proname*/ .proname = "text_lt",
        /*pronamespace*/ .pronamespace = PGOid(1),
        /*proowner*/ .proowner = PGOid(1),
        /*prolang*/ .prolang = PGOid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = InvalidOid,
        /*protransform*/ .protransform = InvalidOid,
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 2,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = PGOid(16),
        /*proargtypes*/ .proargtypes = {PGOid(25), PGOid(25)}}));

std::pair<PGOid, PGProcPtr> ProcProvider::PROC_SCALARLTSEL = std::pair<PGOid, PGProcPtr>(
    PGOid(103),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = PGOid(103),
        /*proname*/ .proname = "scalarltsel",
        /*pronamespace*/ .pronamespace = PGOid(1),
        /*proowner*/ .proowner = PGOid(1),
        /*prolang*/ .prolang = PGOid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = InvalidOid,
        /*protransform*/ .protransform = InvalidOid,
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 's',
        /*pronargs*/ .pronargs = 4,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = PGOid(701),
        /*proargtypes*/ .proargtypes = {PGOid(2281), PGOid(26), PGOid(2281), PGOid(23)}}));

std::pair<PGOid, PGProcPtr> ProcProvider::PROC_SCALARLTJOINSEL = std::pair<PGOid, PGProcPtr>(
    PGOid(107),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = PGOid(107),
        /*proname*/ .proname = "scalarltjoinsel",
        /*pronamespace*/ .pronamespace = PGOid(1),
        /*proowner*/ .proowner = PGOid(1),
        /*prolang*/ .prolang = PGOid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = InvalidOid,
        /*protransform*/ .protransform = InvalidOid,
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 's',
        /*pronargs*/ .pronargs = 5,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = PGOid(701),
        /*proargtypes*/ .proargtypes = {PGOid(2281), PGOid(26), PGOid(2281), PGOid(21), PGOid(2281)}}));

std::pair<PGOid, PGProcPtr> ProcProvider::PROC_FLOAT32EQ = std::pair<PGOid, PGProcPtr>(
    PGOid(287),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = PGOid(287),
        /*proname*/ .proname = "float4eq",
        /*pronamespace*/ .pronamespace = PGOid(1),
        /*proowner*/ .proowner = PGOid(1),
        /*prolang*/ .prolang = PGOid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = InvalidOid,
        /*protransform*/ .protransform = InvalidOid,
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = true,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 2,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = PGOid(16),
        /*proargtypes*/ .proargtypes = {PGOid(700), PGOid(700)}}));

std::pair<PGOid, PGProcPtr> ProcProvider::PROC_FLOAT32NE = std::pair<PGOid, PGProcPtr>(
    PGOid(288),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = PGOid(288),
        /*proname*/ .proname = "float4ne",
        /*pronamespace*/ .pronamespace = PGOid(1),
        /*proowner*/ .proowner = PGOid(1),
        /*prolang*/ .prolang = PGOid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = InvalidOid,
        /*protransform*/ .protransform = InvalidOid,
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = true,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 2,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = PGOid(16),
        /*proargtypes*/ .proargtypes = {PGOid(700), PGOid(700)}}));

std::pair<PGOid, PGProcPtr> ProcProvider::PROC_FLOAT32LT = std::pair<PGOid, PGProcPtr>(
    PGOid(289),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = PGOid(289),
        /*proname*/ .proname = "float4lt",
        /*pronamespace*/ .pronamespace = PGOid(1),
        /*proowner*/ .proowner = PGOid(1),
        /*prolang*/ .prolang = PGOid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = InvalidOid,
        /*protransform*/ .protransform = InvalidOid,
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = true,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 2,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = PGOid(16),
        /*proargtypes*/ .proargtypes = {PGOid(700), PGOid(700)}}));

std::pair<PGOid, PGProcPtr> ProcProvider::PROC_FLOAT32GT = std::pair<PGOid, PGProcPtr>(
    PGOid(291),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = PGOid(291),
        /*proname*/ .proname = "float4gt",
        /*pronamespace*/ .pronamespace = PGOid(1),
        /*proowner*/ .proowner = PGOid(1),
        /*prolang*/ .prolang = PGOid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = InvalidOid,
        /*protransform*/ .protransform = InvalidOid,
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = true,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 2,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = PGOid(16),
        /*proargtypes*/ .proargtypes = {PGOid(700), PGOid(700)}}));

std::pair<PGOid, PGProcPtr> ProcProvider::PROC_FLOAT32LE = std::pair<PGOid, PGProcPtr>(
    PGOid(290),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = PGOid(290),
        /*proname*/ .proname = "float4le",
        /*pronamespace*/ .pronamespace = PGOid(1),
        /*proowner*/ .proowner = PGOid(1),
        /*prolang*/ .prolang = PGOid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = InvalidOid,
        /*protransform*/ .protransform = InvalidOid,
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = true,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 2,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = PGOid(16),
        /*proargtypes*/ .proargtypes = {PGOid(700), PGOid(700)}}));

std::pair<PGOid, PGProcPtr> ProcProvider::PROC_FLOAT32GE = std::pair<PGOid, PGProcPtr>(
    PGOid(292),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = PGOid(292),
        /*proname*/ .proname = "float4ge",
        /*pronamespace*/ .pronamespace = PGOid(1),
        /*proowner*/ .proowner = PGOid(1),
        /*prolang*/ .prolang = PGOid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = InvalidOid,
        /*protransform*/ .protransform = InvalidOid,
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = true,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 2,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = PGOid(16),
        /*proargtypes*/ .proargtypes = {PGOid(700), PGOid(700)}}));





std::pair<PGOid, PGProcPtr> ProcProvider::PROC_FLOAT64EQ = std::pair<PGOid, PGProcPtr>(
    PGOid(293),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = PGOid(293),
        /*proname*/ .proname = "float8eq",
        /*pronamespace*/ .pronamespace = PGOid(1),
        /*proowner*/ .proowner = PGOid(1),
        /*prolang*/ .prolang = PGOid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = InvalidOid,
        /*protransform*/ .protransform = InvalidOid,
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = true,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 2,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = PGOid(16),
        /*proargtypes*/ .proargtypes = {PGOid(701), PGOid(701)}}));

std::pair<PGOid, PGProcPtr> ProcProvider::PROC_FLOAT64NE = std::pair<PGOid, PGProcPtr>(
    PGOid(294),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = PGOid(294),
        /*proname*/ .proname = "float8ne",
        /*pronamespace*/ .pronamespace = PGOid(1),
        /*proowner*/ .proowner = PGOid(1),
        /*prolang*/ .prolang = PGOid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = InvalidOid,
        /*protransform*/ .protransform = InvalidOid,
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = true,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 2,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = PGOid(16),
        /*proargtypes*/ .proargtypes = {PGOid(701), PGOid(701)}}));

std::pair<PGOid, PGProcPtr> ProcProvider::PROC_FLOAT64LT = std::pair<PGOid, PGProcPtr>(
    PGOid(295),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = PGOid(295),
        /*proname*/ .proname = "float8lt",
        /*pronamespace*/ .pronamespace = PGOid(1),
        /*proowner*/ .proowner = PGOid(1),
        /*prolang*/ .prolang = PGOid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = InvalidOid,
        /*protransform*/ .protransform = InvalidOid,
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = true,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 2,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = PGOid(16),
        /*proargtypes*/ .proargtypes = {PGOid(701), PGOid(701)}}));

std::pair<PGOid, PGProcPtr> ProcProvider::PROC_FLOAT64GT = std::pair<PGOid, PGProcPtr>(
    PGOid(297),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = PGOid(297),
        /*proname*/ .proname = "float8gt",
        /*pronamespace*/ .pronamespace = PGOid(1),
        /*proowner*/ .proowner = PGOid(1),
        /*prolang*/ .prolang = PGOid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = InvalidOid,
        /*protransform*/ .protransform = InvalidOid,
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = true,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 2,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = PGOid(16),
        /*proargtypes*/ .proargtypes = {PGOid(701), PGOid(701)}}));

std::pair<PGOid, PGProcPtr> ProcProvider::PROC_FLOAT64LE = std::pair<PGOid, PGProcPtr>(
    PGOid(296),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = PGOid(296),
        /*proname*/ .proname = "float8le",
        /*pronamespace*/ .pronamespace = PGOid(1),
        /*proowner*/ .proowner = PGOid(1),
        /*prolang*/ .prolang = PGOid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = InvalidOid,
        /*protransform*/ .protransform = InvalidOid,
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = true,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 2,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = PGOid(16),
        /*proargtypes*/ .proargtypes = {PGOid(701), PGOid(701)}}));

std::pair<PGOid, PGProcPtr> ProcProvider::PROC_FLOAT64GE = std::pair<PGOid, PGProcPtr>(
    PGOid(298),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = PGOid(298),
        /*proname*/ .proname = "float8ge",
        /*pronamespace*/ .pronamespace = PGOid(1),
        /*proowner*/ .proowner = PGOid(1),
        /*prolang*/ .prolang = PGOid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = InvalidOid,
        /*protransform*/ .protransform = InvalidOid,
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = true,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 2,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = PGOid(16),
        /*proargtypes*/ .proargtypes = {PGOid(701), PGOid(701)}}));


ProcProvider::OidProcMap ProcProvider::oid_proc_map = {
    ProcProvider::PROC_INT2PL,
	ProcProvider::PROC_INT4PL,
	ProcProvider::PROC_INT24PL,
	ProcProvider::PROC_INT42PL,
    ProcProvider::PROC_INT2MI,
    ProcProvider::PROC_INT4MI,
    ProcProvider::PROC_INT24MI,
    ProcProvider::PROC_INT42MI,
    ProcProvider::PROC_INT2MUL,
    ProcProvider::PROC_INT4MUL,
    ProcProvider::PROC_INT24MUL,
    ProcProvider::PROC_INT42MUL,
    ProcProvider::PROC_INT2DIV,
    ProcProvider::PROC_INT4DIV,
    ProcProvider::PROC_INT24DIV,
    ProcProvider::PROC_INT42DIV,

    ProcProvider::PROC_INT64TOINT16,
	ProcProvider::PROC_INT64TOINT32,
	ProcProvider::PROC_INT64TOFLOAT32,
	ProcProvider::PROC_INT64TOFLOAT64,
	ProcProvider::PROC_INT16TOINT64,
	ProcProvider::PROC_INT16TOINT32,
	ProcProvider::PROC_INT16TOFLOAT32,
	ProcProvider::PROC_INT16TOFLOAT64,
	ProcProvider::PROC_BOOLTOINT32,
	ProcProvider::PROC_BOOLTOSTRING,
	ProcProvider::PROC_FLOAT32TOINT64,
	ProcProvider::PROC_FLOAT32TOINT16,
	ProcProvider::PROC_FLOAT32TOINT32,
	ProcProvider::PROC_FLOAT32TOFLOAT64,
	ProcProvider::PROC_FLOAT64TOINT64,
	ProcProvider::PROC_FLOAT64TOINT16,
	ProcProvider::PROC_FLOAT64TOINT32,
	ProcProvider::PROC_FLOAT64TOFLOAT32,
	ProcProvider::PROC_INT32TOINT64,
	ProcProvider::PROC_INT32TOINT16,
	ProcProvider::PROC_INT32TOFLOAT32,
	ProcProvider::PROC_INT32TOFLOAT64,
	ProcProvider::PROC_DECIMAL64TOINT64,
	ProcProvider::PROC_DECIMAL64TOINT16,
	ProcProvider::PROC_DECIMAL64TOINT32,
	ProcProvider::PROC_DECIMAL64TOFLOAT64,

    ProcProvider::PROC_COUNTANY,
    ProcProvider::PROC_COUNTSTAR,
    ProcProvider::PROC_INT64INCANY,
    ProcProvider::PROC_INT64INC,
    ProcProvider::PROC_INT64PL,
    ProcProvider::PROC_INT64DEC,
    ProcProvider::PROC_INT64DECANY,

    ProcProvider::PROC_TEXTLT,
    ProcProvider::PROC_SCALARLTSEL,
    ProcProvider::PROC_SCALARLTJOINSEL,

    ProcProvider::PROC_FLOAT32EQ,
    ProcProvider::PROC_FLOAT32NE,
    ProcProvider::PROC_FLOAT32LT,
    ProcProvider::PROC_FLOAT32GT,
    ProcProvider::PROC_FLOAT32LE,
    ProcProvider::PROC_FLOAT32GE,

    ProcProvider::PROC_FLOAT64EQ,
    ProcProvider::PROC_FLOAT64NE,
    ProcProvider::PROC_FLOAT64LT,
    ProcProvider::PROC_FLOAT64GT,
    ProcProvider::PROC_FLOAT64LE,
    ProcProvider::PROC_FLOAT64GE,
};

// ProcProvider::ProcProvider(const ContextPtr& context_) : context(context_)
// {
	// oid_proc_map.insert(PROC_INT2PL);
	// oid_proc_map.insert(PROC_INT4PL);
	// oid_proc_map.insert(PROC_INT24PL);
	// oid_proc_map.insert(PROC_INT42PL);
    // oid_proc_map.insert(PROC_INT2MI);
    // oid_proc_map.insert(PROC_INT4MI);
    // oid_proc_map.insert(PROC_INT24MI);
    // oid_proc_map.insert(PROC_INT42MI);
    // oid_proc_map.insert(PROC_INT2MUL);
    // oid_proc_map.insert(PROC_INT4MUL);
    // oid_proc_map.insert(PROC_INT24MUL);
    // oid_proc_map.insert(PROC_INT42MUL);
    // oid_proc_map.insert(PROC_INT2DIV);
    // oid_proc_map.insert(PROC_INT4DIV);
    // oid_proc_map.insert(PROC_INT24DIV);
    // oid_proc_map.insert(PROC_INT42DIV);

    // oid_proc_map.insert(PROC_INT64TOINT16);
	// oid_proc_map.insert(PROC_INT64TOINT32);
	// oid_proc_map.insert(PROC_INT64TOFLOAT32);
	// oid_proc_map.insert(PROC_INT64TOFLOAT64);
	// oid_proc_map.insert(PROC_INT16TOINT64);
	// oid_proc_map.insert(PROC_INT16TOINT32);
	// oid_proc_map.insert(PROC_INT16TOFLOAT32);
	// oid_proc_map.insert(PROC_INT16TOFLOAT64);
	// oid_proc_map.insert(PROC_BOOLTOINT32);
	// oid_proc_map.insert(PROC_BOOLTOSTRING);
	// oid_proc_map.insert(PROC_FLOAT32TOINT64);
	// oid_proc_map.insert(PROC_FLOAT32TOINT16);
	// oid_proc_map.insert(PROC_FLOAT32TOINT32);
	// oid_proc_map.insert(PROC_FLOAT32TOFLOAT64);
	// oid_proc_map.insert(PROC_FLOAT64TOINT64);
	// oid_proc_map.insert(PROC_FLOAT64TOINT16);
	// oid_proc_map.insert(PROC_FLOAT64TOINT32);
	// oid_proc_map.insert(PROC_FLOAT64TOFLOAT32);
	// oid_proc_map.insert(PROC_INT32TOINT64);
	// oid_proc_map.insert(PROC_INT32TOINT16);
	// oid_proc_map.insert(PROC_INT32TOFLOAT32);
	// oid_proc_map.insert(PROC_INT32TOFLOAT64);
	// oid_proc_map.insert(PROC_DECIMAL64TOINT64);
	// oid_proc_map.insert(PROC_DECIMAL64TOINT16);
	// oid_proc_map.insert(PROC_DECIMAL64TOINT32);
	// oid_proc_map.insert(PROC_DECIMAL64TOFLOAT64);
// };

std::unique_ptr<std::vector<PGProcPtr>> ProcProvider::search_procs_by_name(const std::string & func_name)
{
    auto procs = std::make_unique<std::vector<PGProcPtr>>();
    for (auto pair : oid_proc_map)
    {
        if (pair.second->proname == func_name)
        {
            procs->push_back(pair.second);
        }
    }

    return procs;
};

PGProcPtr ProcProvider::getProcByOid(PGOid oid)
{
	auto it = oid_proc_map.find(oid);
	if (it == oid_proc_map.end())
	    return nullptr;
	return it->second;
};

std::optional<std::string> ProcProvider::get_func_name(PGOid oid)
{
    auto proc = getProcByOid(oid);
    if (proc != nullptr)
    {
        return proc->proname;
    }
    return std::nullopt;
};

bool ProcProvider::func_strict(PGOid funcid)
{
    PGProcPtr tp = getProcByOid(funcid);
	if (tp == NULL)
	{
		elog(ERROR, "cache lookup failed for function %u", funcid);
        return InvalidOid;
	}

	return tp->proisstrict;
};

PGOid ProcProvider::get_func_rettype(PGOid funcid)
{
    PGProcPtr tp = getProcByOid(funcid);
	if (tp == NULL)
	{
		elog(ERROR, "cache lookup failed for function %u", funcid);
        return InvalidOid;
	}

	return tp->prorettype;
};

bool ProcProvider::get_func_retset(PGOid funcid)
{
	PGProcPtr tp = getProcByOid(funcid);
	if (tp == NULL)
	{
		elog(ERROR, "cache lookup failed for function %u", funcid);
        return false;
	}

	return tp->proretset;
};

}
