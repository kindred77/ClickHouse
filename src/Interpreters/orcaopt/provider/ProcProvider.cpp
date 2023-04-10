#include <Interpreters/orcaopt/provider/ProcProvider.h>

#include <Interpreters/Context.h>

#ifdef __clang__
#pragma clang diagnostic ignored "-Wunused-parameter"
#else
#pragma GCC diagnostic ignored "-Wunused-parameter"
#endif

using namespace duckdb_libpgquery;

namespace DB
{

std::pair<Oid, PGProcPtr> ProcProvider::PROC_INT2PL = std::pair<Oid, PGProcPtr>(
    Oid(176),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = Oid(176),
        /*proname*/ .proname = "plus",
        /*pronamespace*/ .pronamespace = Oid(1),
        /*proowner*/ .proowner = Oid(1),
        /*prolang*/ .prolang = Oid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = Oid(0),
        /*protransform*/ .protransform = Oid(0),
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 2,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = Oid(21),
        /*proargtypes*/ .proargtypes = {Oid(21), Oid(21)}}));

std::pair<Oid, PGProcPtr> ProcProvider::PROC_INT4PL = std::pair<Oid, PGProcPtr>(
    Oid(177),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = Oid(177),
        /*proname*/ .proname = "plus",
        /*pronamespace*/ .pronamespace = Oid(1),
        /*proowner*/ .proowner = Oid(1),
        /*prolang*/ .prolang = Oid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = Oid(0),
        /*protransform*/ .protransform = Oid(0),
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 2,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = Oid(23),
        /*proargtypes*/ .proargtypes = {Oid(23), Oid(23)}}));

std::pair<Oid, PGProcPtr> ProcProvider::PROC_INT24PL = std::pair<Oid, PGProcPtr>(
    Oid(178),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = Oid(178),
        /*proname*/ .proname = "plus",
        /*pronamespace*/ .pronamespace = Oid(1),
        /*proowner*/ .proowner = Oid(1),
        /*prolang*/ .prolang = Oid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = Oid(0),
        /*protransform*/ .protransform = Oid(0),
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 2,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = Oid(23),
        /*proargtypes*/ .proargtypes = {Oid(21), Oid(23)}}));

std::pair<Oid, PGProcPtr> ProcProvider::PROC_INT42PL = std::pair<Oid, PGProcPtr>(
    Oid(179),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = Oid(179),
        /*proname*/ .proname = "plus",
        /*pronamespace*/ .pronamespace = Oid(1),
        /*proowner*/ .proowner = Oid(1),
        /*prolang*/ .prolang = Oid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = Oid(0),
        /*protransform*/ .protransform = Oid(0),
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 2,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = Oid(23),
        /*proargtypes*/ .proargtypes = {Oid(23), Oid(21)}}));

std::pair<Oid, PGProcPtr> ProcProvider::PROC_INT2MI = std::pair<Oid, PGProcPtr>(
    Oid(180),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = Oid(180),
        /*proname*/ .proname = "minus",
        /*pronamespace*/ .pronamespace = Oid(1),
        /*proowner*/ .proowner = Oid(1),
        /*prolang*/ .prolang = Oid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = Oid(0),
        /*protransform*/ .protransform = Oid(0),
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 2,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = Oid(21),
        /*proargtypes*/ .proargtypes = {Oid(21), Oid(21)}}));

std::pair<Oid, PGProcPtr> ProcProvider::PROC_INT4MI = std::pair<Oid, PGProcPtr>(
    Oid(181),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = Oid(181),
        /*proname*/ .proname = "minus",
        /*pronamespace*/ .pronamespace = Oid(1),
        /*proowner*/ .proowner = Oid(1),
        /*prolang*/ .prolang = Oid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = Oid(0),
        /*protransform*/ .protransform = Oid(0),
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 2,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = Oid(23),
        /*proargtypes*/ .proargtypes = {Oid(23), Oid(23)}}));

std::pair<Oid, PGProcPtr> ProcProvider::PROC_INT24MI = std::pair<Oid, PGProcPtr>(
    Oid(182),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = Oid(182),
        /*proname*/ .proname = "minus",
        /*pronamespace*/ .pronamespace = Oid(1),
        /*proowner*/ .proowner = Oid(1),
        /*prolang*/ .prolang = Oid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = Oid(0),
        /*protransform*/ .protransform = Oid(0),
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 2,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = Oid(23),
        /*proargtypes*/ .proargtypes = {Oid(21), Oid(23)}}));

std::pair<Oid, PGProcPtr> ProcProvider::PROC_INT42MI = std::pair<Oid, PGProcPtr>(
    Oid(183),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = Oid(183),
        /*proname*/ .proname = "minus",
        /*pronamespace*/ .pronamespace = Oid(1),
        /*proowner*/ .proowner = Oid(1),
        /*prolang*/ .prolang = Oid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = Oid(0),
        /*protransform*/ .protransform = Oid(0),
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 2,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = Oid(23),
        /*proargtypes*/ .proargtypes = {Oid(23), Oid(21)}}));
std::pair<Oid, PGProcPtr> ProcProvider::PROC_INT2MUL = std::pair<Oid, PGProcPtr>(
    Oid(152),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = Oid(152),
        /*proname*/ .proname = "multiply",
        /*pronamespace*/ .pronamespace = Oid(1),
        /*proowner*/ .proowner = Oid(1),
        /*prolang*/ .prolang = Oid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = Oid(0),
        /*protransform*/ .protransform = Oid(0),
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 2,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = Oid(21),
        /*proargtypes*/ .proargtypes = {Oid(21), Oid(21)}}));

std::pair<Oid, PGProcPtr> ProcProvider::PROC_INT4MUL = std::pair<Oid, PGProcPtr>(
    Oid(141),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = Oid(141),
        /*proname*/ .proname = "multiply",
        /*pronamespace*/ .pronamespace = Oid(1),
        /*proowner*/ .proowner = Oid(1),
        /*prolang*/ .prolang = Oid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = Oid(0),
        /*protransform*/ .protransform = Oid(0),
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 2,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = Oid(23),
        /*proargtypes*/ .proargtypes = {Oid(23), Oid(23)}}));

std::pair<Oid, PGProcPtr> ProcProvider::PROC_INT24MUL = std::pair<Oid, PGProcPtr>(
    Oid(170),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = Oid(170),
        /*proname*/ .proname = "multiply",
        /*pronamespace*/ .pronamespace = Oid(1),
        /*proowner*/ .proowner = Oid(1),
        /*prolang*/ .prolang = Oid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = Oid(0),
        /*protransform*/ .protransform = Oid(0),
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 2,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = Oid(23),
        /*proargtypes*/ .proargtypes = {Oid(21), Oid(23)}}));

std::pair<Oid, PGProcPtr> ProcProvider::PROC_INT42MUL = std::pair<Oid, PGProcPtr>(
    Oid(171),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = Oid(171),
        /*proname*/ .proname = "multiply",
        /*pronamespace*/ .pronamespace = Oid(1),
        /*proowner*/ .proowner = Oid(1),
        /*prolang*/ .prolang = Oid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = Oid(0),
        /*protransform*/ .protransform = Oid(0),
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 2,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = Oid(23),
        /*proargtypes*/ .proargtypes = {Oid(23), Oid(21)}}));

std::pair<Oid, PGProcPtr> ProcProvider::PROC_INT2DIV = std::pair<Oid, PGProcPtr>(
    Oid(153),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = Oid(153),
        /*proname*/ .proname = "devide",
        /*pronamespace*/ .pronamespace = Oid(1),
        /*proowner*/ .proowner = Oid(1),
        /*prolang*/ .prolang = Oid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = Oid(0),
        /*protransform*/ .protransform = Oid(0),
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 2,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = Oid(21),
        /*proargtypes*/ .proargtypes = {Oid(21), Oid(21)}}));

std::pair<Oid, PGProcPtr> ProcProvider::PROC_INT4DIV = std::pair<Oid, PGProcPtr>(
    Oid(154),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = Oid(154),
        /*proname*/ .proname = "devide",
        /*pronamespace*/ .pronamespace = Oid(1),
        /*proowner*/ .proowner = Oid(1),
        /*prolang*/ .prolang = Oid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = Oid(0),
        /*protransform*/ .protransform = Oid(0),
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 2,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = Oid(23),
        /*proargtypes*/ .proargtypes = {Oid(23), Oid(23)}}));

std::pair<Oid, PGProcPtr> ProcProvider::PROC_INT24DIV = std::pair<Oid, PGProcPtr>(
    Oid(172),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = Oid(172),
        /*proname*/ .proname = "devide",
        /*pronamespace*/ .pronamespace = Oid(1),
        /*proowner*/ .proowner = Oid(1),
        /*prolang*/ .prolang = Oid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = Oid(0),
        /*protransform*/ .protransform = Oid(0),
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 2,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = Oid(23),
        /*proargtypes*/ .proargtypes = {Oid(21), Oid(23)}}));

std::pair<Oid, PGProcPtr> ProcProvider::PROC_INT42DIV = std::pair<Oid, PGProcPtr>(
    Oid(173),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = Oid(173),
        /*proname*/ .proname = "devide",
        /*pronamespace*/ .pronamespace = Oid(1),
        /*proowner*/ .proowner = Oid(1),
        /*prolang*/ .prolang = Oid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = Oid(0),
        /*protransform*/ .protransform = Oid(0),
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 2,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = Oid(23),
        /*proargtypes*/ .proargtypes = {Oid(23), Oid(21)}}));

std::pair<Oid, PGProcPtr> ProcProvider::PROC_INT64TOINT16 = std::pair<Oid, PGProcPtr>(
    Oid(714),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = Oid(714),
        /*proname*/ .proname = "toInt16",
        /*pronamespace*/ .pronamespace = Oid(1),
        /*proowner*/ .proowner = Oid(1),
        /*prolang*/ .prolang = Oid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = Oid(0),
        /*protransform*/ .protransform = Oid(0),
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 1,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = Oid(21),
        /*proargtypes*/ .proargtypes = {Oid(20)}}));

std::pair<Oid, PGProcPtr> ProcProvider::PROC_INT64TOINT32 = std::pair<Oid, PGProcPtr>(
    Oid(480),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = Oid(480),
        /*proname*/ .proname = "toInt32",
        /*pronamespace*/ .pronamespace = Oid(1),
        /*proowner*/ .proowner = Oid(1),
        /*prolang*/ .prolang = Oid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = Oid(0),
        /*protransform*/ .protransform = Oid(0),
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 1,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = Oid(23),
        /*proargtypes*/ .proargtypes = {Oid(20)}}));

std::pair<Oid, PGProcPtr> ProcProvider::PROC_INT64TOFLOAT32 = std::pair<Oid, PGProcPtr>(
    Oid(652),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = Oid(652),
        /*proname*/ .proname = "toFloat32",
        /*pronamespace*/ .pronamespace = Oid(1),
        /*proowner*/ .proowner = Oid(1),
        /*prolang*/ .prolang = Oid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = Oid(0),
        /*protransform*/ .protransform = Oid(0),
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 1,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = Oid(700),
        /*proargtypes*/ .proargtypes = {Oid(20)}}));

std::pair<Oid, PGProcPtr> ProcProvider::PROC_INT64TOFLOAT64 = std::pair<Oid, PGProcPtr>(
    Oid(482),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = Oid(482),
        /*proname*/ .proname = "toFloat64",
        /*pronamespace*/ .pronamespace = Oid(1),
        /*proowner*/ .proowner = Oid(1),
        /*prolang*/ .prolang = Oid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = Oid(0),
        /*protransform*/ .protransform = Oid(0),
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 1,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = Oid(701),
        /*proargtypes*/ .proargtypes = {Oid(20)}}));

std::pair<Oid, PGProcPtr> ProcProvider::PROC_INT16TOINT64 = std::pair<Oid, PGProcPtr>(
    Oid(754),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = Oid(754),
        /*proname*/ .proname = "toInt64",
        /*pronamespace*/ .pronamespace = Oid(1),
        /*proowner*/ .proowner = Oid(1),
        /*prolang*/ .prolang = Oid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = Oid(0),
        /*protransform*/ .protransform = Oid(0),
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 1,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = Oid(20),
        /*proargtypes*/ .proargtypes = {Oid(21)}}));

std::pair<Oid, PGProcPtr> ProcProvider::PROC_INT16TOINT32 = std::pair<Oid, PGProcPtr>(
    Oid(313),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = Oid(313),
        /*proname*/ .proname = "toInt32",
        /*pronamespace*/ .pronamespace = Oid(1),
        /*proowner*/ .proowner = Oid(1),
        /*prolang*/ .prolang = Oid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = Oid(0),
        /*protransform*/ .protransform = Oid(0),
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 1,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = Oid(23),
        /*proargtypes*/ .proargtypes = {Oid(21)}}));

std::pair<Oid, PGProcPtr> ProcProvider::PROC_INT16TOFLOAT32 = std::pair<Oid, PGProcPtr>(
    Oid(236),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = Oid(236),
        /*proname*/ .proname = "toFloat32",
        /*pronamespace*/ .pronamespace = Oid(1),
        /*proowner*/ .proowner = Oid(1),
        /*prolang*/ .prolang = Oid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = Oid(0),
        /*protransform*/ .protransform = Oid(0),
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 1,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = Oid(700),
        /*proargtypes*/ .proargtypes = {Oid(21)}}));

std::pair<Oid, PGProcPtr> ProcProvider::PROC_INT16TOFLOAT64 = std::pair<Oid, PGProcPtr>(
    Oid(235),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = Oid(235),
        /*proname*/ .proname = "toFloat64",
        /*pronamespace*/ .pronamespace = Oid(1),
        /*proowner*/ .proowner = Oid(1),
        /*prolang*/ .prolang = Oid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = Oid(0),
        /*protransform*/ .protransform = Oid(0),
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 1,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = Oid(701),
        /*proargtypes*/ .proargtypes = {Oid(21)}}));

std::pair<Oid, PGProcPtr> ProcProvider::PROC_BOOLTOINT32 = std::pair<Oid, PGProcPtr>(
    Oid(2558),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = Oid(2558),
        /*proname*/ .proname = "toInt32",
        /*pronamespace*/ .pronamespace = Oid(1),
        /*proowner*/ .proowner = Oid(1),
        /*prolang*/ .prolang = Oid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = Oid(0),
        /*protransform*/ .protransform = Oid(0),
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 1,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = Oid(23),
        /*proargtypes*/ .proargtypes = {Oid(16)}}));

std::pair<Oid, PGProcPtr> ProcProvider::PROC_BOOLTOSTRING = std::pair<Oid, PGProcPtr>(
    Oid(2971),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = Oid(2971),
        /*proname*/ .proname = "toString",
        /*pronamespace*/ .pronamespace = Oid(1),
        /*proowner*/ .proowner = Oid(1),
        /*prolang*/ .prolang = Oid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = Oid(0),
        /*protransform*/ .protransform = Oid(0),
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 1,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = Oid(25),
        /*proargtypes*/ .proargtypes = {Oid(16)}}));

// std::pair<Oid, PGProcPtr> ProcProvider::PROC_BOOLTOFIXEDSTRING = std::pair<Oid, PGProcPtr>(
//     Oid(2971),
//     std::make_shared<Form_pg_proc>(Form_pg_proc{
//         .oid = Oid(2971),
//         /*proname*/ .proname = "toFixedString",
//         /*pronamespace*/ .pronamespace = Oid(1),
//         /*proowner*/ .proowner = Oid(1),
//         /*prolang*/ .prolang = Oid(12),
//         /*procost*/ .procost = 1,
//         /*prorows*/ .prorows = 0.0,
//         /*provariadic*/ .provariadic = Oid(0),
//         /*protransform*/ .protransform = Oid(0),
//         /*proisagg*/ .proisagg = false,
//         /*proiswindow*/ .proiswindow = false,
//         /*prosecdef*/ .prosecdef = false,
//         /*proleakproof*/ .proleakproof = false,
//         /*proisstrict*/ .proisstrict = true,
//         /*proretset*/ .proretset = false,
//         /*provolatile*/ .provolatile = 'i',
//         /*pronargs*/ .pronargs = 1,
//         /*pronargdefaults*/ .pronargdefaults = 0,
//         /*prorettype*/ .prorettype = Oid(25),
//         /*proargtypes*/ .proargtypes = {Oid(16)}}));

std::pair<Oid, PGProcPtr> ProcProvider::PROC_FLOAT32TOINT64 = std::pair<Oid, PGProcPtr>(
    Oid(653),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = Oid(653),
        /*proname*/ .proname = "toInt64",
        /*pronamespace*/ .pronamespace = Oid(1),
        /*proowner*/ .proowner = Oid(1),
        /*prolang*/ .prolang = Oid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = Oid(0),
        /*protransform*/ .protransform = Oid(0),
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 1,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = Oid(20),
        /*proargtypes*/ .proargtypes = {Oid(700)}}));

std::pair<Oid, PGProcPtr> ProcProvider::PROC_FLOAT32TOINT16 = std::pair<Oid, PGProcPtr>(
    Oid(238),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = Oid(238),
        /*proname*/ .proname = "toInt16",
        /*pronamespace*/ .pronamespace = Oid(1),
        /*proowner*/ .proowner = Oid(1),
        /*prolang*/ .prolang = Oid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = Oid(0),
        /*protransform*/ .protransform = Oid(0),
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 1,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = Oid(21),
        /*proargtypes*/ .proargtypes = {Oid(700)}}));

std::pair<Oid, PGProcPtr> ProcProvider::PROC_FLOAT32TOINT32 = std::pair<Oid, PGProcPtr>(
    Oid(319),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = Oid(319),
        /*proname*/ .proname = "toInt32",
        /*pronamespace*/ .pronamespace = Oid(1),
        /*proowner*/ .proowner = Oid(1),
        /*prolang*/ .prolang = Oid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = Oid(0),
        /*protransform*/ .protransform = Oid(0),
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 1,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = Oid(23),
        /*proargtypes*/ .proargtypes = {Oid(700)}}));

std::pair<Oid, PGProcPtr> ProcProvider::PROC_FLOAT32TOFLOAT64 = std::pair<Oid, PGProcPtr>(
    Oid(311),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = Oid(311),
        /*proname*/ .proname = "toFloat64",
        /*pronamespace*/ .pronamespace = Oid(1),
        /*proowner*/ .proowner = Oid(1),
        /*prolang*/ .prolang = Oid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = Oid(0),
        /*protransform*/ .protransform = Oid(0),
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 1,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = Oid(701),
        /*proargtypes*/ .proargtypes = {Oid(700)}}));

std::pair<Oid, PGProcPtr> ProcProvider::PROC_FLOAT64TOINT64 = std::pair<Oid, PGProcPtr>(
    Oid(483),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = Oid(483),
        /*proname*/ .proname = "toInt64",
        /*pronamespace*/ .pronamespace = Oid(1),
        /*proowner*/ .proowner = Oid(1),
        /*prolang*/ .prolang = Oid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = Oid(0),
        /*protransform*/ .protransform = Oid(0),
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 1,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = Oid(20),
        /*proargtypes*/ .proargtypes = {Oid(701)}}));

std::pair<Oid, PGProcPtr> ProcProvider::PROC_FLOAT64TOINT16 = std::pair<Oid, PGProcPtr>(
    Oid(237),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = Oid(237),
        /*proname*/ .proname = "toInt16",
        /*pronamespace*/ .pronamespace = Oid(1),
        /*proowner*/ .proowner = Oid(1),
        /*prolang*/ .prolang = Oid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = Oid(0),
        /*protransform*/ .protransform = Oid(0),
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 1,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = Oid(21),
        /*proargtypes*/ .proargtypes = {Oid(701)}}));

std::pair<Oid, PGProcPtr> ProcProvider::PROC_FLOAT64TOINT32 = std::pair<Oid, PGProcPtr>(
    Oid(317),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = Oid(317),
        /*proname*/ .proname = "toInt32",
        /*pronamespace*/ .pronamespace = Oid(1),
        /*proowner*/ .proowner = Oid(1),
        /*prolang*/ .prolang = Oid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = Oid(0),
        /*protransform*/ .protransform = Oid(0),
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 1,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = Oid(23),
        /*proargtypes*/ .proargtypes = {Oid(701)}}));

std::pair<Oid, PGProcPtr> ProcProvider::PROC_FLOAT64TOFLOAT32 = std::pair<Oid, PGProcPtr>(
    Oid(312),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = Oid(312),
        /*proname*/ .proname = "toFloat32",
        /*pronamespace*/ .pronamespace = Oid(1),
        /*proowner*/ .proowner = Oid(1),
        /*prolang*/ .prolang = Oid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = Oid(0),
        /*protransform*/ .protransform = Oid(0),
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 1,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = Oid(700),
        /*proargtypes*/ .proargtypes = {Oid(701)}}));

std::pair<Oid, PGProcPtr> ProcProvider::PROC_INT32TOINT64 = std::pair<Oid, PGProcPtr>(
    Oid(481),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = Oid(481),
        /*proname*/ .proname = "toInt64",
        /*pronamespace*/ .pronamespace = Oid(1),
        /*proowner*/ .proowner = Oid(1),
        /*prolang*/ .prolang = Oid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = Oid(0),
        /*protransform*/ .protransform = Oid(0),
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 1,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = Oid(20),
        /*proargtypes*/ .proargtypes = {Oid(23)}}));

std::pair<Oid, PGProcPtr> ProcProvider::PROC_INT32TOINT16 = std::pair<Oid, PGProcPtr>(
    Oid(314),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = Oid(314),
        /*proname*/ .proname = "toInt16",
        /*pronamespace*/ .pronamespace = Oid(1),
        /*proowner*/ .proowner = Oid(1),
        /*prolang*/ .prolang = Oid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = Oid(0),
        /*protransform*/ .protransform = Oid(0),
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 1,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = Oid(21),
        /*proargtypes*/ .proargtypes = {Oid(23)}}));

std::pair<Oid, PGProcPtr> ProcProvider::PROC_INT32TOFLOAT32 = std::pair<Oid, PGProcPtr>(
    Oid(318),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = Oid(318),
        /*proname*/ .proname = "toFloat32",
        /*pronamespace*/ .pronamespace = Oid(1),
        /*proowner*/ .proowner = Oid(1),
        /*prolang*/ .prolang = Oid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = Oid(0),
        /*protransform*/ .protransform = Oid(0),
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 1,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = Oid(700),
        /*proargtypes*/ .proargtypes = {Oid(23)}}));

std::pair<Oid, PGProcPtr> ProcProvider::PROC_INT32TOFLOAT64 = std::pair<Oid, PGProcPtr>(
    Oid(316),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = Oid(316),
        /*proname*/ .proname = "toFloat64",
        /*pronamespace*/ .pronamespace = Oid(1),
        /*proowner*/ .proowner = Oid(1),
        /*prolang*/ .prolang = Oid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = Oid(0),
        /*protransform*/ .protransform = Oid(0),
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 1,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = Oid(701),
        /*proargtypes*/ .proargtypes = {Oid(23)}}));

// std::pair<Oid, PGProcPtr> ProcProvider::PROC_DATETODATETIME = std::pair<Oid, PGProcPtr>(
//     Oid(2024),
//     std::make_shared<Form_pg_proc>(Form_pg_proc{
//         .oid = Oid(2024),
//         /*proname*/ .proname = "toDateTime",
//         /*pronamespace*/ .pronamespace = Oid(1),
//         /*proowner*/ .proowner = Oid(1),
//         /*prolang*/ .prolang = Oid(12),
//         /*procost*/ .procost = 1,
//         /*prorows*/ .prorows = 0.0,
//         /*provariadic*/ .provariadic = Oid(0),
//         /*protransform*/ .protransform = Oid(0),
//         /*proisagg*/ .proisagg = false,
//         /*proiswindow*/ .proiswindow = false,
//         /*prosecdef*/ .prosecdef = false,
//         /*proleakproof*/ .proleakproof = false,
//         /*proisstrict*/ .proisstrict = true,
//         /*proretset*/ .proretset = false,
//         /*provolatile*/ .provolatile = 'i',
//         /*pronargs*/ .pronargs = 1,
//         /*pronargdefaults*/ .pronargdefaults = 0,
//         /*prorettype*/ .prorettype = Oid(1114),
//         /*proargtypes*/ .proargtypes = {Oid(1082)}}));

// std::pair<Oid, PGProcPtr> ProcProvider::PROC_DATETODATETIME64 = std::pair<Oid, PGProcPtr>(
//     Oid(1174),
//     std::make_shared<Form_pg_proc>(Form_pg_proc{
//         .oid = Oid(1174),
//         /*proname*/ .proname = "toDateTime64",
//         /*pronamespace*/ .pronamespace = Oid(1),
//         /*proowner*/ .proowner = Oid(1),
//         /*prolang*/ .prolang = Oid(12),
//         /*procost*/ .procost = 1,
//         /*prorows*/ .prorows = 0.0,
//         /*provariadic*/ .provariadic = Oid(0),
//         /*protransform*/ .protransform = Oid(0),
//         /*proisagg*/ .proisagg = false,
//         /*proiswindow*/ .proiswindow = false,
//         /*prosecdef*/ .prosecdef = false,
//         /*proleakproof*/ .proleakproof = false,
//         /*proisstrict*/ .proisstrict = true,
//         /*proretset*/ .proretset = false,
//         /*provolatile*/ .provolatile = 'i',
//         /*pronargs*/ .pronargs = 1,
//         /*pronargdefaults*/ .pronargdefaults = 0,
//         /*prorettype*/ .prorettype = Oid(1184),
//         /*proargtypes*/ .proargtypes = {Oid(1082)}}));

std::pair<Oid, PGProcPtr> ProcProvider::PROC_DECIMAL64TOINT64 = std::pair<Oid, PGProcPtr>(
    Oid(1779),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = Oid(1779),
        /*proname*/ .proname = "toInt64",
        /*pronamespace*/ .pronamespace = Oid(1),
        /*proowner*/ .proowner = Oid(1),
        /*prolang*/ .prolang = Oid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = Oid(0),
        /*protransform*/ .protransform = Oid(0),
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 1,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = Oid(20),
        /*proargtypes*/ .proargtypes = {Oid(1700)}}));

std::pair<Oid, PGProcPtr> ProcProvider::PROC_DECIMAL64TOINT16 = std::pair<Oid, PGProcPtr>(
    Oid(1783),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = Oid(1783),
        /*proname*/ .proname = "toInt16",
        /*pronamespace*/ .pronamespace = Oid(1),
        /*proowner*/ .proowner = Oid(1),
        /*prolang*/ .prolang = Oid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = Oid(0),
        /*protransform*/ .protransform = Oid(0),
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 1,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = Oid(21),
        /*proargtypes*/ .proargtypes = {Oid(1700)}}));

std::pair<Oid, PGProcPtr> ProcProvider::PROC_DECIMAL64TOINT32 = std::pair<Oid, PGProcPtr>(
    Oid(1744),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = Oid(1744),
        /*proname*/ .proname = "toInt32",
        /*pronamespace*/ .pronamespace = Oid(1),
        /*proowner*/ .proowner = Oid(1),
        /*prolang*/ .prolang = Oid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = Oid(0),
        /*protransform*/ .protransform = Oid(0),
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 1,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = Oid(23),
        /*proargtypes*/ .proargtypes = {Oid(1700)}}));

std::pair<Oid, PGProcPtr> ProcProvider::PROC_DECIMAL64TOFLOAT64 = std::pair<Oid, PGProcPtr>(
    Oid(1746),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = Oid(1746),
        /*proname*/ .proname = "toFloat64",
        /*pronamespace*/ .pronamespace = Oid(1),
        /*proowner*/ .proowner = Oid(1),
        /*prolang*/ .prolang = Oid(12),
        /*procost*/ .procost = 1,
        /*prorows*/ .prorows = 0.0,
        /*provariadic*/ .provariadic = Oid(0),
        /*protransform*/ .protransform = Oid(0),
        /*proisagg*/ .proisagg = false,
        /*proiswindow*/ .proiswindow = false,
        /*prosecdef*/ .prosecdef = false,
        /*proleakproof*/ .proleakproof = false,
        /*proisstrict*/ .proisstrict = true,
        /*proretset*/ .proretset = false,
        /*provolatile*/ .provolatile = 'i',
        /*pronargs*/ .pronargs = 1,
        /*pronargdefaults*/ .pronargdefaults = 0,
        /*prorettype*/ .prorettype = Oid(701),
        /*proargtypes*/ .proargtypes = {Oid(1700)}}));

ProcProvider::ProcProvider(const ContextPtr& context_) : context(context_)
{
	oid_proc_map.insert(PROC_INT2PL);
	oid_proc_map.insert(PROC_INT4PL);
	oid_proc_map.insert(PROC_INT24PL);
	oid_proc_map.insert(PROC_INT42PL);
    oid_proc_map.insert(PROC_INT2MI);
    oid_proc_map.insert(PROC_INT4MI);
    oid_proc_map.insert(PROC_INT24MI);
    oid_proc_map.insert(PROC_INT42MI);
    oid_proc_map.insert(PROC_INT2MUL);
    oid_proc_map.insert(PROC_INT4MUL);
    oid_proc_map.insert(PROC_INT24MUL);
    oid_proc_map.insert(PROC_INT42MUL);
    oid_proc_map.insert(PROC_INT2DIV);
    oid_proc_map.insert(PROC_INT4DIV);
    oid_proc_map.insert(PROC_INT24DIV);
    oid_proc_map.insert(PROC_INT42DIV);

    oid_proc_map.insert(PROC_INT64TOINT16);
	oid_proc_map.insert(PROC_INT64TOINT32);
	oid_proc_map.insert(PROC_INT64TOFLOAT32);
	oid_proc_map.insert(PROC_INT64TOFLOAT64);
	oid_proc_map.insert(PROC_INT16TOINT64);
	oid_proc_map.insert(PROC_INT16TOINT32);
	oid_proc_map.insert(PROC_INT16TOFLOAT32);
	oid_proc_map.insert(PROC_INT16TOFLOAT64);
	oid_proc_map.insert(PROC_BOOLTOINT32);
	oid_proc_map.insert(PROC_BOOLTOSTRING);
	oid_proc_map.insert(PROC_FLOAT32TOINT64);
	oid_proc_map.insert(PROC_FLOAT32TOINT16);
	oid_proc_map.insert(PROC_FLOAT32TOINT32);
	oid_proc_map.insert(PROC_FLOAT32TOFLOAT64);
	oid_proc_map.insert(PROC_FLOAT64TOINT64);
	oid_proc_map.insert(PROC_FLOAT64TOINT16);
	oid_proc_map.insert(PROC_FLOAT64TOINT32);
	oid_proc_map.insert(PROC_FLOAT64TOFLOAT32);
	oid_proc_map.insert(PROC_INT32TOINT64);
	oid_proc_map.insert(PROC_INT32TOINT16);
	oid_proc_map.insert(PROC_INT32TOFLOAT32);
	oid_proc_map.insert(PROC_INT32TOFLOAT64);
	oid_proc_map.insert(PROC_DECIMAL64TOINT64);
	oid_proc_map.insert(PROC_DECIMAL64TOINT16);
	oid_proc_map.insert(PROC_DECIMAL64TOINT32);
	oid_proc_map.insert(PROC_DECIMAL64TOFLOAT64);
};

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

PGProcPtr ProcProvider::getProcByOid(Oid oid) const
{
	auto it = oid_proc_map.find(oid);
	if (it == oid_proc_map.end())
	    return nullptr;
	return it->second;
};

bool ProcProvider::get_func_retset(Oid funcid)
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
