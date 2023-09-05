#include <Interpreters/orcaopt/provider/ProcProvider.h>

#include <Interpreters/Context.h>

using namespace duckdb_libpgquery;

namespace DB
{

std::pair<PGOid, PGProcPtr> ProcProvider::PROC_INT2PL = std::pair<PGOid, PGProcPtr>(
    PGOid(176),
    std::make_shared<Form_pg_proc>(Form_pg_proc{
        .oid = PGOid(176),
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
        /*prorettype*/ .prorettype = PGOid(21),
        /*proargtypes*/ .proargtypes = {PGOid(21), PGOid(21)}}));

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

PGProcPtr ProcProvider::getProcByOid(PGOid oid) const
{
	auto it = oid_proc_map.find(oid);
	if (it == oid_proc_map.end())
	    return nullptr;
	return it->second;
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
