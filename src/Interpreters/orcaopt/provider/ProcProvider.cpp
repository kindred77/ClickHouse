#include <Interpreters/orcaopt/provider/ProcProvider.h>

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

// ProcProvider::ProcProvider()
// {
// 	oid_proc_map.insert(PROC_INT2PL);
// 	oid_proc_map.insert(PROC_INT4PL);
// 	oid_proc_map.insert(PROC_INT24PL);
// 	oid_proc_map.insert(PROC_INT42PL);
// };

PGProcPtr ProcProvider::getProcByOid(Oid oid) const
{
	auto it = oid_proc_map.find(oid);
	if (it == oid_proc_map.end())
	    return {};
	return it->second;
};

bool ProcProvider::get_func_retset(Oid funcid)
{
	PGProcPtr tp = getProcByOid(funcid);
	if (tp == NULL)
	{
		elog(ERROR, "cache lookup failed for function %u", funcid);
	}

	return tp->proretset;
};

}
