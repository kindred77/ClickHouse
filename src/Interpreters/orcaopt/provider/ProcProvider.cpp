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

NEW_PROC(FLOAT8_NUMERIC, 1746, "float8", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 701, {PGOid(1700)})
NEW_PROC(INT4_NUMERIC, 1744, "int4", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 23, {PGOid(1700)})
NEW_PROC(INT2_NUMERIC, 1783, "int2", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 21, {PGOid(1700)})
NEW_PROC(INT8_NUMERIC, 1779, "int8", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 20, {PGOid(1700)})
NEW_PROC(FLOAT8_INT4, 316, "float8", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 701, {PGOid(23)})
NEW_PROC(FLOAT4_INT4, 318, "float4", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 700, {PGOid(23)})
NEW_PROC(INT2_INT4, 314, "int2", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 21, {PGOid(23)})
NEW_PROC(INT2_FLOAT8, 237, "int2", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 21, {PGOid(701)})
NEW_PROC(INT2_FLOAT4, 238, "int2", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 21, {PGOid(700)})
NEW_PROC(TEXT, 2971, "text", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 25, {PGOid(16)})
NEW_PROC(FLOAT8_INT2, 235, "float8", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 701, {PGOid(21)})
NEW_PROC(FLOAT4_INT2, 236, "float4", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 700, {PGOid(21)})
NEW_PROC(INT4_INT2, 313, "int4", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 23, {PGOid(21)})
NEW_PROC(FLOAT8, 482, "float8", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 701, {PGOid(20)})
NEW_PROC(INT4, 480, "int4", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 23, {PGOid(20)})
NEW_PROC(INT4_FLOAT8, 317, "int4", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 23, {PGOid(701)})
NEW_PROC(INT2, 714, "int2", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 21, {PGOid(20)})
NEW_PROC(INT42DIV, 173, "int42div", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 2, 0, 23, {PGOid(23), PGOid(21)})
NEW_PROC(INT24DIV, 172, "int24div", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 2, 0, 23, {PGOid(21), PGOid(23)})
NEW_PROC(INT4DIV, 154, "int4div", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 2, 0, 23, {PGOid(23), PGOid(23)})
NEW_PROC(INT2DIV, 153, "int2div", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 2, 0, 21, {PGOid(21), PGOid(21)})
NEW_PROC(INT42MUL, 171, "int42mul", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 2, 0, 23, {PGOid(23), PGOid(21)})
NEW_PROC(INT4MUL, 141, "int4mul", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 2, 0, 23, {PGOid(23), PGOid(23)})
NEW_PROC(INT42MI, 183, "int42mi", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 2, 0, 23, {PGOid(23), PGOid(21)})
NEW_PROC(INT24MI, 182, "int24mi", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 2, 0, 23, {PGOid(21), PGOid(23)})
NEW_PROC(INT4MI, 181, "int4mi", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 2, 0, 23, {PGOid(23), PGOid(23)})
NEW_PROC(NUMERIC_NE, 1719, "numeric_ne", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 2, 0, 16, {PGOid(1700), PGOid(1700)})
NEW_PROC(NUMERIC_EQ, 1718, "numeric_eq", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 2, 0, 16, {PGOid(1700), PGOid(1700)})
NEW_PROC(TIMESTAMPTZ_NE, 1153, "timestamptz_ne", 11, 10, 12, 1, 0, 0, 0, false, false, false, true, true, false, 'i', 2, 0, 16, {PGOid(1184), PGOid(1184)})
NEW_PROC(TIMESTAMPTZ_EQ, 1152, "timestamptz_eq", 11, 10, 12, 1, 0, 0, 0, false, false, false, true, true, false, 'i', 2, 0, 16, {PGOid(1184), PGOid(1184)})
NEW_PROC(BYTEALE, 1950, "byteale", 11, 10, 12, 1, 0, 0, 0, false, false, false, true, true, false, 'i', 2, 0, 16, {PGOid(17), PGOid(17)})
NEW_PROC(TIMESTAMPTZ_GE, 1156, "timestamptz_ge", 11, 10, 12, 1, 0, 0, 0, false, false, false, true, true, false, 'i', 2, 0, 16, {PGOid(1184), PGOid(1184)})
NEW_PROC(INT24MUL, 170, "int24mul", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 2, 0, 23, {PGOid(21), PGOid(23)})
NEW_PROC(BYTEALT, 1949, "bytealt", 11, 10, 12, 1, 0, 0, 0, false, false, false, true, true, false, 'i', 2, 0, 16, {PGOid(17), PGOid(17)})
NEW_PROC(TIMESTAMPTZ_LE, 1155, "timestamptz_le", 11, 10, 12, 1, 0, 0, 0, false, false, false, true, true, false, 'i', 2, 0, 16, {PGOid(1184), PGOid(1184)})
NEW_PROC(BYTEAGT, 1951, "byteagt", 11, 10, 12, 1, 0, 0, 0, false, false, false, true, true, false, 'i', 2, 0, 16, {PGOid(17), PGOid(17)})
NEW_PROC(TIMESTAMPTZ_GT, 1157, "timestamptz_gt", 11, 10, 12, 1, 0, 0, 0, false, false, false, true, true, false, 'i', 2, 0, 16, {PGOid(1184), PGOid(1184)})
NEW_PROC(BYTEAEQ, 1948, "byteaeq", 11, 10, 12, 1, 0, 0, 0, false, false, false, true, true, false, 'i', 2, 0, 16, {PGOid(17), PGOid(17)})
NEW_PROC(TIMESTAMPTZ_LT, 1154, "timestamptz_lt", 11, 10, 12, 1, 0, 0, 0, false, false, false, true, true, false, 'i', 2, 0, 16, {PGOid(1184), PGOid(1184)})
NEW_PROC(TIMESTAMPTZ_SEND, 2477, "timestamptz_send", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 17, {PGOid(1184)})
NEW_PROC(TIMESTAMPTZ_RECV, 2476, "timestamptz_recv", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 3, 0, 1184, {PGOid(2281), PGOid(26), PGOid(23)})
NEW_PROC(INT8, 754, "int8", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 20, {PGOid(21)})
NEW_PROC(TIMESTAMPTZ_OUT, 1151, "timestamptz_out", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 's', 1, 0, 2275, {PGOid(1184)})
NEW_PROC(TIMESTAMPTZ_IN, 1150, "timestamptz_in", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 's', 3, 0, 1184, {PGOid(2275), PGOid(26), PGOid(23)})
NEW_PROC(TIMESTAMP_NE, 2053, "timestamp_ne", 11, 10, 12, 1, 0, 0, 0, false, false, false, true, true, false, 'i', 2, 0, 16, {PGOid(1114), PGOid(1114)})
NEW_PROC(TEXTEQ, 67, "texteq", 11, 10, 12, 1, 0, 0, 0, false, false, false, true, true, false, 'i', 2, 0, 16, {PGOid(25), PGOid(25)})
NEW_PROC(TIMESTAMP_EQ, 2052, "timestamp_eq", 11, 10, 12, 1, 0, 0, 0, false, false, false, true, true, false, 'i', 2, 0, 16, {PGOid(1114), PGOid(1114)})
NEW_PROC(INT8NE, 468, "int8ne", 11, 10, 12, 1, 0, 0, 0, false, false, false, true, true, false, 'i', 2, 0, 16, {PGOid(20), PGOid(20)})
NEW_PROC(TIMESTAMP_GE, 2056, "timestamp_ge", 11, 10, 12, 1, 0, 0, 0, false, false, false, true, true, false, 'i', 2, 0, 16, {PGOid(1114), PGOid(1114)})
NEW_PROC(INT8EQ, 467, "int8eq", 11, 10, 12, 1, 0, 0, 0, false, false, false, true, true, false, 'i', 2, 0, 16, {PGOid(20), PGOid(20)})
NEW_PROC(TIMESTAMP_LE, 2055, "timestamp_le", 11, 10, 12, 1, 0, 0, 0, false, false, false, true, true, false, 'i', 2, 0, 16, {PGOid(1114), PGOid(1114)})
NEW_PROC(INT8LT, 469, "int8lt", 11, 10, 12, 1, 0, 0, 0, false, false, false, true, true, false, 'i', 2, 0, 16, {PGOid(20), PGOid(20)})
NEW_PROC(TIMESTAMP_GT, 2057, "timestamp_gt", 11, 10, 12, 1, 0, 0, 0, false, false, false, true, true, false, 'i', 2, 0, 16, {PGOid(1114), PGOid(1114)})
NEW_PROC(TIMESTAMP_SEND, 2475, "timestamp_send", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 17, {PGOid(1114)})
NEW_PROC(TIMESTAMP_RECV, 2474, "timestamp_recv", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 3, 0, 1114, {PGOid(2281), PGOid(26), PGOid(23)})
NEW_PROC(TIMESTAMP_OUT, 1313, "timestamp_out", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 's', 1, 0, 2275, {PGOid(1114)})
NEW_PROC(BPCHARNE, 1053, "bpcharne", 11, 10, 12, 1, 0, 0, 0, false, false, false, true, true, false, 'i', 2, 0, 16, {PGOid(1042), PGOid(1042)})
NEW_PROC(BPCHARIN, 1044, "bpcharin", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 3, 0, 1042, {PGOid(2275), PGOid(26), PGOid(23)})
NEW_PROC(NUMERIC_LE, 1723, "numeric_le", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 2, 0, 16, {PGOid(1700), PGOid(1700)})
NEW_PROC(BPCHARTYPMODOUT, 2914, "bpchartypmodout", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 2275, {PGOid(23)})
NEW_PROC(NUMERIC_LT, 1722, "numeric_lt", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 2, 0, 16, {PGOid(1700), PGOid(1700)})
NEW_PROC(BPCHARTYPMODIN, 2913, "bpchartypmodin", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 23, {PGOid(1263)})
NEW_PROC(FLOAT4_NUMERIC, 1745, "float4", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 700, {PGOid(1700)})
NEW_PROC(TEXTNE, 157, "textne", 11, 10, 12, 1, 0, 0, 0, false, false, false, true, true, false, 'i', 2, 0, 16, {PGOid(25), PGOid(25)})
NEW_PROC(INT2MUL, 152, "int2mul", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 2, 0, 21, {PGOid(21), PGOid(21)})
NEW_PROC(TEXT_GE, 743, "text_ge", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 2, 0, 16, {PGOid(25), PGOid(25)})
NEW_PROC(TEXTSEND, 2415, "textsend", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 's', 1, 0, 17, {PGOid(25)})
NEW_PROC(TEXTRECV, 2414, "textrecv", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 's', 1, 0, 25, {PGOid(2281)})
NEW_PROC(TEXTOUT, 47, "textout", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 2275, {PGOid(25)})
NEW_PROC(FLOAT4NE, 288, "float4ne", 11, 10, 12, 1, 0, 0, 0, false, false, false, true, true, false, 'i', 2, 0, 16, {PGOid(700), PGOid(700)})
NEW_PROC(FLOAT4EQ, 287, "float4eq", 11, 10, 12, 1, 0, 0, 0, false, false, false, true, true, false, 'i', 2, 0, 16, {PGOid(700), PGOid(700)})
NEW_PROC(DATE_EQ, 1086, "date_eq", 11, 10, 12, 1, 0, 0, 0, false, false, false, true, true, false, 'i', 2, 0, 16, {PGOid(1082), PGOid(1082)})
NEW_PROC(FLOAT4GE, 292, "float4ge", 11, 10, 12, 1, 0, 0, 0, false, false, false, true, true, false, 'i', 2, 0, 16, {PGOid(700), PGOid(700)})
NEW_PROC(DATE_IN, 1084, "date_in", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 's', 1, 0, 1082, {PGOid(2275)})
NEW_PROC(FLOAT4LE, 290, "float4le", 11, 10, 12, 1, 0, 0, 0, false, false, false, true, true, false, 'i', 2, 0, 16, {PGOid(700), PGOid(700)})
NEW_PROC(DATE_OUT, 1085, "date_out", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 's', 1, 0, 2275, {PGOid(1082)})
NEW_PROC(FLOAT4GT, 291, "float4gt", 11, 10, 12, 1, 0, 0, 0, false, false, false, true, true, false, 'i', 2, 0, 16, {PGOid(700), PGOid(700)})
NEW_PROC(FLOAT4LT, 289, "float4lt", 11, 10, 12, 1, 0, 0, 0, false, false, false, true, true, false, 'i', 2, 0, 16, {PGOid(700), PGOid(700)})
NEW_PROC(FLOAT4OUT, 201, "float4out", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 2275, {PGOid(700)})
NEW_PROC(FLOAT4IN, 200, "float4in", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 700, {PGOid(2275)})
NEW_PROC(INT8DEC, 3546, "int8dec", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 20, {PGOid(20)})
NEW_PROC(INT8DEC_ANY, 3547, "int8dec_any", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 2, 0, 20, {PGOid(20), PGOid(2276)})
NEW_PROC(INT4LT, 66, "int4lt", 11, 10, 12, 1, 0, 0, 0, false, false, false, true, true, false, 'i', 2, 0, 16, {PGOid(23), PGOid(23)})
NEW_PROC(INT8PL, 463, "int8pl", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 2, 0, 20, {PGOid(20), PGOid(20)})
NEW_PROC(TEXTIN, 46, "textin", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 25, {PGOid(2275)})
NEW_PROC(INT4SEND, 2407, "int4send", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 17, {PGOid(23)})
NEW_PROC(INT8INC_ANY, 2804, "int8inc_any", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 2, 0, 20, {PGOid(20), PGOid(2276)})
NEW_PROC(ANY_OUT, 2295, "any_out", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 2275, {PGOid(2276)})
NEW_PROC(ANY_IN, 2294, "any_in", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 2276, {PGOid(2275)})
NEW_PROC(INT8GE, 472, "int8ge", 11, 10, 12, 1, 0, 0, 0, false, false, false, true, true, false, 'i', 2, 0, 16, {PGOid(20), PGOid(20)})
NEW_PROC(INT8LE, 471, "int8le", 11, 10, 12, 1, 0, 0, 0, false, false, false, true, true, false, 'i', 2, 0, 16, {PGOid(20), PGOid(20)})
NEW_PROC(INT8GT, 470, "int8gt", 11, 10, 12, 1, 0, 0, 0, false, false, false, true, true, false, 'i', 2, 0, 16, {PGOid(20), PGOid(20)})
NEW_PROC(INT4RECV, 2406, "int4recv", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 23, {PGOid(2281)})
NEW_PROC(COUNT, 2803, "count", 11, 10, 12, 1, 0, 0, 0, true, false, false, false, false, false, 'i', 0, 0, 20, {PGOid(0)})
NEW_PROC(INT8SEND, 2409, "int8send", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 17, {PGOid(20)})
NEW_PROC(CSTRING_RECV, 2500, "cstring_recv", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 's', 1, 0, 2275, {PGOid(2281)})
NEW_PROC(NUMERIC_OUT, 1702, "numeric_out", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 2275, {PGOid(1700)})
NEW_PROC(INTERNAL_OUT, 2305, "internal_out", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 2275, {PGOid(2281)})
NEW_PROC(OIDLE, 717, "oidle", 11, 10, 12, 1, 0, 0, 0, false, false, false, true, true, false, 'i', 2, 0, 16, {PGOid(26), PGOid(26)})
NEW_PROC(CSTRING_OUT, 2293, "cstring_out", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 2275, {PGOid(2275)})
NEW_PROC(NUMERIC_IN, 1701, "numeric_in", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 3, 0, 1700, {PGOid(2275), PGOid(26), PGOid(23)})
NEW_PROC(INT4_FLOAT4, 319, "int4", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 23, {PGOid(700)})
NEW_PROC(INTERNAL_IN, 2304, "internal_in", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, false, false, 'i', 1, 0, 2281, {PGOid(2275)})
NEW_PROC(OIDLT, 716, "oidlt", 11, 10, 12, 1, 0, 0, 0, false, false, false, true, true, false, 'i', 2, 0, 16, {PGOid(26), PGOid(26)})
NEW_PROC(CSTRING_IN, 2292, "cstring_in", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 2275, {PGOid(2275)})
NEW_PROC(OIDNE, 185, "oidne", 11, 10, 12, 1, 0, 0, 0, false, false, false, true, true, false, 'i', 2, 0, 16, {PGOid(26), PGOid(26)})
NEW_PROC(OIDEQ, 184, "oideq", 11, 10, 12, 1, 0, 0, 0, false, false, false, true, true, false, 'i', 2, 0, 16, {PGOid(26), PGOid(26)})
NEW_PROC(BPCHAREQ, 1048, "bpchareq", 11, 10, 12, 1, 0, 0, 0, false, false, false, true, true, false, 'i', 2, 0, 16, {PGOid(1042), PGOid(1042)})
NEW_PROC(FLOAT8SEND, 2427, "float8send", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 17, {PGOid(701)})
NEW_PROC(INT8IN, 460, "int8in", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 20, {PGOid(2275)})
NEW_PROC(INT2EQ, 63, "int2eq", 11, 10, 12, 1, 0, 0, 0, false, false, false, true, true, false, 'i', 2, 0, 16, {PGOid(21), PGOid(21)})
NEW_PROC(BOOLIN, 1242, "boolin", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 16, {PGOid(2275)})
NEW_PROC(OIDGE, 1639, "oidge", 11, 10, 12, 1, 0, 0, 0, false, false, false, true, true, false, 'i', 2, 0, 16, {PGOid(26), PGOid(26)})
NEW_PROC(FLOAT8RECV, 2426, "float8recv", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 701, {PGOid(2281)})
NEW_PROC(BYTEAIN, 1244, "byteain", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 17, {PGOid(2275)})
NEW_PROC(OIDGT, 1638, "oidgt", 11, 10, 12, 1, 0, 0, 0, false, false, false, true, true, false, 'i', 2, 0, 16, {PGOid(26), PGOid(26)})
NEW_PROC(OIDSEND, 2419, "oidsend", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 17, {PGOid(26)})
NEW_PROC(OIDRECV, 2418, "oidrecv", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 26, {PGOid(2281)})
NEW_PROC(BYTEASEND, 2413, "byteasend", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 17, {PGOid(17)})
NEW_PROC(BYTEAOUT, 31, "byteaout", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 2275, {PGOid(17)})
NEW_PROC(INT8RECV, 2408, "int8recv", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 20, {PGOid(2281)})
NEW_PROC(BYTEARECV, 2412, "bytearecv", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 17, {PGOid(2281)})
NEW_PROC(TIMESTAMP_LT, 2054, "timestamp_lt", 11, 10, 12, 1, 0, 0, 0, false, false, false, true, true, false, 'i', 2, 0, 16, {PGOid(1114), PGOid(1114)})
NEW_PROC(INT8_INT4, 481, "int8", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 20, {PGOid(23)})
NEW_PROC(BOOLNE, 84, "boolne", 11, 10, 12, 1, 0, 0, 0, false, false, false, true, true, false, 'i', 2, 0, 16, {PGOid(16), PGOid(16)})
NEW_PROC(BPCHAROUT, 1045, "bpcharout", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 2275, {PGOid(1042)})
NEW_PROC(INT4IN, 42, "int4in", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 23, {PGOid(2275)})
NEW_PROC(FLOAT4RECV, 2424, "float4recv", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 700, {PGOid(2281)})
NEW_PROC(BOOLEQ, 60, "booleq", 11, 10, 12, 1, 0, 0, 0, false, false, false, true, true, false, 'i', 2, 0, 16, {PGOid(16), PGOid(16)})
NEW_PROC(BOOLGT, 57, "boolgt", 11, 10, 12, 1, 0, 0, 0, false, false, false, true, true, false, 'i', 2, 0, 16, {PGOid(16), PGOid(16)})
NEW_PROC(ARRAY_LT, 391, "array_lt", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 2, 0, 16, {PGOid(2277), PGOid(2277)})
NEW_PROC(NUMERIC_SEND, 2461, "numeric_send", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 17, {PGOid(1700)})
NEW_PROC(DATE_LE, 1088, "date_le", 11, 10, 12, 1, 0, 0, 0, false, false, false, true, true, false, 'i', 2, 0, 16, {PGOid(1082), PGOid(1082)})
NEW_PROC(FLOAT8NE, 294, "float8ne", 11, 10, 12, 1, 0, 0, 0, false, false, false, true, true, false, 'i', 2, 0, 16, {PGOid(701), PGOid(701)})
NEW_PROC(INT4_BOOL, 2558, "int4", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 23, {PGOid(16)})
NEW_PROC(INT2PL, 176, "int2pl", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 2, 0, 21, {PGOid(21), PGOid(21)})
NEW_PROC(FLOAT8GT, 297, "float8gt", 11, 10, 12, 1, 0, 0, 0, false, false, false, true, true, false, 'i', 2, 0, 16, {PGOid(701), PGOid(701)})
NEW_PROC(DATE_NE, 1091, "date_ne", 11, 10, 12, 1, 0, 0, 0, false, false, false, true, true, false, 'i', 2, 0, 16, {PGOid(1082), PGOid(1082)})
NEW_PROC(NEQJOINSEL, 106, "neqjoinsel", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 's', 5, 0, 701, {PGOid(2281), PGOid(26), PGOid(2281), PGOid(21), PGOid(2281)})
NEW_PROC(ARRAY_GE, 396, "array_ge", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 2, 0, 16, {PGOid(2277), PGOid(2277)})
NEW_PROC(FLOAT8LE, 296, "float8le", 11, 10, 12, 1, 0, 0, 0, false, false, false, true, true, false, 'i', 2, 0, 16, {PGOid(701), PGOid(701)})
NEW_PROC(DATE_GE, 1090, "date_ge", 11, 10, 12, 1, 0, 0, 0, false, false, false, true, true, false, 'i', 2, 0, 16, {PGOid(1082), PGOid(1082)})
NEW_PROC(DATE_SEND, 2469, "date_send", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 17, {PGOid(1082)})
NEW_PROC(EQJOINSEL, 105, "eqjoinsel", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 's', 5, 0, 701, {PGOid(2281), PGOid(26), PGOid(2281), PGOid(21), PGOid(2281)})
NEW_PROC(ARRAY_EQ, 744, "array_eq", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 2, 0, 16, {PGOid(2277), PGOid(2277)})
NEW_PROC(FLOAT8_FLOAT4, 311, "float8", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 701, {PGOid(700)})
NEW_PROC(ANYARRAY_IN, 2296, "anyarray_in", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 2277, {PGOid(2275)})
NEW_PROC(INT8_FLOAT4, 653, "int8", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 20, {PGOid(700)})
NEW_PROC(BPCHARLE, 1050, "bpcharle", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 2, 0, 16, {PGOid(1042), PGOid(1042)})
NEW_PROC(INT4EQ, 65, "int4eq", 11, 10, 12, 1, 0, 0, 0, false, false, false, true, true, false, 'i', 2, 0, 16, {PGOid(23), PGOid(23)})
NEW_PROC(INT24PL, 178, "int24pl", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 2, 0, 23, {PGOid(21), PGOid(23)})
NEW_PROC(BOOLRECV, 2436, "boolrecv", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 16, {PGOid(2281)})
NEW_PROC(SCALARGTJOINSEL, 108, "scalargtjoinsel", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 's', 5, 0, 701, {PGOid(2281), PGOid(26), PGOid(2281), PGOid(21), PGOid(2281)})
NEW_PROC(TIMESTAMPTYPMODIN, 2905, "timestamptypmodin", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 23, {PGOid(1263)})
NEW_PROC(INT4GT, 147, "int4gt", 11, 10, 12, 1, 0, 0, 0, false, false, false, true, true, false, 'i', 2, 0, 16, {PGOid(23), PGOid(23)})
NEW_PROC(ANYARRAY_SEND, 2503, "anyarray_send", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 's', 1, 0, 17, {PGOid(2277)})
NEW_PROC(TIMESTAMP_IN, 1312, "timestamp_in", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 's', 3, 0, 1114, {PGOid(2275), PGOid(26), PGOid(23)})
NEW_PROC(FLOAT4_FLOAT8, 312, "float4", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 700, {PGOid(701)})
NEW_PROC(ANYARRAY_OUT, 2297, "anyarray_out", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 's', 1, 0, 2275, {PGOid(2277)})
NEW_PROC(BPCHARGT, 1051, "bpchargt", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 2, 0, 16, {PGOid(1042), PGOid(1042)})
NEW_PROC(BPCHARRECV, 2430, "bpcharrecv", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 's', 3, 0, 1042, {PGOid(2281), PGOid(26), PGOid(23)})
NEW_PROC(SCALARGTSEL, 104, "scalargtsel", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 's', 4, 0, 701, {PGOid(2281), PGOid(26), PGOid(2281), PGOid(23)})
NEW_PROC(BOOLGE, 1692, "boolge", 11, 10, 12, 1, 0, 0, 0, false, false, false, true, true, false, 'i', 2, 0, 16, {PGOid(16), PGOid(16)})
NEW_PROC(FLOAT4SEND, 2425, "float4send", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 17, {PGOid(700)})
NEW_PROC(INT4OUT, 43, "int4out", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 2275, {PGOid(23)})
NEW_PROC(SCALARLTSEL, 103, "scalarltsel", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 's', 4, 0, 701, {PGOid(2281), PGOid(26), PGOid(2281), PGOid(23)})
NEW_PROC(BOOLLE, 1691, "boolle", 11, 10, 12, 1, 0, 0, 0, false, false, false, true, true, false, 'i', 2, 0, 16, {PGOid(16), PGOid(16)})
NEW_PROC(TEXT_GT, 742, "text_gt", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 2, 0, 16, {PGOid(25), PGOid(25)})
NEW_PROC(INT2GE, 151, "int2ge", 11, 10, 12, 1, 0, 0, 0, false, false, false, true, true, false, 'i', 2, 0, 16, {PGOid(21), PGOid(21)})
NEW_PROC(BOOLOUT, 1243, "boolout", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 2275, {PGOid(16)})
NEW_PROC(EQSEL, 101, "eqsel", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 's', 4, 0, 701, {PGOid(2281), PGOid(26), PGOid(2281), PGOid(23)})
NEW_PROC(FLOAT8GE, 298, "float8ge", 11, 10, 12, 1, 0, 0, 0, false, false, false, true, true, false, 'i', 2, 0, 16, {PGOid(701), PGOid(701)})
NEW_PROC(FLOAT8LT, 295, "float8lt", 11, 10, 12, 1, 0, 0, 0, false, false, false, true, true, false, 'i', 2, 0, 16, {PGOid(701), PGOid(701)})
NEW_PROC(DATE_GT, 1089, "date_gt", 11, 10, 12, 1, 0, 0, 0, false, false, false, true, true, false, 'i', 2, 0, 16, {PGOid(1082), PGOid(1082)})
NEW_PROC(INT8_FLOAT8, 483, "int8", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 20, {PGOid(701)})
NEW_PROC(DATE_RECV, 2468, "date_recv", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 1082, {PGOid(2281)})
NEW_PROC(INT2GT, 146, "int2gt", 11, 10, 12, 1, 0, 0, 0, false, false, false, true, true, false, 'i', 2, 0, 16, {PGOid(21), PGOid(21)})
NEW_PROC(FLOAT4, 652, "float4", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 700, {PGOid(20)})
NEW_PROC(BPCHARLT, 1049, "bpcharlt", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 2, 0, 16, {PGOid(1042), PGOid(1042)})
NEW_PROC(INT8OUT, 461, "int8out", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 2275, {PGOid(20)})
NEW_PROC(INT2LT, 64, "int2lt", 11, 10, 12, 1, 0, 0, 0, false, false, false, true, true, false, 'i', 2, 0, 16, {PGOid(21), PGOid(21)})
NEW_PROC(ARRAY_LE, 393, "array_le", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 2, 0, 16, {PGOid(2277), PGOid(2277)})
NEW_PROC(FLOAT8EQ, 293, "float8eq", 11, 10, 12, 1, 0, 0, 0, false, false, false, true, true, false, 'i', 2, 0, 16, {PGOid(701), PGOid(701)})
NEW_PROC(DATE_LT, 1087, "date_lt", 11, 10, 12, 1, 0, 0, 0, false, false, false, true, true, false, 'i', 2, 0, 16, {PGOid(1082), PGOid(1082)})
NEW_PROC(NEQSEL, 102, "neqsel", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 's', 4, 0, 701, {PGOid(2281), PGOid(26), PGOid(2281), PGOid(23)})
NEW_PROC(OIDIN, 1798, "oidin", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 26, {PGOid(2275)})
NEW_PROC(TIMESTAMPTZTYPMODIN, 2907, "timestamptztypmodin", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 23, {PGOid(1263)})
NEW_PROC(TEXT_LT, 740, "text_lt", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 2, 0, 16, {PGOid(25), PGOid(25)})
NEW_PROC(INT4LE, 149, "int4le", 11, 10, 12, 1, 0, 0, 0, false, false, false, true, true, false, 'i', 2, 0, 16, {PGOid(23), PGOid(23)})
NEW_PROC(ANYARRAY_RECV, 2502, "anyarray_recv", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 's', 1, 0, 2277, {PGOid(2281)})
NEW_PROC(INT2SEND, 2405, "int2send", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 17, {PGOid(21)})
NEW_PROC(TIMESTAMPTYPMODOUT, 2906, "timestamptypmodout", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 2275, {PGOid(23)})
NEW_PROC(INT2LE, 148, "int2le", 11, 10, 12, 1, 0, 0, 0, false, false, false, true, true, false, 'i', 2, 0, 16, {PGOid(21), PGOid(21)})
NEW_PROC(ARRAY_GT, 392, "array_gt", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 2, 0, 16, {PGOid(2277), PGOid(2277)})
NEW_PROC(CSTRING_SEND, 2501, "cstring_send", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 's', 1, 0, 17, {PGOid(2275)})
NEW_PROC(INT2RECV, 2404, "int2recv", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 21, {PGOid(2281)})
NEW_PROC(INT2OUT, 39, "int2out", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 2275, {PGOid(21)})
NEW_PROC(ARRAY_NE, 390, "array_ne", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 2, 0, 16, {PGOid(2277), PGOid(2277)})
NEW_PROC(NUMERIC_RECV, 2460, "numeric_recv", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 3, 0, 1700, {PGOid(2281), PGOid(26), PGOid(23)})
NEW_PROC(FLOAT8IN, 214, "float8in", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 701, {PGOid(2275)})
NEW_PROC(OIDOUT, 1799, "oidout", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 2275, {PGOid(26)})
NEW_PROC(TIMESTAMPTZTYPMODOUT, 2908, "timestamptztypmodout", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 2275, {PGOid(23)})
NEW_PROC(TEXT_LE, 741, "text_le", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 2, 0, 16, {PGOid(25), PGOid(25)})
NEW_PROC(INT4GE, 150, "int4ge", 11, 10, 12, 1, 0, 0, 0, false, false, false, true, true, false, 'i', 2, 0, 16, {PGOid(23), PGOid(23)})
NEW_PROC(BOOLSEND, 2437, "boolsend", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 17, {PGOid(16)})
NEW_PROC(ARRAY_TYPANALYZE, 3816, "array_typanalyze", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 's', 1, 0, 16, {PGOid(2281)})
NEW_PROC(BOOLLT, 56, "boollt", 11, 10, 12, 1, 0, 0, 0, false, false, false, true, true, false, 'i', 2, 0, 16, {PGOid(16), PGOid(16)})
NEW_PROC(INT42PL, 179, "int42pl", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 2, 0, 23, {PGOid(23), PGOid(21)})
NEW_PROC(BYTEAGE, 1952, "byteage", 11, 10, 12, 1, 0, 0, 0, false, false, false, true, true, false, 'i', 2, 0, 16, {PGOid(17), PGOid(17)})
NEW_PROC(BPCHARGE, 1052, "bpcharge", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 2, 0, 16, {PGOid(1042), PGOid(1042)})
NEW_PROC(BPCHARSEND, 2431, "bpcharsend", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 's', 1, 0, 17, {PGOid(1042)})
NEW_PROC(INT8INC, 1219, "int8inc", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 20, {PGOid(20)})
NEW_PROC(ARRAY_SEND, 2401, "array_send", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 's', 1, 0, 17, {PGOid(2277)})
NEW_PROC(NUMERIC_GE, 1721, "numeric_ge", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 2, 0, 16, {PGOid(1700), PGOid(1700)})
NEW_PROC(INT2NE, 145, "int2ne", 11, 10, 12, 1, 0, 0, 0, false, false, false, true, true, false, 'i', 2, 0, 16, {PGOid(21), PGOid(21)})
NEW_PROC(INT2IN, 38, "int2in", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 21, {PGOid(2275)})
NEW_PROC(INT4PL, 177, "int4pl", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 2, 0, 23, {PGOid(23), PGOid(23)})
NEW_PROC(COUNT_ANY, 2147, "count", 11, 10, 12, 1, 0, 0, 0, true, false, false, false, false, false, 'i', 1, 0, 20, {PGOid(2276)})
NEW_PROC(SCALARLTJOINSEL, 107, "scalarltjoinsel", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 's', 5, 0, 701, {PGOid(2281), PGOid(26), PGOid(2281), PGOid(21), PGOid(2281)})
NEW_PROC(ARRAY_RECV, 2400, "array_recv", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 's', 3, 0, 2277, {PGOid(2281), PGOid(26), PGOid(23)})
NEW_PROC(NUMERICTYPMODOUT, 2918, "numerictypmodout", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 2275, {PGOid(23)})
NEW_PROC(ARRAY_OUT, 751, "array_out", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 's', 1, 0, 2275, {PGOid(2277)})
NEW_PROC(FLOAT8OUT, 215, "float8out", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 2275, {PGOid(701)})
NEW_PROC(INT2MI, 180, "int2mi", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 2, 0, 21, {PGOid(21), PGOid(21)})
NEW_PROC(BYTEANE, 1953, "byteane", 11, 10, 12, 1, 0, 0, 0, false, false, false, true, true, false, 'i', 2, 0, 16, {PGOid(17), PGOid(17)})
NEW_PROC(NUMERIC_GT, 1720, "numeric_gt", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 2, 0, 16, {PGOid(1700), PGOid(1700)})
NEW_PROC(INT4NE, 144, "int4ne", 11, 10, 12, 1, 0, 0, 0, false, false, false, true, true, false, 'i', 2, 0, 16, {PGOid(23), PGOid(23)})
NEW_PROC(NUMERICTYPMODIN, 2917, "numerictypmodin", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 'i', 1, 0, 23, {PGOid(1263)})
NEW_PROC(ARRAY_IN, 750, "array_in", 11, 10, 12, 1, 0, 0, 0, false, false, false, false, true, false, 's', 3, 0, 2277, {PGOid(2275), PGOid(26), PGOid(23)})

ProcProvider::OidProcMap ProcProvider::oid_proc_map = {
    ProcProvider::PROC_FLOAT8_NUMERIC,
    ProcProvider::PROC_INT4_NUMERIC,
    ProcProvider::PROC_INT2_NUMERIC,
    ProcProvider::PROC_INT8_NUMERIC,
    ProcProvider::PROC_FLOAT8_INT4,
    ProcProvider::PROC_FLOAT4_INT4,
    ProcProvider::PROC_INT2_INT4,
    ProcProvider::PROC_INT2_FLOAT8,
    ProcProvider::PROC_INT2_FLOAT4,
    ProcProvider::PROC_TEXT,
    ProcProvider::PROC_FLOAT8_INT2,
    ProcProvider::PROC_FLOAT4_INT2,
    ProcProvider::PROC_INT4_INT2,
    ProcProvider::PROC_FLOAT8,
    ProcProvider::PROC_INT4,
    ProcProvider::PROC_INT4_FLOAT8,
    ProcProvider::PROC_INT2,
    ProcProvider::PROC_INT42DIV,
    ProcProvider::PROC_INT24DIV,
    ProcProvider::PROC_INT4DIV,
    ProcProvider::PROC_INT2DIV,
    ProcProvider::PROC_INT42MUL,
    ProcProvider::PROC_INT4MUL,
    ProcProvider::PROC_INT42MI,
    ProcProvider::PROC_INT24MI,
    ProcProvider::PROC_INT4MI,
    ProcProvider::PROC_NUMERIC_NE,
    ProcProvider::PROC_NUMERIC_EQ,
    ProcProvider::PROC_TIMESTAMPTZ_NE,
    ProcProvider::PROC_TIMESTAMPTZ_EQ,
    ProcProvider::PROC_BYTEALE,
    ProcProvider::PROC_TIMESTAMPTZ_GE,
    ProcProvider::PROC_INT24MUL,
    ProcProvider::PROC_BYTEALT,
    ProcProvider::PROC_TIMESTAMPTZ_LE,
    ProcProvider::PROC_BYTEAGT,
    ProcProvider::PROC_TIMESTAMPTZ_GT,
    ProcProvider::PROC_BYTEAEQ,
    ProcProvider::PROC_TIMESTAMPTZ_LT,
    ProcProvider::PROC_TIMESTAMPTZ_SEND,
    ProcProvider::PROC_TIMESTAMPTZ_RECV,
    ProcProvider::PROC_INT8,
    ProcProvider::PROC_TIMESTAMPTZ_OUT,
    ProcProvider::PROC_TIMESTAMPTZ_IN,
    ProcProvider::PROC_TIMESTAMP_NE,
    ProcProvider::PROC_TEXTEQ,
    ProcProvider::PROC_TIMESTAMP_EQ,
    ProcProvider::PROC_INT8NE,
    ProcProvider::PROC_TIMESTAMP_GE,
    ProcProvider::PROC_INT8EQ,
    ProcProvider::PROC_TIMESTAMP_LE,
    ProcProvider::PROC_INT8LT,
    ProcProvider::PROC_TIMESTAMP_GT,
    ProcProvider::PROC_TIMESTAMP_SEND,
    ProcProvider::PROC_TIMESTAMP_RECV,
    ProcProvider::PROC_TIMESTAMP_OUT,
    ProcProvider::PROC_BPCHARNE,
    ProcProvider::PROC_BPCHARIN,
    ProcProvider::PROC_NUMERIC_LE,
    ProcProvider::PROC_BPCHARTYPMODOUT,
    ProcProvider::PROC_NUMERIC_LT,
    ProcProvider::PROC_BPCHARTYPMODIN,
    ProcProvider::PROC_FLOAT4_NUMERIC,
    ProcProvider::PROC_TEXTNE,
    ProcProvider::PROC_INT2MUL,
    ProcProvider::PROC_TEXT_GE,
    ProcProvider::PROC_TEXTSEND,
    ProcProvider::PROC_TEXTRECV,
    ProcProvider::PROC_TEXTOUT,
    ProcProvider::PROC_FLOAT4NE,
    ProcProvider::PROC_FLOAT4EQ,
    ProcProvider::PROC_DATE_EQ,
    ProcProvider::PROC_FLOAT4GE,
    ProcProvider::PROC_DATE_IN,
    ProcProvider::PROC_FLOAT4LE,
    ProcProvider::PROC_DATE_OUT,
    ProcProvider::PROC_FLOAT4GT,
    ProcProvider::PROC_FLOAT4LT,
    ProcProvider::PROC_FLOAT4OUT,
    ProcProvider::PROC_FLOAT4IN,
    ProcProvider::PROC_INT8DEC,
    ProcProvider::PROC_INT8DEC_ANY,
    ProcProvider::PROC_INT4LT,
    ProcProvider::PROC_INT8PL,
    ProcProvider::PROC_TEXTIN,
    ProcProvider::PROC_INT4SEND,
    ProcProvider::PROC_INT8INC_ANY,
    ProcProvider::PROC_ANY_OUT,
    ProcProvider::PROC_ANY_IN,
    ProcProvider::PROC_INT8GE,
    ProcProvider::PROC_INT8LE,
    ProcProvider::PROC_INT8GT,
    ProcProvider::PROC_INT4RECV,
    ProcProvider::PROC_COUNT,
    ProcProvider::PROC_INT8SEND,
    ProcProvider::PROC_CSTRING_RECV,
    ProcProvider::PROC_NUMERIC_OUT,
    ProcProvider::PROC_INTERNAL_OUT,
    ProcProvider::PROC_OIDLE,
    ProcProvider::PROC_CSTRING_OUT,
    ProcProvider::PROC_NUMERIC_IN,
    ProcProvider::PROC_INT4_FLOAT4,
    ProcProvider::PROC_INTERNAL_IN,
    ProcProvider::PROC_OIDLT,
    ProcProvider::PROC_CSTRING_IN,
    ProcProvider::PROC_OIDNE,
    ProcProvider::PROC_OIDEQ,
    ProcProvider::PROC_BPCHAREQ,
    ProcProvider::PROC_FLOAT8SEND,
    ProcProvider::PROC_INT8IN,
    ProcProvider::PROC_INT2EQ,
    ProcProvider::PROC_BOOLIN,
    ProcProvider::PROC_OIDGE,
    ProcProvider::PROC_FLOAT8RECV,
    ProcProvider::PROC_BYTEAIN,
    ProcProvider::PROC_OIDGT,
    ProcProvider::PROC_OIDSEND,
    ProcProvider::PROC_OIDRECV,
    ProcProvider::PROC_BYTEASEND,
    ProcProvider::PROC_BYTEAOUT,
    ProcProvider::PROC_INT8RECV,
    ProcProvider::PROC_BYTEARECV,
    ProcProvider::PROC_TIMESTAMP_LT,
    ProcProvider::PROC_INT8_INT4,
    ProcProvider::PROC_BOOLNE,
    ProcProvider::PROC_BPCHAROUT,
    ProcProvider::PROC_INT4IN,
    ProcProvider::PROC_FLOAT4RECV,
    ProcProvider::PROC_BOOLEQ,
    ProcProvider::PROC_BOOLGT,
    ProcProvider::PROC_ARRAY_LT,
    ProcProvider::PROC_NUMERIC_SEND,
    ProcProvider::PROC_DATE_LE,
    ProcProvider::PROC_FLOAT8NE,
    ProcProvider::PROC_INT4_BOOL,
    ProcProvider::PROC_INT2PL,
    ProcProvider::PROC_FLOAT8GT,
    ProcProvider::PROC_DATE_NE,
    ProcProvider::PROC_NEQJOINSEL,
    ProcProvider::PROC_ARRAY_GE,
    ProcProvider::PROC_FLOAT8LE,
    ProcProvider::PROC_DATE_GE,
    ProcProvider::PROC_DATE_SEND,
    ProcProvider::PROC_EQJOINSEL,
    ProcProvider::PROC_ARRAY_EQ,
    ProcProvider::PROC_FLOAT8_FLOAT4,
    ProcProvider::PROC_ANYARRAY_IN,
    ProcProvider::PROC_INT8_FLOAT4,
    ProcProvider::PROC_BPCHARLE,
    ProcProvider::PROC_INT4EQ,
    ProcProvider::PROC_INT24PL,
    ProcProvider::PROC_BOOLRECV,
    ProcProvider::PROC_SCALARGTJOINSEL,
    ProcProvider::PROC_TIMESTAMPTYPMODIN,
    ProcProvider::PROC_INT4GT,
    ProcProvider::PROC_ANYARRAY_SEND,
    ProcProvider::PROC_TIMESTAMP_IN,
    ProcProvider::PROC_FLOAT4_FLOAT8,
    ProcProvider::PROC_ANYARRAY_OUT,
    ProcProvider::PROC_BPCHARGT,
    ProcProvider::PROC_BPCHARRECV,
    ProcProvider::PROC_SCALARGTSEL,
    ProcProvider::PROC_BOOLGE,
    ProcProvider::PROC_FLOAT4SEND,
    ProcProvider::PROC_INT4OUT,
    ProcProvider::PROC_SCALARLTSEL,
    ProcProvider::PROC_BOOLLE,
    ProcProvider::PROC_TEXT_GT,
    ProcProvider::PROC_INT2GE,
    ProcProvider::PROC_BOOLOUT,
    ProcProvider::PROC_EQSEL,
    ProcProvider::PROC_FLOAT8GE,
    ProcProvider::PROC_FLOAT8LT,
    ProcProvider::PROC_DATE_GT,
    ProcProvider::PROC_INT8_FLOAT8,
    ProcProvider::PROC_DATE_RECV,
    ProcProvider::PROC_INT2GT,
    ProcProvider::PROC_FLOAT4,
    ProcProvider::PROC_BPCHARLT,
    ProcProvider::PROC_INT8OUT,
    ProcProvider::PROC_INT2LT,
    ProcProvider::PROC_ARRAY_LE,
    ProcProvider::PROC_FLOAT8EQ,
    ProcProvider::PROC_DATE_LT,
    ProcProvider::PROC_NEQSEL,
    ProcProvider::PROC_OIDIN,
    ProcProvider::PROC_TIMESTAMPTZTYPMODIN,
    ProcProvider::PROC_TEXT_LT,
    ProcProvider::PROC_INT4LE,
    ProcProvider::PROC_ANYARRAY_RECV,
    ProcProvider::PROC_INT2SEND,
    ProcProvider::PROC_TIMESTAMPTYPMODOUT,
    ProcProvider::PROC_INT2LE,
    ProcProvider::PROC_ARRAY_GT,
    ProcProvider::PROC_CSTRING_SEND,
    ProcProvider::PROC_INT2RECV,
    ProcProvider::PROC_INT2OUT,
    ProcProvider::PROC_ARRAY_NE,
    ProcProvider::PROC_NUMERIC_RECV,
    ProcProvider::PROC_FLOAT8IN,
    ProcProvider::PROC_OIDOUT,
    ProcProvider::PROC_TIMESTAMPTZTYPMODOUT,
    ProcProvider::PROC_TEXT_LE,
    ProcProvider::PROC_INT4GE,
    ProcProvider::PROC_BOOLSEND,
    ProcProvider::PROC_ARRAY_TYPANALYZE,
    ProcProvider::PROC_BOOLLT,
    ProcProvider::PROC_INT42PL,
    ProcProvider::PROC_BYTEAGE,
    ProcProvider::PROC_BPCHARGE,
    ProcProvider::PROC_BPCHARSEND,
    ProcProvider::PROC_INT8INC,
    ProcProvider::PROC_ARRAY_SEND,
    ProcProvider::PROC_NUMERIC_GE,
    ProcProvider::PROC_INT2NE,
    ProcProvider::PROC_INT2IN,
    ProcProvider::PROC_INT4PL,
    ProcProvider::PROC_COUNT_ANY,
    ProcProvider::PROC_SCALARLTJOINSEL,
    ProcProvider::PROC_ARRAY_RECV,
    ProcProvider::PROC_NUMERICTYPMODOUT,
    ProcProvider::PROC_ARRAY_OUT,
    ProcProvider::PROC_FLOAT8OUT,
    ProcProvider::PROC_INT2MI,
    ProcProvider::PROC_BYTEANE,
    ProcProvider::PROC_NUMERIC_GT,
    ProcProvider::PROC_INT4NE,
    ProcProvider::PROC_NUMERICTYPMODIN,
    ProcProvider::PROC_ARRAY_IN,
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
