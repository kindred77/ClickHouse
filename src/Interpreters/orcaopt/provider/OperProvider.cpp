#include <Interpreters/orcaopt/provider/OperProvider.h>

#include <Interpreters/orcaopt/provider/TypeProvider.h>

#include <Interpreters/Context.h>

using namespace duckdb_libpgquery;

//#define NEW_OPER(OPRVARNM, OID, OPRNAME, OPRNAMESPACE, OPROWNER, OPRKIND, OPRCANMERGE, OPRCANHASH, OPRLEFT, OPRRIGHT, OPRRESULT, OPRCOM, OPRNEGATE, OPRLSORTOP, OPRRSORTOP, OPRLTCMPOP, OPRGTCMPOP, OPRCODE, OPRREST, OPRJOIN)
            // /*oprlsortop*/ .oprlsortop = PGOid(OPRLSORTOP),
            // /*oprrsortop*/ .oprrsortop = PGOid(OPRRSORTOP),
            // /*oprltcmpop*/ .oprltcmpop = PGOid(OPRLTCMPOP),
            // /*oprgtcmpop*/ .oprgtcmpop = PGOid(OPRGTCMPOP),

#define NEW_OPER(OPRVARNM, OID, OPRNAME, OPRNAMESPACE, OPROWNER, OPRKIND, OPRCANMERGE, OPRCANHASH, OPRLEFT, OPRRIGHT, OPRRESULT, OPRCOM, OPRNEGATE, OPRCODE, OPRREST, OPRJOIN) \
    std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_##OPRVARNM = {PGOid(OID), \
        std::make_shared<Form_pg_operator>(Form_pg_operator{ \
            .oid = PGOid(OID), \
            /*oprname*/ .oprname = OPRNAME, \
            /*oprnamespace*/ .oprnamespace = PGOid(OPRNAMESPACE), \
            /*oprowner*/ .oprowner = PGOid(OPROWNER), \
            /*oprkind*/ .oprkind = OPRKIND, \
            /*oprcanmerge*/ .oprcanmerge = OPRCANMERGE, \
            /*oprcanhash*/ .oprcanhash = OPRCANHASH, \
            /*oprleft*/ .oprleft = PGOid(OPRLEFT), \
            /*oprright*/ .oprright = PGOid(OPRRIGHT), \
            /*oprresult*/ .oprresult = PGOid(OPRRESULT), \
            /*oprcom*/ .oprcom = PGOid(OPRCOM), \
            /*oprnegate*/ .oprnegate = PGOid(OPRNEGATE), \
            /*oprcode*/ .oprcode = PGOid(OPRCODE), \
            /*oprrest*/ .oprrest = PGOid(OPRREST), \
            /*oprjoin*/ .oprjoin = PGOid(OPRJOIN)})};

namespace DB
{

NEW_OPER(INT42DIV, 547, "/", 11, 10, 'b', false, false, 23, 21, 23, 0, 0, 173, 0, 0)
NEW_OPER(INT24DIV, 546, "/", 11, 10, 'b', false, false, 21, 23, 23, 0, 0, 172, 0, 0)
NEW_OPER(INT4DIV, 528, "/", 11, 10, 'b', false, false, 23, 23, 23, 0, 0, 154, 0, 0)
NEW_OPER(INT2DIV, 527, "/", 11, 10, 'b', false, false, 21, 21, 21, 0, 0, 153, 0, 0)
NEW_OPER(INT42MUL, 545, "*", 11, 10, 'b', false, false, 23, 21, 23, 544, 0, 171, 0, 0)
NEW_OPER(INT24MUL, 544, "*", 11, 10, 'b', false, false, 21, 23, 23, 545, 0, 170, 0, 0)
NEW_OPER(INT4MUL, 514, "*", 11, 10, 'b', false, false, 23, 23, 23, 514, 0, 141, 0, 0)
NEW_OPER(INT2MUL, 526, "*", 11, 10, 'b', false, false, 21, 21, 21, 526, 0, 152, 0, 0)
NEW_OPER(INT42MI, 557, "-", 11, 10, 'b', false, false, 23, 21, 23, 0, 0, 183, 0, 0)
NEW_OPER(INT24MI, 556, "-", 11, 10, 'b', false, false, 21, 23, 23, 0, 0, 182, 0, 0)
NEW_OPER(INT4MI, 555, "-", 11, 10, 'b', false, false, 23, 23, 23, 0, 0, 181, 0, 0)
NEW_OPER(INT2MI, 554, "-", 11, 10, 'b', false, false, 21, 21, 21, 0, 0, 180, 0, 0)
NEW_OPER(INT42PL, 553, "+", 11, 10, 'b', false, false, 23, 21, 23, 552, 0, 179, 0, 0)
NEW_OPER(INT24PL, 552, "+", 11, 10, 'b', false, false, 21, 23, 23, 553, 0, 178, 0, 0)
NEW_OPER(INT4PL, 551, "+", 11, 10, 'b', false, false, 23, 23, 23, 551, 0, 177, 0, 0)
NEW_OPER(INT2PL, 550, "+", 11, 10, 'b', false, false, 21, 21, 21, 550, 0, 176, 0, 0)
NEW_OPER(NUMERIC_GE, 1757, ">=", 11, 10, 'b', false, false, 1700, 1700, 16, 1755, 1754, 1721, 104, 108)
NEW_OPER(NUMERIC_LE, 1755, "<=", 11, 10, 'b', false, false, 1700, 1700, 16, 1757, 1756, 1723, 103, 107)
NEW_OPER(NUMERIC_GT, 1756, ">", 11, 10, 'b', false, false, 1700, 1700, 16, 1754, 1755, 1720, 104, 108)
NEW_OPER(TIMESTAMPTZ_NE, 1321, "<>", 11, 10, 'b', false, false, 1184, 1184, 16, 1321, 1320, 1153, 102, 106)
NEW_OPER(TIMESTAMPTZ_GE, 1325, ">=", 11, 10, 'b', false, false, 1184, 1184, 16, 1323, 1322, 1156, 104, 108)
NEW_OPER(TIMESTAMPTZ_LE, 1323, "<=", 11, 10, 'b', false, false, 1184, 1184, 16, 1325, 1324, 1155, 103, 107)
NEW_OPER(TIMESTAMPTZ_GT, 1324, ">", 11, 10, 'b', false, false, 1184, 1184, 16, 1322, 1323, 1157, 104, 108)
NEW_OPER(TIMESTAMPTZ_LT, 1322, "<", 11, 10, 'b', false, false, 1184, 1184, 16, 1324, 1325, 1154, 103, 107)
NEW_OPER(DATE_GE, 1098, ">=", 11, 10, 'b', false, false, 1082, 1082, 16, 1096, 1095, 1090, 104, 108)
NEW_OPER(BPCHARNE, 1057, "<>", 11, 10, 'b', false, false, 1042, 1042, 16, 1057, 1054, 1053, 102, 106)
NEW_OPER(TEXTEQ, 98, "=", 11, 10, 'b', true, true, 25, 25, 16, 98, 531, 67, 101, 105)
NEW_OPER(BPCHARLE, 1059, "<=", 11, 10, 'b', false, false, 1042, 1042, 16, 1061, 1060, 1050, 103, 107)
NEW_OPER(TEXT_LE, 665, "<=", 11, 10, 'b', false, false, 25, 25, 16, 667, 666, 741, 103, 107)
NEW_OPER(BPCHAREQ, 1054, "=", 11, 10, 'b', true, true, 1042, 1042, 16, 1054, 1057, 1048, 101, 105)
NEW_OPER(BPCHARGT, 1060, ">", 11, 10, 'b', false, false, 1042, 1042, 16, 1058, 1059, 1051, 104, 108)
NEW_OPER(TEXT_GT, 666, ">", 11, 10, 'b', false, false, 25, 25, 16, 664, 665, 742, 104, 108)
NEW_OPER(BPCHARLT, 1058, "<", 11, 10, 'b', false, false, 1042, 1042, 16, 1060, 1061, 1049, 103, 107)
NEW_OPER(TEXT_LT, 664, "<", 11, 10, 'b', false, false, 25, 25, 16, 666, 667, 740, 103, 107)
NEW_OPER(FLOAT4GE, 625, ">=", 11, 10, 'b', false, false, 700, 700, 16, 624, 622, 292, 104, 108)
NEW_OPER(FLOAT4LE, 624, "<=", 11, 10, 'b', false, false, 700, 700, 16, 625, 623, 290, 103, 107)
NEW_OPER(FLOAT4GT, 623, ">", 11, 10, 'b', false, false, 700, 700, 16, 622, 624, 291, 104, 108)
NEW_OPER(ARRAY_GE, 1075, ">=", 11, 10, 'b', false, false, 2277, 2277, 16, 1074, 1072, 396, 104, 108)
NEW_OPER(TIMESTAMP_EQ, 2060, "=", 11, 10, 'b', true, true, 1114, 1114, 16, 2060, 2061, 2052, 101, 105)
NEW_OPER(OIDNE, 608, "<>", 11, 10, 'b', false, false, 26, 26, 16, 608, 607, 185, 102, 106)
NEW_OPER(INT8NE, 411, "<>", 11, 10, 'b', false, false, 20, 20, 16, 411, 410, 468, 102, 106)
NEW_OPER(OIDEQ, 607, "=", 11, 10, 'b', true, true, 26, 26, 16, 607, 608, 184, 101, 105)
NEW_OPER(INT8EQ, 410, "=", 11, 10, 'b', true, true, 20, 20, 16, 410, 411, 467, 101, 105)
NEW_OPER(TIMESTAMP_LT, 2062, "<", 11, 10, 'b', false, false, 1114, 1114, 16, 2064, 2065, 2054, 103, 107)
NEW_OPER(OIDGT, 610, ">", 11, 10, 'b', false, false, 26, 26, 16, 609, 611, 1638, 104, 108)
NEW_OPER(INT8GT, 413, ">", 11, 10, 'b', false, false, 20, 20, 16, 412, 414, 470, 104, 108)
NEW_OPER(ARRAY_NE, 1071, "<>", 11, 10, 'b', false, false, 2277, 2277, 16, 1071, 1070, 390, 102, 106)
NEW_OPER(BOOLEQ, 91, "=", 11, 10, 'b', true, true, 16, 16, 16, 91, 85, 60, 101, 105)
NEW_OPER(TIMESTAMP_NE, 2061, "<>", 11, 10, 'b', false, false, 1114, 1114, 16, 2061, 2060, 2053, 102, 106)
NEW_OPER(OIDLT, 609, "<", 11, 10, 'b', false, false, 26, 26, 16, 610, 612, 716, 103, 107)
NEW_OPER(INT8LT, 412, "<", 11, 10, 'b', false, false, 20, 20, 16, 413, 415, 469, 103, 107)
NEW_OPER(BOOLNE, 85, "<>", 11, 10, 'b', false, false, 16, 16, 16, 85, 91, 84, 102, 106)
NEW_OPER(ARRAY_EQ, 1070, "=", 11, 10, 'b', true, true, 2277, 2277, 16, 1070, 1071, 744, 101, 105)
NEW_OPER(NUMERIC_LT, 1754, "<", 11, 10, 'b', false, false, 1700, 1700, 16, 1756, 1757, 1722, 103, 107)
NEW_OPER(NUMERIC_NE, 1753, "<>", 11, 10, 'b', false, false, 1700, 1700, 16, 1753, 1752, 1719, 102, 106)
NEW_OPER(ARRAY_LE, 1074, "<=", 11, 10, 'b', false, false, 2277, 2277, 16, 1075, 1073, 393, 103, 107)
NEW_OPER(INT2EQ, 94, "=", 11, 10, 'b', true, true, 21, 21, 16, 94, 519, 63, 101, 105)
NEW_OPER(TIMESTAMP_GT, 2064, ">", 11, 10, 'b', false, false, 1114, 1114, 16, 2062, 2063, 2057, 104, 108)
NEW_OPER(DATE_NE, 1094, "<>", 11, 10, 'b', false, false, 1082, 1082, 16, 1094, 1093, 1091, 102, 106)
NEW_OPER(OIDGE, 612, ">=", 11, 10, 'b', false, false, 26, 26, 16, 611, 609, 1639, 104, 108)
NEW_OPER(INT8GE, 415, ">=", 11, 10, 'b', false, false, 20, 20, 16, 414, 412, 472, 104, 108)
NEW_OPER(NUMERIC_EQ, 1752, "=", 11, 10, 'b', true, true, 1700, 1700, 16, 1752, 1753, 1718, 101, 105)
NEW_OPER(ARRAY_GT, 1073, ">", 11, 10, 'b', false, false, 2277, 2277, 16, 1072, 1074, 392, 104, 108)
NEW_OPER(INT2LT, 95, "<", 11, 10, 'b', false, false, 21, 21, 16, 520, 524, 64, 103, 107)
NEW_OPER(TIMESTAMP_GE, 2065, ">=", 11, 10, 'b', false, false, 1114, 1114, 16, 2063, 2062, 2056, 104, 108)
NEW_OPER(DATE_LT, 1095, "<", 11, 10, 'b', false, false, 1082, 1082, 16, 1097, 1098, 1087, 103, 107)
NEW_OPER(BYTEANE, 1956, "<>", 11, 10, 'b', false, false, 17, 17, 16, 1956, 1955, 1953, 102, 106)
NEW_OPER(BYTEAEQ, 1955, "=", 11, 10, 'b', true, true, 17, 17, 16, 1955, 1956, 1948, 101, 105)
NEW_OPER(BYTEAGE, 1960, ">=", 11, 10, 'b', false, false, 17, 17, 16, 1958, 1957, 1952, 104, 108)
NEW_OPER(BYTEALE, 1958, "<=", 11, 10, 'b', false, false, 17, 17, 16, 1960, 1959, 1950, 103, 107)
NEW_OPER(BYTEAGT, 1959, ">", 11, 10, 'b', false, false, 17, 17, 16, 1957, 1958, 1951, 104, 108)
NEW_OPER(BYTEALT, 1957, "<", 11, 10, 'b', false, false, 17, 17, 16, 1959, 1960, 1949, 103, 107)
NEW_OPER(TIMESTAMP_LE, 2063, "<=", 11, 10, 'b', false, false, 1114, 1114, 16, 2065, 2064, 2055, 103, 107)
NEW_OPER(DATE_EQ, 1093, "=", 11, 10, 'b', true, true, 1082, 1082, 16, 1093, 1094, 1086, 101, 105)
NEW_OPER(OIDLE, 611, "<=", 11, 10, 'b', false, false, 26, 26, 16, 612, 610, 717, 103, 107)
NEW_OPER(INT8LE, 414, "<=", 11, 10, 'b', false, false, 20, 20, 16, 415, 413, 471, 103, 107)
NEW_OPER(ARRAY_LT, 1072, "<", 11, 10, 'b', false, false, 2277, 2277, 16, 1073, 1075, 391, 103, 107)
NEW_OPER(TEXT_GE, 667, ">=", 11, 10, 'b', false, false, 25, 25, 16, 665, 664, 743, 104, 108)
NEW_OPER(BPCHARGE, 1061, ">=", 11, 10, 'b', false, false, 1042, 1042, 16, 1059, 1058, 1052, 104, 108)
NEW_OPER(FLOAT8LE, 673, "<=", 11, 10, 'b', false, false, 701, 701, 16, 675, 674, 296, 103, 107)
NEW_OPER(FLOAT8GE, 675, ">=", 11, 10, 'b', false, false, 701, 701, 16, 673, 672, 298, 104, 108)
NEW_OPER(FLOAT8GT, 674, ">", 11, 10, 'b', false, false, 701, 701, 16, 672, 673, 297, 104, 108)
NEW_OPER(FLOAT8EQ, 670, "=", 11, 10, 'b', true, true, 701, 701, 16, 670, 671, 293, 101, 105)
NEW_OPER(TIMESTAMPTZ_EQ, 1320, "=", 11, 10, 'b', true, true, 1184, 1184, 16, 1320, 1321, 1152, 101, 105)
NEW_OPER(BOOLGT, 59, ">", 11, 10, 'b', false, false, 16, 16, 16, 58, 1694, 57, 104, 108)
NEW_OPER(BOOLLE, 1694, "<=", 11, 10, 'b', false, false, 16, 16, 16, 1695, 59, 1691, 103, 107)
NEW_OPER(INT4EQ, 96, "=", 11, 10, 'b', true, true, 23, 23, 16, 96, 518, 65, 101, 105)
NEW_OPER(INT2NE, 519, "<>", 11, 10, 'b', false, false, 21, 21, 16, 519, 94, 145, 102, 106)
NEW_OPER(DATE_LE, 1096, "<=", 11, 10, 'b', false, false, 1082, 1082, 16, 1098, 1097, 1088, 103, 107)
NEW_OPER(FLOAT4NE, 621, "<>", 11, 10, 'b', false, false, 700, 700, 16, 621, 620, 288, 102, 106)
NEW_OPER(INT2GE, 524, ">=", 11, 10, 'b', false, false, 21, 21, 16, 522, 95, 151, 104, 108)
NEW_OPER(INT4LT, 97, "<", 11, 10, 'b', false, false, 23, 23, 16, 521, 525, 66, 103, 107)
NEW_OPER(INT2GT, 520, ">", 11, 10, 'b', false, false, 21, 21, 16, 95, 522, 146, 104, 108)
NEW_OPER(TEXTNE, 531, "<>", 11, 10, 'b', false, false, 25, 25, 16, 531, 98, 157, 102, 106)
NEW_OPER(BOOLGE, 1695, ">=", 11, 10, 'b', false, false, 16, 16, 16, 1694, 58, 1692, 104, 108)
NEW_OPER(INT2LE, 522, "<=", 11, 10, 'b', false, false, 21, 21, 16, 524, 520, 148, 103, 107)
NEW_OPER(FLOAT8NE, 671, "<>", 11, 10, 'b', false, false, 701, 701, 16, 671, 670, 294, 102, 106)
NEW_OPER(FLOAT4LT, 622, "<", 11, 10, 'b', false, false, 700, 700, 16, 623, 625, 289, 103, 107)
NEW_OPER(INT4GE, 525, ">=", 11, 10, 'b', false, false, 23, 23, 16, 523, 97, 150, 104, 108)
NEW_OPER(FLOAT4EQ, 620, "=", 11, 10, 'b', true, true, 700, 700, 16, 620, 621, 287, 101, 105)
NEW_OPER(INT4LE, 523, "<=", 11, 10, 'b', false, false, 23, 23, 16, 525, 521, 149, 103, 107)
NEW_OPER(INT4GT, 521, ">", 11, 10, 'b', false, false, 23, 23, 16, 97, 523, 147, 104, 108)
NEW_OPER(DATE_GT, 1097, ">", 11, 10, 'b', false, false, 1082, 1082, 16, 1095, 1096, 1089, 104, 108)
NEW_OPER(INT4NE, 518, "<>", 11, 10, 'b', false, false, 23, 23, 16, 518, 96, 144, 102, 106)
NEW_OPER(BOOLLT, 58, "<", 11, 10, 'b', false, false, 16, 16, 16, 59, 1695, 56, 103, 107)
NEW_OPER(FLOAT8LT, 672, "<", 11, 10, 'b', false, false, 701, 701, 16, 674, 675, 295, 103, 107)

OperProvider::OidOperatorMap OperProvider::oid_oper_map = {
    OperProvider::OPER_INT42DIV,
    OperProvider::OPER_INT24DIV,
    OperProvider::OPER_INT4DIV,
    OperProvider::OPER_INT2DIV,
    OperProvider::OPER_INT42MUL,
    OperProvider::OPER_INT24MUL,
    OperProvider::OPER_INT4MUL,
    OperProvider::OPER_INT2MUL,
    OperProvider::OPER_INT42MI,
    OperProvider::OPER_INT24MI,
    OperProvider::OPER_INT4MI,
    OperProvider::OPER_INT2MI,
    OperProvider::OPER_INT42PL,
    OperProvider::OPER_INT24PL,
    OperProvider::OPER_INT4PL,
    OperProvider::OPER_INT2PL,
    OperProvider::OPER_NUMERIC_GE,
    OperProvider::OPER_NUMERIC_LE,
    OperProvider::OPER_NUMERIC_GT,
    OperProvider::OPER_TIMESTAMPTZ_NE,
    OperProvider::OPER_TIMESTAMPTZ_GE,
    OperProvider::OPER_TIMESTAMPTZ_LE,
    OperProvider::OPER_TIMESTAMPTZ_GT,
    OperProvider::OPER_TIMESTAMPTZ_LT,
    OperProvider::OPER_DATE_GE,
    OperProvider::OPER_BPCHARNE,
    OperProvider::OPER_TEXTEQ,
    OperProvider::OPER_BPCHARLE,
    OperProvider::OPER_TEXT_LE,
    OperProvider::OPER_BPCHAREQ,
    OperProvider::OPER_BPCHARGT,
    OperProvider::OPER_TEXT_GT,
    OperProvider::OPER_BPCHARLT,
    OperProvider::OPER_TEXT_LT,
    OperProvider::OPER_FLOAT4GE,
    OperProvider::OPER_FLOAT4LE,
    OperProvider::OPER_FLOAT4GT,
    OperProvider::OPER_ARRAY_GE,
    OperProvider::OPER_TIMESTAMP_EQ,
    OperProvider::OPER_OIDNE,
    OperProvider::OPER_INT8NE,
    OperProvider::OPER_OIDEQ,
    OperProvider::OPER_INT8EQ,
    OperProvider::OPER_TIMESTAMP_LT,
    OperProvider::OPER_OIDGT,
    OperProvider::OPER_INT8GT,
    OperProvider::OPER_ARRAY_NE,
    OperProvider::OPER_BOOLEQ,
    OperProvider::OPER_TIMESTAMP_NE,
    OperProvider::OPER_OIDLT,
    OperProvider::OPER_INT8LT,
    OperProvider::OPER_BOOLNE,
    OperProvider::OPER_ARRAY_EQ,
    OperProvider::OPER_NUMERIC_LT,
    OperProvider::OPER_NUMERIC_NE,
    OperProvider::OPER_ARRAY_LE,
    OperProvider::OPER_INT2EQ,
    OperProvider::OPER_TIMESTAMP_GT,
    OperProvider::OPER_DATE_NE,
    OperProvider::OPER_OIDGE,
    OperProvider::OPER_INT8GE,
    OperProvider::OPER_NUMERIC_EQ,
    OperProvider::OPER_ARRAY_GT,
    OperProvider::OPER_INT2LT,
    OperProvider::OPER_TIMESTAMP_GE,
    OperProvider::OPER_DATE_LT,
    OperProvider::OPER_BYTEANE,
    OperProvider::OPER_BYTEAEQ,
    OperProvider::OPER_BYTEAGE,
    OperProvider::OPER_BYTEALE,
    OperProvider::OPER_BYTEAGT,
    OperProvider::OPER_BYTEALT,
    OperProvider::OPER_TIMESTAMP_LE,
    OperProvider::OPER_DATE_EQ,
    OperProvider::OPER_OIDLE,
    OperProvider::OPER_INT8LE,
    OperProvider::OPER_ARRAY_LT,
    OperProvider::OPER_TEXT_GE,
    OperProvider::OPER_BPCHARGE,
    OperProvider::OPER_FLOAT8LE,
    OperProvider::OPER_FLOAT8GE,
    OperProvider::OPER_FLOAT8GT,
    OperProvider::OPER_FLOAT8EQ,
    OperProvider::OPER_TIMESTAMPTZ_EQ,
    OperProvider::OPER_BOOLGT,
    OperProvider::OPER_BOOLLE,
    OperProvider::OPER_INT4EQ,
    OperProvider::OPER_INT2NE,
    OperProvider::OPER_DATE_LE,
    OperProvider::OPER_FLOAT4NE,
    OperProvider::OPER_INT2GE,
    OperProvider::OPER_INT4LT,
    OperProvider::OPER_INT2GT,
    OperProvider::OPER_TEXTNE,
    OperProvider::OPER_BOOLGE,
    OperProvider::OPER_INT2LE,
    OperProvider::OPER_FLOAT8NE,
    OperProvider::OPER_FLOAT4LT,
    OperProvider::OPER_INT4GE,
    OperProvider::OPER_FLOAT4EQ,
    OperProvider::OPER_INT4LE,
    OperProvider::OPER_INT4GT,
    OperProvider::OPER_DATE_GT,
    OperProvider::OPER_INT4NE,
    OperProvider::OPER_BOOLLT,
    OperProvider::OPER_FLOAT8LT,

};

// OperProvider::OperProvider(const ContextPtr& context_) : context(context_)
// {
	// oid_oper_map.insert(OPER_INT2PL);
	// oid_oper_map.insert(OPER_INT4PL);
	// oid_oper_map.insert(OPER_INT24PL);
	// oid_oper_map.insert(OPER_INT42PL);
    // oid_oper_map.insert(OPER_INT2MI);
    // oid_oper_map.insert(OPER_INT4MI);
    // oid_oper_map.insert(OPER_INT24MI);
    // oid_oper_map.insert(OPER_INT42MI);
    // oid_oper_map.insert(OPER_INT2MUL);
    // oid_oper_map.insert(OPER_INT4MUL);
    // oid_oper_map.insert(OPER_INT24MUL);
    // oid_oper_map.insert(OPER_INT42MUL);
    // oid_oper_map.insert(OPER_INT2DIV);
    // oid_oper_map.insert(OPER_INT4DIV);
    // oid_oper_map.insert(OPER_INT24DIV);
    // oid_oper_map.insert(OPER_INT42DIV);
    // oid_oper_map.insert(OPER_FLOAT32EQ);
    // oid_oper_map.insert(OPER_FLOAT32NE);
    // oid_oper_map.insert(OPER_FLOAT32LT);
    // oid_oper_map.insert(OPER_FLOAT32LE);
    // oid_oper_map.insert(OPER_FLOAT32GT);
    // oid_oper_map.insert(OPER_FLOAT32GE);
    // oid_oper_map.insert(OPER_FLOAT64EQ);
    // oid_oper_map.insert(OPER_FLOAT64NE);
    // oid_oper_map.insert(OPER_FLOAT64LT);
    // oid_oper_map.insert(OPER_FLOAT64LE);
    // oid_oper_map.insert(OPER_FLOAT64GT);
    // oid_oper_map.insert(OPER_FLOAT64GE);
    // oid_oper_map.insert(OPER_BOOLEQ);
    // oid_oper_map.insert(OPER_BOOLNE);
    // oid_oper_map.insert(OPER_BOOLLT);
    // oid_oper_map.insert(OPER_BOOLLE);
    // oid_oper_map.insert(OPER_BOOLGT);
    // oid_oper_map.insert(OPER_BOOLGE);
    // oid_oper_map.insert(OPER_INT16EQ);
    // oid_oper_map.insert(OPER_INT16NE);
    // oid_oper_map.insert(OPER_INT16LT);
    // oid_oper_map.insert(OPER_INT16LE);
    // oid_oper_map.insert(OPER_INT16GT);
    // oid_oper_map.insert(OPER_INT16GE);
    // oid_oper_map.insert(OPER_INT32EQ);
    // oid_oper_map.insert(OPER_INT32NE);
    // oid_oper_map.insert(OPER_INT32LT);
    // oid_oper_map.insert(OPER_INT32LE);
    // oid_oper_map.insert(OPER_INT32GT);
    // oid_oper_map.insert(OPER_INT32GE);
    // oid_oper_map.insert(OPER_INT64EQ);
    // oid_oper_map.insert(OPER_INT64NE);
    // oid_oper_map.insert(OPER_INT64LT);
    // oid_oper_map.insert(OPER_INT64LE);
    // oid_oper_map.insert(OPER_INT64GT);
    // oid_oper_map.insert(OPER_INT64GE);
    // oid_oper_map.insert(OPER_STRINGEQ);
    // oid_oper_map.insert(OPER_STRINGNE);
    // oid_oper_map.insert(OPER_STRINGLT);
    // oid_oper_map.insert(OPER_STRINGLE);
    // oid_oper_map.insert(OPER_STRINGGT);
    // oid_oper_map.insert(OPER_STRINGGE);
    // oid_oper_map.insert(OPER_FIXEDSTRINGEQ);
    // oid_oper_map.insert(OPER_FIXEDSTRINGNE);
    // oid_oper_map.insert(OPER_FIXEDSTRINGLT);
    // oid_oper_map.insert(OPER_FIXEDSTRINGLE);
    // oid_oper_map.insert(OPER_FIXEDSTRINGGT);
    // oid_oper_map.insert(OPER_FIXEDSTRINGGE);
    // oid_oper_map.insert(OPER_DATEEQ);
    // oid_oper_map.insert(OPER_DATENE);
    // oid_oper_map.insert(OPER_DATELT);
    // oid_oper_map.insert(OPER_DATELE);
    // oid_oper_map.insert(OPER_DATEGT);
    // oid_oper_map.insert(OPER_DATEGE);
    // oid_oper_map.insert(OPER_DATETIMEEQ);
    // oid_oper_map.insert(OPER_DATETIMENE);
    // oid_oper_map.insert(OPER_DATETIMELT);
    // oid_oper_map.insert(OPER_DATETIMELE);
    // oid_oper_map.insert(OPER_DATETIMEGT);
    // oid_oper_map.insert(OPER_DATETIMEGE);
    // oid_oper_map.insert(OPER_DATETIME64EQ);
    // oid_oper_map.insert(OPER_DATETIME64NE);
    // oid_oper_map.insert(OPER_DATETIME64LT);
    // oid_oper_map.insert(OPER_DATETIME64LE);
    // oid_oper_map.insert(OPER_DATETIME64GT);
    // oid_oper_map.insert(OPER_DATETIME64GE);
    // oid_oper_map.insert(OPER_DECIMAL64EQ);
    // oid_oper_map.insert(OPER_DECIMAL64NE);
    // oid_oper_map.insert(OPER_DECIMAL64LT);
    // oid_oper_map.insert(OPER_DECIMAL64LE);
    // oid_oper_map.insert(OPER_DECIMAL64GT);
    // oid_oper_map.insert(OPER_DECIMAL64GE);
// };

PGOperatorPtr
OperProvider::getOperByOID(PGOid oid)
{
	auto it = oid_oper_map.find(oid);
	if (it == oid_oper_map.end())
	    return nullptr;
	return it->second;
};

PGOid
OperProvider::getOperByName(duckdb_libpgquery::PGList *names, PGOid oprleft, PGOid oprright)
{
    char * schemaname;
    char * opername;
	DeconstructQualifiedName(names, &schemaname, &opername);

	for (auto oper_pair : oid_oper_map)
    {
        if (std::string(oper_pair.second->oprname) == std::string(opername)
            && oper_pair.second->oprleft == oprleft
            && oper_pair.second->oprright == oprright)
        {
            return oper_pair.first;
        }
    }
	return InvalidOid;
};

bool OperProvider::OperatorExists(duckdb_libpgquery::PGOid oid)
{
    auto it = oid_oper_map.find(oid);
	if (it == oid_oper_map.end())
	    return false;
	return true;
};

FuncCandidateListPtr
OperProvider::OpernameGetCandidates(PGList * names, char oprkind, bool missing_schema_ok)
{
    char * schemaname;
    char * opername;
	DeconstructQualifiedName(names, &schemaname, &opername);
    
    FuncCandidateListPtr resultList = nullptr;
    
    for (auto oper_pair : oid_oper_map)
    {
        if (oprkind && oper_pair.second->oprkind != oprkind)
			continue;

        if (std::string(oper_pair.second->oprname) != std::string(opername))
        {
            continue;
        }

        if (resultList)
        {
            FuncCandidateListPtr prevResult = nullptr;

            for (prevResult = resultList; prevResult; prevResult = prevResult->next)
            {
                if (oper_pair.second->oprleft == prevResult->args[0] && oper_pair.second->oprright == prevResult->args[1])
                    break;
            }

            if (prevResult)
            {
                /* We have a match with a previous result */
                // Assert(pathpos != prevResult->pathpos);
                // if (pathpos > prevResult->pathpos)
                //     continue; /* keep previous result */
                /* replace previous result */
                // prevResult->pathpos = pathpos;
                prevResult->oid = oper_pair.second->oid;
                continue; /* args are same, of course */
            }
        }

        auto newResult = std::make_shared<FuncCandidateList>();

		newResult->pathpos = 0;
		newResult->oid = oper_pair.second->oid;
		newResult->nargs = 2;
		newResult->nvargs = 0;
		newResult->ndargs = 0;
		newResult->argnumbers = NULL;
        newResult->args = new PGOid[newResult->nargs];
		newResult->args[0] = oper_pair.second->oprleft;
        newResult->args[1] = oper_pair.second->oprright;
		newResult->next = resultList;
		resultList = newResult;
    }

    return resultList;
};

/*
 * get_opcode
 *
 *		Returns the regproc id of the routine used to implement an
 *		operator given the operator oid.
 */
PGOid OperProvider::get_opcode(PGOid opno)
{
    PGOperatorPtr op = getOperByOID(opno);
    if (op != NULL)
    {
        return op->oprcode;
    }
    else
        return InvalidOid;
};

PGOid OperProvider::get_commutator(PGOid opno)
{
    PGOperatorPtr op = getOperByOID(opno);
    if (op != NULL)
    {
        return op->oprcom;
    }
    else
        return InvalidOid;
};

PGOid OperProvider::get_negator(PGOid opno)
{
    PGOperatorPtr op = getOperByOID(opno);
    if (op != NULL)
    {
        return op->oprnegate;
    }
    else
        return InvalidOid;
};

// bool OperProvider::get_ordering_op_properties(Oid opno, Oid * opfamily, Oid * opcintype, int16 * strategy)
// {
    // bool result = false;
    // CatCList * catlist;
    // int i;

    // /* ensure outputs are initialized on failure */
    // *opfamily = InvalidOid;
    // *opcintype = InvalidOid;
    // *strategy = 0;

    // /*
	//  * Search pg_amop to see if the target operator is registered as the "<"
	//  * or ">" operator of any btree opfamily.
	//  */
    // catlist = SearchSysCacheList1(AMOPOPID, ObjectIdGetDatum(opno));

    // for (i = 0; i < catlist->n_members; i++)
    // {
    //     HeapTuple tuple = &catlist->members[i]->tuple;
    //     Form_pg_amop aform = (Form_pg_amop)GETSTRUCT(tuple);

    //     /* must be btree */
    //     if (aform->amopmethod != BTREE_AM_OID)
    //         continue;

    //     if (aform->amopstrategy == BTLessStrategyNumber || aform->amopstrategy == BTGreaterStrategyNumber)
    //     {
    //         /* Found it ... should have consistent input types */
    //         if (aform->amoplefttype == aform->amoprighttype)
    //         {
    //             /* Found a suitable opfamily, return info */
    //             *opfamily = aform->amopfamily;
    //             *opcintype = aform->amoplefttype;
    //             *strategy = aform->amopstrategy;
    //             result = true;
    //             break;
    //         }
    //     }
    // }

    // ReleaseSysCacheList(catlist);

    // return result;

//     return true;
// };

// Oid OperProvider::get_opfamily_member(Oid opfamily, Oid lefttype, Oid righttype, int16 strategy)
// {
    // HeapTuple tp;
    // Form_pg_amop amop_tup;
    // Oid result;

    // tp = SearchSysCache4(
    //     AMOPSTRATEGY, ObjectIdGetDatum(opfamily), ObjectIdGetDatum(lefttype), ObjectIdGetDatum(righttype), Int16GetDatum(strategy));
    // if (!HeapTupleIsValid(tp))
    //     return InvalidOid;
    // amop_tup = (Form_pg_amop)GETSTRUCT(tp);
    // result = amop_tup->amopopr;
    // ReleaseSysCache(tp);
    // return result;

//     return InvalidOid;
// };

// Oid OperProvider::get_equality_op_for_ordering_op(Oid opno, bool * reverse)
// {
    // Oid result = InvalidOid;
    // Oid opfamily;
    // Oid opcintype;
    // int16 strategy;

    // /* Find the operator in pg_amop */
    // if (get_ordering_op_properties(opno, &opfamily, &opcintype, &strategy))
    // {
    //     /* Found a suitable opfamily, get matching equality operator */
    //     result = get_opfamily_member(opfamily, opcintype, opcintype, BTEqualStrategyNumber);
    //     if (reverse)
    //         *reverse = (strategy == BTGreaterStrategyNumber);
    // }

    // return result;

//     return InvalidOid;
// };

// bool OperProvider::op_hashjoinable(Oid opno, Oid inputtype)
// {
//     bool result = false;
    // HeapTuple tp;
    // TypeCacheEntry * typentry;

    /* As in op_mergejoinable, let the typcache handle the hard cases */
    /* Eventually we'll need a similar case for record_eq ... */
    // if (opno == ARRAY_EQ_OP)
    // {
    //     typentry = lookup_type_cache(inputtype, TYPECACHE_HASH_PROC);
    //     if (typentry->hash_proc == F_HASH_ARRAY)
    //         result = true;
    // }
    // else
    // {
//         PGOperatorPtr op = getOperByOID(opno);
//         if (op != NULL)
//         {
//             result = op->oprcanhash;
//         }
//     }
//     return result;
// };

PGSortGroupOperPtr OperProvider::get_sort_grp_oper_by_typeid(PGOid type_id)
{
    auto result = std::make_shared<Sort_group_operator>();

    auto type_entry = TypeProvider::getTypeByOid(type_id);

    result->type_id = type_id;
    result->typlen = type_entry->typlen;
    result->typbyval = type_entry->typbyval;
    result->typalign = type_entry->typalign;
    result->typstorage = type_entry->typstorage;
    result->typtype = type_entry->typtype;
    result->typrelid = type_entry->typrelid;

    /*

    select po.opcintype,pa.amopopr,
    (select pop.oprname from pg_operator pop where pop.oid=pa.amopopr) as oprname,
    case when po.opcmethod=403 then 'BTREE_AM_OID' else 'HASH_AM_OID' end as opcmethod,
    case when po.opcmethod=405 and pa.amopstrategy=1 then 'HTEqualStrategyNumber'
    when po.opcmethod=403 and pa.amopstrategy=1 then 'BTLessStrategyNumber'
    when po.opcmethod=403 and pa.amopstrategy=2 then 'BTLessEqualStrategyNumber'
    when po.opcmethod=403 and pa.amopstrategy=3 then 'BTEqualStrategyNumber'
    when po.opcmethod=403 and pa.amopstrategy=4 then 'BTGreaterEqualStrategyNumber'
    when po.opcmethod=403 and pa.amopstrategy=5 then 'BTGreaterStrategyNumber'
    else 'Unknown: '||po.opcmethod||'-'||pa.amopstrategy
    end as amopstrategy
    from pg_amop pa
    inner join pg_opclass po on pa.amopfamily = po.opcfamily
    and pa.amoplefttype = po.opcintype and pa.amoprighttype = po.opcintype
    and pa.amopstrategy in (1,2,3,4,5)
    where po.opcmethod in (403, 405)
    and po.opcdefault = true
    and po.opcintype = 700;

    */

    result->lt_opr = type_entry->lt_opr;
    result->eq_opr = type_entry->eq_opr;
    result->gt_opr = type_entry->gt_opr;
    result->hash_proc = type_entry->hash_proc;
    result->cmp_proc = type_entry->cmp_proc;


    return result;
};

}
