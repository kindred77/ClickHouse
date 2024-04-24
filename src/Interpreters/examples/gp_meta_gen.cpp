#include <pqxx/pqxx>
#include <string>
#include <iostream>

#include <common/parser_common.hpp>

#include <Interpreters/orcaopt/metagen/common.h>
#include <Interpreters/orcaopt/metagen/Agg.h>
#include <Interpreters/orcaopt/metagen/Proc.h>

using namespace pqxx;
using namespace duckdb_libpgquery;
using namespace DB;

int main(int argc, char ** argv)
{
    std::vector<PGOid> agg_init_oids = {
        2147, //COUNTANY
        2803  //COUNTSTAR
    };
    std::vector<PGOid> type_init_oids = {
        700, //Float32
        701, //Float64
        16, //Int8
        21, //Int16
        23, //Int32
        20, //Int64
        25, //String
        1042, //FixedString
        1082, //Date
        1114, //DateTime
        1184, //DateTime64
        1700, //Decimal64
        2276, //any
        2281, //internal
        26, //oid
    };
    std::vector<PGOid> oper_init_oids = {
        550, //INT2PL
        551, //INT4PL
        552, //INT24PL
        553, //INT42PL
        554, //INT2MI
        555, //INT4MI
        556, //INT24MI
        557, //INT42MI
        526, //INT2MUL
        514, //INT4MUL
        544, //INT24MUL
        545, //INT42MUL
        527, //INT2DIV
        528, //INT4DIV
        546, //INT24DIV
        547, //INT42DIV
        620, //FLOAT32EQ
        621, //FLOAT32NE
        622, //FLOAT32LT
        623, //FLOAT32GT
        624, //FLOAT32LE
        625, //FLOAT32GE
        670, //FLOAT64EQ
        671, //FLOAT64NE
        672, //FLOAT64LT
        673, //FLOAT64LE
        674, //FLOAT64GT
        675, //FLOAT64GE
        91, //BOOLEQ
        85, //BOOLNE
        58, //BOOLLT
        59, //BOOLGT
        1694, //BOOLLE
        1695, //BOOLGE
        94, //INT16EQ
        95, //INT16LT
        519, //INT16NE
        520, //INT16GT
        522, //INT16LE
        524, //INT16GE
        96, //INT32EQ
        97, //INT32LT
        518, //INT32NE
        521, //INT32GT
        523, //INT32LE
        525, //INT32GE
        410, //INT64EQ
        411, //INT64NE
        412, //INT64LT
        413, //INT64GT
        414, //INT64LE
        415, //INT64GE
        98, //STRINGEQ
        531, //STRINGNE
        664, //STRINGLT
        665, //STRINGLE
        666, //STRINGGT
        667, //STRINGGE
        1054, //FIXEDSTRINGEQ
        1057, //FIXEDSTRINGNE
        1058, //FIXEDSTRINGLT
        1059, //FIXEDSTRINGLE
        1060, //FIXEDSTRINGGT
        1061, //FIXEDSTRINGGE
        1093, //DATEEQ
        1094, //DATENE
        1095, //DATELT
        1096, //DATELE
        1097, //DATEGT
        1098, //DATEGE
        2060, //DATETIMEEQ
        2061, //DATETIMENE
        2062, //DATETIMELT
        2063, //DATETIMELE
        2064, //DATETIMEGT
        2065, //DATETIMEGE
        1320, //DATETIME64EQ
        1321, //DATETIME64NE
        1322, //DATETIME64LT
        1323, //DATETIME64LE
        1324, //DATETIME64GT
        1325, //DATETIME64GE
        1752, //DECIMAL64EQ
        1753, //DECIMAL64NE
        1754, //DECIMAL64LT
        1755, //DECIMAL64LE
        1756, //DECIMAL64GT
        1757, //DECIMAL64GE
    };
    std::vector<std::tuple<PGOid, PGOid>> cast_init_oids = {
        {20, 21}, //INT64_TO_INT16
        {20, 23}, //INT64_TO_INT32
        {20, 700}, //INT64_TO_FLOAT32
        {20, 701}, //INT64_TO_FLOAT64
        {21, 20}, //INT16_TO_INT64
        {21, 23}, //INT16_TO_INT32
        {21, 700}, //INT16_TO_FLOAT32
        {21, 701}, //INT16_TO_FLOAT64
        {16, 23}, //BOOL_TO_INT32
        {16, 25}, //BOOL_TO_STRING
        {700, 20}, //FLOAT32_TO_INT64
        {700, 21}, //FLOAT32_TO_INT16
        {700, 23}, //FLOAT32_TO_INT32
        {700, 701}, //FLOAT32_TO_FLOAT64
        {701, 20}, //FLOAT64_TO_INT64
        {701, 21}, //FLOAT64_TO_INT16
        {701, 23}, //FLOAT64_TO_INT32
        {701, 700}, //FLOAT64_TO_FLOAT32
        {23, 20}, //INT32_TO_INT64
        {23, 21}, //INT32_TO_INT16
        {23, 700}, //INT32_TO_FLOAT32
        {23, 701}, //INT32_TO_FLOAT64
        {1700, 20}, //DECIMAL64_TO_INT64
        {1700, 21}, //DECIMAL64_TO_INT16
        {1700, 23}, //DECIMAL64_TO_INT32
        {1700, 700}, //DECIMAL64_TO_FLOAT32
        {1700, 701}, //DECIMAL64_TO_FLOAT64
    };
    try
    {
        PGConnectionPtr conn = std::make_shared<connection>("dbname=postgres user=kindred hostaddr=127.0.0.1 port=5432");
        Agg::init(conn, 16);
    }
    catch(const std::exception& e)
    {
        std::cerr << e.what() << '\n';
    }
    
    return 0;
};
