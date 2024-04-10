#include <Interpreters/orcaopt/provider/OperProvider.h>

#include <Interpreters/orcaopt/provider/TypeProvider.h>

#include <Interpreters/Context.h>

using namespace duckdb_libpgquery;

#define NEW_OPER(OPRVARNM, OID, OPRNAME, OPRNAMESPACE, OPROWNER, OPRKIND, OPRCANMERGE, OPRCANHASH, OPRLEFT, OPRRIGHT, OPRRESULT, OPRCOM, OPRNEGATE, OPRLSORTOP, OPRRSORTOP, OPRLTCMPOP, OPRGTCMPOP, OPRCODE, OPRREST, OPRJOIN) \
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
            /*oprlsortop*/ .oprlsortop = PGOid(OPRLSORTOP), \
            /*oprrsortop*/ .oprrsortop = PGOid(OPRRSORTOP), \
            /*oprltcmpop*/ .oprltcmpop = PGOid(OPRLTCMPOP), \
            /*oprgtcmpop*/ .oprgtcmpop = PGOid(OPRGTCMPOP), \
            /*oprcode*/ .oprcode = PGOid(OPRCODE), \
            /*oprrest*/ .oprrest = PGOid(OPRREST), \
            /*oprjoin*/ .oprjoin = PGOid(OPRJOIN)})};

namespace DB
{

NEW_OPER(INT2PL, 550, "+", 1, 1, 'b', false, false, 21, 21, 21, 550, 0, 0, 0, 0, 0, 176, 0, 0)
NEW_OPER(INT4PL, 551, "+", 1, 1, 'b', false, false, 23, 23, 23, 551, 0, 0, 0, 0, 0, 177, 0, 0)
NEW_OPER(INT24PL, 552, "+", 1, 1, 'b', false, false, 21, 23, 23, 553, 0, 0, 0, 0, 0, 178, 0, 0)
NEW_OPER(INT42PL, 553, "+", 1, 1, 'b', false, false, 23, 21, 23, 552, 0, 0, 0, 0, 0, 179, 0, 0)
NEW_OPER(INT2MI, 554, "-", 1, 1, 'b', false, false, 21, 21, 21, 0, 0, 0, 0, 0, 0, 180, 0, 0)

// std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_INT2PL = std::pair<PGOid, PGOperatorPtr>(
//     PGOid(550),
//     std::make_shared<Form_pg_operator>(Form_pg_operator{
//         .oid = PGOid(550),
//         /*oprname*/ .oprname = "+",
//         /*oprnamespace*/ .oprnamespace = PGOid(1),
//         /*oprowner*/ .oprowner = PGOid(1),
//         /*oprkind*/ .oprkind = 'b',
//         /*oprcanmerge*/ .oprcanmerge = false,
//         /*oprcanhash*/ .oprcanhash = false,
//         /*oprleft*/ .oprleft = PGOid(21),
//         /*oprright*/ .oprright = PGOid(21),
//         /*oprresult*/ .oprresult = PGOid(21),
//         /*oprcom*/ .oprcom = PGOid(550),
//         /*oprnegate*/ .oprnegate = PGOid(0),
//         /*oprlsortop*/ .oprlsortop = PGOid(0),
//         /*oprrsortop*/ .oprrsortop = PGOid(0),
//         /*oprltcmpop*/ .oprltcmpop = PGOid(0),
//         /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
//         /*oprcode*/ .oprcode = PGOid(176),
//         /*oprrest*/ .oprrest = PGOid(0),
//         /*oprjoin*/ .oprjoin = PGOid(0)}));

// std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_INT4PL = std::pair<PGOid, PGOperatorPtr>(
//     PGOid(551),
//     std::make_shared<Form_pg_operator>(Form_pg_operator{
//         .oid = PGOid(551),
//         /*oprname*/ .oprname = "+",
//         /*oprnamespace*/ .oprnamespace = PGOid(1),
//         /*oprowner*/ .oprowner = PGOid(1),
//         /*oprkind*/ .oprkind = 'b',
//         /*oprcanmerge*/ .oprcanmerge = false,
//         /*oprcanhash*/ .oprcanhash = false,
//         /*oprleft*/ .oprleft = PGOid(23),
//         /*oprright*/ .oprright = PGOid(23),
//         /*oprresult*/ .oprresult = PGOid(23),
//         /*oprcom*/ .oprcom = PGOid(551),
//         /*oprnegate*/ .oprnegate = PGOid(0),
//         /*oprlsortop*/ .oprlsortop = PGOid(0),
//         /*oprrsortop*/ .oprrsortop = PGOid(0),
//         /*oprltcmpop*/ .oprltcmpop = PGOid(0),
//         /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
//         /*oprcode*/ .oprcode = PGOid(177),
//         /*oprrest*/ .oprrest = PGOid(0),
//         /*oprjoin*/ .oprjoin = PGOid(0)}));

// std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_INT24PL = std::pair<PGOid, PGOperatorPtr>(
//     PGOid(552),
//     std::make_shared<Form_pg_operator>(Form_pg_operator{
//         .oid = PGOid(552),
//         /*oprname*/ .oprname = "+",
//         /*oprnamespace*/ .oprnamespace = PGOid(1),
//         /*oprowner*/ .oprowner = PGOid(1),
//         /*oprkind*/ .oprkind = 'b',
//         /*oprcanmerge*/ .oprcanmerge = false,
//         /*oprcanhash*/ .oprcanhash = false,
//         /*oprleft*/ .oprleft = PGOid(21),
//         /*oprright*/ .oprright = PGOid(23),
//         /*oprresult*/ .oprresult = PGOid(23),
//         /*oprcom*/ .oprcom = PGOid(553),
//         /*oprnegate*/ .oprnegate = PGOid(0),
//         /*oprlsortop*/ .oprlsortop = PGOid(0),
//         /*oprrsortop*/ .oprrsortop = PGOid(0),
//         /*oprltcmpop*/ .oprltcmpop = PGOid(0),
//         /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
//         /*oprcode*/ .oprcode = PGOid(178),
//         /*oprrest*/ .oprrest = PGOid(0),
//         /*oprjoin*/ .oprjoin = PGOid(0)}));

// std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_INT42PL = std::pair<PGOid, PGOperatorPtr>(
//     PGOid(553),
//     std::make_shared<Form_pg_operator>(Form_pg_operator{
//         .oid = PGOid(553),
//         /*oprname*/ .oprname = "+",
//         /*oprnamespace*/ .oprnamespace = PGOid(1),
//         /*oprowner*/ .oprowner = PGOid(1),
//         /*oprkind*/ .oprkind = 'b',
//         /*oprcanmerge*/ .oprcanmerge = false,
//         /*oprcanhash*/ .oprcanhash = false,
//         /*oprleft*/ .oprleft = PGOid(23),
//         /*oprright*/ .oprright = PGOid(21),
//         /*oprresult*/ .oprresult = PGOid(23),
//         /*oprcom*/ .oprcom = PGOid(552),
//         /*oprnegate*/ .oprnegate = PGOid(0),
//         /*oprlsortop*/ .oprlsortop = PGOid(0),
//         /*oprrsortop*/ .oprrsortop = PGOid(0),
//         /*oprltcmpop*/ .oprltcmpop = PGOid(0),
//         /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
//         /*oprcode*/ .oprcode = PGOid(179),
//         /*oprrest*/ .oprrest = PGOid(0),
//         /*oprjoin*/ .oprjoin = PGOid(0)}));

// std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_INT2MI = std::pair<PGOid, PGOperatorPtr>(
//     PGOid(554),
//     std::make_shared<Form_pg_operator>(Form_pg_operator{
//         .oid = PGOid(554),
//         /*oprname*/ .oprname = "-",
//         /*oprnamespace*/ .oprnamespace = PGOid(1),
//         /*oprowner*/ .oprowner = PGOid(1),
//         /*oprkind*/ .oprkind = 'b',
//         /*oprcanmerge*/ .oprcanmerge = false,
//         /*oprcanhash*/ .oprcanhash = false,
//         /*oprleft*/ .oprleft = PGOid(21),
//         /*oprright*/ .oprright = PGOid(21),
//         /*oprresult*/ .oprresult = PGOid(21),
//         /*oprcom*/ .oprcom = PGOid(0),
//         /*oprnegate*/ .oprnegate = PGOid(0),
//         /*oprlsortop*/ .oprlsortop = PGOid(0),
//         /*oprrsortop*/ .oprrsortop = PGOid(0),
//         /*oprltcmpop*/ .oprltcmpop = PGOid(0),
//         /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
//         /*oprcode*/ .oprcode = PGOid(180),
//         /*oprrest*/ .oprrest = PGOid(0),
//         /*oprjoin*/ .oprjoin = PGOid(0)}));

std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_INT4MI = std::pair<PGOid, PGOperatorPtr>(
    PGOid(555),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = PGOid(555),
        /*oprname*/ .oprname = "-",
        /*oprnamespace*/ .oprnamespace = PGOid(1),
        /*oprowner*/ .oprowner = PGOid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = PGOid(23),
        /*oprright*/ .oprright = PGOid(23),
        /*oprresult*/ .oprresult = PGOid(23),
        /*oprcom*/ .oprcom = PGOid(0),
        /*oprnegate*/ .oprnegate = PGOid(0),
        /*oprlsortop*/ .oprlsortop = PGOid(0),
        /*oprrsortop*/ .oprrsortop = PGOid(0),
        /*oprltcmpop*/ .oprltcmpop = PGOid(0),
        /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
        /*oprcode*/ .oprcode = PGOid(181),
        /*oprrest*/ .oprrest = PGOid(0),
        /*oprjoin*/ .oprjoin = PGOid(0)}));

std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_INT24MI = std::pair<PGOid, PGOperatorPtr>(
    PGOid(556),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = PGOid(556),
        /*oprname*/ .oprname = "-",
        /*oprnamespace*/ .oprnamespace = PGOid(1),
        /*oprowner*/ .oprowner = PGOid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = PGOid(21),
        /*oprright*/ .oprright = PGOid(23),
        /*oprresult*/ .oprresult = PGOid(23),
        /*oprcom*/ .oprcom = PGOid(0),
        /*oprnegate*/ .oprnegate = PGOid(0),
        /*oprlsortop*/ .oprlsortop = PGOid(0),
        /*oprrsortop*/ .oprrsortop = PGOid(0),
        /*oprltcmpop*/ .oprltcmpop = PGOid(0),
        /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
        /*oprcode*/ .oprcode = PGOid(182),
        /*oprrest*/ .oprrest = PGOid(0),
        /*oprjoin*/ .oprjoin = PGOid(0)}));

std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_INT42MI = std::pair<PGOid, PGOperatorPtr>(
    PGOid(557),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = PGOid(557),
        /*oprname*/ .oprname = "-",
        /*oprnamespace*/ .oprnamespace = PGOid(1),
        /*oprowner*/ .oprowner = PGOid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = PGOid(23),
        /*oprright*/ .oprright = PGOid(21),
        /*oprresult*/ .oprresult = PGOid(23),
        /*oprcom*/ .oprcom = PGOid(0),
        /*oprnegate*/ .oprnegate = PGOid(0),
        /*oprlsortop*/ .oprlsortop = PGOid(0),
        /*oprrsortop*/ .oprrsortop = PGOid(0),
        /*oprltcmpop*/ .oprltcmpop = PGOid(0),
        /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
        /*oprcode*/ .oprcode = PGOid(183),
        /*oprrest*/ .oprrest = PGOid(0),
        /*oprjoin*/ .oprjoin = PGOid(0)}));

std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_INT2MUL = std::pair<PGOid, PGOperatorPtr>(
    PGOid(526),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = PGOid(526),
        /*oprname*/ .oprname = "*",
        /*oprnamespace*/ .oprnamespace = PGOid(1),
        /*oprowner*/ .oprowner = PGOid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = PGOid(21),
        /*oprright*/ .oprright = PGOid(21),
        /*oprresult*/ .oprresult = PGOid(21),
        /*oprcom*/ .oprcom = PGOid(526),
        /*oprnegate*/ .oprnegate = PGOid(0),
        /*oprlsortop*/ .oprlsortop = PGOid(0),
        /*oprrsortop*/ .oprrsortop = PGOid(0),
        /*oprltcmpop*/ .oprltcmpop = PGOid(0),
        /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
        /*oprcode*/ .oprcode = PGOid(152),
        /*oprrest*/ .oprrest = PGOid(0),
        /*oprjoin*/ .oprjoin = PGOid(0)}));

std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_INT4MUL = std::pair<PGOid, PGOperatorPtr>(
    PGOid(514),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = PGOid(514),
        /*oprname*/ .oprname = "*",
        /*oprnamespace*/ .oprnamespace = PGOid(1),
        /*oprowner*/ .oprowner = PGOid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = PGOid(23),
        /*oprright*/ .oprright = PGOid(23),
        /*oprresult*/ .oprresult = PGOid(23),
        /*oprcom*/ .oprcom = PGOid(514),
        /*oprnegate*/ .oprnegate = PGOid(0),
        /*oprlsortop*/ .oprlsortop = PGOid(0),
        /*oprrsortop*/ .oprrsortop = PGOid(0),
        /*oprltcmpop*/ .oprltcmpop = PGOid(0),
        /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
        /*oprcode*/ .oprcode = PGOid(141),
        /*oprrest*/ .oprrest = PGOid(0),
        /*oprjoin*/ .oprjoin = PGOid(0)}));

std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_INT24MUL = std::pair<PGOid, PGOperatorPtr>(
    PGOid(544),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = PGOid(544),
        /*oprname*/ .oprname = "*",
        /*oprnamespace*/ .oprnamespace = PGOid(1),
        /*oprowner*/ .oprowner = PGOid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = PGOid(21),
        /*oprright*/ .oprright = PGOid(23),
        /*oprresult*/ .oprresult = PGOid(23),
        /*oprcom*/ .oprcom = PGOid(545),
        /*oprnegate*/ .oprnegate = PGOid(0),
        /*oprlsortop*/ .oprlsortop = PGOid(0),
        /*oprrsortop*/ .oprrsortop = PGOid(0),
        /*oprltcmpop*/ .oprltcmpop = PGOid(0),
        /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
        /*oprcode*/ .oprcode = PGOid(170),
        /*oprrest*/ .oprrest = PGOid(0),
        /*oprjoin*/ .oprjoin = PGOid(0)}));

std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_INT42MUL = std::pair<PGOid, PGOperatorPtr>(
    PGOid(545),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = PGOid(545),
        /*oprname*/ .oprname = "*",
        /*oprnamespace*/ .oprnamespace = PGOid(1),
        /*oprowner*/ .oprowner = PGOid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = PGOid(23),
        /*oprright*/ .oprright = PGOid(21),
        /*oprresult*/ .oprresult = PGOid(23),
        /*oprcom*/ .oprcom = PGOid(544),
        /*oprnegate*/ .oprnegate = PGOid(0),
        /*oprlsortop*/ .oprlsortop = PGOid(0),
        /*oprrsortop*/ .oprrsortop = PGOid(0),
        /*oprltcmpop*/ .oprltcmpop = PGOid(0),
        /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
        /*oprcode*/ .oprcode = PGOid(171),
        /*oprrest*/ .oprrest = PGOid(0),
        /*oprjoin*/ .oprjoin = PGOid(0)}));

std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_INT2DIV = std::pair<PGOid, PGOperatorPtr>(
    PGOid(527),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = PGOid(527),
        /*oprname*/ .oprname = "/",
        /*oprnamespace*/ .oprnamespace = PGOid(1),
        /*oprowner*/ .oprowner = PGOid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = PGOid(21),
        /*oprright*/ .oprright = PGOid(21),
        /*oprresult*/ .oprresult = PGOid(21),
        /*oprcom*/ .oprcom = PGOid(0),
        /*oprnegate*/ .oprnegate = PGOid(0),
        /*oprlsortop*/ .oprlsortop = PGOid(0),
        /*oprrsortop*/ .oprrsortop = PGOid(0),
        /*oprltcmpop*/ .oprltcmpop = PGOid(0),
        /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
        /*oprcode*/ .oprcode = PGOid(153),
        /*oprrest*/ .oprrest = PGOid(0),
        /*oprjoin*/ .oprjoin = PGOid(0)}));

std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_INT4DIV = std::pair<PGOid, PGOperatorPtr>(
    PGOid(528),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = PGOid(528),
        /*oprname*/ .oprname = "/",
        /*oprnamespace*/ .oprnamespace = PGOid(1),
        /*oprowner*/ .oprowner = PGOid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = PGOid(23),
        /*oprright*/ .oprright = PGOid(23),
        /*oprresult*/ .oprresult = PGOid(23),
        /*oprcom*/ .oprcom = PGOid(0),
        /*oprnegate*/ .oprnegate = PGOid(0),
        /*oprlsortop*/ .oprlsortop = PGOid(0),
        /*oprrsortop*/ .oprrsortop = PGOid(0),
        /*oprltcmpop*/ .oprltcmpop = PGOid(0),
        /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
        /*oprcode*/ .oprcode = PGOid(154),
        /*oprrest*/ .oprrest = PGOid(0),
        /*oprjoin*/ .oprjoin = PGOid(0)}));

std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_INT24DIV = std::pair<PGOid, PGOperatorPtr>(
    PGOid(546),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = PGOid(546),
        /*oprname*/ .oprname = "/",
        /*oprnamespace*/ .oprnamespace = PGOid(1),
        /*oprowner*/ .oprowner = PGOid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = PGOid(21),
        /*oprright*/ .oprright = PGOid(23),
        /*oprresult*/ .oprresult = PGOid(23),
        /*oprcom*/ .oprcom = PGOid(0),
        /*oprnegate*/ .oprnegate = PGOid(0),
        /*oprlsortop*/ .oprlsortop = PGOid(0),
        /*oprrsortop*/ .oprrsortop = PGOid(0),
        /*oprltcmpop*/ .oprltcmpop = PGOid(0),
        /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
        /*oprcode*/ .oprcode = PGOid(172),
        /*oprrest*/ .oprrest = PGOid(0),
        /*oprjoin*/ .oprjoin = PGOid(0)}));

std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_INT42DIV = std::pair<PGOid, PGOperatorPtr>(
    PGOid(547),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = PGOid(547),
        /*oprname*/ .oprname = "/",
        /*oprnamespace*/ .oprnamespace = PGOid(1),
        /*oprowner*/ .oprowner = PGOid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = PGOid(23),
        /*oprright*/ .oprright = PGOid(21),
        /*oprresult*/ .oprresult = PGOid(23),
        /*oprcom*/ .oprcom = PGOid(0),
        /*oprnegate*/ .oprnegate = PGOid(0),
        /*oprlsortop*/ .oprlsortop = PGOid(0),
        /*oprrsortop*/ .oprrsortop = PGOid(0),
        /*oprltcmpop*/ .oprltcmpop = PGOid(0),
        /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
        /*oprcode*/ .oprcode = PGOid(173),
        /*oprrest*/ .oprrest = PGOid(0),
        /*oprjoin*/ .oprjoin = PGOid(0)}));

std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_FLOAT32EQ = std::pair<PGOid, PGOperatorPtr>(
    PGOid(620),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = PGOid(620),
        /*oprname*/ .oprname = "=",
        /*oprnamespace*/ .oprnamespace = PGOid(1),
        /*oprowner*/ .oprowner = PGOid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = true,
        /*oprcanhash*/ .oprcanhash = true,
        /*oprleft*/ .oprleft = PGOid(700),
        /*oprright*/ .oprright = PGOid(700),
        /*oprresult*/ .oprresult = PGOid(16),
        /*oprcom*/ .oprcom = PGOid(620),
        /*oprnegate*/ .oprnegate = PGOid(621),
        /*oprlsortop*/ .oprlsortop = PGOid(0),
        /*oprrsortop*/ .oprrsortop = PGOid(0),
        /*oprltcmpop*/ .oprltcmpop = PGOid(0),
        /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
        /*oprcode*/ .oprcode = PGOid(287),
        /*oprrest*/ .oprrest = PGOid(0),
        /*oprjoin*/ .oprjoin = PGOid(0)}));

std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_FLOAT32NE = std::pair<PGOid, PGOperatorPtr>(
    PGOid(621),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = PGOid(621),
        /*oprname*/ .oprname = "<>",
        /*oprnamespace*/ .oprnamespace = PGOid(1),
        /*oprowner*/ .oprowner = PGOid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = PGOid(700),
        /*oprright*/ .oprright = PGOid(700),
        /*oprresult*/ .oprresult = PGOid(16),
        /*oprcom*/ .oprcom = PGOid(621),
        /*oprnegate*/ .oprnegate = PGOid(620),
        /*oprlsortop*/ .oprlsortop = PGOid(0),
        /*oprrsortop*/ .oprrsortop = PGOid(0),
        /*oprltcmpop*/ .oprltcmpop = PGOid(0),
        /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
        /*oprcode*/ .oprcode = PGOid(288),
        /*oprrest*/ .oprrest = PGOid(0),
        /*oprjoin*/ .oprjoin = PGOid(0)}));

std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_FLOAT32LT = std::pair<PGOid, PGOperatorPtr>(
    PGOid(622),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = PGOid(622),
        /*oprname*/ .oprname = "<",
        /*oprnamespace*/ .oprnamespace = PGOid(1),
        /*oprowner*/ .oprowner = PGOid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = PGOid(700),
        /*oprright*/ .oprright = PGOid(700),
        /*oprresult*/ .oprresult = PGOid(16),
        /*oprcom*/ .oprcom = PGOid(623),
        /*oprnegate*/ .oprnegate = PGOid(625),
        /*oprlsortop*/ .oprlsortop = PGOid(0),
        /*oprrsortop*/ .oprrsortop = PGOid(0),
        /*oprltcmpop*/ .oprltcmpop = PGOid(0),
        /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
        /*oprcode*/ .oprcode = PGOid(289),
        /*oprrest*/ .oprrest = PGOid(0),
        /*oprjoin*/ .oprjoin = PGOid(0)}));

std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_FLOAT32GT = std::pair<PGOid, PGOperatorPtr>(
    PGOid(623),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = PGOid(623),
        /*oprname*/ .oprname = ">",
        /*oprnamespace*/ .oprnamespace = PGOid(1),
        /*oprowner*/ .oprowner = PGOid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = PGOid(700),
        /*oprright*/ .oprright = PGOid(700),
        /*oprresult*/ .oprresult = PGOid(16),
        /*oprcom*/ .oprcom = PGOid(622),
        /*oprnegate*/ .oprnegate = PGOid(624),
        /*oprlsortop*/ .oprlsortop = PGOid(0),
        /*oprrsortop*/ .oprrsortop = PGOid(0),
        /*oprltcmpop*/ .oprltcmpop = PGOid(0),
        /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
        /*oprcode*/ .oprcode = PGOid(291),
        /*oprrest*/ .oprrest = PGOid(0),
        /*oprjoin*/ .oprjoin = PGOid(0)}));

std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_FLOAT32LE = std::pair<PGOid, PGOperatorPtr>(
    PGOid(624),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = PGOid(624),
        /*oprname*/ .oprname = "<=",
        /*oprnamespace*/ .oprnamespace = PGOid(1),
        /*oprowner*/ .oprowner = PGOid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = PGOid(700),
        /*oprright*/ .oprright = PGOid(700),
        /*oprresult*/ .oprresult = PGOid(16),
        /*oprcom*/ .oprcom = PGOid(625),
        /*oprnegate*/ .oprnegate = PGOid(623),
        /*oprlsortop*/ .oprlsortop = PGOid(0),
        /*oprrsortop*/ .oprrsortop = PGOid(0),
        /*oprltcmpop*/ .oprltcmpop = PGOid(0),
        /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
        /*oprcode*/ .oprcode = PGOid(290),
        /*oprrest*/ .oprrest = PGOid(0),
        /*oprjoin*/ .oprjoin = PGOid(0)}));

std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_FLOAT32GE = std::pair<PGOid, PGOperatorPtr>(
    PGOid(625),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = PGOid(625),
        /*oprname*/ .oprname = ">=",
        /*oprnamespace*/ .oprnamespace = PGOid(1),
        /*oprowner*/ .oprowner = PGOid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = PGOid(700),
        /*oprright*/ .oprright = PGOid(700),
        /*oprresult*/ .oprresult = PGOid(16),
        /*oprcom*/ .oprcom = PGOid(624),
        /*oprnegate*/ .oprnegate = PGOid(622),
        /*oprlsortop*/ .oprlsortop = PGOid(0),
        /*oprrsortop*/ .oprrsortop = PGOid(0),
        /*oprltcmpop*/ .oprltcmpop = PGOid(0),
        /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
        /*oprcode*/ .oprcode = PGOid(292),
        /*oprrest*/ .oprrest = PGOid(0),
        /*oprjoin*/ .oprjoin = PGOid(0)}));

std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_FLOAT64EQ = std::pair<PGOid, PGOperatorPtr>(
    PGOid(670),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = PGOid(670),
        /*oprname*/ .oprname = "=",
        /*oprnamespace*/ .oprnamespace = PGOid(1),
        /*oprowner*/ .oprowner = PGOid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = true,
        /*oprcanhash*/ .oprcanhash = true,
        /*oprleft*/ .oprleft = PGOid(701),
        /*oprright*/ .oprright = PGOid(701),
        /*oprresult*/ .oprresult = PGOid(16),
        /*oprcom*/ .oprcom = PGOid(670),
        /*oprnegate*/ .oprnegate = PGOid(671),
        /*oprlsortop*/ .oprlsortop = PGOid(0),
        /*oprrsortop*/ .oprrsortop = PGOid(0),
        /*oprltcmpop*/ .oprltcmpop = PGOid(0),
        /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
        /*oprcode*/ .oprcode = PGOid(293),
        /*oprrest*/ .oprrest = PGOid(0),
        /*oprjoin*/ .oprjoin = PGOid(0)}));

std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_FLOAT64NE = std::pair<PGOid, PGOperatorPtr>(
    PGOid(671),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = PGOid(671),
        /*oprname*/ .oprname = "<>",
        /*oprnamespace*/ .oprnamespace = PGOid(1),
        /*oprowner*/ .oprowner = PGOid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = PGOid(701),
        /*oprright*/ .oprright = PGOid(701),
        /*oprresult*/ .oprresult = PGOid(16),
        /*oprcom*/ .oprcom = PGOid(671),
        /*oprnegate*/ .oprnegate = PGOid(670),
        /*oprlsortop*/ .oprlsortop = PGOid(0),
        /*oprrsortop*/ .oprrsortop = PGOid(0),
        /*oprltcmpop*/ .oprltcmpop = PGOid(0),
        /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
        /*oprcode*/ .oprcode = PGOid(294),
        /*oprrest*/ .oprrest = PGOid(0),
        /*oprjoin*/ .oprjoin = PGOid(0)}));

std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_FLOAT64LT = std::pair<PGOid, PGOperatorPtr>(
    PGOid(672),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = PGOid(672),
        /*oprname*/ .oprname = "<",
        /*oprnamespace*/ .oprnamespace = PGOid(1),
        /*oprowner*/ .oprowner = PGOid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = PGOid(701),
        /*oprright*/ .oprright = PGOid(701),
        /*oprresult*/ .oprresult = PGOid(16),
        /*oprcom*/ .oprcom = PGOid(674),
        /*oprnegate*/ .oprnegate = PGOid(675),
        /*oprlsortop*/ .oprlsortop = PGOid(0),
        /*oprrsortop*/ .oprrsortop = PGOid(0),
        /*oprltcmpop*/ .oprltcmpop = PGOid(0),
        /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
        /*oprcode*/ .oprcode = PGOid(295),
        /*oprrest*/ .oprrest = PGOid(0),
        /*oprjoin*/ .oprjoin = PGOid(0)}));

std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_FLOAT64LE = std::pair<PGOid, PGOperatorPtr>(
    PGOid(673),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = PGOid(673),
        /*oprname*/ .oprname = "<=",
        /*oprnamespace*/ .oprnamespace = PGOid(1),
        /*oprowner*/ .oprowner = PGOid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = PGOid(701),
        /*oprright*/ .oprright = PGOid(701),
        /*oprresult*/ .oprresult = PGOid(16),
        /*oprcom*/ .oprcom = PGOid(675),
        /*oprnegate*/ .oprnegate = PGOid(674),
        /*oprlsortop*/ .oprlsortop = PGOid(0),
        /*oprrsortop*/ .oprrsortop = PGOid(0),
        /*oprltcmpop*/ .oprltcmpop = PGOid(0),
        /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
        /*oprcode*/ .oprcode = PGOid(296),
        /*oprrest*/ .oprrest = PGOid(0),
        /*oprjoin*/ .oprjoin = PGOid(0)}));

std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_FLOAT64GT = std::pair<PGOid, PGOperatorPtr>(
    PGOid(674),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = PGOid(674),
        /*oprname*/ .oprname = ">",
        /*oprnamespace*/ .oprnamespace = PGOid(1),
        /*oprowner*/ .oprowner = PGOid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = PGOid(701),
        /*oprright*/ .oprright = PGOid(701),
        /*oprresult*/ .oprresult = PGOid(16),
        /*oprcom*/ .oprcom = PGOid(672),
        /*oprnegate*/ .oprnegate = PGOid(673),
        /*oprlsortop*/ .oprlsortop = PGOid(0),
        /*oprrsortop*/ .oprrsortop = PGOid(0),
        /*oprltcmpop*/ .oprltcmpop = PGOid(0),
        /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
        /*oprcode*/ .oprcode = PGOid(297),
        /*oprrest*/ .oprrest = PGOid(0),
        /*oprjoin*/ .oprjoin = PGOid(0)}));

std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_FLOAT64GE = std::pair<PGOid, PGOperatorPtr>(
    PGOid(675),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = PGOid(675),
        /*oprname*/ .oprname = ">=",
        /*oprnamespace*/ .oprnamespace = PGOid(1),
        /*oprowner*/ .oprowner = PGOid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = PGOid(701),
        /*oprright*/ .oprright = PGOid(701),
        /*oprresult*/ .oprresult = PGOid(16),
        /*oprcom*/ .oprcom = PGOid(673),
        /*oprnegate*/ .oprnegate = PGOid(672),
        /*oprlsortop*/ .oprlsortop = PGOid(0),
        /*oprrsortop*/ .oprrsortop = PGOid(0),
        /*oprltcmpop*/ .oprltcmpop = PGOid(0),
        /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
        /*oprcode*/ .oprcode = PGOid(298),
        /*oprrest*/ .oprrest = PGOid(0),
        /*oprjoin*/ .oprjoin = PGOid(0)}));

std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_BOOLEQ = std::pair<PGOid, PGOperatorPtr>(
    PGOid(91),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = PGOid(91),
        /*oprname*/ .oprname = "=",
        /*oprnamespace*/ .oprnamespace = PGOid(1),
        /*oprowner*/ .oprowner = PGOid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = true,
        /*oprcanhash*/ .oprcanhash = true,
        /*oprleft*/ .oprleft = PGOid(16),
        /*oprright*/ .oprright = PGOid(16),
        /*oprresult*/ .oprresult = PGOid(16),
        /*oprcom*/ .oprcom = PGOid(91),
        /*oprnegate*/ .oprnegate = PGOid(85),
        /*oprlsortop*/ .oprlsortop = PGOid(0),
        /*oprrsortop*/ .oprrsortop = PGOid(0),
        /*oprltcmpop*/ .oprltcmpop = PGOid(0),
        /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
        /*oprcode*/ .oprcode = PGOid(60),
        /*oprrest*/ .oprrest = PGOid(0),
        /*oprjoin*/ .oprjoin = PGOid(0)}));

std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_BOOLNE = std::pair<PGOid, PGOperatorPtr>(
    PGOid(85),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = PGOid(85),
        /*oprname*/ .oprname = "<>",
        /*oprnamespace*/ .oprnamespace = PGOid(1),
        /*oprowner*/ .oprowner = PGOid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = PGOid(16),
        /*oprright*/ .oprright = PGOid(16),
        /*oprresult*/ .oprresult = PGOid(16),
        /*oprcom*/ .oprcom = PGOid(85),
        /*oprnegate*/ .oprnegate = PGOid(91),
        /*oprlsortop*/ .oprlsortop = PGOid(0),
        /*oprrsortop*/ .oprrsortop = PGOid(0),
        /*oprltcmpop*/ .oprltcmpop = PGOid(0),
        /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
        /*oprcode*/ .oprcode = PGOid(84),
        /*oprrest*/ .oprrest = PGOid(0),
        /*oprjoin*/ .oprjoin = PGOid(0)}));

std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_BOOLLT = std::pair<PGOid, PGOperatorPtr>(
    PGOid(58),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = PGOid(58),
        /*oprname*/ .oprname = "<",
        /*oprnamespace*/ .oprnamespace = PGOid(1),
        /*oprowner*/ .oprowner = PGOid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = PGOid(16),
        /*oprright*/ .oprright = PGOid(16),
        /*oprresult*/ .oprresult = PGOid(16),
        /*oprcom*/ .oprcom = PGOid(59),
        /*oprnegate*/ .oprnegate = PGOid(1695),
        /*oprlsortop*/ .oprlsortop = PGOid(0),
        /*oprrsortop*/ .oprrsortop = PGOid(0),
        /*oprltcmpop*/ .oprltcmpop = PGOid(0),
        /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
        /*oprcode*/ .oprcode = PGOid(56),
        /*oprrest*/ .oprrest = PGOid(0),
        /*oprjoin*/ .oprjoin = PGOid(0)}));

std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_BOOLGT = std::pair<PGOid, PGOperatorPtr>(
    PGOid(59),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = PGOid(59),
        /*oprname*/ .oprname = ">",
        /*oprnamespace*/ .oprnamespace = PGOid(1),
        /*oprowner*/ .oprowner = PGOid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = PGOid(16),
        /*oprright*/ .oprright = PGOid(16),
        /*oprresult*/ .oprresult = PGOid(16),
        /*oprcom*/ .oprcom = PGOid(58),
        /*oprnegate*/ .oprnegate = PGOid(1694),
        /*oprlsortop*/ .oprlsortop = PGOid(0),
        /*oprrsortop*/ .oprrsortop = PGOid(0),
        /*oprltcmpop*/ .oprltcmpop = PGOid(0),
        /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
        /*oprcode*/ .oprcode = PGOid(57),
        /*oprrest*/ .oprrest = PGOid(0),
        /*oprjoin*/ .oprjoin = PGOid(0)}));

std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_BOOLLE = std::pair<PGOid, PGOperatorPtr>(
    PGOid(1694),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = PGOid(1694),
        /*oprname*/ .oprname = "<=",
        /*oprnamespace*/ .oprnamespace = PGOid(1),
        /*oprowner*/ .oprowner = PGOid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = PGOid(16),
        /*oprright*/ .oprright = PGOid(16),
        /*oprresult*/ .oprresult = PGOid(16),
        /*oprcom*/ .oprcom = PGOid(1695),
        /*oprnegate*/ .oprnegate = PGOid(59),
        /*oprlsortop*/ .oprlsortop = PGOid(0),
        /*oprrsortop*/ .oprrsortop = PGOid(0),
        /*oprltcmpop*/ .oprltcmpop = PGOid(0),
        /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
        /*oprcode*/ .oprcode = PGOid(0),
        /*oprrest*/ .oprrest = PGOid(0),
        /*oprjoin*/ .oprjoin = PGOid(0)}));

std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_BOOLGE = std::pair<PGOid, PGOperatorPtr>(
    PGOid(1695),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = PGOid(1695),
        /*oprname*/ .oprname = ">=",
        /*oprnamespace*/ .oprnamespace = PGOid(1),
        /*oprowner*/ .oprowner = PGOid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = PGOid(16),
        /*oprright*/ .oprright = PGOid(16),
        /*oprresult*/ .oprresult = PGOid(16),
        /*oprcom*/ .oprcom = PGOid(1694),
        /*oprnegate*/ .oprnegate = PGOid(58),
        /*oprlsortop*/ .oprlsortop = PGOid(0),
        /*oprrsortop*/ .oprrsortop = PGOid(0),
        /*oprltcmpop*/ .oprltcmpop = PGOid(0),
        /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
        /*oprcode*/ .oprcode = PGOid(0),
        /*oprrest*/ .oprrest = PGOid(0),
        /*oprjoin*/ .oprjoin = PGOid(0)}));

std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_INT16EQ = std::pair<PGOid, PGOperatorPtr>(
    PGOid(94),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = PGOid(94),
        /*oprname*/ .oprname = "=",
        /*oprnamespace*/ .oprnamespace = PGOid(1),
        /*oprowner*/ .oprowner = PGOid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = true,
        /*oprcanhash*/ .oprcanhash = true,
        /*oprleft*/ .oprleft = PGOid(21),
        /*oprright*/ .oprright = PGOid(21),
        /*oprresult*/ .oprresult = PGOid(16),
        /*oprcom*/ .oprcom = PGOid(94),
        /*oprnegate*/ .oprnegate = PGOid(519),
        /*oprlsortop*/ .oprlsortop = PGOid(0),
        /*oprrsortop*/ .oprrsortop = PGOid(0),
        /*oprltcmpop*/ .oprltcmpop = PGOid(0),
        /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
        /*oprcode*/ .oprcode = PGOid(0),
        /*oprrest*/ .oprrest = PGOid(0),
        /*oprjoin*/ .oprjoin = PGOid(0)}));

std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_INT16LT = std::pair<PGOid, PGOperatorPtr>(
    PGOid(95),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = PGOid(95),
        /*oprname*/ .oprname = "<",
        /*oprnamespace*/ .oprnamespace = PGOid(1),
        /*oprowner*/ .oprowner = PGOid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = PGOid(21),
        /*oprright*/ .oprright = PGOid(21),
        /*oprresult*/ .oprresult = PGOid(16),
        /*oprcom*/ .oprcom = PGOid(520),
        /*oprnegate*/ .oprnegate = PGOid(524),
        /*oprlsortop*/ .oprlsortop = PGOid(0),
        /*oprrsortop*/ .oprrsortop = PGOid(0),
        /*oprltcmpop*/ .oprltcmpop = PGOid(0),
        /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
        /*oprcode*/ .oprcode = PGOid(0),
        /*oprrest*/ .oprrest = PGOid(0),
        /*oprjoin*/ .oprjoin = PGOid(0)}));

std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_INT16NE = std::pair<PGOid, PGOperatorPtr>(
    PGOid(519),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = PGOid(519),
        /*oprname*/ .oprname = "<>",
        /*oprnamespace*/ .oprnamespace = PGOid(1),
        /*oprowner*/ .oprowner = PGOid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = PGOid(21),
        /*oprright*/ .oprright = PGOid(21),
        /*oprresult*/ .oprresult = PGOid(16),
        /*oprcom*/ .oprcom = PGOid(519),
        /*oprnegate*/ .oprnegate = PGOid(94),
        /*oprlsortop*/ .oprlsortop = PGOid(0),
        /*oprrsortop*/ .oprrsortop = PGOid(0),
        /*oprltcmpop*/ .oprltcmpop = PGOid(0),
        /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
        /*oprcode*/ .oprcode = PGOid(0),
        /*oprrest*/ .oprrest = PGOid(0),
        /*oprjoin*/ .oprjoin = PGOid(0)}));

std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_INT16GT = std::pair<PGOid, PGOperatorPtr>(
    PGOid(520),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = PGOid(520),
        /*oprname*/ .oprname = ">",
        /*oprnamespace*/ .oprnamespace = PGOid(1),
        /*oprowner*/ .oprowner = PGOid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = PGOid(21),
        /*oprright*/ .oprright = PGOid(21),
        /*oprresult*/ .oprresult = PGOid(16),
        /*oprcom*/ .oprcom = PGOid(95),
        /*oprnegate*/ .oprnegate = PGOid(522),
        /*oprlsortop*/ .oprlsortop = PGOid(0),
        /*oprrsortop*/ .oprrsortop = PGOid(0),
        /*oprltcmpop*/ .oprltcmpop = PGOid(0),
        /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
        /*oprcode*/ .oprcode = PGOid(0),
        /*oprrest*/ .oprrest = PGOid(0),
        /*oprjoin*/ .oprjoin = PGOid(0)}));

std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_INT16LE = std::pair<PGOid, PGOperatorPtr>(
    PGOid(522),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = PGOid(522),
        /*oprname*/ .oprname = "<=",
        /*oprnamespace*/ .oprnamespace = PGOid(1),
        /*oprowner*/ .oprowner = PGOid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = PGOid(21),
        /*oprright*/ .oprright = PGOid(21),
        /*oprresult*/ .oprresult = PGOid(16),
        /*oprcom*/ .oprcom = PGOid(524),
        /*oprnegate*/ .oprnegate = PGOid(520),
        /*oprlsortop*/ .oprlsortop = PGOid(0),
        /*oprrsortop*/ .oprrsortop = PGOid(0),
        /*oprltcmpop*/ .oprltcmpop = PGOid(0),
        /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
        /*oprcode*/ .oprcode = PGOid(0),
        /*oprrest*/ .oprrest = PGOid(0),
        /*oprjoin*/ .oprjoin = PGOid(0)}));

std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_INT16GE = std::pair<PGOid, PGOperatorPtr>(
    PGOid(524),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = PGOid(524),
        /*oprname*/ .oprname = ">=",
        /*oprnamespace*/ .oprnamespace = PGOid(1),
        /*oprowner*/ .oprowner = PGOid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = PGOid(21),
        /*oprright*/ .oprright = PGOid(21),
        /*oprresult*/ .oprresult = PGOid(16),
        /*oprcom*/ .oprcom = PGOid(522),
        /*oprnegate*/ .oprnegate = PGOid(95),
        /*oprlsortop*/ .oprlsortop = PGOid(0),
        /*oprrsortop*/ .oprrsortop = PGOid(0),
        /*oprltcmpop*/ .oprltcmpop = PGOid(0),
        /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
        /*oprcode*/ .oprcode = PGOid(0),
        /*oprrest*/ .oprrest = PGOid(0),
        /*oprjoin*/ .oprjoin = PGOid(0)}));

std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_INT32EQ = std::pair<PGOid, PGOperatorPtr>(
    PGOid(96),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = PGOid(96),
        /*oprname*/ .oprname = "=",
        /*oprnamespace*/ .oprnamespace = PGOid(1),
        /*oprowner*/ .oprowner = PGOid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = true,
        /*oprcanhash*/ .oprcanhash = true,
        /*oprleft*/ .oprleft = PGOid(23),
        /*oprright*/ .oprright = PGOid(23),
        /*oprresult*/ .oprresult = PGOid(16),
        /*oprcom*/ .oprcom = PGOid(96),
        /*oprnegate*/ .oprnegate = PGOid(518),
        /*oprlsortop*/ .oprlsortop = PGOid(0),
        /*oprrsortop*/ .oprrsortop = PGOid(0),
        /*oprltcmpop*/ .oprltcmpop = PGOid(0),
        /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
        /*oprcode*/ .oprcode = PGOid(0),
        /*oprrest*/ .oprrest = PGOid(0),
        /*oprjoin*/ .oprjoin = PGOid(0)}));

std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_INT32LT = std::pair<PGOid, PGOperatorPtr>(
    PGOid(97),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = PGOid(97),
        /*oprname*/ .oprname = "<",
        /*oprnamespace*/ .oprnamespace = PGOid(1),
        /*oprowner*/ .oprowner = PGOid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = PGOid(23),
        /*oprright*/ .oprright = PGOid(23),
        /*oprresult*/ .oprresult = PGOid(16),
        /*oprcom*/ .oprcom = PGOid(521),
        /*oprnegate*/ .oprnegate = PGOid(525),
        /*oprlsortop*/ .oprlsortop = PGOid(0),
        /*oprrsortop*/ .oprrsortop = PGOid(0),
        /*oprltcmpop*/ .oprltcmpop = PGOid(0),
        /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
        /*oprcode*/ .oprcode = PGOid(0),
        /*oprrest*/ .oprrest = PGOid(0),
        /*oprjoin*/ .oprjoin = PGOid(0)}));

std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_INT32NE = std::pair<PGOid, PGOperatorPtr>(
    PGOid(518),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = PGOid(518),
        /*oprname*/ .oprname = "<>",
        /*oprnamespace*/ .oprnamespace = PGOid(1),
        /*oprowner*/ .oprowner = PGOid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = PGOid(23),
        /*oprright*/ .oprright = PGOid(23),
        /*oprresult*/ .oprresult = PGOid(16),
        /*oprcom*/ .oprcom = PGOid(518),
        /*oprnegate*/ .oprnegate = PGOid(96),
        /*oprlsortop*/ .oprlsortop = PGOid(0),
        /*oprrsortop*/ .oprrsortop = PGOid(0),
        /*oprltcmpop*/ .oprltcmpop = PGOid(0),
        /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
        /*oprcode*/ .oprcode = PGOid(0),
        /*oprrest*/ .oprrest = PGOid(0),
        /*oprjoin*/ .oprjoin = PGOid(0)}));

std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_INT32GT = std::pair<PGOid, PGOperatorPtr>(
    PGOid(521),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = PGOid(521),
        /*oprname*/ .oprname = ">",
        /*oprnamespace*/ .oprnamespace = PGOid(1),
        /*oprowner*/ .oprowner = PGOid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = PGOid(23),
        /*oprright*/ .oprright = PGOid(23),
        /*oprresult*/ .oprresult = PGOid(16),
        /*oprcom*/ .oprcom = PGOid(97),
        /*oprnegate*/ .oprnegate = PGOid(523),
        /*oprlsortop*/ .oprlsortop = PGOid(0),
        /*oprrsortop*/ .oprrsortop = PGOid(0),
        /*oprltcmpop*/ .oprltcmpop = PGOid(0),
        /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
        /*oprcode*/ .oprcode = PGOid(0),
        /*oprrest*/ .oprrest = PGOid(0),
        /*oprjoin*/ .oprjoin = PGOid(0)}));

std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_INT32LE = std::pair<PGOid, PGOperatorPtr>(
    PGOid(523),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = PGOid(523),
        /*oprname*/ .oprname = "<=",
        /*oprnamespace*/ .oprnamespace = PGOid(1),
        /*oprowner*/ .oprowner = PGOid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = PGOid(23),
        /*oprright*/ .oprright = PGOid(23),
        /*oprresult*/ .oprresult = PGOid(16),
        /*oprcom*/ .oprcom = PGOid(525),
        /*oprnegate*/ .oprnegate = PGOid(521),
        /*oprlsortop*/ .oprlsortop = PGOid(0),
        /*oprrsortop*/ .oprrsortop = PGOid(0),
        /*oprltcmpop*/ .oprltcmpop = PGOid(0),
        /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
        /*oprcode*/ .oprcode = PGOid(0),
        /*oprrest*/ .oprrest = PGOid(0),
        /*oprjoin*/ .oprjoin = PGOid(0)}));

std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_INT32GE = std::pair<PGOid, PGOperatorPtr>(
    PGOid(525),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = PGOid(525),
        /*oprname*/ .oprname = ">=",
        /*oprnamespace*/ .oprnamespace = PGOid(1),
        /*oprowner*/ .oprowner = PGOid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = PGOid(23),
        /*oprright*/ .oprright = PGOid(23),
        /*oprresult*/ .oprresult = PGOid(16),
        /*oprcom*/ .oprcom = PGOid(523),
        /*oprnegate*/ .oprnegate = PGOid(97),
        /*oprlsortop*/ .oprlsortop = PGOid(0),
        /*oprrsortop*/ .oprrsortop = PGOid(0),
        /*oprltcmpop*/ .oprltcmpop = PGOid(0),
        /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
        /*oprcode*/ .oprcode = PGOid(0),
        /*oprrest*/ .oprrest = PGOid(0),
        /*oprjoin*/ .oprjoin = PGOid(0)}));

std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_INT64EQ = std::pair<PGOid, PGOperatorPtr>(
    PGOid(410),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = PGOid(410),
        /*oprname*/ .oprname = "=",
        /*oprnamespace*/ .oprnamespace = PGOid(1),
        /*oprowner*/ .oprowner = PGOid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = true,
        /*oprcanhash*/ .oprcanhash = true,
        /*oprleft*/ .oprleft = PGOid(20),
        /*oprright*/ .oprright = PGOid(20),
        /*oprresult*/ .oprresult = PGOid(16),
        /*oprcom*/ .oprcom = PGOid(410),
        /*oprnegate*/ .oprnegate = PGOid(411),
        /*oprlsortop*/ .oprlsortop = PGOid(0),
        /*oprrsortop*/ .oprrsortop = PGOid(0),
        /*oprltcmpop*/ .oprltcmpop = PGOid(0),
        /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
        /*oprcode*/ .oprcode = PGOid(0),
        /*oprrest*/ .oprrest = PGOid(0),
        /*oprjoin*/ .oprjoin = PGOid(0)}));

std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_INT64NE = std::pair<PGOid, PGOperatorPtr>(
    PGOid(411),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = PGOid(411),
        /*oprname*/ .oprname = "<>",
        /*oprnamespace*/ .oprnamespace = PGOid(1),
        /*oprowner*/ .oprowner = PGOid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = PGOid(20),
        /*oprright*/ .oprright = PGOid(20),
        /*oprresult*/ .oprresult = PGOid(16),
        /*oprcom*/ .oprcom = PGOid(411),
        /*oprnegate*/ .oprnegate = PGOid(410),
        /*oprlsortop*/ .oprlsortop = PGOid(0),
        /*oprrsortop*/ .oprrsortop = PGOid(0),
        /*oprltcmpop*/ .oprltcmpop = PGOid(0),
        /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
        /*oprcode*/ .oprcode = PGOid(0),
        /*oprrest*/ .oprrest = PGOid(0),
        /*oprjoin*/ .oprjoin = PGOid(0)}));

std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_INT64LT = std::pair<PGOid, PGOperatorPtr>(
    PGOid(412),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = PGOid(412),
        /*oprname*/ .oprname = "<",
        /*oprnamespace*/ .oprnamespace = PGOid(1),
        /*oprowner*/ .oprowner = PGOid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = PGOid(20),
        /*oprright*/ .oprright = PGOid(20),
        /*oprresult*/ .oprresult = PGOid(16),
        /*oprcom*/ .oprcom = PGOid(413),
        /*oprnegate*/ .oprnegate = PGOid(415),
        /*oprlsortop*/ .oprlsortop = PGOid(0),
        /*oprrsortop*/ .oprrsortop = PGOid(0),
        /*oprltcmpop*/ .oprltcmpop = PGOid(0),
        /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
        /*oprcode*/ .oprcode = PGOid(0),
        /*oprrest*/ .oprrest = PGOid(0),
        /*oprjoin*/ .oprjoin = PGOid(0)}));

std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_INT64GT = std::pair<PGOid, PGOperatorPtr>(
    PGOid(413),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = PGOid(413),
        /*oprname*/ .oprname = ">",
        /*oprnamespace*/ .oprnamespace = PGOid(1),
        /*oprowner*/ .oprowner = PGOid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = PGOid(20),
        /*oprright*/ .oprright = PGOid(20),
        /*oprresult*/ .oprresult = PGOid(16),
        /*oprcom*/ .oprcom = PGOid(412),
        /*oprnegate*/ .oprnegate = PGOid(414),
        /*oprlsortop*/ .oprlsortop = PGOid(0),
        /*oprrsortop*/ .oprrsortop = PGOid(0),
        /*oprltcmpop*/ .oprltcmpop = PGOid(0),
        /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
        /*oprcode*/ .oprcode = PGOid(0),
        /*oprrest*/ .oprrest = PGOid(0),
        /*oprjoin*/ .oprjoin = PGOid(0)}));

std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_INT64LE = std::pair<PGOid, PGOperatorPtr>(
    PGOid(414),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = PGOid(414),
        /*oprname*/ .oprname = "<=",
        /*oprnamespace*/ .oprnamespace = PGOid(1),
        /*oprowner*/ .oprowner = PGOid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = PGOid(20),
        /*oprright*/ .oprright = PGOid(20),
        /*oprresult*/ .oprresult = PGOid(16),
        /*oprcom*/ .oprcom = PGOid(415),
        /*oprnegate*/ .oprnegate = PGOid(413),
        /*oprlsortop*/ .oprlsortop = PGOid(0),
        /*oprrsortop*/ .oprrsortop = PGOid(0),
        /*oprltcmpop*/ .oprltcmpop = PGOid(0),
        /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
        /*oprcode*/ .oprcode = PGOid(0),
        /*oprrest*/ .oprrest = PGOid(0),
        /*oprjoin*/ .oprjoin = PGOid(0)}));

std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_INT64GE = std::pair<PGOid, PGOperatorPtr>(
    PGOid(415),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = PGOid(415),
        /*oprname*/ .oprname = ">=",
        /*oprnamespace*/ .oprnamespace = PGOid(1),
        /*oprowner*/ .oprowner = PGOid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = PGOid(20),
        /*oprright*/ .oprright = PGOid(20),
        /*oprresult*/ .oprresult = PGOid(16),
        /*oprcom*/ .oprcom = PGOid(414),
        /*oprnegate*/ .oprnegate = PGOid(412),
        /*oprlsortop*/ .oprlsortop = PGOid(0),
        /*oprrsortop*/ .oprrsortop = PGOid(0),
        /*oprltcmpop*/ .oprltcmpop = PGOid(0),
        /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
        /*oprcode*/ .oprcode = PGOid(0),
        /*oprrest*/ .oprrest = PGOid(0),
        /*oprjoin*/ .oprjoin = PGOid(0)}));

std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_STRINGEQ = std::pair<PGOid, PGOperatorPtr>(
    PGOid(98),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = PGOid(98),
        /*oprname*/ .oprname = "=",
        /*oprnamespace*/ .oprnamespace = PGOid(1),
        /*oprowner*/ .oprowner = PGOid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = true,
        /*oprcanhash*/ .oprcanhash = true,
        /*oprleft*/ .oprleft = PGOid(25),
        /*oprright*/ .oprright = PGOid(25),
        /*oprresult*/ .oprresult = PGOid(16),
        /*oprcom*/ .oprcom = PGOid(98),
        /*oprnegate*/ .oprnegate = PGOid(531),
        /*oprlsortop*/ .oprlsortop = PGOid(0),
        /*oprrsortop*/ .oprrsortop = PGOid(0),
        /*oprltcmpop*/ .oprltcmpop = PGOid(0),
        /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
        /*oprcode*/ .oprcode = PGOid(0),
        /*oprrest*/ .oprrest = PGOid(0),
        /*oprjoin*/ .oprjoin = PGOid(0)}));

std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_STRINGNE = std::pair<PGOid, PGOperatorPtr>(
    PGOid(531),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = PGOid(531),
        /*oprname*/ .oprname = "<>",
        /*oprnamespace*/ .oprnamespace = PGOid(1),
        /*oprowner*/ .oprowner = PGOid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = PGOid(25),
        /*oprright*/ .oprright = PGOid(25),
        /*oprresult*/ .oprresult = PGOid(16),
        /*oprcom*/ .oprcom = PGOid(531),
        /*oprnegate*/ .oprnegate = PGOid(98),
        /*oprlsortop*/ .oprlsortop = PGOid(0),
        /*oprrsortop*/ .oprrsortop = PGOid(0),
        /*oprltcmpop*/ .oprltcmpop = PGOid(0),
        /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
        /*oprcode*/ .oprcode = PGOid(0),
        /*oprrest*/ .oprrest = PGOid(0),
        /*oprjoin*/ .oprjoin = PGOid(0)}));

std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_STRINGLT = std::pair<PGOid, PGOperatorPtr>(
    PGOid(664),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = PGOid(664),
        /*oprname*/ .oprname = "<",
        /*oprnamespace*/ .oprnamespace = PGOid(1),
        /*oprowner*/ .oprowner = PGOid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = PGOid(25),
        /*oprright*/ .oprright = PGOid(25),
        /*oprresult*/ .oprresult = PGOid(16),
        /*oprcom*/ .oprcom = PGOid(666),
        /*oprnegate*/ .oprnegate = PGOid(667),
        /*oprlsortop*/ .oprlsortop = PGOid(0),
        /*oprrsortop*/ .oprrsortop = PGOid(0),
        /*oprltcmpop*/ .oprltcmpop = PGOid(0),
        /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
        /*oprcode*/ .oprcode = PGOid(740),//text_lt
        /*oprrest*/ .oprrest = PGOid(103),//scalarltsel
        /*oprjoin*/ .oprjoin = PGOid(107)}));//scalarltjoinsel

std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_STRINGLE = std::pair<PGOid, PGOperatorPtr>(
    PGOid(665),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = PGOid(665),
        /*oprname*/ .oprname = "<=",
        /*oprnamespace*/ .oprnamespace = PGOid(1),
        /*oprowner*/ .oprowner = PGOid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = PGOid(25),
        /*oprright*/ .oprright = PGOid(25),
        /*oprresult*/ .oprresult = PGOid(16),
        /*oprcom*/ .oprcom = PGOid(667),
        /*oprnegate*/ .oprnegate = PGOid(666),
        /*oprlsortop*/ .oprlsortop = PGOid(0),
        /*oprrsortop*/ .oprrsortop = PGOid(0),
        /*oprltcmpop*/ .oprltcmpop = PGOid(0),
        /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
        /*oprcode*/ .oprcode = PGOid(0),
        /*oprrest*/ .oprrest = PGOid(0),
        /*oprjoin*/ .oprjoin = PGOid(0)}));

std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_STRINGGT = std::pair<PGOid, PGOperatorPtr>(
    PGOid(666),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = PGOid(666),
        /*oprname*/ .oprname = ">",
        /*oprnamespace*/ .oprnamespace = PGOid(1),
        /*oprowner*/ .oprowner = PGOid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = PGOid(25),
        /*oprright*/ .oprright = PGOid(25),
        /*oprresult*/ .oprresult = PGOid(16),
        /*oprcom*/ .oprcom = PGOid(664),
        /*oprnegate*/ .oprnegate = PGOid(665),
        /*oprlsortop*/ .oprlsortop = PGOid(0),
        /*oprrsortop*/ .oprrsortop = PGOid(0),
        /*oprltcmpop*/ .oprltcmpop = PGOid(0),
        /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
        /*oprcode*/ .oprcode = PGOid(0),
        /*oprrest*/ .oprrest = PGOid(0),
        /*oprjoin*/ .oprjoin = PGOid(0)}));

std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_STRINGGE = std::pair<PGOid, PGOperatorPtr>(
    PGOid(667),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = PGOid(667),
        /*oprname*/ .oprname = ">=",
        /*oprnamespace*/ .oprnamespace = PGOid(1),
        /*oprowner*/ .oprowner = PGOid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = PGOid(25),
        /*oprright*/ .oprright = PGOid(25),
        /*oprresult*/ .oprresult = PGOid(16),
        /*oprcom*/ .oprcom = PGOid(665),
        /*oprnegate*/ .oprnegate = PGOid(664),
        /*oprlsortop*/ .oprlsortop = PGOid(0),
        /*oprrsortop*/ .oprrsortop = PGOid(0),
        /*oprltcmpop*/ .oprltcmpop = PGOid(0),
        /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
        /*oprcode*/ .oprcode = PGOid(0),
        /*oprrest*/ .oprrest = PGOid(0),
        /*oprjoin*/ .oprjoin = PGOid(0)}));

std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_FIXEDSTRINGEQ = std::pair<PGOid, PGOperatorPtr>(
    PGOid(1054),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = PGOid(1054),
        /*oprname*/ .oprname = "=",
        /*oprnamespace*/ .oprnamespace = PGOid(1),
        /*oprowner*/ .oprowner = PGOid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = true,
        /*oprcanhash*/ .oprcanhash = true,
        /*oprleft*/ .oprleft = PGOid(1042),
        /*oprright*/ .oprright = PGOid(1042),
        /*oprresult*/ .oprresult = PGOid(16),
        /*oprcom*/ .oprcom = PGOid(1054),
        /*oprnegate*/ .oprnegate = PGOid(1057),
        /*oprlsortop*/ .oprlsortop = PGOid(0),
        /*oprrsortop*/ .oprrsortop = PGOid(0),
        /*oprltcmpop*/ .oprltcmpop = PGOid(0),
        /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
        /*oprcode*/ .oprcode = PGOid(0),
        /*oprrest*/ .oprrest = PGOid(0),
        /*oprjoin*/ .oprjoin = PGOid(0)}));

std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_FIXEDSTRINGNE = std::pair<PGOid, PGOperatorPtr>(
    PGOid(1057),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = PGOid(1057),
        /*oprname*/ .oprname = "<>",
        /*oprnamespace*/ .oprnamespace = PGOid(1),
        /*oprowner*/ .oprowner = PGOid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = PGOid(1042),
        /*oprright*/ .oprright = PGOid(1042),
        /*oprresult*/ .oprresult = PGOid(16),
        /*oprcom*/ .oprcom = PGOid(1057),
        /*oprnegate*/ .oprnegate = PGOid(1054),
        /*oprlsortop*/ .oprlsortop = PGOid(0),
        /*oprrsortop*/ .oprrsortop = PGOid(0),
        /*oprltcmpop*/ .oprltcmpop = PGOid(0),
        /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
        /*oprcode*/ .oprcode = PGOid(0),
        /*oprrest*/ .oprrest = PGOid(0),
        /*oprjoin*/ .oprjoin = PGOid(0)}));

std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_FIXEDSTRINGLT = std::pair<PGOid, PGOperatorPtr>(
    PGOid(1058),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = PGOid(1058),
        /*oprname*/ .oprname = "<",
        /*oprnamespace*/ .oprnamespace = PGOid(1),
        /*oprowner*/ .oprowner = PGOid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = PGOid(1042),
        /*oprright*/ .oprright = PGOid(1042),
        /*oprresult*/ .oprresult = PGOid(16),
        /*oprcom*/ .oprcom = PGOid(1060),
        /*oprnegate*/ .oprnegate = PGOid(1061),
        /*oprlsortop*/ .oprlsortop = PGOid(0),
        /*oprrsortop*/ .oprrsortop = PGOid(0),
        /*oprltcmpop*/ .oprltcmpop = PGOid(0),
        /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
        /*oprcode*/ .oprcode = PGOid(0),
        /*oprrest*/ .oprrest = PGOid(0),
        /*oprjoin*/ .oprjoin = PGOid(0)}));

std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_FIXEDSTRINGLE = std::pair<PGOid, PGOperatorPtr>(
    PGOid(1059),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = PGOid(1059),
        /*oprname*/ .oprname = "<=",
        /*oprnamespace*/ .oprnamespace = PGOid(1),
        /*oprowner*/ .oprowner = PGOid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = PGOid(1042),
        /*oprright*/ .oprright = PGOid(1042),
        /*oprresult*/ .oprresult = PGOid(16),
        /*oprcom*/ .oprcom = PGOid(1061),
        /*oprnegate*/ .oprnegate = PGOid(1060),
        /*oprlsortop*/ .oprlsortop = PGOid(0),
        /*oprrsortop*/ .oprrsortop = PGOid(0),
        /*oprltcmpop*/ .oprltcmpop = PGOid(0),
        /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
        /*oprcode*/ .oprcode = PGOid(0),
        /*oprrest*/ .oprrest = PGOid(0),
        /*oprjoin*/ .oprjoin = PGOid(0)}));

std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_FIXEDSTRINGGT = std::pair<PGOid, PGOperatorPtr>(
    PGOid(1060),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = PGOid(1060),
        /*oprname*/ .oprname = ">",
        /*oprnamespace*/ .oprnamespace = PGOid(1),
        /*oprowner*/ .oprowner = PGOid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = PGOid(1042),
        /*oprright*/ .oprright = PGOid(1042),
        /*oprresult*/ .oprresult = PGOid(16),
        /*oprcom*/ .oprcom = PGOid(1058),
        /*oprnegate*/ .oprnegate = PGOid(1059),
        /*oprlsortop*/ .oprlsortop = PGOid(0),
        /*oprrsortop*/ .oprrsortop = PGOid(0),
        /*oprltcmpop*/ .oprltcmpop = PGOid(0),
        /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
        /*oprcode*/ .oprcode = PGOid(0),
        /*oprrest*/ .oprrest = PGOid(0),
        /*oprjoin*/ .oprjoin = PGOid(0)}));

std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_FIXEDSTRINGGE = std::pair<PGOid, PGOperatorPtr>(
    PGOid(1061),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = PGOid(1061),
        /*oprname*/ .oprname = ">=",
        /*oprnamespace*/ .oprnamespace = PGOid(1),
        /*oprowner*/ .oprowner = PGOid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = PGOid(1042),
        /*oprright*/ .oprright = PGOid(1042),
        /*oprresult*/ .oprresult = PGOid(16),
        /*oprcom*/ .oprcom = PGOid(1059),
        /*oprnegate*/ .oprnegate = PGOid(1058),
        /*oprlsortop*/ .oprlsortop = PGOid(0),
        /*oprrsortop*/ .oprrsortop = PGOid(0),
        /*oprltcmpop*/ .oprltcmpop = PGOid(0),
        /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
        /*oprcode*/ .oprcode = PGOid(0),
        /*oprrest*/ .oprrest = PGOid(0),
        /*oprjoin*/ .oprjoin = PGOid(0)}));

std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_DATEEQ = std::pair<PGOid, PGOperatorPtr>(
    PGOid(1093),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = PGOid(1093),
        /*oprname*/ .oprname = "=",
        /*oprnamespace*/ .oprnamespace = PGOid(1),
        /*oprowner*/ .oprowner = PGOid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = true,
        /*oprcanhash*/ .oprcanhash = true,
        /*oprleft*/ .oprleft = PGOid(1082),
        /*oprright*/ .oprright = PGOid(1082),
        /*oprresult*/ .oprresult = PGOid(16),
        /*oprcom*/ .oprcom = PGOid(1093),
        /*oprnegate*/ .oprnegate = PGOid(1094),
        /*oprlsortop*/ .oprlsortop = PGOid(0),
        /*oprrsortop*/ .oprrsortop = PGOid(0),
        /*oprltcmpop*/ .oprltcmpop = PGOid(0),
        /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
        /*oprcode*/ .oprcode = PGOid(0),
        /*oprrest*/ .oprrest = PGOid(0),
        /*oprjoin*/ .oprjoin = PGOid(0)}));

std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_DATENE = std::pair<PGOid, PGOperatorPtr>(
    PGOid(1094),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = PGOid(1094),
        /*oprname*/ .oprname = "<>",
        /*oprnamespace*/ .oprnamespace = PGOid(1),
        /*oprowner*/ .oprowner = PGOid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = PGOid(1082),
        /*oprright*/ .oprright = PGOid(1082),
        /*oprresult*/ .oprresult = PGOid(16),
        /*oprcom*/ .oprcom = PGOid(1094),
        /*oprnegate*/ .oprnegate = PGOid(1093),
        /*oprlsortop*/ .oprlsortop = PGOid(0),
        /*oprrsortop*/ .oprrsortop = PGOid(0),
        /*oprltcmpop*/ .oprltcmpop = PGOid(0),
        /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
        /*oprcode*/ .oprcode = PGOid(0),
        /*oprrest*/ .oprrest = PGOid(0),
        /*oprjoin*/ .oprjoin = PGOid(0)}));

std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_DATELT = std::pair<PGOid, PGOperatorPtr>(
    PGOid(1095),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = PGOid(1095),
        /*oprname*/ .oprname = "<",
        /*oprnamespace*/ .oprnamespace = PGOid(1),
        /*oprowner*/ .oprowner = PGOid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = PGOid(1082),
        /*oprright*/ .oprright = PGOid(1082),
        /*oprresult*/ .oprresult = PGOid(16),
        /*oprcom*/ .oprcom = PGOid(1097),
        /*oprnegate*/ .oprnegate = PGOid(1098),
        /*oprlsortop*/ .oprlsortop = PGOid(0),
        /*oprrsortop*/ .oprrsortop = PGOid(0),
        /*oprltcmpop*/ .oprltcmpop = PGOid(0),
        /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
        /*oprcode*/ .oprcode = PGOid(0),
        /*oprrest*/ .oprrest = PGOid(0),
        /*oprjoin*/ .oprjoin = PGOid(0)}));

std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_DATELE = std::pair<PGOid, PGOperatorPtr>(
    PGOid(1096),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = PGOid(1096),
        /*oprname*/ .oprname = "<=",
        /*oprnamespace*/ .oprnamespace = PGOid(1),
        /*oprowner*/ .oprowner = PGOid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = PGOid(1082),
        /*oprright*/ .oprright = PGOid(1082),
        /*oprresult*/ .oprresult = PGOid(16),
        /*oprcom*/ .oprcom = PGOid(1098),
        /*oprnegate*/ .oprnegate = PGOid(1097),
        /*oprlsortop*/ .oprlsortop = PGOid(0),
        /*oprrsortop*/ .oprrsortop = PGOid(0),
        /*oprltcmpop*/ .oprltcmpop = PGOid(0),
        /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
        /*oprcode*/ .oprcode = PGOid(0),
        /*oprrest*/ .oprrest = PGOid(0),
        /*oprjoin*/ .oprjoin = PGOid(0)}));

std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_DATEGT = std::pair<PGOid, PGOperatorPtr>(
    PGOid(1097),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = PGOid(1097),
        /*oprname*/ .oprname = ">",
        /*oprnamespace*/ .oprnamespace = PGOid(1),
        /*oprowner*/ .oprowner = PGOid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = PGOid(1082),
        /*oprright*/ .oprright = PGOid(1082),
        /*oprresult*/ .oprresult = PGOid(16),
        /*oprcom*/ .oprcom = PGOid(1095),
        /*oprnegate*/ .oprnegate = PGOid(1096),
        /*oprlsortop*/ .oprlsortop = PGOid(0),
        /*oprrsortop*/ .oprrsortop = PGOid(0),
        /*oprltcmpop*/ .oprltcmpop = PGOid(0),
        /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
        /*oprcode*/ .oprcode = PGOid(0),
        /*oprrest*/ .oprrest = PGOid(0),
        /*oprjoin*/ .oprjoin = PGOid(0)}));

std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_DATEGE = std::pair<PGOid, PGOperatorPtr>(
    PGOid(1098),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = PGOid(1098),
        /*oprname*/ .oprname = ">=",
        /*oprnamespace*/ .oprnamespace = PGOid(1),
        /*oprowner*/ .oprowner = PGOid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = PGOid(1082),
        /*oprright*/ .oprright = PGOid(1082),
        /*oprresult*/ .oprresult = PGOid(16),
        /*oprcom*/ .oprcom = PGOid(1096),
        /*oprnegate*/ .oprnegate = PGOid(1095),
        /*oprlsortop*/ .oprlsortop = PGOid(0),
        /*oprrsortop*/ .oprrsortop = PGOid(0),
        /*oprltcmpop*/ .oprltcmpop = PGOid(0),
        /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
        /*oprcode*/ .oprcode = PGOid(0),
        /*oprrest*/ .oprrest = PGOid(0),
        /*oprjoin*/ .oprjoin = PGOid(0)}));

std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_DATETIMEEQ = std::pair<PGOid, PGOperatorPtr>(
    PGOid(2060),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = PGOid(2060),
        /*oprname*/ .oprname = "=",
        /*oprnamespace*/ .oprnamespace = PGOid(1),
        /*oprowner*/ .oprowner = PGOid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = true,
        /*oprcanhash*/ .oprcanhash = true,
        /*oprleft*/ .oprleft = PGOid(1114),
        /*oprright*/ .oprright = PGOid(1114),
        /*oprresult*/ .oprresult = PGOid(16),
        /*oprcom*/ .oprcom = PGOid(2060),
        /*oprnegate*/ .oprnegate = PGOid(2061),
        /*oprlsortop*/ .oprlsortop = PGOid(0),
        /*oprrsortop*/ .oprrsortop = PGOid(0),
        /*oprltcmpop*/ .oprltcmpop = PGOid(0),
        /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
        /*oprcode*/ .oprcode = PGOid(0),
        /*oprrest*/ .oprrest = PGOid(0),
        /*oprjoin*/ .oprjoin = PGOid(0)}));

std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_DATETIMENE = std::pair<PGOid, PGOperatorPtr>(
    PGOid(2061),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = PGOid(2061),
        /*oprname*/ .oprname = "<>",
        /*oprnamespace*/ .oprnamespace = PGOid(1),
        /*oprowner*/ .oprowner = PGOid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = PGOid(1114),
        /*oprright*/ .oprright = PGOid(1114),
        /*oprresult*/ .oprresult = PGOid(16),
        /*oprcom*/ .oprcom = PGOid(2061),
        /*oprnegate*/ .oprnegate = PGOid(2060),
        /*oprlsortop*/ .oprlsortop = PGOid(0),
        /*oprrsortop*/ .oprrsortop = PGOid(0),
        /*oprltcmpop*/ .oprltcmpop = PGOid(0),
        /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
        /*oprcode*/ .oprcode = PGOid(0),
        /*oprrest*/ .oprrest = PGOid(0),
        /*oprjoin*/ .oprjoin = PGOid(0)}));

std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_DATETIMELT = std::pair<PGOid, PGOperatorPtr>(
    PGOid(2062),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = PGOid(2062),
        /*oprname*/ .oprname = "<",
        /*oprnamespace*/ .oprnamespace = PGOid(1),
        /*oprowner*/ .oprowner = PGOid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = PGOid(1114),
        /*oprright*/ .oprright = PGOid(1114),
        /*oprresult*/ .oprresult = PGOid(16),
        /*oprcom*/ .oprcom = PGOid(2064),
        /*oprnegate*/ .oprnegate = PGOid(2065),
        /*oprlsortop*/ .oprlsortop = PGOid(0),
        /*oprrsortop*/ .oprrsortop = PGOid(0),
        /*oprltcmpop*/ .oprltcmpop = PGOid(0),
        /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
        /*oprcode*/ .oprcode = PGOid(0),
        /*oprrest*/ .oprrest = PGOid(0),
        /*oprjoin*/ .oprjoin = PGOid(0)}));

std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_DATETIMELE = std::pair<PGOid, PGOperatorPtr>(
    PGOid(2063),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = PGOid(2063),
        /*oprname*/ .oprname = "<=",
        /*oprnamespace*/ .oprnamespace = PGOid(1),
        /*oprowner*/ .oprowner = PGOid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = PGOid(1114),
        /*oprright*/ .oprright = PGOid(1114),
        /*oprresult*/ .oprresult = PGOid(16),
        /*oprcom*/ .oprcom = PGOid(2065),
        /*oprnegate*/ .oprnegate = PGOid(2064),
        /*oprlsortop*/ .oprlsortop = PGOid(0),
        /*oprrsortop*/ .oprrsortop = PGOid(0),
        /*oprltcmpop*/ .oprltcmpop = PGOid(0),
        /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
        /*oprcode*/ .oprcode = PGOid(0),
        /*oprrest*/ .oprrest = PGOid(0),
        /*oprjoin*/ .oprjoin = PGOid(0)}));

std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_DATETIMEGT = std::pair<PGOid, PGOperatorPtr>(
    PGOid(2064),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = PGOid(2064),
        /*oprname*/ .oprname = ">",
        /*oprnamespace*/ .oprnamespace = PGOid(1),
        /*oprowner*/ .oprowner = PGOid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = PGOid(1114),
        /*oprright*/ .oprright = PGOid(1114),
        /*oprresult*/ .oprresult = PGOid(16),
        /*oprcom*/ .oprcom = PGOid(2062),
        /*oprnegate*/ .oprnegate = PGOid(2063),
        /*oprlsortop*/ .oprlsortop = PGOid(0),
        /*oprrsortop*/ .oprrsortop = PGOid(0),
        /*oprltcmpop*/ .oprltcmpop = PGOid(0),
        /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
        /*oprcode*/ .oprcode = PGOid(0),
        /*oprrest*/ .oprrest = PGOid(0),
        /*oprjoin*/ .oprjoin = PGOid(0)}));

std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_DATETIMEGE = std::pair<PGOid, PGOperatorPtr>(
    PGOid(2065),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = PGOid(2065),
        /*oprname*/ .oprname = ">=",
        /*oprnamespace*/ .oprnamespace = PGOid(1),
        /*oprowner*/ .oprowner = PGOid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = PGOid(1114),
        /*oprright*/ .oprright = PGOid(1114),
        /*oprresult*/ .oprresult = PGOid(16),
        /*oprcom*/ .oprcom = PGOid(2063),
        /*oprnegate*/ .oprnegate = PGOid(2062),
        /*oprlsortop*/ .oprlsortop = PGOid(0),
        /*oprrsortop*/ .oprrsortop = PGOid(0),
        /*oprltcmpop*/ .oprltcmpop = PGOid(0),
        /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
        /*oprcode*/ .oprcode = PGOid(0),
        /*oprrest*/ .oprrest = PGOid(0),
        /*oprjoin*/ .oprjoin = PGOid(0)}));

std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_DATETIME64EQ = std::pair<PGOid, PGOperatorPtr>(
    PGOid(1320),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = PGOid(1320),
        /*oprname*/ .oprname = "=",
        /*oprnamespace*/ .oprnamespace = PGOid(1),
        /*oprowner*/ .oprowner = PGOid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = true,
        /*oprcanhash*/ .oprcanhash = true,
        /*oprleft*/ .oprleft = PGOid(1184),
        /*oprright*/ .oprright = PGOid(1184),
        /*oprresult*/ .oprresult = PGOid(16),
        /*oprcom*/ .oprcom = PGOid(1320),
        /*oprnegate*/ .oprnegate = PGOid(1321),
        /*oprlsortop*/ .oprlsortop = PGOid(0),
        /*oprrsortop*/ .oprrsortop = PGOid(0),
        /*oprltcmpop*/ .oprltcmpop = PGOid(0),
        /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
        /*oprcode*/ .oprcode = PGOid(0),
        /*oprrest*/ .oprrest = PGOid(0),
        /*oprjoin*/ .oprjoin = PGOid(0)}));

std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_DATETIME64NE = std::pair<PGOid, PGOperatorPtr>(
    PGOid(1321),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = PGOid(1321),
        /*oprname*/ .oprname = "<>",
        /*oprnamespace*/ .oprnamespace = PGOid(1),
        /*oprowner*/ .oprowner = PGOid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = PGOid(1184),
        /*oprright*/ .oprright = PGOid(1184),
        /*oprresult*/ .oprresult = PGOid(16),
        /*oprcom*/ .oprcom = PGOid(1321),
        /*oprnegate*/ .oprnegate = PGOid(1320),
        /*oprlsortop*/ .oprlsortop = PGOid(0),
        /*oprrsortop*/ .oprrsortop = PGOid(0),
        /*oprltcmpop*/ .oprltcmpop = PGOid(0),
        /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
        /*oprcode*/ .oprcode = PGOid(0),
        /*oprrest*/ .oprrest = PGOid(0),
        /*oprjoin*/ .oprjoin = PGOid(0)}));

std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_DATETIME64LT = std::pair<PGOid, PGOperatorPtr>(
    PGOid(1322),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = PGOid(1322),
        /*oprname*/ .oprname = "<",
        /*oprnamespace*/ .oprnamespace = PGOid(1),
        /*oprowner*/ .oprowner = PGOid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = PGOid(1184),
        /*oprright*/ .oprright = PGOid(1184),
        /*oprresult*/ .oprresult = PGOid(16),
        /*oprcom*/ .oprcom = PGOid(1324),
        /*oprnegate*/ .oprnegate = PGOid(1325),
        /*oprlsortop*/ .oprlsortop = PGOid(0),
        /*oprrsortop*/ .oprrsortop = PGOid(0),
        /*oprltcmpop*/ .oprltcmpop = PGOid(0),
        /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
        /*oprcode*/ .oprcode = PGOid(0),
        /*oprrest*/ .oprrest = PGOid(0),
        /*oprjoin*/ .oprjoin = PGOid(0)}));

std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_DATETIME64LE = std::pair<PGOid, PGOperatorPtr>(
    PGOid(1323),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = PGOid(1323),
        /*oprname*/ .oprname = "<=",
        /*oprnamespace*/ .oprnamespace = PGOid(1),
        /*oprowner*/ .oprowner = PGOid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = PGOid(1184),
        /*oprright*/ .oprright = PGOid(1184),
        /*oprresult*/ .oprresult = PGOid(16),
        /*oprcom*/ .oprcom = PGOid(1325),
        /*oprnegate*/ .oprnegate = PGOid(1324),
        /*oprlsortop*/ .oprlsortop = PGOid(0),
        /*oprrsortop*/ .oprrsortop = PGOid(0),
        /*oprltcmpop*/ .oprltcmpop = PGOid(0),
        /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
        /*oprcode*/ .oprcode = PGOid(0),
        /*oprrest*/ .oprrest = PGOid(0),
        /*oprjoin*/ .oprjoin = PGOid(0)}));

std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_DATETIME64GT = std::pair<PGOid, PGOperatorPtr>(
    PGOid(1324),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = PGOid(1324),
        /*oprname*/ .oprname = ">",
        /*oprnamespace*/ .oprnamespace = PGOid(1),
        /*oprowner*/ .oprowner = PGOid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = PGOid(1184),
        /*oprright*/ .oprright = PGOid(1184),
        /*oprresult*/ .oprresult = PGOid(16),
        /*oprcom*/ .oprcom = PGOid(1322),
        /*oprnegate*/ .oprnegate = PGOid(1323),
        /*oprlsortop*/ .oprlsortop = PGOid(0),
        /*oprrsortop*/ .oprrsortop = PGOid(0),
        /*oprltcmpop*/ .oprltcmpop = PGOid(0),
        /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
        /*oprcode*/ .oprcode = PGOid(0),
        /*oprrest*/ .oprrest = PGOid(0),
        /*oprjoin*/ .oprjoin = PGOid(0)}));

std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_DATETIME64GE = std::pair<PGOid, PGOperatorPtr>(
    PGOid(1325),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = PGOid(1325),
        /*oprname*/ .oprname = ">=",
        /*oprnamespace*/ .oprnamespace = PGOid(1),
        /*oprowner*/ .oprowner = PGOid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = PGOid(1184),
        /*oprright*/ .oprright = PGOid(1184),
        /*oprresult*/ .oprresult = PGOid(16),
        /*oprcom*/ .oprcom = PGOid(1323),
        /*oprnegate*/ .oprnegate = PGOid(1322),
        /*oprlsortop*/ .oprlsortop = PGOid(0),
        /*oprrsortop*/ .oprrsortop = PGOid(0),
        /*oprltcmpop*/ .oprltcmpop = PGOid(0),
        /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
        /*oprcode*/ .oprcode = PGOid(0),
        /*oprrest*/ .oprrest = PGOid(0),
        /*oprjoin*/ .oprjoin = PGOid(0)}));

std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_DECIMAL64EQ = std::pair<PGOid, PGOperatorPtr>(
    PGOid(1752),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = PGOid(1752),
        /*oprname*/ .oprname = "=",
        /*oprnamespace*/ .oprnamespace = PGOid(1),
        /*oprowner*/ .oprowner = PGOid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = true,
        /*oprcanhash*/ .oprcanhash = true,
        /*oprleft*/ .oprleft = PGOid(1700),
        /*oprright*/ .oprright = PGOid(1700),
        /*oprresult*/ .oprresult = PGOid(16),
        /*oprcom*/ .oprcom = PGOid(1752),
        /*oprnegate*/ .oprnegate = PGOid(1753),
        /*oprlsortop*/ .oprlsortop = PGOid(0),
        /*oprrsortop*/ .oprrsortop = PGOid(0),
        /*oprltcmpop*/ .oprltcmpop = PGOid(0),
        /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
        /*oprcode*/ .oprcode = PGOid(0),
        /*oprrest*/ .oprrest = PGOid(0),
        /*oprjoin*/ .oprjoin = PGOid(0)}));

std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_DECIMAL64NE = std::pair<PGOid, PGOperatorPtr>(
    PGOid(1753),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = PGOid(1753),
        /*oprname*/ .oprname = "<>",
        /*oprnamespace*/ .oprnamespace = PGOid(1),
        /*oprowner*/ .oprowner = PGOid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = PGOid(1700),
        /*oprright*/ .oprright = PGOid(1700),
        /*oprresult*/ .oprresult = PGOid(16),
        /*oprcom*/ .oprcom = PGOid(1753),
        /*oprnegate*/ .oprnegate = PGOid(1752),
        /*oprlsortop*/ .oprlsortop = PGOid(0),
        /*oprrsortop*/ .oprrsortop = PGOid(0),
        /*oprltcmpop*/ .oprltcmpop = PGOid(0),
        /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
        /*oprcode*/ .oprcode = PGOid(0),
        /*oprrest*/ .oprrest = PGOid(0),
        /*oprjoin*/ .oprjoin = PGOid(0)}));

std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_DECIMAL64LT = std::pair<PGOid, PGOperatorPtr>(
    PGOid(1754),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = PGOid(1754),
        /*oprname*/ .oprname = "<",
        /*oprnamespace*/ .oprnamespace = PGOid(1),
        /*oprowner*/ .oprowner = PGOid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = PGOid(1700),
        /*oprright*/ .oprright = PGOid(1700),
        /*oprresult*/ .oprresult = PGOid(16),
        /*oprcom*/ .oprcom = PGOid(1756),
        /*oprnegate*/ .oprnegate = PGOid(1757),
        /*oprlsortop*/ .oprlsortop = PGOid(0),
        /*oprrsortop*/ .oprrsortop = PGOid(0),
        /*oprltcmpop*/ .oprltcmpop = PGOid(0),
        /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
        /*oprcode*/ .oprcode = PGOid(0),
        /*oprrest*/ .oprrest = PGOid(0),
        /*oprjoin*/ .oprjoin = PGOid(0)}));

std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_DECIMAL64LE = std::pair<PGOid, PGOperatorPtr>(
    PGOid(1755),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = PGOid(1755),
        /*oprname*/ .oprname = "<=",
        /*oprnamespace*/ .oprnamespace = PGOid(1),
        /*oprowner*/ .oprowner = PGOid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = PGOid(1700),
        /*oprright*/ .oprright = PGOid(1700),
        /*oprresult*/ .oprresult = PGOid(16),
        /*oprcom*/ .oprcom = PGOid(1757),
        /*oprnegate*/ .oprnegate = PGOid(1756),
        /*oprlsortop*/ .oprlsortop = PGOid(0),
        /*oprrsortop*/ .oprrsortop = PGOid(0),
        /*oprltcmpop*/ .oprltcmpop = PGOid(0),
        /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
        /*oprcode*/ .oprcode = PGOid(0),
        /*oprrest*/ .oprrest = PGOid(0),
        /*oprjoin*/ .oprjoin = PGOid(0)}));

std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_DECIMAL64GT = std::pair<PGOid, PGOperatorPtr>(
    PGOid(1756),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = PGOid(1756),
        /*oprname*/ .oprname = ">",
        /*oprnamespace*/ .oprnamespace = PGOid(1),
        /*oprowner*/ .oprowner = PGOid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = PGOid(1700),
        /*oprright*/ .oprright = PGOid(1700),
        /*oprresult*/ .oprresult = PGOid(16),
        /*oprcom*/ .oprcom = PGOid(1754),
        /*oprnegate*/ .oprnegate = PGOid(1755),
        /*oprlsortop*/ .oprlsortop = PGOid(0),
        /*oprrsortop*/ .oprrsortop = PGOid(0),
        /*oprltcmpop*/ .oprltcmpop = PGOid(0),
        /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
        /*oprcode*/ .oprcode = PGOid(0),
        /*oprrest*/ .oprrest = PGOid(0),
        /*oprjoin*/ .oprjoin = PGOid(0)}));

std::pair<PGOid, PGOperatorPtr> OperProvider::OPER_DECIMAL64GE = std::pair<PGOid, PGOperatorPtr>(
    PGOid(1757),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = PGOid(1757),
        /*oprname*/ .oprname = ">=",
        /*oprnamespace*/ .oprnamespace = PGOid(1),
        /*oprowner*/ .oprowner = PGOid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = PGOid(1700),
        /*oprright*/ .oprright = PGOid(1700),
        /*oprresult*/ .oprresult = PGOid(16),
        /*oprcom*/ .oprcom = PGOid(1755),
        /*oprnegate*/ .oprnegate = PGOid(1754),
        /*oprlsortop*/ .oprlsortop = PGOid(0),
        /*oprrsortop*/ .oprrsortop = PGOid(0),
        /*oprltcmpop*/ .oprltcmpop = PGOid(0),
        /*oprgtcmpop*/ .oprgtcmpop = PGOid(0),
        /*oprcode*/ .oprcode = PGOid(0),
        /*oprrest*/ .oprrest = PGOid(0),
        /*oprjoin*/ .oprjoin = PGOid(0)}));

OperProvider::OidOperatorMap OperProvider::oid_oper_map = {
    OperProvider::OPER_INT2PL,
	OperProvider::OPER_INT4PL,
	OperProvider::OPER_INT24PL,
	OperProvider::OPER_INT42PL,
    OperProvider::OPER_INT2MI,
    OperProvider::OPER_INT4MI,
    OperProvider::OPER_INT24MI,
    OperProvider::OPER_INT42MI,
    OperProvider::OPER_INT2MUL,
    OperProvider::OPER_INT4MUL,
    OperProvider::OPER_INT24MUL,
    OperProvider::OPER_INT42MUL,
    OperProvider::OPER_INT2DIV,
    OperProvider::OPER_INT4DIV,
    OperProvider::OPER_INT24DIV,
    OperProvider::OPER_INT42DIV,
    OperProvider::OPER_FLOAT32EQ,
    OperProvider::OPER_FLOAT32NE,
    OperProvider::OPER_FLOAT32LT,
    OperProvider::OPER_FLOAT32LE,
    OperProvider::OPER_FLOAT32GT,
    OperProvider::OPER_FLOAT32GE,
    OperProvider::OPER_FLOAT64EQ,
    OperProvider::OPER_FLOAT64NE,
    OperProvider::OPER_FLOAT64LT,
    OperProvider::OPER_FLOAT64LE,
    OperProvider::OPER_FLOAT64GT,
    OperProvider::OPER_FLOAT64GE,
    OperProvider::OPER_BOOLEQ,
    OperProvider::OPER_BOOLNE,
    OperProvider::OPER_BOOLLT,
    OperProvider::OPER_BOOLLE,
    OperProvider::OPER_BOOLGT,
    OperProvider::OPER_BOOLGE,
    OperProvider::OPER_INT16EQ,
    OperProvider::OPER_INT16NE,
    OperProvider::OPER_INT16LT,
    OperProvider::OPER_INT16LE,
    OperProvider::OPER_INT16GT,
    OperProvider::OPER_INT16GE,
    OperProvider::OPER_INT32EQ,
    OperProvider::OPER_INT32NE,
    OperProvider::OPER_INT32LT,
    OperProvider::OPER_INT32LE,
    OperProvider::OPER_INT32GT,
    OperProvider::OPER_INT32GE,
    OperProvider::OPER_INT64EQ,
    OperProvider::OPER_INT64NE,
    OperProvider::OPER_INT64LT,
    OperProvider::OPER_INT64LE,
    OperProvider::OPER_INT64GT,
    OperProvider::OPER_INT64GE,
    OperProvider::OPER_STRINGEQ,
    OperProvider::OPER_STRINGNE,
    OperProvider::OPER_STRINGLT,
    OperProvider::OPER_STRINGLE,
    OperProvider::OPER_STRINGGT,
    OperProvider::OPER_STRINGGE,
    OperProvider::OPER_FIXEDSTRINGEQ,
    OperProvider::OPER_FIXEDSTRINGNE,
    OperProvider::OPER_FIXEDSTRINGLT,
    OperProvider::OPER_FIXEDSTRINGLE,
    OperProvider::OPER_FIXEDSTRINGGT,
    OperProvider::OPER_FIXEDSTRINGGE,
    OperProvider::OPER_DATEEQ,
    OperProvider::OPER_DATENE,
    OperProvider::OPER_DATELT,
    OperProvider::OPER_DATELE,
    OperProvider::OPER_DATEGT,
    OperProvider::OPER_DATEGE,
    OperProvider::OPER_DATETIMEEQ,
    OperProvider::OPER_DATETIMENE,
    OperProvider::OPER_DATETIMELT,
    OperProvider::OPER_DATETIMELE,
    OperProvider::OPER_DATETIMEGT,
    OperProvider::OPER_DATETIMEGE,
    OperProvider::OPER_DATETIME64EQ,
    OperProvider::OPER_DATETIME64NE,
    OperProvider::OPER_DATETIME64LT,
    OperProvider::OPER_DATETIME64LE,
    OperProvider::OPER_DATETIME64GT,
    OperProvider::OPER_DATETIME64GE,
    OperProvider::OPER_DECIMAL64EQ,
    OperProvider::OPER_DECIMAL64NE,
    OperProvider::OPER_DECIMAL64LT,
    OperProvider::OPER_DECIMAL64LE,
    OperProvider::OPER_DECIMAL64GT,
    OperProvider::OPER_DECIMAL64GE
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
