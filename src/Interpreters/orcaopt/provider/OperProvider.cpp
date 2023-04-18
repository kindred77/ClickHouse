#include <Interpreters/orcaopt/provider/OperProvider.h>

#include <Interpreters/orcaopt/provider/TypeProvider.h>

#include <Interpreters/Context.h>

#ifdef __clang__
#pragma clang diagnostic ignored "-Wunused-parameter"
#else
#pragma GCC diagnostic ignored "-Wunused-parameter"
#endif

using namespace duckdb_libpgquery;

namespace DB
{

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_INT2PL = std::pair<Oid, PGOperatorPtr>(
    Oid(550),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(550),
        /*oprname*/ .oprname = "+",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = Oid(21),
        /*oprright*/ .oprright = Oid(21),
        /*oprresult*/ .oprresult = Oid(21),
        /*oprcom*/ .oprcom = Oid(550),
        /*oprnegate*/ .oprnegate = Oid(0),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(176),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_INT4PL = std::pair<Oid, PGOperatorPtr>(
    Oid(551),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(551),
        /*oprname*/ .oprname = "+",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = Oid(23),
        /*oprright*/ .oprright = Oid(23),
        /*oprresult*/ .oprresult = Oid(23),
        /*oprcom*/ .oprcom = Oid(551),
        /*oprnegate*/ .oprnegate = Oid(0),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(177),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_INT24PL = std::pair<Oid, PGOperatorPtr>(
    Oid(552),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(552),
        /*oprname*/ .oprname = "+",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = Oid(21),
        /*oprright*/ .oprright = Oid(23),
        /*oprresult*/ .oprresult = Oid(23),
        /*oprcom*/ .oprcom = Oid(553),
        /*oprnegate*/ .oprnegate = Oid(0),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(178),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_INT42PL = std::pair<Oid, PGOperatorPtr>(
    Oid(553),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(553),
        /*oprname*/ .oprname = "+",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = Oid(23),
        /*oprright*/ .oprright = Oid(21),
        /*oprresult*/ .oprresult = Oid(23),
        /*oprcom*/ .oprcom = Oid(552),
        /*oprnegate*/ .oprnegate = Oid(0),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(179),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_INT2MI = std::pair<Oid, PGOperatorPtr>(
    Oid(554),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(554),
        /*oprname*/ .oprname = "-",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = Oid(21),
        /*oprright*/ .oprright = Oid(21),
        /*oprresult*/ .oprresult = Oid(21),
        /*oprcom*/ .oprcom = Oid(0),
        /*oprnegate*/ .oprnegate = Oid(0),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(180),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_INT4MI = std::pair<Oid, PGOperatorPtr>(
    Oid(555),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(555),
        /*oprname*/ .oprname = "-",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = Oid(23),
        /*oprright*/ .oprright = Oid(23),
        /*oprresult*/ .oprresult = Oid(23),
        /*oprcom*/ .oprcom = Oid(0),
        /*oprnegate*/ .oprnegate = Oid(0),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(181),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_INT24MI = std::pair<Oid, PGOperatorPtr>(
    Oid(556),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(556),
        /*oprname*/ .oprname = "-",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = Oid(21),
        /*oprright*/ .oprright = Oid(23),
        /*oprresult*/ .oprresult = Oid(23),
        /*oprcom*/ .oprcom = Oid(0),
        /*oprnegate*/ .oprnegate = Oid(0),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(182),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_INT42MI = std::pair<Oid, PGOperatorPtr>(
    Oid(557),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(557),
        /*oprname*/ .oprname = "-",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = Oid(23),
        /*oprright*/ .oprright = Oid(21),
        /*oprresult*/ .oprresult = Oid(23),
        /*oprcom*/ .oprcom = Oid(0),
        /*oprnegate*/ .oprnegate = Oid(0),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(183),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_INT2MUL = std::pair<Oid, PGOperatorPtr>(
    Oid(526),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(526),
        /*oprname*/ .oprname = "*",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = Oid(21),
        /*oprright*/ .oprright = Oid(21),
        /*oprresult*/ .oprresult = Oid(21),
        /*oprcom*/ .oprcom = Oid(526),
        /*oprnegate*/ .oprnegate = Oid(0),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(152),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_INT4MUL = std::pair<Oid, PGOperatorPtr>(
    Oid(514),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(514),
        /*oprname*/ .oprname = "*",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = Oid(23),
        /*oprright*/ .oprright = Oid(23),
        /*oprresult*/ .oprresult = Oid(23),
        /*oprcom*/ .oprcom = Oid(514),
        /*oprnegate*/ .oprnegate = Oid(0),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(141),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_INT24MUL = std::pair<Oid, PGOperatorPtr>(
    Oid(544),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(544),
        /*oprname*/ .oprname = "*",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = Oid(21),
        /*oprright*/ .oprright = Oid(23),
        /*oprresult*/ .oprresult = Oid(23),
        /*oprcom*/ .oprcom = Oid(545),
        /*oprnegate*/ .oprnegate = Oid(0),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(170),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_INT42MUL = std::pair<Oid, PGOperatorPtr>(
    Oid(545),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(545),
        /*oprname*/ .oprname = "*",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = Oid(23),
        /*oprright*/ .oprright = Oid(21),
        /*oprresult*/ .oprresult = Oid(23),
        /*oprcom*/ .oprcom = Oid(544),
        /*oprnegate*/ .oprnegate = Oid(0),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(171),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_INT2DIV = std::pair<Oid, PGOperatorPtr>(
    Oid(527),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(527),
        /*oprname*/ .oprname = "/",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = Oid(21),
        /*oprright*/ .oprright = Oid(21),
        /*oprresult*/ .oprresult = Oid(21),
        /*oprcom*/ .oprcom = Oid(0),
        /*oprnegate*/ .oprnegate = Oid(0),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(153),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_INT4DIV = std::pair<Oid, PGOperatorPtr>(
    Oid(528),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(528),
        /*oprname*/ .oprname = "/",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = Oid(23),
        /*oprright*/ .oprright = Oid(23),
        /*oprresult*/ .oprresult = Oid(23),
        /*oprcom*/ .oprcom = Oid(0),
        /*oprnegate*/ .oprnegate = Oid(0),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(154),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_INT24DIV = std::pair<Oid, PGOperatorPtr>(
    Oid(546),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(546),
        /*oprname*/ .oprname = "/",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = Oid(21),
        /*oprright*/ .oprright = Oid(23),
        /*oprresult*/ .oprresult = Oid(23),
        /*oprcom*/ .oprcom = Oid(0),
        /*oprnegate*/ .oprnegate = Oid(0),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(172),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_INT42DIV = std::pair<Oid, PGOperatorPtr>(
    Oid(547),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(547),
        /*oprname*/ .oprname = "/",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = Oid(23),
        /*oprright*/ .oprright = Oid(21),
        /*oprresult*/ .oprresult = Oid(23),
        /*oprcom*/ .oprcom = Oid(0),
        /*oprnegate*/ .oprnegate = Oid(0),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(173),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_FLOAT32EQ = std::pair<Oid, PGOperatorPtr>(
    Oid(620),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(620),
        /*oprname*/ .oprname = "=",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = true,
        /*oprcanhash*/ .oprcanhash = true,
        /*oprleft*/ .oprleft = Oid(700),
        /*oprright*/ .oprright = Oid(700),
        /*oprresult*/ .oprresult = Oid(16),
        /*oprcom*/ .oprcom = Oid(620),
        /*oprnegate*/ .oprnegate = Oid(621),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(0),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_FLOAT32NE = std::pair<Oid, PGOperatorPtr>(
    Oid(621),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(621),
        /*oprname*/ .oprname = "<>",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = Oid(700),
        /*oprright*/ .oprright = Oid(700),
        /*oprresult*/ .oprresult = Oid(16),
        /*oprcom*/ .oprcom = Oid(621),
        /*oprnegate*/ .oprnegate = Oid(620),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(0),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_FLOAT32LT = std::pair<Oid, PGOperatorPtr>(
    Oid(622),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(622),
        /*oprname*/ .oprname = "<",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = Oid(700),
        /*oprright*/ .oprright = Oid(700),
        /*oprresult*/ .oprresult = Oid(16),
        /*oprcom*/ .oprcom = Oid(623),
        /*oprnegate*/ .oprnegate = Oid(625),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(0),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_FLOAT32GT = std::pair<Oid, PGOperatorPtr>(
    Oid(623),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(623),
        /*oprname*/ .oprname = ">",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = Oid(700),
        /*oprright*/ .oprright = Oid(700),
        /*oprresult*/ .oprresult = Oid(16),
        /*oprcom*/ .oprcom = Oid(622),
        /*oprnegate*/ .oprnegate = Oid(624),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(0),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_FLOAT32LE = std::pair<Oid, PGOperatorPtr>(
    Oid(624),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(624),
        /*oprname*/ .oprname = "<=",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = Oid(700),
        /*oprright*/ .oprright = Oid(700),
        /*oprresult*/ .oprresult = Oid(16),
        /*oprcom*/ .oprcom = Oid(625),
        /*oprnegate*/ .oprnegate = Oid(623),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(0),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_FLOAT32GE = std::pair<Oid, PGOperatorPtr>(
    Oid(625),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(625),
        /*oprname*/ .oprname = ">=",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = Oid(700),
        /*oprright*/ .oprright = Oid(700),
        /*oprresult*/ .oprresult = Oid(16),
        /*oprcom*/ .oprcom = Oid(624),
        /*oprnegate*/ .oprnegate = Oid(622),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(0),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_FLOAT64EQ = std::pair<Oid, PGOperatorPtr>(
    Oid(670),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(670),
        /*oprname*/ .oprname = "=",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = true,
        /*oprcanhash*/ .oprcanhash = true,
        /*oprleft*/ .oprleft = Oid(701),
        /*oprright*/ .oprright = Oid(701),
        /*oprresult*/ .oprresult = Oid(16),
        /*oprcom*/ .oprcom = Oid(670),
        /*oprnegate*/ .oprnegate = Oid(671),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(0),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_FLOAT64NE = std::pair<Oid, PGOperatorPtr>(
    Oid(671),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(671),
        /*oprname*/ .oprname = "<>",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = Oid(701),
        /*oprright*/ .oprright = Oid(701),
        /*oprresult*/ .oprresult = Oid(16),
        /*oprcom*/ .oprcom = Oid(671),
        /*oprnegate*/ .oprnegate = Oid(670),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(0),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_FLOAT64LT = std::pair<Oid, PGOperatorPtr>(
    Oid(672),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(672),
        /*oprname*/ .oprname = "<",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = Oid(701),
        /*oprright*/ .oprright = Oid(701),
        /*oprresult*/ .oprresult = Oid(16),
        /*oprcom*/ .oprcom = Oid(674),
        /*oprnegate*/ .oprnegate = Oid(675),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(0),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_FLOAT64LE = std::pair<Oid, PGOperatorPtr>(
    Oid(673),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(673),
        /*oprname*/ .oprname = "<=",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = Oid(701),
        /*oprright*/ .oprright = Oid(701),
        /*oprresult*/ .oprresult = Oid(16),
        /*oprcom*/ .oprcom = Oid(675),
        /*oprnegate*/ .oprnegate = Oid(674),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(0),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_FLOAT64GT = std::pair<Oid, PGOperatorPtr>(
    Oid(674),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(674),
        /*oprname*/ .oprname = ">",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = Oid(701),
        /*oprright*/ .oprright = Oid(701),
        /*oprresult*/ .oprresult = Oid(16),
        /*oprcom*/ .oprcom = Oid(672),
        /*oprnegate*/ .oprnegate = Oid(673),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(0),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_FLOAT64GE = std::pair<Oid, PGOperatorPtr>(
    Oid(675),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(675),
        /*oprname*/ .oprname = ">=",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = Oid(701),
        /*oprright*/ .oprright = Oid(701),
        /*oprresult*/ .oprresult = Oid(16),
        /*oprcom*/ .oprcom = Oid(673),
        /*oprnegate*/ .oprnegate = Oid(672),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(0),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_BOOLEQ = std::pair<Oid, PGOperatorPtr>(
    Oid(91),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(91),
        /*oprname*/ .oprname = "=",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = true,
        /*oprcanhash*/ .oprcanhash = true,
        /*oprleft*/ .oprleft = Oid(16),
        /*oprright*/ .oprright = Oid(16),
        /*oprresult*/ .oprresult = Oid(16),
        /*oprcom*/ .oprcom = Oid(91),
        /*oprnegate*/ .oprnegate = Oid(85),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(0),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_BOOLNE = std::pair<Oid, PGOperatorPtr>(
    Oid(85),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(85),
        /*oprname*/ .oprname = "<>",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = Oid(16),
        /*oprright*/ .oprright = Oid(16),
        /*oprresult*/ .oprresult = Oid(16),
        /*oprcom*/ .oprcom = Oid(85),
        /*oprnegate*/ .oprnegate = Oid(91),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(0),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_BOOLLT = std::pair<Oid, PGOperatorPtr>(
    Oid(58),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(58),
        /*oprname*/ .oprname = "<",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = Oid(16),
        /*oprright*/ .oprright = Oid(16),
        /*oprresult*/ .oprresult = Oid(16),
        /*oprcom*/ .oprcom = Oid(59),
        /*oprnegate*/ .oprnegate = Oid(1695),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(0),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_BOOLGT = std::pair<Oid, PGOperatorPtr>(
    Oid(59),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(59),
        /*oprname*/ .oprname = ">",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = Oid(16),
        /*oprright*/ .oprright = Oid(16),
        /*oprresult*/ .oprresult = Oid(16),
        /*oprcom*/ .oprcom = Oid(58),
        /*oprnegate*/ .oprnegate = Oid(1694),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(0),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_BOOLLE = std::pair<Oid, PGOperatorPtr>(
    Oid(1694),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(1694),
        /*oprname*/ .oprname = "<=",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = Oid(16),
        /*oprright*/ .oprright = Oid(16),
        /*oprresult*/ .oprresult = Oid(16),
        /*oprcom*/ .oprcom = Oid(1695),
        /*oprnegate*/ .oprnegate = Oid(59),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(0),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_BOOLGE = std::pair<Oid, PGOperatorPtr>(
    Oid(1695),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(1695),
        /*oprname*/ .oprname = ">=",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = Oid(16),
        /*oprright*/ .oprright = Oid(16),
        /*oprresult*/ .oprresult = Oid(16),
        /*oprcom*/ .oprcom = Oid(1694),
        /*oprnegate*/ .oprnegate = Oid(58),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(0),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_INT16EQ = std::pair<Oid, PGOperatorPtr>(
    Oid(94),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(94),
        /*oprname*/ .oprname = "=",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = true,
        /*oprcanhash*/ .oprcanhash = true,
        /*oprleft*/ .oprleft = Oid(21),
        /*oprright*/ .oprright = Oid(21),
        /*oprresult*/ .oprresult = Oid(16),
        /*oprcom*/ .oprcom = Oid(94),
        /*oprnegate*/ .oprnegate = Oid(519),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(0),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_INT16LT = std::pair<Oid, PGOperatorPtr>(
    Oid(95),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(95),
        /*oprname*/ .oprname = "<",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = Oid(21),
        /*oprright*/ .oprright = Oid(21),
        /*oprresult*/ .oprresult = Oid(16),
        /*oprcom*/ .oprcom = Oid(520),
        /*oprnegate*/ .oprnegate = Oid(524),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(0),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_INT16NE = std::pair<Oid, PGOperatorPtr>(
    Oid(519),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(519),
        /*oprname*/ .oprname = "<>",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = Oid(21),
        /*oprright*/ .oprright = Oid(21),
        /*oprresult*/ .oprresult = Oid(16),
        /*oprcom*/ .oprcom = Oid(519),
        /*oprnegate*/ .oprnegate = Oid(94),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(0),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_INT16GT = std::pair<Oid, PGOperatorPtr>(
    Oid(520),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(520),
        /*oprname*/ .oprname = ">",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = Oid(21),
        /*oprright*/ .oprright = Oid(21),
        /*oprresult*/ .oprresult = Oid(16),
        /*oprcom*/ .oprcom = Oid(95),
        /*oprnegate*/ .oprnegate = Oid(522),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(0),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_INT16LE = std::pair<Oid, PGOperatorPtr>(
    Oid(522),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(522),
        /*oprname*/ .oprname = "<=",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = Oid(21),
        /*oprright*/ .oprright = Oid(21),
        /*oprresult*/ .oprresult = Oid(16),
        /*oprcom*/ .oprcom = Oid(524),
        /*oprnegate*/ .oprnegate = Oid(520),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(0),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_INT16GE = std::pair<Oid, PGOperatorPtr>(
    Oid(524),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(524),
        /*oprname*/ .oprname = ">=",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = Oid(21),
        /*oprright*/ .oprright = Oid(21),
        /*oprresult*/ .oprresult = Oid(16),
        /*oprcom*/ .oprcom = Oid(522),
        /*oprnegate*/ .oprnegate = Oid(95),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(0),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_INT32EQ = std::pair<Oid, PGOperatorPtr>(
    Oid(96),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(96),
        /*oprname*/ .oprname = "=",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = true,
        /*oprcanhash*/ .oprcanhash = true,
        /*oprleft*/ .oprleft = Oid(23),
        /*oprright*/ .oprright = Oid(23),
        /*oprresult*/ .oprresult = Oid(16),
        /*oprcom*/ .oprcom = Oid(96),
        /*oprnegate*/ .oprnegate = Oid(518),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(0),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_INT32LT = std::pair<Oid, PGOperatorPtr>(
    Oid(97),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(97),
        /*oprname*/ .oprname = "<",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = Oid(23),
        /*oprright*/ .oprright = Oid(23),
        /*oprresult*/ .oprresult = Oid(16),
        /*oprcom*/ .oprcom = Oid(521),
        /*oprnegate*/ .oprnegate = Oid(525),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(0),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_INT32NE = std::pair<Oid, PGOperatorPtr>(
    Oid(518),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(518),
        /*oprname*/ .oprname = "<>",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = Oid(23),
        /*oprright*/ .oprright = Oid(23),
        /*oprresult*/ .oprresult = Oid(16),
        /*oprcom*/ .oprcom = Oid(518),
        /*oprnegate*/ .oprnegate = Oid(96),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(0),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_INT32GT = std::pair<Oid, PGOperatorPtr>(
    Oid(521),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(521),
        /*oprname*/ .oprname = ">",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = Oid(23),
        /*oprright*/ .oprright = Oid(23),
        /*oprresult*/ .oprresult = Oid(16),
        /*oprcom*/ .oprcom = Oid(97),
        /*oprnegate*/ .oprnegate = Oid(523),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(0),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_INT32LE = std::pair<Oid, PGOperatorPtr>(
    Oid(523),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(523),
        /*oprname*/ .oprname = "<=",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = Oid(23),
        /*oprright*/ .oprright = Oid(23),
        /*oprresult*/ .oprresult = Oid(16),
        /*oprcom*/ .oprcom = Oid(525),
        /*oprnegate*/ .oprnegate = Oid(521),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(0),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_INT32GE = std::pair<Oid, PGOperatorPtr>(
    Oid(525),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(525),
        /*oprname*/ .oprname = ">=",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = Oid(23),
        /*oprright*/ .oprright = Oid(23),
        /*oprresult*/ .oprresult = Oid(16),
        /*oprcom*/ .oprcom = Oid(523),
        /*oprnegate*/ .oprnegate = Oid(97),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(0),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_INT64EQ = std::pair<Oid, PGOperatorPtr>(
    Oid(410),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(410),
        /*oprname*/ .oprname = "=",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = true,
        /*oprcanhash*/ .oprcanhash = true,
        /*oprleft*/ .oprleft = Oid(20),
        /*oprright*/ .oprright = Oid(20),
        /*oprresult*/ .oprresult = Oid(16),
        /*oprcom*/ .oprcom = Oid(410),
        /*oprnegate*/ .oprnegate = Oid(411),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(0),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_INT64NE = std::pair<Oid, PGOperatorPtr>(
    Oid(411),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(411),
        /*oprname*/ .oprname = "<>",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = Oid(20),
        /*oprright*/ .oprright = Oid(20),
        /*oprresult*/ .oprresult = Oid(16),
        /*oprcom*/ .oprcom = Oid(411),
        /*oprnegate*/ .oprnegate = Oid(410),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(0),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_INT64LT = std::pair<Oid, PGOperatorPtr>(
    Oid(412),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(412),
        /*oprname*/ .oprname = "<",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = Oid(20),
        /*oprright*/ .oprright = Oid(20),
        /*oprresult*/ .oprresult = Oid(16),
        /*oprcom*/ .oprcom = Oid(413),
        /*oprnegate*/ .oprnegate = Oid(415),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(0),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_INT64GT = std::pair<Oid, PGOperatorPtr>(
    Oid(413),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(413),
        /*oprname*/ .oprname = ">",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = Oid(20),
        /*oprright*/ .oprright = Oid(20),
        /*oprresult*/ .oprresult = Oid(16),
        /*oprcom*/ .oprcom = Oid(412),
        /*oprnegate*/ .oprnegate = Oid(414),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(0),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_INT64LE = std::pair<Oid, PGOperatorPtr>(
    Oid(414),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(414),
        /*oprname*/ .oprname = "<=",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = Oid(20),
        /*oprright*/ .oprright = Oid(20),
        /*oprresult*/ .oprresult = Oid(16),
        /*oprcom*/ .oprcom = Oid(415),
        /*oprnegate*/ .oprnegate = Oid(413),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(0),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_INT64GE = std::pair<Oid, PGOperatorPtr>(
    Oid(415),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(415),
        /*oprname*/ .oprname = ">=",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = Oid(20),
        /*oprright*/ .oprright = Oid(20),
        /*oprresult*/ .oprresult = Oid(16),
        /*oprcom*/ .oprcom = Oid(414),
        /*oprnegate*/ .oprnegate = Oid(412),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(0),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_STRINGEQ = std::pair<Oid, PGOperatorPtr>(
    Oid(98),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(98),
        /*oprname*/ .oprname = "=",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = true,
        /*oprcanhash*/ .oprcanhash = true,
        /*oprleft*/ .oprleft = Oid(25),
        /*oprright*/ .oprright = Oid(25),
        /*oprresult*/ .oprresult = Oid(16),
        /*oprcom*/ .oprcom = Oid(98),
        /*oprnegate*/ .oprnegate = Oid(531),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(0),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_STRINGNE = std::pair<Oid, PGOperatorPtr>(
    Oid(531),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(531),
        /*oprname*/ .oprname = "<>",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = Oid(25),
        /*oprright*/ .oprright = Oid(25),
        /*oprresult*/ .oprresult = Oid(16),
        /*oprcom*/ .oprcom = Oid(531),
        /*oprnegate*/ .oprnegate = Oid(98),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(0),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_STRINGLT = std::pair<Oid, PGOperatorPtr>(
    Oid(664),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(664),
        /*oprname*/ .oprname = "<",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = Oid(25),
        /*oprright*/ .oprright = Oid(25),
        /*oprresult*/ .oprresult = Oid(16),
        /*oprcom*/ .oprcom = Oid(666),
        /*oprnegate*/ .oprnegate = Oid(667),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(0),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_STRINGLE = std::pair<Oid, PGOperatorPtr>(
    Oid(665),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(665),
        /*oprname*/ .oprname = "<=",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = Oid(25),
        /*oprright*/ .oprright = Oid(25),
        /*oprresult*/ .oprresult = Oid(16),
        /*oprcom*/ .oprcom = Oid(667),
        /*oprnegate*/ .oprnegate = Oid(666),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(0),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_STRINGGT = std::pair<Oid, PGOperatorPtr>(
    Oid(666),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(666),
        /*oprname*/ .oprname = ">",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = Oid(25),
        /*oprright*/ .oprright = Oid(25),
        /*oprresult*/ .oprresult = Oid(16),
        /*oprcom*/ .oprcom = Oid(664),
        /*oprnegate*/ .oprnegate = Oid(665),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(0),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_STRINGGE = std::pair<Oid, PGOperatorPtr>(
    Oid(667),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(667),
        /*oprname*/ .oprname = ">=",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = Oid(25),
        /*oprright*/ .oprright = Oid(25),
        /*oprresult*/ .oprresult = Oid(16),
        /*oprcom*/ .oprcom = Oid(665),
        /*oprnegate*/ .oprnegate = Oid(664),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(0),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_FIXEDSTRINGEQ = std::pair<Oid, PGOperatorPtr>(
    Oid(1054),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(1054),
        /*oprname*/ .oprname = "=",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = true,
        /*oprcanhash*/ .oprcanhash = true,
        /*oprleft*/ .oprleft = Oid(1042),
        /*oprright*/ .oprright = Oid(1042),
        /*oprresult*/ .oprresult = Oid(16),
        /*oprcom*/ .oprcom = Oid(1054),
        /*oprnegate*/ .oprnegate = Oid(1057),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(0),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_FIXEDSTRINGNE = std::pair<Oid, PGOperatorPtr>(
    Oid(1057),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(1057),
        /*oprname*/ .oprname = "<>",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = Oid(1042),
        /*oprright*/ .oprright = Oid(1042),
        /*oprresult*/ .oprresult = Oid(16),
        /*oprcom*/ .oprcom = Oid(1057),
        /*oprnegate*/ .oprnegate = Oid(1054),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(0),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_FIXEDSTRINGLT = std::pair<Oid, PGOperatorPtr>(
    Oid(1058),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(1058),
        /*oprname*/ .oprname = "<",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = Oid(1042),
        /*oprright*/ .oprright = Oid(1042),
        /*oprresult*/ .oprresult = Oid(16),
        /*oprcom*/ .oprcom = Oid(1060),
        /*oprnegate*/ .oprnegate = Oid(1061),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(0),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_FIXEDSTRINGLE = std::pair<Oid, PGOperatorPtr>(
    Oid(1059),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(1059),
        /*oprname*/ .oprname = "<=",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = Oid(1042),
        /*oprright*/ .oprright = Oid(1042),
        /*oprresult*/ .oprresult = Oid(16),
        /*oprcom*/ .oprcom = Oid(1061),
        /*oprnegate*/ .oprnegate = Oid(1060),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(0),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_FIXEDSTRINGGT = std::pair<Oid, PGOperatorPtr>(
    Oid(1060),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(1060),
        /*oprname*/ .oprname = ">",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = Oid(1042),
        /*oprright*/ .oprright = Oid(1042),
        /*oprresult*/ .oprresult = Oid(16),
        /*oprcom*/ .oprcom = Oid(1058),
        /*oprnegate*/ .oprnegate = Oid(1059),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(0),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_FIXEDSTRINGGE = std::pair<Oid, PGOperatorPtr>(
    Oid(1061),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(1061),
        /*oprname*/ .oprname = ">=",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = Oid(1042),
        /*oprright*/ .oprright = Oid(1042),
        /*oprresult*/ .oprresult = Oid(16),
        /*oprcom*/ .oprcom = Oid(1059),
        /*oprnegate*/ .oprnegate = Oid(1058),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(0),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_DATEEQ = std::pair<Oid, PGOperatorPtr>(
    Oid(1093),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(1093),
        /*oprname*/ .oprname = "=",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = true,
        /*oprcanhash*/ .oprcanhash = true,
        /*oprleft*/ .oprleft = Oid(1082),
        /*oprright*/ .oprright = Oid(1082),
        /*oprresult*/ .oprresult = Oid(16),
        /*oprcom*/ .oprcom = Oid(1093),
        /*oprnegate*/ .oprnegate = Oid(1094),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(0),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_DATENE = std::pair<Oid, PGOperatorPtr>(
    Oid(1094),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(1094),
        /*oprname*/ .oprname = "<>",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = Oid(1082),
        /*oprright*/ .oprright = Oid(1082),
        /*oprresult*/ .oprresult = Oid(16),
        /*oprcom*/ .oprcom = Oid(1094),
        /*oprnegate*/ .oprnegate = Oid(1093),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(0),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_DATELT = std::pair<Oid, PGOperatorPtr>(
    Oid(1095),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(1095),
        /*oprname*/ .oprname = "<",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = Oid(1082),
        /*oprright*/ .oprright = Oid(1082),
        /*oprresult*/ .oprresult = Oid(16),
        /*oprcom*/ .oprcom = Oid(1097),
        /*oprnegate*/ .oprnegate = Oid(1098),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(0),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_DATELE = std::pair<Oid, PGOperatorPtr>(
    Oid(1096),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(1096),
        /*oprname*/ .oprname = "<=",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = Oid(1082),
        /*oprright*/ .oprright = Oid(1082),
        /*oprresult*/ .oprresult = Oid(16),
        /*oprcom*/ .oprcom = Oid(1098),
        /*oprnegate*/ .oprnegate = Oid(1097),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(0),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_DATEGT = std::pair<Oid, PGOperatorPtr>(
    Oid(1097),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(1097),
        /*oprname*/ .oprname = ">",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = Oid(1082),
        /*oprright*/ .oprright = Oid(1082),
        /*oprresult*/ .oprresult = Oid(16),
        /*oprcom*/ .oprcom = Oid(1095),
        /*oprnegate*/ .oprnegate = Oid(1096),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(0),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_DATEGE = std::pair<Oid, PGOperatorPtr>(
    Oid(1098),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(1098),
        /*oprname*/ .oprname = ">=",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = Oid(1082),
        /*oprright*/ .oprright = Oid(1082),
        /*oprresult*/ .oprresult = Oid(16),
        /*oprcom*/ .oprcom = Oid(1096),
        /*oprnegate*/ .oprnegate = Oid(1095),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(0),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_DATETIMEEQ = std::pair<Oid, PGOperatorPtr>(
    Oid(2060),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(2060),
        /*oprname*/ .oprname = "=",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = true,
        /*oprcanhash*/ .oprcanhash = true,
        /*oprleft*/ .oprleft = Oid(1114),
        /*oprright*/ .oprright = Oid(1114),
        /*oprresult*/ .oprresult = Oid(16),
        /*oprcom*/ .oprcom = Oid(2060),
        /*oprnegate*/ .oprnegate = Oid(2061),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(0),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_DATETIMENE = std::pair<Oid, PGOperatorPtr>(
    Oid(2061),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(2061),
        /*oprname*/ .oprname = "<>",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = Oid(1114),
        /*oprright*/ .oprright = Oid(1114),
        /*oprresult*/ .oprresult = Oid(16),
        /*oprcom*/ .oprcom = Oid(2061),
        /*oprnegate*/ .oprnegate = Oid(2060),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(0),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_DATETIMELT = std::pair<Oid, PGOperatorPtr>(
    Oid(2062),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(2062),
        /*oprname*/ .oprname = "<",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = Oid(1114),
        /*oprright*/ .oprright = Oid(1114),
        /*oprresult*/ .oprresult = Oid(16),
        /*oprcom*/ .oprcom = Oid(2064),
        /*oprnegate*/ .oprnegate = Oid(2065),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(0),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_DATETIMELE = std::pair<Oid, PGOperatorPtr>(
    Oid(2063),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(2063),
        /*oprname*/ .oprname = "<=",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = Oid(1114),
        /*oprright*/ .oprright = Oid(1114),
        /*oprresult*/ .oprresult = Oid(16),
        /*oprcom*/ .oprcom = Oid(2065),
        /*oprnegate*/ .oprnegate = Oid(2064),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(0),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_DATETIMEGT = std::pair<Oid, PGOperatorPtr>(
    Oid(2064),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(2064),
        /*oprname*/ .oprname = ">",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = Oid(1114),
        /*oprright*/ .oprright = Oid(1114),
        /*oprresult*/ .oprresult = Oid(16),
        /*oprcom*/ .oprcom = Oid(2062),
        /*oprnegate*/ .oprnegate = Oid(2063),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(0),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_DATETIMEGE = std::pair<Oid, PGOperatorPtr>(
    Oid(2065),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(2065),
        /*oprname*/ .oprname = ">=",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = Oid(1114),
        /*oprright*/ .oprright = Oid(1114),
        /*oprresult*/ .oprresult = Oid(16),
        /*oprcom*/ .oprcom = Oid(2063),
        /*oprnegate*/ .oprnegate = Oid(2062),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(0),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_DATETIME64EQ = std::pair<Oid, PGOperatorPtr>(
    Oid(1320),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(1320),
        /*oprname*/ .oprname = "=",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = true,
        /*oprcanhash*/ .oprcanhash = true,
        /*oprleft*/ .oprleft = Oid(1184),
        /*oprright*/ .oprright = Oid(1184),
        /*oprresult*/ .oprresult = Oid(16),
        /*oprcom*/ .oprcom = Oid(1320),
        /*oprnegate*/ .oprnegate = Oid(1321),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(0),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_DATETIME64NE = std::pair<Oid, PGOperatorPtr>(
    Oid(1321),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(1321),
        /*oprname*/ .oprname = "<>",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = Oid(1184),
        /*oprright*/ .oprright = Oid(1184),
        /*oprresult*/ .oprresult = Oid(16),
        /*oprcom*/ .oprcom = Oid(1321),
        /*oprnegate*/ .oprnegate = Oid(1320),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(0),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_DATETIME64LT = std::pair<Oid, PGOperatorPtr>(
    Oid(1322),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(1322),
        /*oprname*/ .oprname = "<",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = Oid(1184),
        /*oprright*/ .oprright = Oid(1184),
        /*oprresult*/ .oprresult = Oid(16),
        /*oprcom*/ .oprcom = Oid(1324),
        /*oprnegate*/ .oprnegate = Oid(1325),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(0),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_DATETIME64LE = std::pair<Oid, PGOperatorPtr>(
    Oid(1323),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(1323),
        /*oprname*/ .oprname = "<=",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = Oid(1184),
        /*oprright*/ .oprright = Oid(1184),
        /*oprresult*/ .oprresult = Oid(16),
        /*oprcom*/ .oprcom = Oid(1325),
        /*oprnegate*/ .oprnegate = Oid(1324),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(0),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_DATETIME64GT = std::pair<Oid, PGOperatorPtr>(
    Oid(1324),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(1324),
        /*oprname*/ .oprname = ">",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = Oid(1184),
        /*oprright*/ .oprright = Oid(1184),
        /*oprresult*/ .oprresult = Oid(16),
        /*oprcom*/ .oprcom = Oid(1322),
        /*oprnegate*/ .oprnegate = Oid(1323),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(0),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_DATETIME64GE = std::pair<Oid, PGOperatorPtr>(
    Oid(1325),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(1325),
        /*oprname*/ .oprname = ">=",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = Oid(1184),
        /*oprright*/ .oprright = Oid(1184),
        /*oprresult*/ .oprresult = Oid(16),
        /*oprcom*/ .oprcom = Oid(1323),
        /*oprnegate*/ .oprnegate = Oid(1322),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(0),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_DECIMAL64EQ = std::pair<Oid, PGOperatorPtr>(
    Oid(1752),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(1752),
        /*oprname*/ .oprname = "=",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = true,
        /*oprcanhash*/ .oprcanhash = true,
        /*oprleft*/ .oprleft = Oid(1700),
        /*oprright*/ .oprright = Oid(1700),
        /*oprresult*/ .oprresult = Oid(16),
        /*oprcom*/ .oprcom = Oid(1752),
        /*oprnegate*/ .oprnegate = Oid(1753),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(0),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_DECIMAL64NE = std::pair<Oid, PGOperatorPtr>(
    Oid(1753),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(1753),
        /*oprname*/ .oprname = "<>",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = Oid(1700),
        /*oprright*/ .oprright = Oid(1700),
        /*oprresult*/ .oprresult = Oid(16),
        /*oprcom*/ .oprcom = Oid(1753),
        /*oprnegate*/ .oprnegate = Oid(1752),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(0),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_DECIMAL64LT = std::pair<Oid, PGOperatorPtr>(
    Oid(1754),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(1754),
        /*oprname*/ .oprname = "<",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = Oid(1700),
        /*oprright*/ .oprright = Oid(1700),
        /*oprresult*/ .oprresult = Oid(16),
        /*oprcom*/ .oprcom = Oid(1756),
        /*oprnegate*/ .oprnegate = Oid(1757),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(0),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_DECIMAL64LE = std::pair<Oid, PGOperatorPtr>(
    Oid(1755),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(1755),
        /*oprname*/ .oprname = "<=",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = Oid(1700),
        /*oprright*/ .oprright = Oid(1700),
        /*oprresult*/ .oprresult = Oid(16),
        /*oprcom*/ .oprcom = Oid(1757),
        /*oprnegate*/ .oprnegate = Oid(1756),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(0),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_DECIMAL64GT = std::pair<Oid, PGOperatorPtr>(
    Oid(1756),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(1756),
        /*oprname*/ .oprname = ">",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = Oid(1700),
        /*oprright*/ .oprright = Oid(1700),
        /*oprresult*/ .oprresult = Oid(16),
        /*oprcom*/ .oprcom = Oid(1754),
        /*oprnegate*/ .oprnegate = Oid(1755),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(0),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

std::pair<Oid, PGOperatorPtr> OperProvider::OPER_DECIMAL64GE = std::pair<Oid, PGOperatorPtr>(
    Oid(1757),
    std::make_shared<Form_pg_operator>(Form_pg_operator{
        .oid = Oid(1757),
        /*oprname*/ .oprname = ">=",
        /*oprnamespace*/ .oprnamespace = Oid(1),
        /*oprowner*/ .oprowner = Oid(1),
        /*oprkind*/ .oprkind = 'b',
        /*oprcanmerge*/ .oprcanmerge = false,
        /*oprcanhash*/ .oprcanhash = false,
        /*oprleft*/ .oprleft = Oid(1700),
        /*oprright*/ .oprright = Oid(1700),
        /*oprresult*/ .oprresult = Oid(16),
        /*oprcom*/ .oprcom = Oid(1755),
        /*oprnegate*/ .oprnegate = Oid(1754),
        /*oprlsortop*/ .oprlsortop = Oid(0),
        /*oprrsortop*/ .oprrsortop = Oid(0),
        /*oprltcmpop*/ .oprltcmpop = Oid(0),
        /*oprgtcmpop*/ .oprgtcmpop = Oid(0),
        /*oprcode*/ .oprcode = Oid(0),
        /*oprrest*/ .oprrest = Oid(0),
        /*oprjoin*/ .oprjoin = Oid(0)}));

OperProvider::OperProvider(const ContextPtr& context_) : context(context_)
{
	oid_oper_map.insert(OPER_INT2PL);
	oid_oper_map.insert(OPER_INT4PL);
	oid_oper_map.insert(OPER_INT24PL);
	oid_oper_map.insert(OPER_INT42PL);
    oid_oper_map.insert(OPER_INT2MI);
    oid_oper_map.insert(OPER_INT4MI);
    oid_oper_map.insert(OPER_INT24MI);
    oid_oper_map.insert(OPER_INT42MI);
    oid_oper_map.insert(OPER_INT2MUL);
    oid_oper_map.insert(OPER_INT4MUL);
    oid_oper_map.insert(OPER_INT24MUL);
    oid_oper_map.insert(OPER_INT42MUL);
    oid_oper_map.insert(OPER_INT2DIV);
    oid_oper_map.insert(OPER_INT4DIV);
    oid_oper_map.insert(OPER_INT24DIV);
    oid_oper_map.insert(OPER_INT42DIV);
    oid_oper_map.insert(OPER_FLOAT32EQ);
    oid_oper_map.insert(OPER_FLOAT32NE);
    oid_oper_map.insert(OPER_FLOAT32LT);
    oid_oper_map.insert(OPER_FLOAT32LE);
    oid_oper_map.insert(OPER_FLOAT32GT);
    oid_oper_map.insert(OPER_FLOAT32GE);
    oid_oper_map.insert(OPER_FLOAT64EQ);
    oid_oper_map.insert(OPER_FLOAT64NE);
    oid_oper_map.insert(OPER_FLOAT64LT);
    oid_oper_map.insert(OPER_FLOAT64LE);
    oid_oper_map.insert(OPER_FLOAT64GT);
    oid_oper_map.insert(OPER_FLOAT64GE);
    oid_oper_map.insert(OPER_BOOLEQ);
    oid_oper_map.insert(OPER_BOOLNE);
    oid_oper_map.insert(OPER_BOOLLT);
    oid_oper_map.insert(OPER_BOOLLE);
    oid_oper_map.insert(OPER_BOOLGT);
    oid_oper_map.insert(OPER_BOOLGE);
    oid_oper_map.insert(OPER_INT16EQ);
    oid_oper_map.insert(OPER_INT16NE);
    oid_oper_map.insert(OPER_INT16LT);
    oid_oper_map.insert(OPER_INT16LE);
    oid_oper_map.insert(OPER_INT16GT);
    oid_oper_map.insert(OPER_INT16GE);
    oid_oper_map.insert(OPER_INT32EQ);
    oid_oper_map.insert(OPER_INT32NE);
    oid_oper_map.insert(OPER_INT32LT);
    oid_oper_map.insert(OPER_INT32LE);
    oid_oper_map.insert(OPER_INT32GT);
    oid_oper_map.insert(OPER_INT32GE);
    oid_oper_map.insert(OPER_INT64EQ);
    oid_oper_map.insert(OPER_INT64NE);
    oid_oper_map.insert(OPER_INT64LT);
    oid_oper_map.insert(OPER_INT64LE);
    oid_oper_map.insert(OPER_INT64GT);
    oid_oper_map.insert(OPER_INT64GE);
    oid_oper_map.insert(OPER_STRINGEQ);
    oid_oper_map.insert(OPER_STRINGNE);
    oid_oper_map.insert(OPER_STRINGLT);
    oid_oper_map.insert(OPER_STRINGLE);
    oid_oper_map.insert(OPER_STRINGGT);
    oid_oper_map.insert(OPER_STRINGGE);
    oid_oper_map.insert(OPER_FIXEDSTRINGEQ);
    oid_oper_map.insert(OPER_FIXEDSTRINGNE);
    oid_oper_map.insert(OPER_FIXEDSTRINGLT);
    oid_oper_map.insert(OPER_FIXEDSTRINGLE);
    oid_oper_map.insert(OPER_FIXEDSTRINGGT);
    oid_oper_map.insert(OPER_FIXEDSTRINGGE);
    oid_oper_map.insert(OPER_DATEEQ);
    oid_oper_map.insert(OPER_DATENE);
    oid_oper_map.insert(OPER_DATELT);
    oid_oper_map.insert(OPER_DATELE);
    oid_oper_map.insert(OPER_DATEGT);
    oid_oper_map.insert(OPER_DATEGE);
    oid_oper_map.insert(OPER_DATETIMEEQ);
    oid_oper_map.insert(OPER_DATETIMENE);
    oid_oper_map.insert(OPER_DATETIMELT);
    oid_oper_map.insert(OPER_DATETIMELE);
    oid_oper_map.insert(OPER_DATETIMEGT);
    oid_oper_map.insert(OPER_DATETIMEGE);
    oid_oper_map.insert(OPER_DATETIME64EQ);
    oid_oper_map.insert(OPER_DATETIME64NE);
    oid_oper_map.insert(OPER_DATETIME64LT);
    oid_oper_map.insert(OPER_DATETIME64LE);
    oid_oper_map.insert(OPER_DATETIME64GT);
    oid_oper_map.insert(OPER_DATETIME64GE);
    oid_oper_map.insert(OPER_DECIMAL64EQ);
    oid_oper_map.insert(OPER_DECIMAL64NE);
    oid_oper_map.insert(OPER_DECIMAL64LT);
    oid_oper_map.insert(OPER_DECIMAL64LE);
    oid_oper_map.insert(OPER_DECIMAL64GT);
    oid_oper_map.insert(OPER_DECIMAL64GE);
};

PGOperatorPtr
OperProvider::getOperByOID(Oid oid) const
{
	auto it = oid_oper_map.find(oid);
	if (it == oid_oper_map.end())
	    return nullptr;
	return it->second;
};

Oid
OperProvider::getOperByName(duckdb_libpgquery::PGList *names, Oid oprleft, Oid oprright) const
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
        newResult->args = new Oid[newResult->nargs];
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
Oid OperProvider::get_opcode(Oid opno)
{
    PGOperatorPtr op = getOperByOID(opno);
    if (op != NULL)
    {
        return op->oprcode;
    }
    else
        return InvalidOid;
};

Oid OperProvider::get_commutator(Oid opno)
{
    PGOperatorPtr op = getOperByOID(opno);
    if (op != NULL)
    {
        return op->oprcom;
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

PGSortGroupOperPtr OperProvider::get_sort_grp_oper_by_typeid(Oid type_id)
{
    auto result = std::make_shared<Sort_group_operator>();

    auto type_entry = type_provider->getTypeByOid(type_id);

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
