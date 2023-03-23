#include <Interpreters/orcaopt/provider/OperProvider.h>

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

OperProvider::OperProvider()
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
        newResult->args = new Oid[2];
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

bool OperProvider::get_ordering_op_properties(Oid opno, Oid * opfamily, Oid * opcintype, int16 * strategy)
{
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

    return true;
};

Oid OperProvider::get_opfamily_member(Oid opfamily, Oid lefttype, Oid righttype, int16 strategy)
{
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

    return InvalidOid;
};

Oid OperProvider::get_equality_op_for_ordering_op(Oid opno, bool * reverse)
{
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

    return InvalidOid;
};

bool OperProvider::op_hashjoinable(Oid opno, Oid inputtype)
{
    bool result = false;
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
        PGOperatorPtr op = getOperByOID(opno);
        if (op != NULL)
        {
            result = op->oprcanhash;
        }
    // }
    return result;
};

PGSortGroupOperPtr OperProvider::get_sort_group_operators(Oid type_id)
{
    auto result = std::make_shared<Sort_group_operator>();
    return result;
};

}
