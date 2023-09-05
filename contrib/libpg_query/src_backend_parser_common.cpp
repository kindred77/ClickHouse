#include "common/parser_common.hpp"
#include "common/common_macro.hpp"
#include "common/common_datum.hpp"

namespace duckdb_libpgquery {

bool SQL_inheritance = true;

PGOid MyDatabaseId = InvalidOid;

ErrorContextCallback *error_context_stack = NULL;

bool optimizer_multilevel_partitioning = false;

PGTupleDescPtr PGCreateTemplateTupleDesc(int natts, bool hasoid)
{
	//TODO kindred
    // PGTupleDesc * desc;
    // char * stg;
    // int attroffset;

    // /*
	//  * sanity checks
	//  */
    // Assert(natts >= 0)

    /*
	 * Allocate enough memory for the tuple descriptor, including the
	 * attribute rows, and set up the attribute row pointers.
	 *
	 * Note: we assume that sizeof(struct tupleDesc) is a multiple of the
	 * struct pointer alignment requirement, and hence we don't need to insert
	 * alignment padding between the struct and the array of attribute row
	 * pointers.
	 *
	 * Note: Only the fixed part of pg_attribute rows is included in tuple
	 * descriptors, so we only need ATTRIBUTE_FIXED_PART_SIZE space per attr.
	 * That might need alignment padding, however.
	 */
    // attroffset = sizeof(struct tupleDesc) + natts * sizeof(Form_pg_attribute);
    // attroffset = MAXALIGN(attroffset);
    // stg = (char *)palloc(attroffset + natts * MAXALIGN(ATTRIBUTE_FIXED_PART_SIZE));
    // desc = (PGTupleDesc *)stg;

    // if (natts > 0)
    // {
    //     Form_pg_attribute * attrs;
    //     int i;

    //     attrs = (Form_pg_attribute *)(stg + sizeof(struct tupleDesc));
    //     desc->attrs = attrs;
    //     stg += attroffset;
    //     for (i = 0; i < natts; i++)
    //     {
    //         attrs[i] = (Form_pg_attribute)stg;
    //         stg += MAXALIGN(ATTRIBUTE_FIXED_PART_SIZE);
    //     }
    // }
    // else
    //     desc->attrs = NULL;

    /*
	 * Initialize other fields of the tupdesc.
	 */
    // desc->natts = natts;
    // desc->constr = NULL;
    // desc->tdtypeid = RECORDOID;
    // desc->tdtypmod = -1;
    // desc->tdhasoid = hasoid;
    // desc->tdrefcount = -1; /* assume not reference-counted */

    // return desc;

	auto result = std::make_shared<PGTupleDesc>();
	result->natts = natts;
    result->attrs.resize(natts);
	result->tdtypeid = RECORDOID;
    result->tdtypmod = -1;
    result->tdhasoid = hasoid;
    result->tdrefcount = -1;

	return result;
};

PGTupleDescPtr PGCreateTupleDescCopy(PGTupleDescPtr tupdesc)
{
    PGTupleDescPtr desc = PGCreateTemplateTupleDesc(tupdesc->natts, tupdesc->tdhasoid);

    for (int i = 0; i < desc->natts; i++)
    {
        desc->attrs.push_back(std::make_shared<Form_pg_attribute>(*tupdesc->attrs[i].get()));
        desc->attrs[i]->attnotnull = false;
        desc->attrs[i]->atthasdef = false;
    }

    desc->tdtypeid = tupdesc->tdtypeid;
    desc->tdtypmod = tupdesc->tdtypmod;

    return desc;
};

PGTupleDescPtr PGCreateTupleDescCopyConstr(PGTupleDescPtr tupdesc)
{
    PGTupleDescPtr desc = PGCreateTemplateTupleDesc(tupdesc->natts, tupdesc->tdhasoid);

    for (int i = 0; i < desc->natts; i++)
    {
        desc->attrs.push_back(std::make_shared<Form_pg_attribute>(*tupdesc->attrs[i].get()));
    }

    if (tupdesc->constr != nullptr)
    {
        PGTupleConstrPtr cpy = std::make_shared<PGTupleConstr>();

        cpy->has_not_null = tupdesc->constr->has_not_null;

        if ((cpy->num_defval = tupdesc->constr->num_defval) > 0)
        {
            //cpy->defval = (AttrDefault *)palloc(cpy->num_defval * sizeof(AttrDefault));
            //memcpy(cpy->defval, tupdesc->constr->defval, cpy->num_defval * sizeof(AttrDefault));
            for (auto def_val : tupdesc->constr->defval)
            {
                cpy->defval.push_back(std::make_shared<PGAttrDefault>(*def_val.get()));
            }
            for (int i = cpy->num_defval - 1; i >= 0; i--)
            {
                if (tupdesc->constr->defval[i]->adbin != "")
                    cpy->defval[i]->adbin = tupdesc->constr->defval[i]->adbin;
            }
        }

        if ((cpy->num_check = tupdesc->constr->num_check) > 0)
        {
            //cpy->check = (ConstrCheck *)palloc(cpy->num_check * sizeof(ConstrCheck));
            //memcpy(cpy->check, tupdesc->constr->check, cpy->num_check * sizeof(ConstrCheck));
            for (auto chck : tupdesc->constr->check)
            {
                cpy->check.push_back(std::make_shared<PGConstrCheck>(*chck.get()));
            }

            for (int i = cpy->num_check - 1; i >= 0; i--)
            {
                if (tupdesc->constr->check[i]->ccname != "")
                    cpy->check[i]->ccname = tupdesc->constr->check[i]->ccname;
                if (tupdesc->constr->check[i]->ccbin != "")
                    cpy->check[i]->ccbin = tupdesc->constr->check[i]->ccbin;
                cpy->check[i]->ccvalid = tupdesc->constr->check[i]->ccvalid;
                cpy->check[i]->ccnoinherit = tupdesc->constr->check[i]->ccnoinherit;
            }
        }

        desc->constr = cpy;
    }

    desc->tdtypeid = tupdesc->tdtypeid;
    desc->tdtypmod = tupdesc->tdtypmod;

    return desc;
};

void PGTupleDescInitEntryCollation(PGTupleDescPtr desc, PGAttrNumber attributeNumber, PGOid collationid)
{
    /*
	 * sanity checks
	 */
    Assert(PointerIsValid(desc.get()))
    Assert(attributeNumber >= 1)
    Assert(attributeNumber <= desc->natts)

    desc->attrs[attributeNumber - 1]->attcollation = collationid;
};

void PGTupleDescCopyEntry(PGTupleDescPtr dst, PGAttrNumber dstAttno, PGTupleDescPtr src, PGAttrNumber srcAttno)
{
    /*
	 * sanity checks
	 */
    Assert(src != nullptr)
    Assert(dst != nullptr)
    Assert(srcAttno >= 1)
    Assert(srcAttno <= src->natts)
    Assert(dstAttno >= 1)
    Assert(dstAttno <= dst->natts)

	dst->attrs[dstAttno - 1] = src->attrs[srcAttno - 1];
    //memcpy(dst->attrs[dstAttno - 1], src->attrs[srcAttno - 1], ATTRIBUTE_FIXED_PART_SIZE);

    /*
	 * Aside from updating the attno, we'd better reset attcacheoff.
	 *
	 * XXX Actually, to be entirely safe we'd need to reset the attcacheoff of
	 * all following columns in dst as well.  Current usage scenarios don't
	 * require that though, because all following columns will get initialized
	 * by other uses of this function or TupleDescInitEntry.  So we cheat a
	 * bit to avoid a useless O(N^2) penalty.
	 */
    dst->attrs[dstAttno - 1]->attnum = dstAttno;
    dst->attrs[dstAttno - 1]->attcacheoff = -1;

    /* since we're not copying constraints or defaults, clear these */
    dst->attrs[dstAttno - 1]->attnotnull = false;
    dst->attrs[dstAttno - 1]->atthasdef = false;
};

size_t strlcpy(char *dst, const char *src, size_t siz)
{
	char	   *d = dst;
	const char *s = src;
	size_t		n = siz;

	/* Copy as many bytes as will fit */
	if (n != 0)
	{
		while (--n != 0)
		{
			if ((*d++ = *s++) == '\0')
				break;
		}
	}

	/* Not enough room in dst, add NUL and traverse rest of src */
	if (n == 0)
	{
		if (siz != 0)
			*d = '\0';			/* NUL-terminate dst */
		while (*s++)
			;
	}

	return (s - src - 1);		/* count does not include NUL */
};

int count_nonjunk_tlist_entries(duckdb_libpgquery::PGList *tlist)
{
	int			len = 0;
    duckdb_libpgquery::PGListCell * l;

    foreach(l, tlist)
	{
		duckdb_libpgquery::PGTargetEntry *tle = (duckdb_libpgquery::PGTargetEntry *) lfirst(l);

		if (!tle->resjunk)
			len++;
	}
	return len;
};

PGParseState * make_parsestate(PGParseState *parentParseState)
{
	auto pstate = new PGParseState();

	pstate->parentParseState = parentParseState;

	/* Fill in fields that don't start at null/false/zero */
	pstate->p_next_resno = 1;

	if (parentParseState)
	{
		pstate->p_sourcetext = parentParseState->p_sourcetext;
		/* all hooks are copied from parent */
		//pstate->p_pre_columnref_hook = parentParseState->p_pre_columnref_hook;
		//pstate->p_post_columnref_hook = parentParseState->p_post_columnref_hook;
		//pstate->p_paramref_hook = parentParseState->p_paramref_hook;
		//pstate->p_coerce_param_hook = parentParseState->p_coerce_param_hook;
		pstate->p_ref_hook_state = parentParseState->p_ref_hook_state;
	}

	return pstate;
};

duckdb_libpgquery::PGValue * makeString(char * str)
{
	using duckdb_libpgquery::PGValue;
	using duckdb_libpgquery::T_PGValue;
    PGValue * v = makeNode(PGValue);

    v->type = duckdb_libpgquery::T_PGString;
    v->val.str = str;
    return v;
};

int parser_errposition(PGParseState *pstate, int location)
{
	int			pos;

	/* No-op if location was not provided */
	if (location < 0)
		return 0;
	/* Can't do anything if source text is not available */
	if (pstate == NULL || pstate->p_sourcetext == NULL)
		return 0;
	/* Convert offset to character number */
	pos = duckdb_libpgquery::pg_mbstrlen_with_len(pstate->p_sourcetext, location) + 1;
	/* And pass it to the ereport mechanism */
	return duckdb_libpgquery::errposition(pos);
};

void free_parsestate(PGParseState *pstate)
{
	delete pstate;
	pstate = NULL;
};

void pcb_error_callback(void * arg)
{
    PGParseCallbackState * pcbstate = (PGParseCallbackState *)arg;

	//TODO kindred
    //if (geterrcode() != ERRCODE_QUERY_CANCELED)
        (void)parser_errposition(pcbstate->pstate, pcbstate->location);
};

void
setup_parser_errposition_callback(PGParseCallbackState *pcbstate,
								  PGParseState *pstate, int location)
{
	/* Setup error traceback support for ereport() */
	pcbstate->pstate = pstate;
	pcbstate->location = location;
	pcbstate->errcallback.callback = pcb_error_callback;
	pcbstate->errcallback.arg = (void *) pcbstate;
	pcbstate->errcallback.previous = error_context_stack;
	error_context_stack = &pcbstate->errcallback;
};

void
cancel_parser_errposition_callback(PGParseCallbackState *pcbstate)
{
	/* Pop the error context stack */
	error_context_stack = pcbstate->errcallback.previous;
};

duckdb_libpgquery::PGTargetEntry * get_sortgroupref_tle(PGIndex sortref, duckdb_libpgquery::PGList * targetList)
{
	using duckdb_libpgquery::errcode;
	using duckdb_libpgquery::ereport;
	using duckdb_libpgquery::errmsg;

    duckdb_libpgquery::PGListCell * l;

    foreach (l, targetList)
    {
        duckdb_libpgquery::PGTargetEntry * tle = (duckdb_libpgquery::PGTargetEntry *)lfirst(l);

        if (tle->ressortgroupref == sortref)
            return tle;
    }

    /*
	 * XXX: we probably should catch this earlier, but we have a
	 * few queries in the regression suite that hit this.
	 */
    ereport(ERROR, (errcode(PG_ERRCODE_SYNTAX_ERROR), errmsg("ORDER/GROUP BY expression not found in targetlist")));
    return NULL; /* keep compiler quiet */
};

duckdb_libpgquery::PGTargetEntry * get_sortgroupclause_tle(
	duckdb_libpgquery::PGSortGroupClause * sgClause, 
	duckdb_libpgquery::PGList * targetList)
{
    return get_sortgroupref_tle(sgClause->tleSortGroupRef, targetList);
};

duckdb_libpgquery::PGNode *
get_sortgroupclause_expr(duckdb_libpgquery::PGSortGroupClause * sgClause, duckdb_libpgquery::PGList * targetList)
{
    duckdb_libpgquery::PGTargetEntry * tle = get_sortgroupclause_tle(sgClause, targetList);

    return (duckdb_libpgquery::PGNode *)tle->expr;
};

bool scanint8(const char * str, bool errorOK, int64 * result)
{
	using duckdb_libpgquery::ereport;
	using duckdb_libpgquery::errcode;
	using duckdb_libpgquery::errmsg;

    const char * ptr = str;
    int64 tmp = 0;
    int sign = 1;

    /*
	 * Do our own scan, rather than relying on sscanf which might be broken
	 * for long long.
	 */

    /* skip leading spaces */
    while (*ptr && isspace((unsigned char)*ptr))
        ptr++;

    /* handle sign */
    if (*ptr == '-')
    {
        ptr++;

        /*
		 * Do an explicit check for INT64_MIN.  Ugly though this is, it's
		 * cleaner than trying to get the loop below to handle it portably.
		 */
        if (strncmp(ptr, "9223372036854775808", 19) == 0)
        {
            tmp = -INT64CONST(0x7fffffffffffffff) - 1;
            ptr += 19;
            goto gotdigits;
        }
        sign = -1;
    }
    else if (*ptr == '+')
        ptr++;

    /* require at least one digit */
    if (!isdigit((unsigned char)*ptr))
    {
        if (errorOK)
            return false;
        else
            ereport(ERROR, (errcode(PG_ERRCODE_INVALID_TEXT_REPRESENTATION), errmsg("invalid input syntax for integer: \"%s\"", str)));
    }

    /* process digits */
    while (*ptr && isdigit((unsigned char)*ptr))
    {
        int64 newtmp = tmp * 10 + (*ptr++ - '0');

        if ((newtmp / 10) != tmp) /* overflow? */
        {
            if (errorOK)
                return false;
            else
                ereport(ERROR, (errcode(PG_ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("value \"%s\" is out of range for type bigint", str)));
        }
        tmp = newtmp;
    }

gotdigits:

    /* allow trailing whitespace, but not other trailing chars */
    while (*ptr != '\0' && isspace((unsigned char)*ptr))
        ptr++;

    if (*ptr != '\0')
    {
        if (errorOK)
            return false;
        else
            ereport(ERROR, (errcode(PG_ERRCODE_INVALID_TEXT_REPRESENTATION), errmsg("invalid input syntax for integer: \"%s\"", str)));
    }

    *result = (sign < 0) ? -tmp : tmp;

    return true;
};

std::string PGNameListToString(duckdb_libpgquery::PGList * names)
{
	using duckdb_libpgquery::elog;
	using duckdb_libpgquery::PGListCell;
	using duckdb_libpgquery::PGNode;
	using duckdb_libpgquery::T_PGString;
	using duckdb_libpgquery::PGAStar;
	using duckdb_libpgquery::PGValue;
	using duckdb_libpgquery::T_PGAStar;

    std::string result = "";
    PGListCell * l;

    foreach (l, names)
    {
        PGNode * name = (PGNode *)lfirst(l);

        if (l != list_head(names))
		{
            //appendStringInfoChar(&string, '.');
			result += ".";
		}

        if (IsA(name, PGString))
		{
            //appendStringInfoString(&string, strVal(name));
			result += std::string(strVal(name));
		}
        else if (IsA(name, PGAStar))
		{
            //appendStringInfoString(&string, "*");
			result += "*";
		}
        else
            elog(ERROR, "unexpected node type in name list: %d", (int)nodeTag(name));
    }

    return result;
};

void DeconstructQualifiedName(duckdb_libpgquery::PGList * names, char ** nspname_p, char ** objname_p)
{
	using duckdb_libpgquery::ereport;
	using duckdb_libpgquery::errcode;
	using duckdb_libpgquery::errmsg;
	using duckdb_libpgquery::PGValue;

    //char * catalogname;
    char * schemaname = NULL;
    char * objname = NULL;

    switch (list_length(names))
    {
        case 1:
            objname = strVal(linitial(names));
            break;
        case 2:
            schemaname = strVal(linitial(names));
            objname = strVal(lsecond(names));
            break;
        // case 3:
        //     catalogname = strVal(linitial(names));
        //     schemaname = strVal(lsecond(names));
        //     objname = strVal(lthird(names));

        //     /*
		// 	 * We check the catalog name and then ignore it.
		// 	 */
        //     if (strcmp(catalogname, get_database_name(MyDatabaseId)) != 0)
        //         ereport(
        //             ERROR,
        //             (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
        //              errmsg("cross-database references are not implemented: %s", NameListToString(names))));
        //     break;
        default:
            ereport(
                ERROR,
                (errcode(PG_ERRCODE_SYNTAX_ERROR), errmsg("improper qualified name (too many dotted names): %s", PGNameListToString(names).c_str())));
            break;
    }

    *nspname_p = schemaname;
    *objname_p = objname;
};

duckdb_libpgquery::PGList * stringToQualifiedNameList(const char * string)
{
	using duckdb_libpgquery::ereport;
	using duckdb_libpgquery::errcode;
	using duckdb_libpgquery::errmsg;
	using duckdb_libpgquery::pstrdup;
	using duckdb_libpgquery::lappend;
	using duckdb_libpgquery::PGList;
	using duckdb_libpgquery::PGListCell;

    PGList * result = NULL;

	std::string rawname = std::string(string);

	std::vector<std::string> vecSegTag;
	boost::split(vecSegTag, rawname, boost::is_any_of(","));

	if (vecSegTag.size() == 0)
	{
		ereport(ERROR, (errcode(PG_ERRCODE_INVALID_NAME), errmsg("invalid name syntax")));
	}

    for (auto l : vecSegTag)
    {
        result = lappend(result, makeString(pstrdup(l.c_str())));
    }

    return result;
};

duckdb_libpgquery::PGRangeVar * makeRangeVarFromNameList(duckdb_libpgquery::PGList * names)
{
	using duckdb_libpgquery::ereport;
	using duckdb_libpgquery::errcode;
	using duckdb_libpgquery::errmsg;
	using duckdb_libpgquery::PGValue;
	using duckdb_libpgquery::makeRangeVar;

    duckdb_libpgquery::PGRangeVar * rel = makeRangeVar(NULL, NULL, -1);

    switch (list_length(names))
    {
        case 1:
            rel->relname = strVal(linitial(names));
            break;
        case 2:
            rel->schemaname = strVal(linitial(names));
            rel->relname = strVal(lsecond(names));
            break;
        case 3:
            rel->catalogname = strVal(linitial(names));
            rel->schemaname = strVal(lsecond(names));
            rel->relname = strVal(lthird(names));
            break;
        default:
            ereport(
                ERROR,
                (errcode(PG_ERRCODE_SYNTAX_ERROR), errmsg("improper relation name (too many dotted names): %s", NameListToString(names))));
            break;
    }

    return rel;
};

int pg_strcasecmp(const char * s1, const char * s2)
{
    for (;;)
    {
        unsigned char ch1 = (unsigned char)*s1++;
        unsigned char ch2 = (unsigned char)*s2++;

        if (ch1 != ch2)
        {
            if (ch1 >= 'A' && ch1 <= 'Z')
                ch1 += 'a' - 'A';
            else if (IS_HIGHBIT_SET(ch1) && isupper(ch1))
                ch1 = tolower(ch1);

            if (ch2 >= 'A' && ch2 <= 'Z')
                ch2 += 'a' - 'A';
            else if (IS_HIGHBIT_SET(ch2) && isupper(ch2))
                ch2 = tolower(ch2);

            if (ch1 != ch2)
                return (int)ch1 - (int)ch2;
        }
        if (ch1 == 0)
            break;
    }
    return 0;
};

}
