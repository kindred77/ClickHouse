/*--------------------------------------------------------------------
 * Symbols referenced in this file:
 * - bms_is_member
 *--------------------------------------------------------------------
 */

#include "nodes/bitmapset.hpp"
#include "pg_functions.hpp"

#define WORDNUM(x)	((x) / BITS_PER_BITMAPWORD)
#define BITNUM(x)	((x) % BITS_PER_BITMAPWORD)

#define BITMAPSET_SIZE(nwords)	\
	(offsetof(PGBitmapset, words) + (nwords) * sizeof(bitmapword))

namespace duckdb_libpgquery {

/*
 * bms_is_member - is X a member of A?
 */
bool bms_is_member(int x, const PGBitmapset *a)
{
	int			wordnum,
				bitnum;

	/* XXX better to just return false for x<0 ? */
	if (x < 0)
		elog(ERROR, "negative bitmapset member not allowed");
	if (a == NULL)
		return false;
	wordnum = WORDNUM(x);
	bitnum = BITNUM(x);
	if (wordnum >= a->nwords)
		return false;
	if ((a->words[wordnum] & ((bitmapword) 1 << bitnum)) != 0)
		return true;
	return false;
};

/*
 * bms_make_singleton - build a bitmapset containing a single member
 */
PGBitmapset *
bms_make_singleton(int x)
{
	PGBitmapset  *result;
	int			wordnum,
				bitnum;

	if (x < 0)
		elog(ERROR, "negative bitmapset member not allowed");
	wordnum = WORDNUM(x);
	bitnum = BITNUM(x);
	//kindred
	//result = (PGBitmapset *) palloc0(BITMAPSET_SIZE(wordnum + 1));
	result = (PGBitmapset *) palloc(BITMAPSET_SIZE(wordnum + 1));
	result->nwords = wordnum + 1;
	result->words[wordnum] = ((bitmapword) 1 << bitnum);
	return result;
}

/*
 * bms_add_member - add a specified member to set
 *
 * Input set is modified or recycled!
 */
PGBitmapset * bms_add_member(PGBitmapset *a, int x)
{
	int			wordnum,
				bitnum;

	if (x < 0)
		elog(ERROR, "negative bitmapset member not allowed");
	if (a == NULL)
		return bms_make_singleton(x);
	wordnum = WORDNUM(x);
	bitnum = BITNUM(x);

	/* enlarge the set if necessary */
	if (wordnum >= a->nwords)
	{
		int			oldnwords = a->nwords;
		int			i;

		a = (PGBitmapset *) repalloc(a, BITMAPSET_SIZE(wordnum + 1));
		a->nwords = wordnum + 1;
		/* zero out the enlarged portion */
		for (i = oldnwords; i < a->nwords; i++)
			a->words[i] = 0;
	}

	a->words[wordnum] |= ((bitmapword) 1 << bitnum);
	return a;
};

/*
 * bms_is_empty - is a set empty?
 *
 * This is even faster than bms_membership().
 */
bool
bms_is_empty(const PGBitmapset *a)
{
	int			nwords;
	int			wordnum;

	if (a == NULL)
		return true;
	nwords = a->nwords;
	for (wordnum = 0; wordnum < nwords; wordnum++)
	{
		bitmapword	w = a->words[wordnum];

		if (w != 0)
			return false;
	}
	return true;
};

bool bms_equal(const PGBitmapset *a, const PGBitmapset *b)
{
	const PGBitmapset *shorter;
	const PGBitmapset *longer;
	int			shortlen;
	int			longlen;
	int			i;

	/* Handle cases where either input is NULL */
	if (a == NULL)
	{
		if (b == NULL)
			return true;
		return bms_is_empty(b);
	}
	else if (b == NULL)
		return bms_is_empty(a);
	/* Identify shorter and longer input */
	if (a->nwords <= b->nwords)
	{
		shorter = a;
		longer = b;
	}
	else
	{
		shorter = b;
		longer = a;
	}
	/* And process */
	shortlen = shorter->nwords;
	for (i = 0; i < shortlen; i++)
	{
		if (shorter->words[i] != longer->words[i])
			return false;
	}
	longlen = longer->nwords;
	for (; i < longlen; i++)
	{
		if (longer->words[i] != 0)
			return false;
	}
	return true;
}

}
