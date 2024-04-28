#pragma once

#include <common/parser_common.hpp>
#include <Interpreters/Context.h>

#include <gpos/memory/CMemoryPool.h>

#include <map>

namespace DB
{
/*
pg_proc  -> CMDFunctionGPDB
*/

class Context;
using ContextPtr = std::shared_ptr<const Context>;

class ProcProvider
{
private:
	using OidProcMap = std::map<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr>;
	static OidProcMap oid_proc_map;

	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_FLOAT8_NUMERIC;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT4_NUMERIC;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT2_NUMERIC;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT8_NUMERIC;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_FLOAT8_INT4;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_FLOAT4_INT4;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT2_INT4;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT2_FLOAT8;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT2_FLOAT4;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_TEXT;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_FLOAT8_INT2;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_FLOAT4_INT2;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT4_INT2;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_FLOAT8;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT4;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT4_FLOAT8;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT2;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT42DIV;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT24DIV;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT4DIV;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT2DIV;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT42MUL;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT4MUL;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT42MI;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT24MI;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT4MI;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_NUMERIC_NE;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_NUMERIC_EQ;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_TIMESTAMPTZ_NE;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_TIMESTAMPTZ_EQ;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_BYTEALE;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_TIMESTAMPTZ_GE;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT24MUL;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_BYTEALT;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_TIMESTAMPTZ_LE;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_BYTEAGT;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_TIMESTAMPTZ_GT;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_BYTEAEQ;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_TIMESTAMPTZ_LT;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_TIMESTAMPTZ_SEND;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_TIMESTAMPTZ_RECV;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT8;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_TIMESTAMPTZ_OUT;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_TIMESTAMPTZ_IN;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_TIMESTAMP_NE;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_TEXTEQ;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_TIMESTAMP_EQ;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT8NE;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_TIMESTAMP_GE;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT8EQ;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_TIMESTAMP_LE;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT8LT;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_TIMESTAMP_GT;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_TIMESTAMP_SEND;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_TIMESTAMP_RECV;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_TIMESTAMP_OUT;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_BPCHARNE;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_BPCHARIN;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_NUMERIC_LE;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_BPCHARTYPMODOUT;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_NUMERIC_LT;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_BPCHARTYPMODIN;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_FLOAT4_NUMERIC;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_TEXTNE;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT2MUL;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_TEXT_GE;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_TEXTSEND;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_TEXTRECV;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_TEXTOUT;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_FLOAT4NE;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_FLOAT4EQ;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_DATE_EQ;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_FLOAT4GE;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_DATE_IN;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_FLOAT4LE;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_DATE_OUT;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_FLOAT4GT;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_FLOAT4LT;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_FLOAT4OUT;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_FLOAT4IN;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT8DEC;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT8DEC_ANY;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT4LT;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT8PL;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_TEXTIN;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT4SEND;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT8INC_ANY;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_ANY_OUT;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_ANY_IN;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT8GE;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT8LE;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT8GT;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT4RECV;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_COUNT;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT8SEND;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_CSTRING_RECV;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_NUMERIC_OUT;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INTERNAL_OUT;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_OIDLE;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_CSTRING_OUT;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_NUMERIC_IN;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT4_FLOAT4;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INTERNAL_IN;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_OIDLT;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_CSTRING_IN;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_OIDNE;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_OIDEQ;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_BPCHAREQ;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_FLOAT8SEND;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT8IN;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT2EQ;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_BOOLIN;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_OIDGE;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_FLOAT8RECV;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_BYTEAIN;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_OIDGT;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_OIDSEND;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_OIDRECV;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_BYTEASEND;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_BYTEAOUT;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT8RECV;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_BYTEARECV;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_TIMESTAMP_LT;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT8_INT4;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_BOOLNE;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_BPCHAROUT;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT4IN;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_FLOAT4RECV;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_BOOLEQ;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_BOOLGT;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_ARRAY_LT;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_NUMERIC_SEND;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_DATE_LE;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_FLOAT8NE;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT4_BOOL;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT2PL;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_FLOAT8GT;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_DATE_NE;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_NEQJOINSEL;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_ARRAY_GE;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_FLOAT8LE;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_DATE_GE;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_DATE_SEND;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_EQJOINSEL;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_ARRAY_EQ;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_FLOAT8_FLOAT4;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_ANYARRAY_IN;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT8_FLOAT4;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_BPCHARLE;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT4EQ;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT24PL;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_BOOLRECV;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_SCALARGTJOINSEL;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_TIMESTAMPTYPMODIN;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT4GT;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_ANYARRAY_SEND;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_TIMESTAMP_IN;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_FLOAT4_FLOAT8;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_ANYARRAY_OUT;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_BPCHARGT;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_BPCHARRECV;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_SCALARGTSEL;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_BOOLGE;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_FLOAT4SEND;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT4OUT;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_SCALARLTSEL;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_BOOLLE;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_TEXT_GT;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT2GE;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_BOOLOUT;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_EQSEL;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_FLOAT8GE;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_FLOAT8LT;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_DATE_GT;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT8_FLOAT8;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_DATE_RECV;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT2GT;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_FLOAT4;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_BPCHARLT;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT8OUT;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT2LT;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_ARRAY_LE;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_FLOAT8EQ;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_DATE_LT;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_NEQSEL;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_OIDIN;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_TIMESTAMPTZTYPMODIN;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_TEXT_LT;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT4LE;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_ANYARRAY_RECV;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT2SEND;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_TIMESTAMPTYPMODOUT;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT2LE;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_ARRAY_GT;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_CSTRING_SEND;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT2RECV;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT2OUT;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_ARRAY_NE;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_NUMERIC_RECV;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_FLOAT8IN;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_OIDOUT;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_TIMESTAMPTZTYPMODOUT;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_TEXT_LE;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT4GE;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_BOOLSEND;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_ARRAY_TYPANALYZE;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_BOOLLT;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT42PL;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_BYTEAGE;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_BPCHARGE;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_BPCHARSEND;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT8INC;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_ARRAY_SEND;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_NUMERIC_GE;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT2NE;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT2IN;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT4PL;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_COUNT_ANY;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_SCALARLTJOINSEL;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_ARRAY_RECV;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_NUMERICTYPMODOUT;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_ARRAY_OUT;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_FLOAT8OUT;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT2MI;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_BYTEANE;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_NUMERIC_GT;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_INT4NE;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_NUMERICTYPMODIN;
	static std::pair<duckdb_libpgquery::PGOid, duckdb_libpgquery::PGProcPtr> PROC_ARRAY_IN;


	//ContextPtr context;
public:
	//explicit ProcProvider(gpos::CMemoryPool *mp_, ContextPtr context);
	//explicit ProcProvider(const ContextPtr& context_);
	
	static duckdb_libpgquery::PGProcPtr getProcByOid(duckdb_libpgquery::PGOid oid);

	static std::optional<std::string> get_func_name(duckdb_libpgquery::PGOid oid);

	static duckdb_libpgquery::PGOid get_func_rettype(duckdb_libpgquery::PGOid funcid);

	static bool func_strict(duckdb_libpgquery::PGOid funcid);

	static std::unique_ptr<std::vector<duckdb_libpgquery::PGProcPtr>> search_procs_by_name(const std::string & func_name);

    static bool get_func_retset(duckdb_libpgquery::PGOid funcid);
};

}
