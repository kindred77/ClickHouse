set(LIBPGQUERY_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/libpg_query")

project(duckdb_pg_query CXX C)
if(POLICY CMP0063)
    cmake_policy(SET CMP0063 NEW)
endif()
set(CMAKE_CXX_VISIBILITY_PRESET hidden)

add_library(duckdb_pg_query STATIC
            "${LIBPGQUERY_SOURCE_DIR}/postgres_parser.cpp"
            "${LIBPGQUERY_SOURCE_DIR}/pg_functions.cpp"
#            "${LIBPGQUERY_SOURCE_DIR}/pg_copyfuncs.cpp"
            "${LIBPGQUERY_SOURCE_DIR}/src_backend_nodes_bitmapset.cpp"
            "${LIBPGQUERY_SOURCE_DIR}/src_backend_parser_parser.cpp"
            "${LIBPGQUERY_SOURCE_DIR}/src_backend_parser_gram.cpp"
            "${LIBPGQUERY_SOURCE_DIR}/src_backend_nodes_list.cpp"
            "${LIBPGQUERY_SOURCE_DIR}/src_backend_nodes_funcs.cpp"
            "${LIBPGQUERY_SOURCE_DIR}/src_backend_parser_scansup.cpp"
            "${LIBPGQUERY_SOURCE_DIR}/src_backend_nodes_makefuncs.cpp"
            "${LIBPGQUERY_SOURCE_DIR}/src_backend_nodes_value.cpp"
            "${LIBPGQUERY_SOURCE_DIR}/src_backend_parser_scan.cpp"
            "${LIBPGQUERY_SOURCE_DIR}/src_backend_common_datum.cpp"
            "${LIBPGQUERY_SOURCE_DIR}/src_backend_common_equalfuncs.cpp"
            "${LIBPGQUERY_SOURCE_DIR}/src_backend_common_walkers.cpp"
            "${LIBPGQUERY_SOURCE_DIR}/src_backend_parser_common.cpp"
            "${LIBPGQUERY_SOURCE_DIR}/src_common_keywords.cpp")

target_include_directories(
  duckdb_pg_query
  SYSTEM
  PUBLIC "${LIBPGQUERY_SOURCE_DIR}/include")
set_target_properties(duckdb_pg_query PROPERTIES EXPORT_NAME duckdb_duckdb_pg_query)

message (STATUS "Using duckdb_pg_query : ${LIBPGQUERY_SOURCE_DIR}/include")

install(TARGETS duckdb_pg_query
        EXPORT "${DUCKDB_EXPORT_SET}"
        LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
        ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

#disable_target_warnings(duckdb_pg_query)

set_target_properties(duckdb_pg_query PROPERTIES POSITION_INDEPENDENT_CODE ON)
