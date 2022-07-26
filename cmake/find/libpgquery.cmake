option(ENABLE_LIBPGQUERY "Enalbe libpq_query" ${ENABLE_LIBRARIES})

#if (NOT ENABLE_LIBPGQUERY)
#    return()
#endif()

if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/libpg_query/CMakeLists.txt")
    message (WARNING "submodule contrib/libpg_query is missing. to fix try run: \n git submodule update --init --recursive")
    message (${RECONFIGURE_MESSAGE_LEVEL} "Can't find internal libpg_query library")
    set (ENABLE_LIBPGQUERY 0)
    return()
endif()

set (USE_LIBPGQUERY 1)
set (LIBPGQUERY_LIBRARY libpg_query)
set (LIBPGQUERY_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/libpg_query/include")
set (LIBPGQUERY_ROOT_DIR "${ClickHouse_SOURCE_DIR}/contrib/libpg_query")
message (STATUS "Using libpg_query=${USE_LIBPGQUERY}: ${LIBPGQUERY_INCLUDE_DIR} : ${LIBPGQUERY_LIBRARY}")
