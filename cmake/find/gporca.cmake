OPTION(ENABLE_GPORCA "Enable gporca" ${ENABLE_LIBRARIES})

if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/gporca/gporca.mk")
    message (WARNING "submodule contrib/gporca is missing. to fix try run: \n git submodule update --init --recursive")
    set (ENABLE_GPORCA 0)
endif ()

if (NOT CMAKE_SYSTEM_NAME MATCHES "Linux" AND NOT (CMAKE_SYSTEM_NAME MATCHES "Darwin" AND NOT CMAKE_CROSSCOMPILING))
    message (WARNING "gporca disabled in non-Linux and non-native-Darwin environments")
    set (ENABLE_GPORCA 0)
endif ()

if (ENABLE_GPORCA)

    set (USE_GPORCA 1)
    set (GPORCA_LIBRARY gporca)

    set (GPORCA_INCLUDE_DIR
        "${ClickHouse_SOURCE_DIR}/contrib/gporca/libgpdbcost/include"
        "${ClickHouse_SOURCE_DIR}/contrib/gporca/libgpopt/include"
        "${ClickHouse_SOURCE_DIR}/contrib/gporca/libgpos/include"
        "${ClickHouse_SOURCE_DIR}/contrib/gporca/libnaucrates/include"
    )

endif ()

message (STATUS "Using gporca=${USE_GPORCA}: ${GPORCA_INCLUDE_DIR} : ${GPORCA_LIBRARY}")
