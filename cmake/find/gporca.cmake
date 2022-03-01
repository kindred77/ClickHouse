#OPTION(ENABLE_GPORCA "Enable gporca" ${ENABLE_LIBRARIES})

#if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/gporca/CMakeLists.txt")
#    message (WARNING "submodule contrib/gporca is missing. to fix try run: \n git submodule update --init --recursive")
#    set (ENABLE_GPORCA 0)
#endif ()

#if (NOT CMAKE_SYSTEM_NAME MATCHES "Linux" AND NOT (CMAKE_SYSTEM_NAME MATCHES "Darwin" AND NOT CMAKE_CROSSCOMPILING))
#    message (WARNING "gporca disabled in non-Linux and non-native-Darwin environments")
#    set (ENABLE_GPORCA 0)
#endif ()

#if (ENABLE_GPORCA)

#    set (USE_GPORCA 1)
#    set (GPORCA_LIBRARY gporca)

#    set (GPORCA_INCLUDE_DIR
#        "${ClickHouse_SOURCE_DIR}/contrib/gporca/libgpdbcost/include"
#        "${ClickHouse_SOURCE_DIR}/contrib/gporca/libgpopt/include"
#        "${ClickHouse_SOURCE_DIR}/contrib/gporca/libgpos/include"
#        "${ClickHouse_SOURCE_DIR}/contrib/gporca/libnaucrates/include"
#    )

#endif ()

#message (STATUS "Using gporca=${USE_GPORCA}: ${GPORCA_INCLUDE_DIR} : ${GPORCA_LIBRARY}")


# Xerces
find_path(GPXERCES_INCLUDE_DIR xercesc/sax2/DefaultHandler.hpp)
find_library(GPXERCES_LIBRARY NAMES xerces-c libxerces-c)
#set(XERCES_LIBRARIES ${XERCES_LIBRARY})
#set(XERCES_INCLUDE_DIRS ${XERCES_INCLUDE_DIR})
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Xerces DEFAULT_MSG
                                  GPXERCES_LIBRARY GPXERCES_INCLUDE_DIR)
                                  
# gporca
find_package_handle_standard_args(gporca DEFAULT_MSG
                                  GPORCA_INCLUDE_DIR GPORCA_LIBRARY_GPDBCOST GPORCA_LIBRARY_GPOPT GPORCA_LIBRARY_GPOS GPORCA_LIBRARY_NAUCRATES)
                                  
find_path(GPORCA_INCLUDE_DIR gpopt/version.h)

find_library(GPORCA_LIBRARY_GPDBCOST NAMES libgpdbcost)
find_library(GPORCA_LIBRARY_GPOPT NAMES libgpopt)
find_library(GPORCA_LIBRARY_GPOS NAMES libgpos)
find_library(GPORCA_LIBRARY_NAUCRATES NAMES libnaucrates)
#set(GPORCA_LIBRARIES ${ORCA_LIBRARY})
#set(GPORCA_INCLUDE_DIRS ${ORCA_INCLUDE_DIR})

#gp
#find_package_handle_standard_args(gp DEFAULT_MSG
#                                  GP_INCLUDE_DIR GP_LIBRARY_PQ GP_LIBRARY_PGTYPES GP_LIBRARY_PGCOMMON GP_LIBRARY_PGPORT GP_LIBRARY_GPPC)
                                  
#find_path(GP_INCLUDE_DIR libpq/libpq.h)

#find_library(GP_LIBRARY_PQ NAMES libpq)
#find_library(GP_LIBRARY_PGTYPES NAMES libpgtypes)
#find_library(GP_LIBRARY_PGCOMMON NAMES libpgcommon)
#find_library(GP_LIBRARY_PGPORT NAMES libpgport)
#find_library(GP_LIBRARY_GPPC NAMES libgppc)

