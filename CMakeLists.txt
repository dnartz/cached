cmake_minimum_required(VERSION 3.3)
project(cached)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free")

include(ExternalProject)

SET(JEMALLOC_DIR ${CMAKE_SOURCE_DIR}/jemalloc)
SET(LIBEV_DIR ${CMAKE_SOURCE_DIR}/libev)

include_directories("include" "jemalloc/include/jemalloc")

ExternalProject_Add(libjemalloc
        SOURCE_DIR ${JEMALLOC_DIR}
        GIT_TAG master
        CONFIGURE_COMMAND ./autogen.sh && ./configure
        BUILD_COMMAND make
        BUILD_IN_SOURCE 1
        INSTALL_COMMAND "")

ExternalProject_Add(libev
        SOURCE_DIR ${LIBEV_DIR}
        CONFIGURE_COMMAND ./autogen.sh && ./configure
        BUILD_COMMAND make CFLAGS='-std=c99'
        BUILD_IN_SOURCE 1
        INSTALL_COMMAND "")

set(SERVER_HEADERS
        libev/ev.h
        include/master.h
        include/setting.h
        include/common.h
        include/connection.h
        include/worker.h
        include/murmur3.h
        jemalloc/include/jemalloc/jemalloc.h)

set(SERVER_SOURCE_FILES
        ${SERVER_HEADERS}
        worker.cpp
        server.cpp connection.cpp assoc.cpp include/murmur3.h murmur3.c)

add_executable(cached-server ${SERVER_SOURCE_FILES})
add_dependencies(cached-server libev libjemalloc)

target_link_libraries(cached-server ${LIBEV_DIR}/.libs/libev.a ${JEMALLOC_DIR}/lib/libjemalloc.a)

target_link_libraries(cached-server ${LIBEV_DIR}/.libs/libev.a)
