# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

cmake_minimum_required(VERSION 3.20)
project(weaver_utp_interop LANGUAGES CXX)

# Use an already built reference, with its existing system dependencies.
# No source or dependency is downloaded or updated here.
set(UTP_REFERENCE_REPO "" CACHE PATH "Pinned reference Git checkout")
set(UTP_NATIVE_BUILD "" CACHE PATH "Existing native reference CMake build")
if(NOT UTP_REFERENCE_REPO OR NOT UTP_NATIVE_BUILD)
    message(FATAL_ERROR "Set UTP_REFERENCE_REPO and UTP_NATIVE_BUILD")
endif()
if(WIN32)
    message(FATAL_ERROR "The helper's alert pipe requires a POSIX host")
endif()

find_package(Git REQUIRED)
execute_process(
    COMMAND "${GIT_EXECUTABLE}" -C "${UTP_REFERENCE_REPO}" rev-parse HEAD
    RESULT_VARIABLE reference_status
    OUTPUT_VARIABLE reference_revision
    OUTPUT_STRIP_TRAILING_WHITESPACE
)
if(NOT reference_status EQUAL 0 OR NOT reference_revision STREQUAL "b960e0d59f12970b719a96d07034c5a197fe5734")
    message(FATAL_ERROR "Reference checkout is not at the required revision")
endif()
execute_process(
    COMMAND "${GIT_EXECUTABLE}" -C "${UTP_REFERENCE_REPO}" diff --quiet HEAD -- src include CMakeLists.txt cmake
    RESULT_VARIABLE reference_dirty
)
if(NOT reference_dirty EQUAL 0)
    message(FATAL_ERROR "Reference source or build configuration has local changes")
endif()

# A native build may use an exported source snapshot. Verify its source
# against the pinned checkout rather than trusting its directory name.
file(STRINGS "${UTP_NATIVE_BUILD}/CMakeCache.txt" native_source_entry
    REGEX "^CMAKE_HOME_DIRECTORY:INTERNAL=")
string(REGEX REPLACE "^CMAKE_HOME_DIRECTORY:INTERNAL=" "" native_source "${native_source_entry}")
if(NOT IS_DIRECTORY "${native_source}")
    message(FATAL_ERROR "Native build has no available source directory")
endif()
foreach(component src include CMakeLists.txt cmake)
    execute_process(
        COMMAND "${GIT_EXECUTABLE}" diff --no-index --quiet --
            "${UTP_REFERENCE_REPO}/${component}" "${native_source}/${component}"
        RESULT_VARIABLE sources_differ
    )
    if(NOT sources_differ EQUAL 0)
        message(FATAL_ERROR "Native build source differs from the pinned reference: ${component}")
    endif()
endforeach()

# Import the native target's own compile definitions and transitive link
# requirements, including the caller's selected system crypto library.
find_package(LibtorrentRasterbar CONFIG REQUIRED
    PATHS "${UTP_NATIVE_BUILD}/LibtorrentRasterbar" NO_DEFAULT_PATH)
add_executable(weaver-utp-interop interop.cpp)
target_compile_features(weaver-utp-interop PRIVATE cxx_std_17)
target_link_libraries(weaver-utp-interop PRIVATE LibtorrentRasterbar::torrent-rasterbar)
if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
    target_compile_options(weaver-utp-interop PRIVATE -Wall -Wextra)
endif()
