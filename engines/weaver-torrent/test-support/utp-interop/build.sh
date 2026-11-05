#!/bin/sh
# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
set -eu

if [ "$#" -ne 3 ]; then
    echo "usage: sh build.sh REFERENCE_GIT_CHECKOUT NATIVE_CMAKE_BUILD HELPER_BUILD_DIR" >&2
    exit 2
fi
interop_source=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
cmake -S "$interop_source" -B "$3" \
    "-DUTP_REFERENCE_REPO=$1" "-DUTP_NATIVE_BUILD=$2"
cmake --build "$3" --target weaver-utp-interop
