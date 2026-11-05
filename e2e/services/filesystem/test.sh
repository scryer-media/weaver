#!/bin/sh
# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
set -eu

# Standalone bounded filesystem lane; no existing Compose project is used.
# Run: sh e2e/services/filesystem/test.sh
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
cd "$root/e2e"
export E2E_CONTAINER_ENGINE="${E2E_CONTAINER_ENGINE:-${CONTAINER_ENGINE:-auto}}"
go build -o ./.bin/weaver-e2e ./cmd/weaver-e2e
exec ./.bin/weaver-e2e filesystem
