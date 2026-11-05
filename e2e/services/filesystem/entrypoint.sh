#!/bin/sh
# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
set -eu
# Compose prepares configuration before calling this entrypoint. Only the
# download roots use FUSE; SQLite and the configuration stay on local storage.
exec python3 /fs-fixture/run.py --mount /data/intermediate --mount /data/complete -- /fs-fixture/product-entrypoint.sh "$@"
