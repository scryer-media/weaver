// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { Navigate, type RouteObject } from "react-router";

/**
 * Settings addresses that have moved, under /settings, and where each lands
 * now, so existing bookmarks keep working. Proxies live in Networking, the
 * former post-processing panel in Scripts, and the former Bandwidth page's
 * ceiling and quota usage on the Egress page.
 */
export const SETTINGS_REDIRECTS: RouteObject[] = [
  ["proxies", "networking/proxies"],
  ["bandwidth", "networking/egress"],
  ["networking", "networking/overview"],
  ["networking/bandwidth", "networking/egress"],
  ["post-processing", "scripts/configuration"],
  ["scripts", "scripts/configuration"],
].map(([path, to]) => ({ path, element: <Navigate to={`/settings/${to}`} replace /> }));
