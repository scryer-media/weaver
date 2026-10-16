// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

/// <reference types="vite/client" />

declare const __WEAVER_DEV_BACKEND_ORIGIN__: string | undefined;

interface Window {
  /** Base URL path injected by the server for reverse proxy support (e.g. "/weaver"). */
  __WEAVER_BASE__?: string;
}
