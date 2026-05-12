/// <reference types="vite/client" />

declare const __WEAVER_ENABLE_DIAGNOSTICS__: boolean;
declare const __WEAVER_DEV_BACKEND_ORIGIN__: string | undefined;

interface Window {
  /** Base URL path injected by the server for reverse proxy support (e.g. "/weaver"). */
  __WEAVER_BASE__?: string;
}
