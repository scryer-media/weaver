/// <reference types="vite/client" />

interface Window {
  /** Base URL path injected by the server for reverse proxy support (e.g. "/weaver"). */
  __WEAVER_BASE__?: string;
}
