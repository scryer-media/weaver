// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { defineConfig, type Plugin, type ResolvedConfig } from "vite";
import react, { reactCompilerPreset } from "@vitejs/plugin-react";
import babel from "@rolldown/plugin-babel";
import { compression } from "vite-plugin-compression2";
import path from "path";
import { ACCENTS, MODES, PALETTES } from "./src/lib/appearance.ts";
import {
  OFFLINE_APPEARANCE_CACHE,
  OFFLINE_APPEARANCE_PATH,
} from "./src/lib/offline-appearance.ts";

const BACKEND_ORIGIN =
  process.env.WEAVER_DEV_PROXY_TARGET?.trim() || "http://127.0.0.1:9090";
const DEV_WATCH_USE_POLLING =
  process.env.WEAVER_VITE_USE_POLLING?.trim() === "true";
const DEV_WATCH_POLL_INTERVAL = Number.parseInt(
  process.env.WEAVER_VITE_POLL_INTERVAL_MS?.trim() || "250",
  10,
);
const PWA_CACHE_PREFIX = "weaver-shell";
const PUBLIC_PWA_ASSETS = [
  "/manifest.webmanifest",
  "/favicon-adaptive.svg",
  "/favicon.ico",
  "/favicon-light-32.png",
  "/favicon-dark-32.png",
  "/apple-touch-icon.png",
  "/apple-touch-icon-180.png",
  "/apple-touch-icon-167.png",
  "/apple-touch-icon-152.png",
  "/apple-touch-icon-120.png",
  "/app-icon-dark-192.png",
  "/app-icon-dark-512.png",
  "/app-icon-maskable-192.png",
  "/app-icon-maskable-512.png",
];

function buildManualPwaPlugin(): Plugin {
  let resolvedConfig: ResolvedConfig | null = null;

  return {
    name: "weaver-manual-pwa",
    apply: "build",
    configResolved(config) {
      resolvedConfig = config;
    },
    generateBundle(_options, bundle) {
      const base = resolvedConfig?.base ?? "/";
      const normalize = (value: string) => {
        const trimmedBase = base.endsWith("/") ? base.slice(0, -1) : base;
        if (!trimmedBase) {
          return value;
        }
        return `${trimmedBase}${value}`;
      };

      const assetUrls = Object.values(bundle)
        .map((entry) => {
          if (entry.type !== "asset" && entry.type !== "chunk") {
            return null;
          }
          return normalize(`/${entry.fileName}`);
        })
        .filter((entry): entry is string => entry !== null)
        .filter((entry) => !entry.endsWith("/sw.js"))
        .filter((entry) => !entry.endsWith(".gz"));

      const precacheUrls = Array.from(
        new Set([
          normalize("/"),
          normalize("/index.html"),
          ...PUBLIC_PWA_ASSETS.map(normalize),
          ...assetUrls,
        ]),
      );

      const cacheVersion = `${PWA_CACHE_PREFIX}-${Date.now()}`;
      const swSource = `const CACHE_NAME = ${JSON.stringify(cacheVersion)};
const CACHE_PREFIX = ${JSON.stringify(PWA_CACHE_PREFIX)};
const PRECACHE_URLS = ${JSON.stringify(precacheUrls, null, 2)};
const APPEARANCE_CACHE = ${JSON.stringify(OFFLINE_APPEARANCE_CACHE)};
const APPEARANCE_URL = new URL(${JSON.stringify(OFFLINE_APPEARANCE_PATH)}, self.registration.scope);
const PALETTES = ${JSON.stringify(PALETTES)};
const MODES = ${JSON.stringify(MODES)};
const ACCENTS = ${JSON.stringify(ACCENTS)};
const SHELL_APPEARANCE = /^(\\s*<!doctype html>\\s*<html\\b[^>]*? )data-palette="(?:${PALETTES.join("|")})" data-mode="(?:${MODES.join("|")})" data-accent="(?:${ACCENTS.join("|")})?"/;
const SHELL_REQUEST_TIME = "x-weaver-shell-request-time";
const STATIC_ASSET_RE = /\\.(?:js|css|woff2?|png|webp|svg|ico)$/i;
const BYPASS_SUFFIXES = ["/graphql", "/metrics"];

const BASE_PATH = new URL(self.registration.scope).pathname;

function inScope(url) {
  return url.origin === self.location.origin && url.pathname.startsWith(BASE_PATH);
}

function shouldBypass(request) {
  if (request.method !== "GET") {
    return true;
  }
  const url = new URL(request.url);
  if (!inScope(url)) {
    return true;
  }
  const localPath = "/" + url.pathname.slice(BASE_PATH.length);
  return BYPASS_SUFFIXES.some((suffix) => localPath.startsWith(suffix));
}

function shellResponse(response, body) {
  const headers = new Headers(response.headers);
  headers.delete("content-encoding");
  headers.delete("content-length");
  headers.delete("etag");
  return new Response(body, {
    headers,
    status: response.status,
    statusText: response.statusText,
  });
}

function stampShell(response, started) {
  const cached = shellResponse(response, response.body);
  cached.headers.set(SHELL_REQUEST_TIME, String(started));
  return cached;
}

async function cacheNavigation(response, started) {
  if (
    !response.headers.get("content-type")?.startsWith("text/html") ||
    !response.url ||
    !inScope(new URL(response.url))
  ) {
    return;
  }
  const cache = await caches.open(CACHE_NAME);
  await cache.put(self.registration.scope, stampShell(response, started));
}

async function installShell() {
  const started = Date.now();
  const cache = await caches.open(CACHE_NAME);
  await cache.addAll(PRECACHE_URLS);
  const shell = await cache.match(self.registration.scope);
  if (shell) {
    await cache.put(self.registration.scope, stampShell(shell, started));
  }
}

async function offlineShell() {
  const cache = await caches.open(CACHE_NAME);
  const shell =
    (await cache.match(self.registration.scope)) ||
    (await cache.match(new URL("index.html", self.registration.scope)));
  if (!shell) {
    return Response.error();
  }
  try {
    const snapshot = await caches.match(APPEARANCE_URL, { cacheName: APPEARANCE_CACHE });
    if (!snapshot) {
      return shell;
    }
    const { appearance, savedAt } = await snapshot.json();
    // A save confirmed during the navigation still supersedes that request.
    if (
      !Number.isFinite(savedAt) ||
      savedAt < Number(shell.headers.get(SHELL_REQUEST_TIME)) ||
      !PALETTES.includes(appearance?.palette) ||
      !MODES.includes(appearance.mode) ||
      (appearance.accent !== null && !ACCENTS.includes(appearance.accent))
    ) {
      return shell;
    }
    const markup = await shell.clone().text();
    const updated = markup.replace(SHELL_APPEARANCE, (_, prefix) =>
      prefix +
      'data-palette="' + appearance.palette +
      '" data-mode="' + appearance.mode +
      '" data-accent="' + (appearance.accent ?? "") + '"',
    );
    return updated === markup ? shell : shellResponse(shell, updated);
  } catch {
    return shell;
  }
}

async function deleteOldCaches() {
  const cacheNames = await caches.keys();
  await Promise.all(
    cacheNames
      .filter((name) => name.startsWith(CACHE_PREFIX) && name !== CACHE_NAME)
      .map((name) => caches.delete(name)),
  );
}

self.addEventListener("install", (event) => {
  event.waitUntil(installShell());
});

self.addEventListener("activate", (event) => {
  event.waitUntil(deleteOldCaches().then(() => self.clients.claim()));
});

self.addEventListener("message", (event) => {
  if (event.data?.type === "SKIP_WAITING") {
    self.skipWaiting();
  }
});

self.addEventListener("fetch", (event) => {
  const { request } = event;
  if (shouldBypass(request)) {
    return;
  }

  const url = new URL(request.url);
  if (request.mode === "navigate") {
    const started = Date.now();
    const network = fetch(request);
    event.waitUntil(
      network
        .then((response) => cacheNavigation(response.clone(), started))
        .catch(() => undefined),
    );
    event.respondWith(network.catch(offlineShell));
    return;
  }

  if (!STATIC_ASSET_RE.test(url.pathname)) {
    return;
  }

  event.respondWith(
    caches.open(CACHE_NAME).then(async (cache) => {
      const cached = await cache.match(request);
      const networkFetch = fetch(request)
        .then((response) => {
          if (response.ok) {
            void cache.put(request, response.clone());
          }
          return response;
        })
        .catch(() => cached);

      if (cached) {
        event.waitUntil(networkFetch.then(() => undefined));
        return cached;
      }

      return networkFetch;
    }),
  );
});
`;

      this.emitFile({
        type: "asset",
        fileName: "sw.js",
        source: swSource,
      });
    },
  };
}

function buildDevSessionPlugin(): Plugin {
  return {
    name: "weaver-dev-session",
    apply: "serve",
    configureServer(server) {
      server.middlewares.use("/__weaver/session", async (_req, res) => {
        try {
          const response = await fetch(BACKEND_ORIGIN);
          const setCookie = response.headers.get("set-cookie");
          if (setCookie) {
            res.setHeader("set-cookie", setCookie);
          }
          res.statusCode = response.ok ? 204 : response.status;
        } catch {
          res.statusCode = 502;
        }
        res.end();
      });
    },
  };
}

export default defineConfig(({ mode }) => ({
  base: "./",
  define: {
    __WEAVER_DEV_BACKEND_ORIGIN__:
      mode === "development" ? JSON.stringify(BACKEND_ORIGIN) : "undefined",
  },
  plugins: [
    react(),
    ...(mode === "production"
      ? [babel({ presets: [reactCompilerPreset()] })]
      : []),
    compression({
      include: /\.(js|css|svg|webmanifest|json)$/i,
      exclude: /sw\.js$/,
      algorithms: ["gzip"],
    }),
    buildManualPwaPlugin(),
    buildDevSessionPlugin(),
  ],
  build: {
    target: "es2022",
    rolldownOptions: {
      output: {
        manualChunks(id) {
          if (!id.includes("node_modules")) {
            return undefined;
          }

          if (
            id.includes("/react/") ||
            id.includes("/react-dom/") ||
            id.includes("/react-router/")
          ) {
            return "react-vendor";
          }

          if (
            id.includes("/urql/") ||
            id.includes("/graphql/") ||
            id.includes("/graphql-ws/")
          ) {
            return "graphql-vendor";
          }

          if (id.includes("/lucide-react/")) {
            return "ui-vendor";
          }

          if (id.includes("/@fontsource/")) {
            return "font-vendor";
          }

          return undefined;
        },
      },
    },
  },
  resolve: {
    alias: {
      "@": path.resolve(import.meta.dirname, "./src"),
    },
  },
  server: {
    watch: DEV_WATCH_USE_POLLING
      ? {
          usePolling: true,
          interval: Number.isFinite(DEV_WATCH_POLL_INTERVAL)
            ? DEV_WATCH_POLL_INTERVAL
            : 250,
        }
      : undefined,
    proxy: {
      "/graphql": {
        target: BACKEND_ORIGIN,
        ws: true,
      },
      "/api": {
        target: BACKEND_ORIGIN,
      },
    },
  },
}));
