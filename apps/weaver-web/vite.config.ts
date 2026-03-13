import { defineConfig, type Plugin, type ResolvedConfig } from "vite";
import react from "@vitejs/plugin-react";
import { compression } from "vite-plugin-compression2";
import path from "path";

const BACKEND_ORIGIN = "http://localhost:6789";
const PWA_CACHE_PREFIX = "weaver-shell";
const PUBLIC_PWA_ASSETS = [
  "/manifest.webmanifest",
  "/favicon-adaptive.svg",
  "/favicon.ico",
  "/favicon-light-32.png",
  "/favicon-dark-32.png",
  "/apple-touch-icon.png",
  "/app-icon-dark-192.png",
  "/app-icon-dark-512.png",
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
const STATIC_ASSET_RE = /\\.(?:js|css|woff2?|png|webp|svg|ico)$/i;
const BYPASS_SUFFIXES = ["/graphql", "/metrics"];

function isSameOrigin(url) {
  return url.origin === self.location.origin;
}

function getBasePath() {
  return new URL(self.registration.scope).pathname;
}

function shouldBypass(request) {
  if (request.method !== "GET") {
    return true;
  }
  const url = new URL(request.url);
  if (!isSameOrigin(url)) {
    return true;
  }
  const base = getBasePath();
  const localPath = url.pathname.startsWith(base)
    ? "/" + url.pathname.slice(base.length)
    : url.pathname;
  return BYPASS_SUFFIXES.some((suffix) => localPath.startsWith(suffix));
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
  event.waitUntil(
    caches.open(CACHE_NAME).then((cache) => cache.addAll(PRECACHE_URLS)),
  );
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
    event.respondWith(
      fetch(request)
        .then((response) => {
          const copy = response.clone();
          event.waitUntil(
            caches.open(CACHE_NAME).then((cache) => cache.put(${JSON.stringify(
              normalize("/"),
            )}, copy)),
          );
          return response;
        })
        .catch(async () => {
          const cache = await caches.open(CACHE_NAME);
          return (
            (await cache.match(request)) ||
            (await cache.match(${JSON.stringify(normalize("/"))})) ||
            (await cache.match(${JSON.stringify(normalize("/index.html"))}))
          );
        }),
    );
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

/**
 * Dev-only plugin: fetches the backend's index.html at startup to extract
 * the ephemeral session token, then injects it into Vite's dev HTML.
 */
function buildDevSessionPlugin(): Plugin {
  let sessionScript = "";

  return {
    name: "weaver-dev-session",
    apply: "serve",
    async configureServer() {
      try {
        const response = await fetch(BACKEND_ORIGIN);
        const html = await response.text();
        const match = /window\.__WEAVER_SESSION__\s*=\s*"([^"]+)"/.exec(html);
        if (match?.[1]) {
          sessionScript = `<script>window.__WEAVER_SESSION__=${JSON.stringify(match[1])}</script>`;
        }
      } catch {
        // Backend not running — session token will be undefined; API calls
        // will 401 but the dev server still starts.
      }
    },
    transformIndexHtml(html) {
      if (!sessionScript) return html;
      return html.replace("</head>", `${sessionScript}\n  </head>`);
    },
  };
}

export default defineConfig({
  base: "./",
  plugins: [
    react(),
    compression({
      include: /\.(js|css|svg|webmanifest|json)$/i,
      exclude: /sw\.js$/,
      algorithms: ["gzip"],
    }),
    buildManualPwaPlugin(),
    buildDevSessionPlugin(),
  ],
  build: {
    rollupOptions: {
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

          if (
            id.includes("/radix-ui/") ||
            id.includes("/lucide-react/") ||
            id.includes("/next-themes/")
          ) {
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
      "@": path.resolve(__dirname, "./src"),
    },
  },
  server: {
    proxy: {
      "/graphql": {
        target: BACKEND_ORIGIN,
        ws: true,
      },
      "/admin": {
        target: BACKEND_ORIGIN,
      },
    },
  },
});
