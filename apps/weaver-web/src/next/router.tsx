import type { ComponentType } from "react";
import { createBrowserRouter, Navigate } from "react-router";
import { RouteErrorPage } from "@/lib/error-page";
import { NextRouteFallback } from "./shell/route-states";

const basename = window.__WEAVER_BASE__ || "/";

/**
 * The interface's route table. Every screen loads on its own the first time it
 * is visited.
 */
function lazyRoute<TModule extends Record<string, unknown>, TKey extends keyof TModule>(
  importer: () => Promise<TModule>,
  exportName: TKey,
) {
  return {
    HydrateFallback: NextRouteFallback,
    lazy: async () => {
      const module = await importer();
      return { Component: module[exportName] as ComponentType };
    },
  };
}

export const nextRouter = createBrowserRouter(
  [
    {
      errorElement: <RouteErrorPage />,
      children: [
        { index: true, ...lazyRoute(() => import("./pages/DownloadsPage"), "DownloadsPage") },
        { path: "history", ...lazyRoute(() => import("./pages/HistoryPage"), "HistoryPage") },
        {
          path: "jobs/:id",
          ...lazyRoute(() => import("./pages/JobDetailPage"), "JobDetailPage"),
        },
        {
          path: "monitoring",
          ...lazyRoute(() => import("./pages/MonitoringPage"), "MonitoringPage"),
        },
        {
          path: "system-info",
          ...lazyRoute(() => import("./pages/SystemInfoPage"), "SystemInfoPage"),
        },
        { path: "logs", ...lazyRoute(() => import("./pages/LogsPage"), "LogsPage") },
        {
          path: "tools/nzb-analyzer",
          ...lazyRoute(() => import("./pages/NzbAnalyzerPage"), "NzbAnalyzerPage"),
        },
        {
          path: "settings",
          children: [
            { index: true, element: <Navigate to="general" replace /> },
            { path: "proxies", element: <Navigate to="/settings/networking/proxies" replace /> },
            { path: "bandwidth", element: <Navigate to="/settings/networking/bandwidth" replace /> },
            { path: "networking", element: <Navigate to="/settings/networking/overview" replace /> },
            {
              path: ":panel/*",
              ...lazyRoute(() => import("./pages/settings/SettingsPage"), "SettingsPage"),
            },
          ],
        },
        // Older paths whose screens now live inside others (the upload page,
        // the standalone server and category editors), and anything else
        // unrecognised, land on Downloads rather than an error page.
        { path: "*", element: <Navigate to="/" replace /> },
      ],
    },
  ],
  { basename },
);
