import type { ComponentType } from "react";
import { createBrowserRouter, Navigate } from "react-router";
import { NextRouteError, NextRouteFallback } from "./shell/route-states";

const basename = window.__WEAVER_BASE__ || "/";

/**
 * The Next UI's own route table.
 *
 * It deliberately shares no route objects with the classic router: the two
 * interfaces are independent trees mounted by `App`, and only one of them ever
 * exists in a given document. The paths match the classic ones so a bookmarked
 * URL keeps working across the toggle.
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
      errorElement: <NextRouteError />,
      children: [
        { index: true, ...lazyRoute(() => import("./pages/TransfersPage"), "TransfersPage") },
        { path: "history", ...lazyRoute(() => import("./pages/CompletedPage"), "CompletedPage") },
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
          path: "settings",
          children: [
            { index: true, element: <Navigate to="general" replace /> },
            {
              path: ":panel",
              ...lazyRoute(() => import("./pages/settings/SettingsPage"), "SettingsPage"),
            },
          ],
        },
        // The classic UI has screens this one folds into others (the upload
        // page, the standalone server and category editors). Anything
        // unrecognised lands on Transfers rather than an error page.
        { path: "*", element: <Navigate to="/" replace /> },
      ],
    },
  ],
  { basename },
);
