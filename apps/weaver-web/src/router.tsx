import type { ComponentType } from "react";
import { createBrowserRouter, Navigate } from "react-router";
import { Layout } from "@/components/Layout";
import { RouteErrorPage } from "@/components/RouteErrorPage";
import { RouteFallback } from "@/components/RouteFallback";

const basename = window.__WEAVER_BASE__ || "/";

/**
 * Every lazy route ships a `HydrateFallback`. React Router renders nothing at all
 * during the initial load when the matched chain has a pending `lazy` module and
 * no route declares a fallback - it truncates the matches to the root route and
 * renders `null` (plus a "No `HydrateFallback` element provided" warning), which
 * left `#root` empty until the first route module resolved. With the fallback on
 * the lazy route, the root `Layout` shell paints immediately and only the outlet
 * region shows the placeholder.
 */
function lazyNamedRoute<TModule extends Record<string, unknown>, TKey extends keyof TModule>(
  importer: () => Promise<TModule>,
  exportName: TKey,
) {
  return {
    HydrateFallback: RouteFallback,
    lazy: async () => {
      const module = await importer();
      return {
        Component: module[exportName] as ComponentType,
      };
    },
  };
}

function lazyEmbeddedRoute<TModule extends Record<string, unknown>, TKey extends keyof TModule>(
  importer: () => Promise<TModule>,
  exportName: TKey,
  props: Record<string, unknown>,
) {
  return {
    HydrateFallback: RouteFallback,
    lazy: async () => {
      const module = await importer();
      const BaseComponent = module[exportName] as ComponentType<Record<string, unknown>>;

      function EmbeddedRouteComponent() {
        return <BaseComponent {...props} />;
      }

      EmbeddedRouteComponent.displayName = `${String(exportName)}EmbeddedRoute`;

      return {
        Component: EmbeddedRouteComponent,
      };
    },
  };
}

export const router = createBrowserRouter([
  {
    element: <Layout />,
    errorElement: <RouteErrorPage />,
    children: [
      {
        index: true,
        ...lazyNamedRoute(() => import("@/pages/JobList"), "JobList"),
      },
      {
        path: "jobs/:id",
        ...lazyNamedRoute(() => import("@/pages/JobDetail"), "JobDetail"),
      },
      {
        path: "upload",
        ...lazyNamedRoute(() => import("@/pages/Upload"), "Upload"),
      },
      {
        path: "monitoring",
        ...lazyNamedRoute(() => import("@/pages/MetricsPage"), "MetricsPage"),
      },
      {
        path: "history",
        ...lazyNamedRoute(() => import("@/pages/History"), "History"),
      },
      {
        path: "logs",
        ...lazyNamedRoute(() => import("@/pages/LogViewerPage"), "LogViewerPage"),
      },
      { path: "servers", element: <Navigate to="/settings/servers" replace /> },
      { path: "categories", element: <Navigate to="/settings/categories" replace /> },
      {
        path: "settings",
        ...lazyNamedRoute(() => import("@/pages/settings/SettingsLayout"), "SettingsLayout"),
        children: [
          { index: true, element: <Navigate to="general" replace /> },
          {
            path: "general",
            ...lazyNamedRoute(
              () => import("@/pages/settings/GeneralSettingsPage"),
              "GeneralSettingsPage",
            ),
          },
          {
            path: "bandwidth",
            ...lazyNamedRoute(
              () => import("@/pages/settings/BandwidthCapSettingsPage"),
              "BandwidthCapSettingsPage",
            ),
          },
          {
            path: "security",
            ...lazyNamedRoute(
              () => import("@/pages/settings/SecuritySettingsPage"),
              "SecuritySettingsPage",
            ),
          },
          {
            path: "backup",
            ...lazyNamedRoute(
              () => import("@/pages/settings/BackupSettingsPage"),
              "BackupSettingsPage",
            ),
          },
          {
            path: "rss",
            ...lazyNamedRoute(
              () => import("@/pages/settings/RssSettingsPage"),
              "RssSettingsPage",
            ),
          },
          {
            path: "watch-folder",
            ...lazyNamedRoute(
              () => import("@/pages/settings/WatchFolderSettingsPage"),
              "WatchFolderSettingsPage",
            ),
          },
          {
            path: "post-processing",
            ...lazyNamedRoute(
              () => import("@/pages/settings/PostProcessingSettingsPage"),
              "PostProcessingSettingsPage",
            ),
          },
          {
            path: "schedules",
            ...lazyNamedRoute(
              () => import("@/pages/settings/ScheduleSettingsPage"),
              "ScheduleSettingsPage",
            ),
          },
          {
            path: "categories",
            ...lazyEmbeddedRoute(
              () => import("@/pages/Categories"),
              "Categories",
              { embedded: true },
            ),
          },
          {
            path: "servers",
            ...lazyEmbeddedRoute(
              () => import("@/pages/Servers"),
              "Servers",
              { embedded: true },
            ),
          },
        ],
      },
    ],
  },
], { basename });
