import type { ComponentType } from "react";
import { createBrowserRouter, Navigate } from "react-router";
import { Layout } from "@/components/Layout";
import { RouteErrorPage } from "@/components/RouteErrorPage";

const basename = window.__WEAVER_BASE__ || "/";

function lazyNamedRoute<TModule extends Record<string, unknown>, TKey extends keyof TModule>(
  importer: () => Promise<TModule>,
  exportName: TKey,
) {
  return async () => {
    const module = await importer();
    return {
      Component: module[exportName] as ComponentType,
    };
  };
}

function lazyEmbeddedRoute<TModule extends Record<string, unknown>, TKey extends keyof TModule>(
  importer: () => Promise<TModule>,
  exportName: TKey,
  props: Record<string, unknown>,
) {
  return async () => {
    const module = await importer();
    const BaseComponent = module[exportName] as ComponentType<Record<string, unknown>>;

    function EmbeddedRouteComponent() {
      return <BaseComponent {...props} />;
    }

    EmbeddedRouteComponent.displayName = `${String(exportName)}EmbeddedRoute`;

    return {
      Component: EmbeddedRouteComponent,
    };
  };
}

export const router = createBrowserRouter([
  {
    element: <Layout />,
    errorElement: <RouteErrorPage />,
    children: [
      {
        index: true,
        lazy: lazyNamedRoute(() => import("@/pages/JobList"), "JobList"),
      },
      {
        path: "jobs/:id",
        lazy: lazyNamedRoute(() => import("@/pages/JobDetail"), "JobDetail"),
      },
      {
        path: "upload",
        lazy: lazyNamedRoute(() => import("@/pages/Upload"), "Upload"),
      },
      {
        path: "monitoring",
        lazy: lazyNamedRoute(() => import("@/pages/MetricsPage"), "MetricsPage"),
      },
      {
        path: "history",
        lazy: lazyNamedRoute(() => import("@/pages/History"), "History"),
      },
      {
        path: "logs",
        lazy: lazyNamedRoute(() => import("@/pages/LogViewerPage"), "LogViewerPage"),
      },
      { path: "servers", element: <Navigate to="/settings/servers" replace /> },
      { path: "categories", element: <Navigate to="/settings/categories" replace /> },
      {
        path: "settings",
        lazy: lazyNamedRoute(() => import("@/pages/settings/SettingsLayout"), "SettingsLayout"),
        children: [
          { index: true, element: <Navigate to="general" replace /> },
          {
            path: "general",
            lazy: lazyNamedRoute(
              () => import("@/pages/settings/GeneralSettingsPage"),
              "GeneralSettingsPage",
            ),
          },
          {
            path: "bandwidth",
            lazy: lazyNamedRoute(
              () => import("@/pages/settings/BandwidthCapSettingsPage"),
              "BandwidthCapSettingsPage",
            ),
          },
          {
            path: "security",
            lazy: lazyNamedRoute(
              () => import("@/pages/settings/SecuritySettingsPage"),
              "SecuritySettingsPage",
            ),
          },
          {
            path: "backup",
            lazy: lazyNamedRoute(
              () => import("@/pages/settings/BackupSettingsPage"),
              "BackupSettingsPage",
            ),
          },
          {
            path: "rss",
            lazy: lazyNamedRoute(
              () => import("@/pages/settings/RssSettingsPage"),
              "RssSettingsPage",
            ),
          },
          {
            path: "schedules",
            lazy: lazyNamedRoute(
              () => import("@/pages/settings/ScheduleSettingsPage"),
              "ScheduleSettingsPage",
            ),
          },
          {
            path: "categories",
            lazy: lazyEmbeddedRoute(
              () => import("@/pages/Categories"),
              "Categories",
              { embedded: true },
            ),
          },
          {
            path: "servers",
            lazy: lazyEmbeddedRoute(
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
