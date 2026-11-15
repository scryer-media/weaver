// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { lazy, Suspense, type ComponentType, type LazyExoticComponent } from "react";
import { Navigate, useNavigate, useParams } from "react-router";
import { useTranslate } from "@/lib/context/translate-context";
import { Tabs } from "../../components/Tabs";
import { withoutTorrentOnly } from "../../data/torrents-shown";
import { TorrentsOnly, useTorrentsShown } from "../../data/use-torrents-shown";
import { NextRouteFallback } from "../../shell/route-states";

interface Dashboard {
  id: string;
  label: string;
  page: LazyExoticComponent<ComponentType>;
  /** Shown only while torrents are on. */
  torrentOnly?: boolean;
}

/**
 * Every Monitoring dashboard, in nav order; the first is the default. A new
 * dashboard is one entry here.
 */
const DASHBOARDS: readonly Dashboard[] = [
  {
    id: "overview",
    label: "next.monitoring.dash.overview",
    page: lazy(() => import("./OverviewDashboard").then((module) => ({ default: module.OverviewDashboard }))),
  },
  {
    id: "providers",
    label: "next.monitoring.dash.providers",
    page: lazy(() => import("./ProvidersDashboard").then((module) => ({ default: module.ProvidersDashboard }))),
  },
  {
    id: "integrity",
    label: "next.monitoring.dash.integrity",
    page: lazy(() => import("./IntegrityDashboard").then((module) => ({ default: module.IntegrityDashboard }))),
  },
  {
    id: "torrents",
    label: "next.monitoring.dash.torrents",
    torrentOnly: true,
    page: lazy(() => import("./TorrentsDashboard").then((module) => ({ default: module.TorrentsDashboard }))),
  },
  {
    id: "engine",
    label: "next.monitoring.dash.engine",
    page: lazy(() => import("../MonitoringPage").then((module) => ({ default: module.MonitoringPage }))),
  },
];

/** The dashboards' own nav, set above each one's content. */
export function DashboardNav({ active }: { active: string }) {
  const t = useTranslate();
  const navigate = useNavigate();
  const shown = withoutTorrentOnly(DASHBOARDS, useTorrentsShown(), (dashboard) => dashboard.torrentOnly === true);
  return (
    <Tabs
      tabs={shown.map((dashboard) => ({ id: dashboard.id, label: t(dashboard.label) }))}
      active={active}
      onSelect={(id) => navigate(id === DASHBOARDS[0]!.id ? "/monitoring" : `/monitoring/${id}`)}
    />
  );
}

export function MonitoringRoute() {
  const { dashboard: id } = useParams();
  const dashboard = id === undefined ? DASHBOARDS[0]! : DASHBOARDS.find((entry) => entry.id === id);
  if (dashboard === undefined) {
    return <Navigate to="/monitoring" replace />;
  }
  const Page = dashboard.page;
  const page = (
    <Suspense fallback={<NextRouteFallback />}>
      <Page />
    </Suspense>
  );
  // A torrent-only dashboard waits for whether torrents are on, and leaves
  // for the first dashboard once they are known to be off.
  return dashboard.torrentOnly ? (
    <TorrentsOnly pending={<NextRouteFallback />} fallback={<Navigate to="/monitoring" replace />}>
      {page}
    </TorrentsOnly>
  ) : (
    page
  );
}
