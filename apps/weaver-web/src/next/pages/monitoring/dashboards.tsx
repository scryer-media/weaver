// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { lazy, Suspense, useEffect, useMemo, useState, type ComponentType, type LazyExoticComponent } from "react";
import { Navigate, useNavigate, useParams } from "react-router";
import { useTranslate } from "@/lib/context/translate-context";
import { Tabs } from "../../components/Tabs";
import { withoutTorrentOnly } from "../../data/torrents-shown";
import { TorrentsOnly, useTorrentsShown } from "../../data/use-torrents-shown";
import { LoadingMark } from "@/lib/loading-mark";
import { NextShell } from "../../shell/NextShell";
import { AttentionBlock, UptimeBlock } from "../../shell/rail-blocks";
import { ShellHostsContext, type ShellHosts } from "./shell-slots";

/** A dashboard's page, loaded on first use, and the load itself. */
function lazyPage<K extends string>(load: () => Promise<Record<K, ComponentType>>, name: K) {
  return { page: lazy(() => load().then((module) => ({ default: module[name] }))), load };
}

interface Dashboard {
  id: string;
  label: string;
  page: LazyExoticComponent<ComponentType>;
  /** Fetches the page's code, so a later switch to it does not wait. */
  load: () => Promise<unknown>;
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
    ...lazyPage(() => import("./OverviewDashboard"), "OverviewDashboard"),
  },
  {
    id: "providers",
    label: "next.monitoring.dash.providers",
    ...lazyPage(() => import("./ProvidersDashboard"), "ProvidersDashboard"),
  },
  {
    id: "integrity",
    label: "next.monitoring.dash.integrity",
    ...lazyPage(() => import("./IntegrityDashboard"), "IntegrityDashboard"),
  },
  {
    id: "torrents",
    label: "next.monitoring.dash.torrents",
    torrentOnly: true,
    ...lazyPage(() => import("./TorrentsDashboard"), "TorrentsDashboard"),
  },
  {
    id: "engine",
    label: "next.monitoring.dash.engine",
    ...lazyPage(() => import("../MonitoringPage"), "MonitoringPage"),
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
      testId="monitoring"
      onSelect={(id) => navigate(id === DASHBOARDS[0]!.id ? "/monitoring" : `/monitoring/${id}`)}
    />
  );
}

/**
 * Monitoring keeps one shell while its dashboards change: the rail, the title
 * bar and the nav stay up, and a dashboard fills the content and the slots it
 * uses. Every dashboard's code is fetched once Monitoring opens, so a switch
 * draws the next dashboard at once.
 */
export function MonitoringRoute() {
  const t = useTranslate();
  const { dashboard: id } = useParams();
  const dashboard = id === undefined ? DASHBOARDS[0]! : DASHBOARDS.find((entry) => entry.id === id);
  const [note, setNote] = useState<HTMLElement | null>(null);
  const [controls, setControls] = useState<HTMLElement | null>(null);
  const [strip, setStrip] = useState<HTMLElement | null>(null);
  const [status, setStatus] = useState<HTMLElement | null>(null);
  const hosts = useMemo<ShellHosts>(() => ({ note, controls, strip, status }), [note, controls, strip, status]);
  useEffect(() => {
    for (const entry of DASHBOARDS) {
      void entry.load();
    }
  }, []);
  if (dashboard === undefined) {
    return <Navigate to="/monitoring" replace />;
  }
  const Page = dashboard.page;
  const loading = (
    <div role="status" aria-busy="true" className="flex flex-1 items-center justify-center bg-wv-list">
      <LoadingMark className="h-10" reveal />
    </div>
  );
  const page = <Suspense fallback={loading}><Page /></Suspense>;
  return (
    <NextShell
      title={t("next.nav.monitoring")}
      note={<span ref={setNote} className="contents" />}
      controls={<div ref={setControls} className="contents" />}
      railMiddle={<AttentionBlock />}
      railFooter={<UptimeBlock />}
      beforeContent={
        <>
          <DashboardNav active={dashboard.id} />
          <div ref={setStrip} className="contents" />
        </>
      }
      statusRight={<span ref={setStatus} className="contents" />}
    >
      <ShellHostsContext value={hosts}>
        {/* A torrent-only dashboard waits for whether torrents are on, and
            leaves for the first dashboard once they are known to be off. */}
        {dashboard.torrentOnly ? (
          <TorrentsOnly pending={loading} fallback={<Navigate to="/monitoring" replace />}>
            {page}
          </TorrentsOnly>
        ) : (
          page
        )}
      </ShellHostsContext>
    </NextShell>
  );
}
