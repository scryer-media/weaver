// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import type { ComponentType } from "react";
import type { IconName } from "../../../components/icons";
import type { PanelListItem } from "../../../shell/rail-blocks";
import { withoutTorrentOnly } from "../../../data/torrents-shown";
import { BackupPanel } from "./BackupPanel";
import { CategoriesPanel } from "./CategoriesPanel";
import { GeneralPanel } from "./GeneralPanel";
import { ScriptConfigurationPanel, ScriptListPanel } from "./PostProcessingPanel";
import { ProvidersPanel } from "./ProvidersPanel";
import { NetworkingPanel } from "./NetworkingPanel";
import { RssPanel } from "./RssPanel";
import { SchedulesPanel } from "./SchedulesPanel";
import { ScriptRunsPanel } from "./ScriptRunsPanel";
import { SecretsPanel } from "./SecretsPanel";
import { SecurityPanel } from "./SecurityPanel";
import { TorrentsPanel } from "./TorrentsPanel";
import { WatchFolderPanel } from "./WatchFolderPanel";

/**
 * The settings panels, in rail order.
 *
 * Former slugs redirect to where their settings live now (see `../redirects`),
 * so existing bookmarks keep working.
 */

export type PanelGroup = "networking" | "scripts";

/** The rail row each group of panels nests under: its name and its one icon. */
export const PANEL_GROUPS: Record<PanelGroup, { label: string; icon: IconName }> = {
  networking: { label: "settings.networking", icon: "networking" },
  scripts: { label: "next.settings.panel.scripts", icon: "postProcessing" },
};

export interface PanelDefinition {
  slug: string;
  /** The rail row the panel nests under, with the others that share it. */
  group?: PanelGroup;
  /** Translation key of the panel's name. */
  label: string;
  /** Translation key of the mono subtitle beside the panel's title in the top bar, when it carries something the name does not. */
  note?: string;
  /** Right-hand rail tag; `"count:providers"` resolves to the server count. */
  tag?: "beta" | "count:providers";
  /** The rail icon, from `ICONS`. */
  icon: IconName;
  /** The torrents' own panel: there only while the server has torrents. */
  torrentOnly?: true;
  Component: ComponentType;
}

export const SETTINGS_PANELS: readonly PanelDefinition[] = [
  {
    slug: "general",
    label: "next.settings.panel.general",
    icon: "general",
    Component: GeneralPanel,
  },
  {
    slug: "servers",
    label: "next.settings.panel.servers",
    note: "next.settings.panel.serversNote",
    tag: "count:providers",
    icon: "providers",
    Component: ProvidersPanel,
  },
  {
    slug: "security",
    label: "next.settings.panel.security",
    icon: "security",
    Component: SecurityPanel,
  },
  {
    slug: "rss",
    label: "next.settings.panel.rss",
    icon: "rss",
    Component: RssPanel,
  },
  {
    slug: "categories",
    label: "next.settings.panel.categories",
    icon: "categories",
    Component: CategoriesPanel,
  },
  {
    slug: "torrents",
    label: "next.settings.panel.torrents",
    note: "next.settings.panel.torrentsNote",
    icon: "downloads",
    torrentOnly: true,
    Component: TorrentsPanel,
  },
  {
    slug: "networking/overview",
    group: "networking",
    label: "settings.networkOverview",
    icon: "networkOverview",
    Component: NetworkingPanel,
  },
  {
    slug: "networking/egress",
    group: "networking",
    label: "settings.networkEgress",
    icon: "egress",
    Component: NetworkingPanel,
  },
  {
    slug: "networking/proxies",
    group: "networking",
    label: "settings.proxies",
    icon: "proxies",
    Component: NetworkingPanel,
  },
  {
    slug: "networking/routes",
    group: "networking",
    label: "settings.networkRoutes",
    icon: "routes",
    Component: NetworkingPanel,
  },
  {
    slug: "networking/dns",
    group: "networking",
    label: "next.dns.title",
    icon: "dns",
    Component: NetworkingPanel,
  },
  {
    slug: "networking/speed",
    group: "networking",
    label: "next.networking.speed.title",
    icon: "speedTest",
    Component: NetworkingPanel,
  },
  {
    slug: "schedules",
    label: "next.settings.panel.schedules",
    icon: "schedules",
    Component: SchedulesPanel,
  },
  {
    slug: "scripts/configuration",
    group: "scripts",
    label: "next.settings.panel.scriptsConfiguration",
    tag: "beta",
    icon: "scriptConfiguration",
    Component: ScriptConfigurationPanel,
  },
  {
    slug: "scripts/secrets",
    group: "scripts",
    label: "next.settings.panel.secrets",
    tag: "beta",
    icon: "secrets",
    Component: SecretsPanel,
  },
  {
    slug: "scripts/list",
    group: "scripts",
    label: "next.settings.panel.scriptJobs",
    tag: "beta",
    icon: "scriptList",
    Component: ScriptListPanel,
  },
  {
    slug: "scripts/runs",
    group: "scripts",
    label: "next.settings.panel.scriptRuns",
    tag: "beta",
    icon: "scriptRuns",
    Component: ScriptRunsPanel,
  },
  {
    slug: "watch-folder",
    label: "next.settings.panel.watchFolder",
    icon: "watchFolder",
    Component: WatchFolderPanel,
  },
  {
    slug: "backup",
    label: "next.settings.panel.backup",
    icon: "backup",
    Component: BackupPanel,
  },
];

/**
 * The panels there are: every one while the server has torrents, and all but
 * the torrents' own while it has none or has not said. The rail, the search
 * and a direct visit all go by this list, so a panel left out of it is, to
 * each of them, a panel that does not exist.
 */
export function settingsPanels(torrentsAvailable: boolean): PanelDefinition[] {
  return withoutTorrentOnly(SETTINGS_PANELS, torrentsAvailable, (panel) => panel.torrentOnly === true);
}

/** Whether `slug` is a panel there only while the server has torrents. */
export function isTorrentOnlyPanel(slug: string | undefined): boolean {
  return SETTINGS_PANELS.some((panel) => panel.slug === slug && panel.torrentOnly === true);
}

export function findPanel(
  slug: string | undefined,
  panels: readonly PanelDefinition[] = SETTINGS_PANELS,
): PanelDefinition | undefined {
  return panels.find((panel) => panel.slug === slug);
}

/**
 * A panel's heading in the search results: its rail name, after its group's
 * when it nests under one, since "Proxies" alone does not say where it lives.
 */
export function panelSearchTitle(
  t: (key: string, params?: Record<string, string>) => string,
  entry: PanelDefinition,
): string {
  const panel = t(entry.label);
  return entry.group
    ? t("next.settings.searchPanelPath", { group: t(PANEL_GROUPS[entry.group].label), panel })
    : panel;
}

/**
 * The rail's rows: every ungrouped panel at the top level, and each group as
 * one row there with its panels nested under it.
 */
export function settingsRail(
  t: (key: string) => string,
  providerCount: number,
  panels: readonly PanelDefinition[] = SETTINGS_PANELS,
): PanelListItem[] {
  const items: PanelListItem[] = [];
  const nested = new Map<PanelGroup, PanelListItem[]>();
  for (const entry of panels) {
    const row: PanelListItem = {
      to: `/settings/${entry.slug}`,
      label: t(entry.label),
      icon: entry.icon,
      tag:
        entry.tag === "beta"
          ? t("next.settings.beta")
          : entry.tag === "count:providers" && providerCount > 0
            ? String(providerCount)
            : undefined,
    };
    if (!entry.group) {
      items.push(row);
      continue;
    }
    const siblings = nested.get(entry.group);
    if (siblings) {
      siblings.push(row);
      continue;
    }
    // The group's own row sits where its first panel would, and opens it.
    const children = [row];
    nested.set(entry.group, children);
    const group = PANEL_GROUPS[entry.group];
    items.push({ to: row.to, label: t(group.label), icon: group.icon, children });
  }
  // A tag every panel of a group carries is said once, on the group's row.
  return items.map((item) => {
    const shared = item.children?.[0]?.tag;
    if (shared === undefined || !item.children?.every((child) => child.tag === shared)) {
      return item;
    }
    return {
      ...item,
      tag: shared,
      children: item.children.map((child) => ({ ...child, tag: undefined })),
    };
  });
}
