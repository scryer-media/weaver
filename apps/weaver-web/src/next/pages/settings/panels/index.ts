import type { ComponentType } from "react";
import type { IconName } from "../../../components/icons";
import type { PanelListItem } from "../../../shell/rail-blocks";
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
import { WatchFolderPanel } from "./WatchFolderPanel";

/**
 * The settings panels, in rail order.
 *
 * The former proxies and bandwidth slugs redirect into Networking, and the former
 * post-processing slug into Scripts, so existing bookmarks keep working.
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
  /** Translation key of the mono subtitle beside the panel's title in the top bar. */
  note: string;
  /** Right-hand rail tag; `"count:providers"` resolves to the server count. */
  tag?: "beta" | "count:providers";
  /** The rail icon, from `ICONS`. */
  icon: IconName;
  Component: ComponentType;
}

export const SETTINGS_PANELS: readonly PanelDefinition[] = [
  {
    slug: "general",
    label: "next.settings.panel.general",
    note: "next.settings.panel.generalNote",
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
    note: "next.settings.panel.securityNote",
    icon: "security",
    Component: SecurityPanel,
  },
  {
    slug: "rss",
    label: "next.settings.panel.rss",
    note: "next.settings.panel.rssNote",
    icon: "rss",
    Component: RssPanel,
  },
  {
    slug: "categories",
    label: "next.settings.panel.categories",
    note: "next.settings.panel.categoriesNote",
    icon: "categories",
    Component: CategoriesPanel,
  },
  {
    slug: "networking/overview",
    group: "networking",
    label: "settings.networkOverview",
    note: "settings.networkingDesc",
    tag: "beta",
    icon: "networkOverview",
    Component: NetworkingPanel,
  },
  {
    slug: "networking/egress",
    group: "networking",
    label: "settings.networkEgress",
    note: "settings.networkingDesc",
    icon: "egress",
    Component: NetworkingPanel,
  },
  {
    slug: "networking/proxies",
    group: "networking",
    label: "settings.proxies",
    note: "settings.networkingDesc",
    icon: "proxies",
    Component: NetworkingPanel,
  },
  {
    slug: "networking/routes",
    group: "networking",
    label: "settings.networkRoutes",
    note: "settings.networkingDesc",
    icon: "routes",
    Component: NetworkingPanel,
  },
  {
    slug: "networking/bandwidth",
    group: "networking",
    label: "next.settings.panel.bandwidth",
    note: "settings.networkingDesc",
    icon: "bandwidth",
    Component: NetworkingPanel,
  },
  {
    slug: "schedules",
    label: "next.settings.panel.schedules",
    note: "next.settings.panel.schedulesNote",
    icon: "schedules",
    Component: SchedulesPanel,
  },
  {
    slug: "scripts/configuration",
    group: "scripts",
    label: "next.settings.panel.scriptsConfiguration",
    note: "next.settings.panel.scriptsNote",
    tag: "beta",
    icon: "scriptConfiguration",
    Component: ScriptConfigurationPanel,
  },
  {
    slug: "scripts/list",
    group: "scripts",
    label: "next.settings.panel.scripts",
    note: "next.settings.panel.scriptsNote",
    tag: "beta",
    icon: "scriptList",
    Component: ScriptListPanel,
  },
  {
    slug: "scripts/secrets",
    group: "scripts",
    label: "next.settings.panel.secrets",
    note: "next.settings.panel.secretsNote",
    tag: "beta",
    icon: "secrets",
    Component: SecretsPanel,
  },
  {
    slug: "scripts/runs",
    group: "scripts",
    label: "next.settings.panel.scriptRuns",
    note: "next.settings.panel.scriptRunsNote",
    tag: "beta",
    icon: "scriptRuns",
    Component: ScriptRunsPanel,
  },
  {
    slug: "watch-folder",
    label: "next.settings.panel.watchFolder",
    note: "next.settings.panel.watchFolderNote",
    icon: "watchFolder",
    Component: WatchFolderPanel,
  },
  {
    slug: "backup",
    label: "next.settings.panel.backup",
    note: "next.settings.panel.backupNote",
    icon: "backup",
    Component: BackupPanel,
  },
];

export function findPanel(slug: string | undefined): PanelDefinition | undefined {
  return SETTINGS_PANELS.find((panel) => panel.slug === slug);
}

/**
 * The rail's rows: every ungrouped panel at the top level, and each group as
 * one row there with its panels nested under it.
 */
export function settingsRail(
  t: (key: string) => string,
  providerCount: number,
): PanelListItem[] {
  const items: PanelListItem[] = [];
  const nested = new Map<PanelGroup, PanelListItem[]>();
  for (const entry of SETTINGS_PANELS) {
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
