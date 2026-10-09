import type { ComponentType } from "react";
import type { IconName } from "../../../components/icons";
import { BackupPanel } from "./BackupPanel";
import { CategoriesPanel } from "./CategoriesPanel";
import { GeneralPanel } from "./GeneralPanel";
import { ScriptConfigurationPanel, ScriptListPanel } from "./PostProcessingPanel";
import { ProvidersPanel } from "./ProvidersPanel";
import { NetworkingPanel } from "./NetworkingPanel";
import { RssPanel } from "./RssPanel";
import { SchedulesPanel } from "./SchedulesPanel";
import { ScriptRunsPanel } from "./ScriptRunsPanel";
import { SecurityPanel } from "./SecurityPanel";
import { WatchFolderPanel } from "./WatchFolderPanel";

/**
 * The settings panels, in rail order.
 *
 * The former proxies and bandwidth slugs redirect into Networking, and the former
 * post-processing slug into Scripts, so existing bookmarks keep working.
 */

export interface PanelDefinition {
  slug: string;
  /** The rail heading the panel sits under, with the others that share it. */
  group?: "networking" | "scripts";
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
    icon: "proxies",
    Component: NetworkingPanel,
  },
  {slug:"networking/egress",group:"networking",label:"settings.networkEgress",note:"settings.networkingDesc",icon:"proxies",Component:NetworkingPanel},
  {slug:"networking/proxies",group:"networking",label:"settings.proxies",note:"settings.networkingDesc",icon:"proxies",Component:NetworkingPanel},
  {slug:"networking/routes",group:"networking",label:"settings.networkRoutes",note:"settings.networkingDesc",icon:"proxies",Component:NetworkingPanel},
  {slug:"networking/bandwidth",group:"networking",label:"next.settings.panel.bandwidth",note:"settings.networkingDesc",icon:"proxies",Component:NetworkingPanel},
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
    icon: "postProcessing",
    Component: ScriptConfigurationPanel,
  },
  {
    slug: "scripts/list",
    group: "scripts",
    label: "next.settings.panel.scripts",
    note: "next.settings.panel.scriptsNote",
    tag: "beta",
    icon: "postProcessing",
    Component: ScriptListPanel,
  },
  {
    slug: "scripts/runs",
    group: "scripts",
    label: "next.settings.panel.scriptRuns",
    note: "next.settings.panel.scriptRunsNote",
    tag: "beta",
    icon: "postProcessing",
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
