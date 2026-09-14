import type { ComponentType } from "react";
import type { IconName } from "../../../components/icons";
import { BackupPanel } from "./BackupPanel";
import { BandwidthPanel } from "./BandwidthPanel";
import { CategoriesPanel } from "./CategoriesPanel";
import { GeneralPanel } from "./GeneralPanel";
import { PostProcessingPanel } from "./PostProcessingPanel";
import { ProvidersPanel } from "./ProvidersPanel";
import { ProxiesPanel } from "./ProxiesPanel";
import { RssPanel } from "./RssPanel";
import { SchedulesPanel } from "./SchedulesPanel";
import { SecurityPanel } from "./SecurityPanel";
import { WatchFolderPanel } from "./WatchFolderPanel";

/**
 * The settings panels, in rail order.
 *
 * The list is weaver's, not the prototype's: the handoff draws eight invented
 * panels (Queue, Storage, Notifications …) against a daemon that has eleven
 * real ones, so the slugs below match the classic interface's routes and a
 * bookmarked `/settings/<panel>` keeps working across the switch.
 */

export interface PanelDefinition {
  slug: string;
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
    slug: "proxies",
    label: "next.settings.panel.proxies",
    note: "next.settings.panel.proxiesNote",
    tag: "beta",
    icon: "proxies",
    Component: ProxiesPanel,
  },
  {
    slug: "bandwidth",
    label: "next.settings.panel.bandwidth",
    note: "next.settings.panel.bandwidthNote",
    icon: "bandwidth",
    Component: BandwidthPanel,
  },
  {
    slug: "schedules",
    label: "next.settings.panel.schedules",
    note: "next.settings.panel.schedulesNote",
    icon: "schedules",
    Component: SchedulesPanel,
  },
  {
    slug: "post-processing",
    label: "next.settings.panel.postProcessing",
    note: "next.settings.panel.postProcessingNote",
    tag: "beta",
    icon: "postProcessing",
    Component: PostProcessingPanel,
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
