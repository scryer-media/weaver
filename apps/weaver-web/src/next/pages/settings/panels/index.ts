import type { ComponentType } from "react";
import {
  CalendarClock,
  DatabaseBackup,
  FolderInput,
  FolderTree,
  Gauge,
  Rss,
  Server,
  Shield,
  SlidersHorizontal,
  SquareTerminal,
  Waypoints,
  type LucideIcon,
} from "lucide-react";
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
  label: string;
  /** The mono subtitle beside the panel's title in the top bar. */
  note: string;
  /** Right-hand rail tag; `"count:providers"` resolves to the server count. */
  tag?: "beta" | "count:providers";
  /** The rail icon. A trial: the rail is the one place the redesign draws them. */
  icon: LucideIcon;
  Component: ComponentType;
}

export const SETTINGS_PANELS: readonly PanelDefinition[] = [
  {
    slug: "general",
    label: "General",
    note: "interface, language, downloads",
    icon: SlidersHorizontal,
    Component: GeneralPanel,
  },
  {
    slug: "servers",
    label: "Providers",
    note: "tried in priority order",
    tag: "count:providers",
    icon: Server,
    Component: ProvidersPanel,
  },
  {
    slug: "security",
    label: "Security",
    note: "sign-in, access, API keys",
    icon: Shield,
    Component: SecurityPanel,
  },
  {
    slug: "rss",
    label: "RSS",
    note: "feeds, rules, seen items",
    icon: Rss,
    Component: RssPanel,
  },
  {
    slug: "categories",
    label: "Categories",
    note: "where a release lands",
    icon: FolderTree,
    Component: CategoriesPanel,
  },
  {
    slug: "proxies",
    label: "Proxies",
    note: "tunnels providers and feeds may use",
    icon: Waypoints,
    Component: ProxiesPanel,
  },
  {
    slug: "bandwidth",
    label: "Bandwidth",
    note: "ceilings and the ISP cap",
    icon: Gauge,
    Component: BandwidthPanel,
  },
  {
    slug: "schedules",
    label: "Schedules",
    note: "when weaver pauses and resumes",
    icon: CalendarClock,
    Component: SchedulesPanel,
  },
  {
    slug: "post-processing",
    label: "Post-processing",
    note: "scripts run when a download finishes",
    tag: "beta",
    icon: SquareTerminal,
    Component: PostProcessingPanel,
  },
  {
    slug: "watch-folder",
    label: "Watch folder",
    note: "NZB files picked up from disk",
    icon: FolderInput,
    Component: WatchFolderPanel,
  },
  {
    slug: "backup",
    label: "Backup",
    note: "snapshot and restore",
    icon: DatabaseBackup,
    Component: BackupPanel,
  },
];

export function findPanel(slug: string | undefined): PanelDefinition | undefined {
  return SETTINGS_PANELS.find((panel) => panel.slug === slug);
}
