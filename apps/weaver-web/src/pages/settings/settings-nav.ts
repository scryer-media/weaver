import {
  CalendarClock,
  DatabaseBackup,
  FolderInput,
  Gauge,
  Rss,
  Server,
  ShieldCheck,
  SlidersHorizontal,
  Tags,
  Waypoints,
  Workflow,
  type LucideIcon,
} from "lucide-react";

type SettingsNavEntry = {
  to: string;
  labelKey: string;
  descriptionKey: string;
  icon: LucideIcon;
  beta?: boolean;
};

export const settingsNav: readonly SettingsNavEntry[] = [
  {
    to: "/settings/general",
    labelKey: "settings.general",
    descriptionKey: "settings.generalPageDesc",
    icon: SlidersHorizontal,
  },
  {
    to: "/settings/servers",
    labelKey: "servers.title",
    descriptionKey: "settings.serversDesc",
    icon: Server,
  },
  {
    to: "/settings/security",
    labelKey: "settings.security",
    descriptionKey: "settings.securityDesc",
    icon: ShieldCheck,
  },
  {
    to: "/settings/rss",
    labelKey: "settings.rss",
    descriptionKey: "settings.rssDesc",
    icon: Rss,
  },
  {
    to: "/settings/categories",
    labelKey: "categories.title",
    descriptionKey: "settings.categoriesDesc",
    icon: Tags,
  },
  {
    to: "/settings/proxies",
    labelKey: "settings.proxies",
    descriptionKey: "settings.proxiesDesc",
    icon: Waypoints,
  },
  {
    to: "/settings/bandwidth",
    labelKey: "settings.bandwidthCap",
    descriptionKey: "settings.bandwidthCapDesc",
    icon: Gauge,
  },
  {
    to: "/settings/schedules",
    labelKey: "schedule.title",
    descriptionKey: "schedule.desc",
    icon: CalendarClock,
  },
  {
    to: "/settings/post-processing",
    labelKey: "settings.postProcessing",
    descriptionKey: "settings.postProcessingDesc",
    icon: Workflow,
    beta: true,
  },
  {
    to: "/settings/watch-folder",
    labelKey: "watchFolder.title",
    descriptionKey: "watchFolder.desc",
    icon: FolderInput,
  },
  {
    to: "/settings/backup",
    labelKey: "settings.backupNav",
    descriptionKey: "settings.backupPageDesc",
    icon: DatabaseBackup,
  },
] as const;
