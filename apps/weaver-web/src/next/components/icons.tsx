import {
  Activity,
  ArchiveRestore,
  ArrowDown,
  ArrowDownToLine,
  ArrowLeft,
  ArrowRight,
  ArrowUp,
  ArrowUpDown,
  ArrowUpToLine,
  CalendarClock,
  ChevronDown,
  ChevronLeft,
  Check,
  ChevronRight,
  CircleArrowUp,
  CircleStop,
  CircleX,
  Copy,
  Cpu,
  createLucideIcon,
  DatabaseBackup,
  Eraser,
  ExternalLink,
  FileDown,
  FileSearch,
  FileUp,
  FolderOpen,
  FolderPen,
  FolderPlus,
  FolderTree,
  Gauge,
  Heart,
  History,
  KeyRound,
  Languages,
  ListRestart,
  Lock,
  LockOpen,
  LogOut,
  Menu,
  Pause,
  Play,
  PlugZap,
  Plus,
  RefreshCw,
  RotateCcw,
  RotateCw,
  Rss,
  Save,
  ScrollText,
  Server,
  Settings,
  Shield,
  ShieldCheck,
  SlidersHorizontal,
  SquareTerminal,
  Trash2,
  Undo2,
  Waypoints,
  X,
  type LucideIcon,
} from "lucide-react";

/**
 * A folder watched by an eye — lucide has no such icon, so it is drawn in
 * lucide's own badge style: the folder outline stops short of a small eye in
 * the lower right, the way `FolderSearch` makes room for its magnifier.
 */
const FolderEye = createLucideIcon("folder-eye", [
  [
    "path",
    {
      d: "M9 20H4a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h3.9a2 2 0 0 1 1.69.9l.81 1.2a2 2 0 0 0 1.67.9H20a2 2 0 0 1 2 2v3",
      key: "folder",
    },
  ],
  [
    "path",
    {
      d: "M10.537 17.009a.6.6 0 0 1 0-.418 6.45 6.45 0 0 1 11.926 0 .6.6 0 0 1 0 .418 6.45 6.45 0 0 1-11.926 0",
      key: "eye",
    },
  ],
  ["circle", { cx: "16.5", cy: "16.8", r: "1.6", key: "pupil" }],
]);

/**
 * Every icon the Next interface draws, named by what it means rather than by
 * its picture.
 *
 * Screens take icons from here and never from `lucide-react` directly, so one
 * idea keeps one icon wherever it appears: categories are always the folder
 * tree, adding anything is always the plus, removing anything is always the
 * bin. A new use either reuses a meaning below or adds one here.
 */
export const ICONS = {
  // Places: the navigation, and the settings panels.
  downloads: ArrowDownToLine,
  completed: History,
  monitoring: Activity,
  systemInfo: Cpu,
  logs: ScrollText,
  settings: Settings,
  general: SlidersHorizontal,
  providers: Server,
  security: Shield,
  rss: Rss,
  categories: FolderTree,
  proxies: Waypoints,
  bandwidth: Gauge,
  schedules: CalendarClock,
  postProcessing: SquareTerminal,
  watchFolder: FolderEye,
  backup: DatabaseBackup,

  // Actions.
  add: Plus,
  remove: Trash2,
  forget: Eraser,
  cancelDownload: CircleX,
  stopScripts: CircleStop,
  pause: Pause,
  resume: Play,
  redownload: ListRestart,
  reprocess: RotateCw,
  reset: RotateCcw,
  refresh: RefreshCw,
  topOfQueue: ArrowUpToLine,
  priority: ArrowUpDown,
  moveUp: ArrowUp,
  moveDown: ArrowDown,
  save: Save,
  revert: Undo2,
  back: ArrowLeft,
  go: ArrowRight,
  copy: Copy,
  copied: Check,
  downloadFile: FileDown,
  chooseFile: FileUp,
  inspectFile: FileSearch,
  restore: ArchiveRestore,
  browse: FolderOpen,
  createFolder: FolderPlus,
  changeFolder: FolderPen,
  test: PlugZap,
  trust: ShieldCheck,
  password: KeyRound,
  language: Languages,
  lock: Lock,
  unlock: LockOpen,
  signOut: LogOut,
  close: X,
  menu: Menu,
  update: CircleArrowUp,
  external: ExternalLink,
  sponsor: Heart,

  // Affordances: what a control does when it is not a named action.
  dropdown: ChevronDown,
  expand: ChevronRight,
  open: ChevronRight,
  previous: ChevronLeft,
  next: ChevronRight,
} as const satisfies Record<string, LucideIcon>;

export type IconName = keyof typeof ICONS;

/** One stroke weight everywhere; the size is the only thing a context picks. */
export const ICON_STROKE = 1.75;

export function Icon({ name, size = 14, className }: { name: IconName; size?: number; className?: string }) {
  const Glyph = ICONS[name];
  return <Glyph aria-hidden="true" size={size} strokeWidth={ICON_STROKE} className={className} />;
}
