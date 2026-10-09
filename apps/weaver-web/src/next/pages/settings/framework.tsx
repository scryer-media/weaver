import {
  Fragment,
  createContext,
  useContext,
  useEffect,
  useId,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
  useSyncExternalStore,
  type ReactNode,
} from "react";
import { createPortal } from "react-dom";
import { useTranslate } from "@/lib/context/translate-context";
import { Eyebrow, EmptyState, SectionHeader } from "@/next/components/chrome";
import { PathField } from "@/next/features/DirectoryBrowserDialog";
import {
  NumberField,
  PrimaryButton,
  Segmented,
  Select,
  Slider,
  TextArea,
  TimeField,
  TextField,
  Toggle,
} from "@/next/components/controls";
import { FormRow } from "@/next/components/rows";
import { cn } from "@/lib/utils";
import { filterBlocks, type SearchRegistry } from "./search";

/**
 * The settings screen's shared machinery.
 *
 * Every panel declares itself as data — sections of labelled fields, or a
 * table of records — and one renderer draws all of them. That is what makes
 * the search box, the empty state and the "a section with no surviving rows
 * disappears" rule work identically on eleven panels without any of them
 * implementing it.
 *
 * Save and Revert live in the shell's top bar, so the mounted panel registers
 * its dirty state and its two handlers here.
 */

/* ------------------------------------------------------------------- fields */

export interface SelectOption {
  value: string;
  label: string;
}

export type FieldControl =
  | {
      kind: "toggle";
      value: boolean;
      onChange: (next: boolean) => void;
      disabled?: boolean;
    }
  | {
      kind: "select";
      value: string;
      options: readonly SelectOption[];
      onChange: (next: string) => void;
      className?: string;
    }
  | {
      kind: "segmented";
      value: string;
      options: readonly SelectOption[];
      onChange: (next: string) => void;
    }
  | {
      kind: "text";
      value: string;
      onChange: (next: string) => void;
      placeholder?: string;
      mono?: boolean;
      type?: "text" | "password" | "url";
      className?: string;
      autoComplete?: string;
      /** Keep password managers out. Defaults to on for a password that is not the Weaver login. */
      secret?: boolean;
    }
  /** A folder on the daemon's filesystem: typed, or picked with Browse. */
  | {
      kind: "path";
      value: string;
      onChange: (next: string) => void;
      placeholder?: string;
    }
  | {
      kind: "number";
      value: number;
      onChange: (next: number) => void;
      min?: number;
      max?: number;
      step?: number;
      suffix?: string;
      /** Decimal places the value keeps; whole numbers when omitted. */
      precision?: number;
    }
  | {
      kind: "textarea";
      value: string;
      onChange: (next: string) => void;
      rows?: number;
      placeholder?: string;
      /** Keep password managers out, for key material. */
      secret?: boolean;
    }
  | { kind: "time"; value: string; onChange: (next: string) => void }
  | {
      kind: "slider";
      value: number;
      min: number;
      max: number;
      step: number;
      onChange: (next: number) => void;
      display: string;
    }
  /** A value the daemon owns and the UI only reports. */
  | { kind: "static"; value: ReactNode }
  | { kind: "custom"; control: ReactNode };

export interface FieldSpec {
  id: string;
  label: string;
  help?: string;
  /** Extra text the search box should match — a path, a host, a unit. */
  keywords?: string;
  control: FieldControl;
  /**
   * For a row that only means something behind another row's toggle: `true`
   * slides it shut, `false` slides it open. Rows without it are always shown.
   */
  collapsed?: boolean;
}

/** Draw one field's control. Shared by the panels and by the record editor. */
export function FieldControlView({ spec }: { spec: FieldSpec }) {
  const control = spec.control;
  switch (control.kind) {
    case "toggle":
      return (
        <Toggle
          checked={control.value}
          onChange={control.onChange}
          label={spec.label}
          disabled={control.disabled}
        />
      );
    case "select":
      return (
        <Select
          value={control.value}
          options={control.options}
          onChange={control.onChange}
          label={spec.label}
          className={control.className}
        />
      );
    case "segmented":
      return (
        <Segmented
          value={control.value}
          options={control.options}
          onChange={control.onChange}
          label={spec.label}
        />
      );
    case "text":
      return (
        <TextField
          value={control.value}
          onChange={control.onChange}
          label={spec.label}
          placeholder={control.placeholder}
          mono={control.mono ?? true}
          type={control.type}
          autoComplete={control.autoComplete}
          secret={control.secret}
          className={control.className ?? "w-[268px] max-w-full"}
        />
      );
    case "path":
      return (
        <PathField
          value={control.value}
          onChange={control.onChange}
          label={spec.label}
          placeholder={control.placeholder}
        />
      );
    case "number":
      return (
        <NumberField
          value={control.value}
          onChange={control.onChange}
          label={spec.label}
          min={control.min}
          max={control.max}
          step={control.step}
          suffix={control.suffix}
          precision={control.precision}
        />
      );
    case "textarea":
      return (
        <TextArea
          value={control.value}
          onChange={control.onChange}
          label={spec.label}
          rows={control.rows}
          placeholder={control.placeholder}
          secret={control.secret}
        />
      );
    case "time":
      return (
        <TimeField
          value={control.value}
          onChange={control.onChange}
          label={spec.label}
        />
      );
    case "slider":
      return (
        <Slider
          value={control.value}
          min={control.min}
          max={control.max}
          step={control.step}
          onChange={control.onChange}
          label={spec.label}
          display={control.display}
        />
      );
    case "static":
      return (
        <div className="max-w-[380px] text-right font-wv-mono text-[12px] break-all text-wv-muted">
          {control.value}
        </div>
      );
    case "custom":
      return <>{control.control}</>;
  }
}

/** A run of fields as form rows — the body of a settings section, or a dialog. */
export function FieldRows({ fields }: { fields: readonly FieldSpec[] }) {
  return (
    <>
      {fields.map((field) => {
        const row = (
          <FormRow key={field.id} label={field.label} help={field.help}>
            <FieldControlView spec={field} />
          </FormRow>
        );
        if (field.collapsed === undefined) {
          return row;
        }
        // Animating the grid track from 0fr to 1fr slides the row to its real
        // height without measuring it; `inert` keeps a shut row out of the tab
        // order and away from assistive technology.
        return (
          <div
            key={field.id}
            inert={field.collapsed}
            className={cn(
              "grid transition-[grid-template-rows,opacity] duration-200 ease-out motion-reduce:transition-none",
              field.collapsed ? "grid-rows-[0fr] opacity-0" : "grid-rows-[1fr] opacity-100",
            )}
          >
            <div className="min-h-0 overflow-hidden">{row}</div>
          </div>
        );
      })}
    </>
  );
}

/* ------------------------------------------------------------------- blocks */

export interface SettingsSectionModel {
  kind: "section";
  id: string;
  title: string;
  /** A chip beside the title, such as the beta marker. */
  tag?: ReactNode;
  note?: ReactNode;
  fields: FieldSpec[];
}

export interface SettingsTableRowModel {
  id: string;
  /** Everything about this record the search box should match. */
  searchText: string;
  cells: ReactNode[];
  /**
   * How many columns each cell takes, for a row that holds fewer values than
   * the table has columns. A cell without an entry takes one.
   */
  spans?: number[];
  /**
   * Set on a row that opens in place rather than into an editor: whether it
   * is open now. The table's `onRowClick` toggles it.
   */
  expanded?: boolean;
  /** What an open row shows under its columns, across the whole table. */
  detail?: ReactNode;
}

/** The rows of one kind in a table that lists several kinds under the same columns. */
export interface SettingsTableGroupModel {
  id: string;
  title: string;
  note?: ReactNode;
  rows: SettingsTableRowModel[];
}

export interface SettingsTableModel {
  kind: "table";
  id: string;
  title: string;
  /** A chip beside the title, such as the beta marker. */
  tag?: ReactNode;
  note?: ReactNode;
  /** Raw `grid-template-columns`, so panels keep the handoff's exact tracks. */
  columns: string;
  headers: ReactNode[];
  rows: SettingsTableRowModel[];
  /**
   * Rows that fall into kinds: each group follows `rows` under a heading of
   * its own, and a group with no rows is not drawn. One list with headings
   * keeps one set of columns and one "Add", where a table per kind repeats both.
   */
  groups?: SettingsTableGroupModel[];
  onRowClick?: (id: string) => void;
  empty?: string;
  /**
   * The panel's "Add" for this list, repeated under the empty message. An
   * empty panel's next step belongs where the eye already is, not only in the
   * top bar's corner.
   */
  emptyAction?: { label: string; onClick: () => void };
  /**
   * A trailing row of the table's own actions — "Add rule", "Clear history".
   * The top bar carries a panel's actions; a table that owns more than one
   * list carries the ones that belong to that list.
   */
  footer?: ReactNode;
}

/** An escape hatch for the few surfaces that are neither form nor table. */
export interface SettingsCustomModel {
  kind: "custom";
  id: string;
  title: string;
  /** A chip beside the title, such as the beta marker. */
  tag?: ReactNode;
  note?: ReactNode;
  searchText: string;
  body: ReactNode;
}

export type SettingsBlock =
  SettingsSectionModel | SettingsTableModel | SettingsCustomModel;

/**
 * Render a panel's blocks, filtered by the shell's search box.
 *
 * Without a query the blocks draw where the panel put them. With one, the
 * shell mounts every panel and hides its page; each list of blocks draws only
 * what matched, under that panel's heading in the cross-panel results, and
 * reports how much it found so the heading and the shell's empty state know.
 */
export function SettingsBlocks({
  blocks,
  loading = false,
}: {
  blocks: readonly (SettingsBlock | null)[];
  /** The panel's settings are still on its way; its fields would only show blanks. */
  loading?: boolean;
}) {
  const t = useTranslate();
  const { search } = useShell();
  const scope = useContext(PanelScopeContext);
  const searching = search.trim() !== "";

  const present = blocks.filter(
    (block): block is SettingsBlock => block !== null,
  );
  const visible = filterBlocks(present, search);
  useSearchReport(loading ? 0 : visible.length, loading);

  if (searching && scope !== null) {
    // The panel's own page is hidden while searching; the matches belong in
    // the results, under the panel's heading.
    if (loading || visible.length === 0 || scope.resultsHost === null) {
      return null;
    }
    return createPortal(<BlockList blocks={visible} />, scope.resultsHost);
  }

  if (loading) {
    return <EmptyState loading title={t("next.common.loading")} body={t("next.settings.loadingBody")} />;
  }

  if (visible.length === 0) {
    return (
      <EmptyState
        title={
          !searching
            ? t("next.settings.nothingHere")
            : t("next.settings.noMatch", { search: search.trim() })
        }
        body={
          !searching
            ? t("next.settings.nothingHereBody")
            : t("next.settings.noMatchBody")
        }
      />
    );
  }

  return <BlockList blocks={visible} />;
}

/**
 * Tell the search what this part of a panel found, while there is a query.
 *
 * `SettingsBlocks` reports for itself; a panel that draws its blocks only
 * once its data arrives reports `pending` until then, so the search says
 * "loading" rather than "no match" while that panel cannot answer yet. A
 * layout effect, so the first frame of a search already knows.
 */
export function useSearchReport(count: number, pending: boolean): void {
  const { search, registry } = useShell();
  const scope = useContext(PanelScopeContext);
  const key = useId();
  const slug = scope?.slug ?? null;
  const searching = search.trim() !== "";

  useLayoutEffect(() => {
    if (!searching || slug === null) {
      return;
    }
    registry.report(key, slug, { count, loading: pending });
    return () => registry.remove(key);
  }, [count, key, pending, registry, searching, slug]);
}

function BlockList({ blocks }: { blocks: readonly SettingsBlock[] }) {
  return (
    <>
      {blocks.map((block) => (
        <section key={block.id} aria-label={block.title} className="flex flex-none flex-col">
          <SectionHeader label={block.title} tag={block.tag} note={block.note} />
          {block.kind === "section" ? (
            <FieldRows fields={block.fields} />
          ) : null}
          {block.kind === "custom" ? block.body : null}
          {block.kind === "table" ? <SettingsTable block={block} /> : null}
        </section>
      ))}
    </>
  );
}

/**
 * A settings table scrolls sideways rather than folding.
 *
 * These tables are five columns of machine values — host, connections, transport,
 * role, actions — and the widest of them wants about 640px before its tracks
 * start lying about what they hold. Dropping columns would hide configuration
 * the panel exists to edit, so the table keeps them all and takes its own
 * scroller instead of pushing the page sideways. The scrollbar stays visible
 * here, unlike the tab strips: a table has no other cue that there is more.
 */
function SettingsTable({ block }: { block: SettingsTableModel }) {
  const t = useTranslate();
  const groups = (block.groups ?? []).filter((group) => group.rows.length > 0);
  const row = (entry: SettingsTableRowModel) => {
    const onRowClick = block.onRowClick;
    const interactive = typeof onRowClick === "function";
    // A row that opens in place is a disclosure: it says whether it is open,
    // names what it opens, and answers Space as a button does.
    const disclosure = interactive && entry.expanded !== undefined;
    const detailId = `${block.id}-detail-${entry.id}`;
    const line = (
      <div
        key={disclosure ? undefined : entry.id}
        {...(interactive
          ? {
              role: "button",
              tabIndex: 0,
              ...(disclosure
                ? { "aria-expanded": entry.expanded, "aria-controls": entry.expanded ? detailId : undefined }
                : {}),
              onClick: () => onRowClick?.(entry.id),
              onKeyDown: (event: React.KeyboardEvent) => {
                if (event.target !== event.currentTarget && disclosure) {
                  return;
                }
                if (event.key === "Enter" || (disclosure && event.key === " ")) {
                  event.preventDefault();
                  onRowClick?.(entry.id);
                }
              },
            }
          : {})}
        className={`grid items-center gap-5 border-b border-wv-hairline px-4 sm:px-6 py-3 text-[12.5px] text-wv-fg hover:bg-wv-cell-hover${
          interactive ? " cursor-pointer" : ""
        }`}
        style={{ gridTemplateColumns: block.columns }}
      >
        {entry.cells.map((cell, index) => {
          const span = entry.spans?.[index] ?? 1;
          return (
            <div
              key={index}
              className="flex min-w-0 items-center"
              style={span > 1 ? { gridColumn: `span ${span}` } : undefined}
            >
              {cell}
            </div>
          );
        })}
      </div>
    );
    if (!disclosure) {
      return line;
    }
    return (
      <Fragment key={entry.id}>
        {line}
        {entry.expanded ? (
          <div id={detailId} className="border-b border-wv-hairline px-4 sm:px-6 pt-1 pb-4">
            {entry.detail}
          </div>
        ) : null}
      </Fragment>
    );
  };
  return (
    <div className="min-w-0 overflow-x-auto">
      <div className="min-w-[640px]">
        <div
          className="grid items-center gap-5 border-b border-wv-hairline px-4 sm:px-6 py-2"
          style={{ gridTemplateColumns: block.columns }}
        >
          {block.headers.map((header, index) => (
            <Eyebrow key={index} tone="rail" className="truncate">
              {header}
            </Eyebrow>
          ))}
        </div>
        {block.rows.length === 0 && groups.length === 0 ? (
          <div className="flex flex-col items-start gap-3 px-4 sm:px-6 py-5">
            <span className="text-[13px] text-wv-muted">{block.empty ?? t("next.settings.nothingConfigured")}</span>
            {block.emptyAction === undefined ? null : (
              <PrimaryButton icon="add" onClick={block.emptyAction.onClick}>
                {block.emptyAction.label}
              </PrimaryButton>
            )}
          </div>
        ) : (
          <>
            {block.rows.map(row)}
            {groups.map((group) => (
              <section key={group.id} aria-label={group.title}>
                <div className="flex items-center gap-[10px] border-b border-wv-hairline px-4 sm:px-6 pb-2 pt-4">
                  <Eyebrow>{group.title}</Eyebrow>
                  {group.note === undefined ? null : (
                    <span className="ml-auto truncate font-wv-mono text-[11px] text-wv-note">{group.note}</span>
                  )}
                </div>
                {group.rows.map(row)}
              </section>
            ))}
          </>
        )}
        {block.footer === undefined ? null : (
          <div className="flex flex-none items-center justify-end gap-[10px] border-b border-wv-hairline px-4 sm:px-6 py-[10px]">
            {block.footer}
          </div>
        )}
      </div>
    </div>
  );
}

/* -------------------------------------------------------------------- shell */

export interface PanelFlags {
  dirty: boolean;
  busy: boolean;
  /** Replaces the status bar's "All changes saved" — a result, or a failure. */
  status: string | null;
  /** Colour the status line as a failure rather than as progress. */
  failed: boolean;
}

interface PanelActions {
  save: () => void;
  revert: () => void;
}

interface ShellApi {
  search: string;
  actionsRef: { current: PanelActions | null };
  setFlags: (flags: PanelFlags) => void;
  controlsHost: HTMLElement | null;
  /** What each panel's lists found for the search, for the headings and the empty state. */
  registry: SearchRegistry;
}

const ShellContext = createContext<ShellApi | null>(null);

export function SettingsShellProvider({
  search,
  actionsRef,
  setFlags,
  controlsHost,
  registry,
  children,
}: ShellApi & { children: ReactNode }) {
  const value = useMemo<ShellApi>(
    () => ({ search, actionsRef, setFlags, controlsHost, registry }),
    [actionsRef, controlsHost, registry, search, setFlags],
  );
  return (
    <ShellContext.Provider value={value}>{children}</ShellContext.Provider>
  );
}

function useShell(): ShellApi {
  const shell = useContext(ShellContext);
  if (!shell) {
    throw new Error("Settings panels must render inside SettingsShellProvider");
  }
  return shell;
}

export function useSettingsSearch(): string {
  return useShell().search;
}

/* -------------------------------------------------------------- panel scope */

interface PanelScope {
  slug: string;
  /** The panel the route names: the one whose Save, Revert and controls the top bar carries. */
  active: boolean;
  /** Where this panel's search matches draw, under its heading. */
  resultsHost: HTMLElement | null;
}

const PanelScopeContext = createContext<PanelScope | null>(null);

/** The slug of the panel this component renders in, or null outside one. */
export function useSettingsPanelSlug(): string | null {
  return useContext(PanelScopeContext)?.slug ?? null;
}

/**
 * Whether this panel is the open one. Only the open panel is mounted unless
 * the search box has a query; then every panel is, to answer it, and the
 * others must keep their hands off the top bar and the URL.
 */
export function useSettingsPanelActive(): boolean {
  return useContext(PanelScopeContext)?.active ?? true;
}

/**
 * One panel's place in the settings area.
 *
 * Without a query it draws the panel's page. With one it hides the page — the
 * panel stays mounted, so an open panel keeps its unsaved edits — and draws a
 * heading with whatever the panel's lists found beneath it, or nothing when
 * they found nothing. The open panel's matches stay live; another panel's are
 * a preview, and its heading or a click on them opens that panel with the
 * query kept.
 */
export function SettingsPanelScope({
  slug,
  active,
  title,
  onOpen,
  children,
}: {
  slug: string;
  active: boolean;
  title: string;
  onOpen: () => void;
  children: ReactNode;
}) {
  const t = useTranslate();
  const { search, registry } = useShell();
  const searching = search.trim() !== "";
  const [resultsHost, setResultsHost] = useState<HTMLElement | null>(null);
  const found = useSyncExternalStore(registry.subscribe, () => registry.panel(slug).count);
  const scope = useMemo<PanelScope>(() => ({ slug, active, resultsHost }), [active, resultsHost, slug]);
  const shown = searching && found > 0;

  return (
    <PanelScopeContext.Provider value={scope}>
      <section aria-label={title} hidden={!shown} className="flex flex-none flex-col">
        <div className="flex items-center gap-[10px] border-b border-wv-hairline bg-wv-list px-4 pt-5 pb-2 sm:px-6">
          {active ? (
            <span className="text-[13px] font-medium text-wv-fg">{title}</span>
          ) : (
            <button
              type="button"
              onClick={onOpen}
              aria-label={t("next.settings.searchOpenPanel", { panel: title })}
              className="cursor-pointer text-[13px] font-medium text-wv-fg underline-offset-4 hover:underline"
            >
              {title}
            </button>
          )}
          {active ? <Eyebrow className="ml-auto">{t("next.settings.searchOpenNow")}</Eyebrow> : null}
        </div>
        {/*
          Another panel's matches are a preview: its Save and Revert are not
          in the top bar, so an edit made here could not be kept. `inert`
          keeps them out of reach, and a click anywhere on them opens the
          panel, where they are live.
        */}
        <div
          onClick={active ? undefined : onOpen}
          className={active ? undefined : "cursor-pointer opacity-80"}
        >
          <div ref={setResultsHost} inert={!active} className="flex flex-col" />
        </div>
      </section>
      <div className={searching ? "hidden" : "contents"}>{children}</div>
    </PanelScopeContext.Provider>
  );
}

/**
 * A panel's own controls, teleported into the top bar's left slot.
 *
 * A portal rather than shell state: the node is a new element on every render,
 * and storing it in state above would loop.
 */
export function PanelControls({ children }: { children: ReactNode }) {
  const { controlsHost } = useShell();
  const active = useSettingsPanelActive();
  return controlsHost && active ? createPortal(children, controlsHost) : null;
}

/**
 * Publish a panel's dirty state and its Save/Revert handlers to the top bar.
 *
 * The handlers go through a ref for the same reason: they are fresh closures
 * each render, and shell state would re-render the panel that set them.
 */
export function usePanelState({
  dirty,
  busy = false,
  status = null,
  failed = false,
  save,
  revert,
}: {
  dirty: boolean;
  busy?: boolean;
  status?: string | null;
  failed?: boolean;
  save: () => void;
  revert: () => void;
}): void {
  const shell = useShell();
  // A panel mounted only to answer the search does not own the top bar.
  const active = useSettingsPanelActive();

  useEffect(() => {
    if (active) {
      shell.actionsRef.current = { save, revert };
    }
  });

  useEffect(() => {
    if (active) {
      shell.setFlags({ dirty, busy, status, failed });
    }
  }, [active, busy, dirty, failed, shell, status]);

  useEffect(() => {
    if (!active) {
      return;
    }
    const actions = shell.actionsRef;
    return () => {
      actions.current = null;
      shell.setFlags({
        dirty: false,
        busy: false,
        status: null,
        failed: false,
      });
    };
  }, [active, shell]);
}

/**
 * Publish a status line and nothing else.
 *
 * The list-shaped panels write the moment a dialog is confirmed, so Save and
 * Revert have nothing to act on — but a sync report or a failed delete still
 * belongs in the status bar.
 */
export function usePanelStatus(status: string | null, failed = false): void {
  const shell = useShell();
  const active = useSettingsPanelActive();

  useEffect(() => {
    if (active) {
      shell.setFlags({ dirty: false, busy: false, status, failed });
    }
  }, [active, failed, shell, status]);

  useEffect(() => {
    if (!active) {
      return;
    }
    return () =>
      shell.setFlags({
        dirty: false,
        busy: false,
        status: null,
        failed: false,
      });
  }, [active, shell]);
}

/**
 * A draft of a server-held record.
 *
 * Re-seeds whenever the server's value changes, unless the draft is dirty — a
 * live refetch must never throw away half-typed edits.
 */
export interface Draft<T> {
  value: T | null;
  dirty: boolean;
  set: (patch: Partial<T>) => void;
  replace: (next: T) => void;
  revert: () => void;
  markSaved: () => void;
}

export function useDraft<T>(source: T | null, onEdit?: () => void): Draft<T> {
  const [draft, setDraft] = useState<T | null>(source);
  const [dirty, setDirty] = useState(false);
  const dirtyRef = useRef(dirty);
  dirtyRef.current = dirty;

  useEffect(() => {
    if (source !== null && !dirtyRef.current) {
      setDraft(source);
    }
  }, [source]);

  return {
    value: draft,
    dirty,
    set: (patch: Partial<T>) => {
      setDraft((current) =>
        current === null ? current : { ...current, ...patch },
      );
      setDirty(true);
      onEdit?.();
    },
    replace: (next: T) => {
      setDraft(next);
      setDirty(true);
      onEdit?.();
    },
    revert: () => {
      setDraft(source);
      setDirty(false);
    },
    markSaved: () => setDirty(false),
  };
}
