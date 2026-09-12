import {
  createContext,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
  type ReactNode,
} from "react";
import { createPortal } from "react-dom";
import { Eyebrow, EmptyState, SectionHeader } from "@/next/components/chrome";
import {
  NumberField,
  Segmented,
  Select,
  Slider,
  TextArea,
  TimeField,
  TextField,
  Toggle,
} from "@/next/components/controls";
import { FormRow } from "@/next/components/rows";

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
    }
  | {
      kind: "number";
      value: number;
      onChange: (next: number) => void;
      min?: number;
      max?: number;
      step?: number;
      suffix?: string;
    }
  | {
      kind: "textarea";
      value: string;
      onChange: (next: string) => void;
      rows?: number;
      placeholder?: string;
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
          className={control.className ?? "w-[268px] max-w-full"}
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
      {fields.map((field) => (
        <FormRow key={field.id} label={field.label} help={field.help}>
          <FieldControlView spec={field} />
        </FormRow>
      ))}
    </>
  );
}

/* ------------------------------------------------------------------- blocks */

export interface SettingsSectionModel {
  kind: "section";
  id: string;
  title: string;
  note?: ReactNode;
  fields: FieldSpec[];
}

export interface SettingsTableRowModel {
  id: string;
  /** Everything about this record the search box should match. */
  searchText: string;
  cells: ReactNode[];
}

export interface SettingsTableModel {
  kind: "table";
  id: string;
  title: string;
  note?: ReactNode;
  /** Raw `grid-template-columns`, so panels keep the handoff's exact tracks. */
  columns: string;
  headers: ReactNode[];
  rows: SettingsTableRowModel[];
  onRowClick?: (id: string) => void;
  empty?: string;
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
  note?: ReactNode;
  searchText: string;
  body: ReactNode;
}

export type SettingsBlock =
  SettingsSectionModel | SettingsTableModel | SettingsCustomModel;

function matches(haystack: string, needle: string): boolean {
  return haystack.toLowerCase().includes(needle);
}

/** Render a panel's blocks, filtered by the shell's search box. */
export function SettingsBlocks({
  blocks,
}: {
  blocks: readonly (SettingsBlock | null)[];
}) {
  const search = useSettingsSearch().trim().toLowerCase();

  const present = blocks.filter(
    (block): block is SettingsBlock => block !== null,
  );
  const visible = present
    .map((block): SettingsBlock | null => {
      if (search === "" || matches(block.title, search)) {
        return block;
      }
      if (block.kind === "section") {
        const fields = block.fields.filter((field) =>
          matches(
            `${field.label} ${field.help ?? ""} ${field.keywords ?? ""}`,
            search,
          ),
        );
        return fields.length > 0 ? { ...block, fields } : null;
      }
      if (block.kind === "table") {
        const rows = block.rows.filter((row) =>
          matches(row.searchText, search),
        );
        return rows.length > 0 ? { ...block, rows } : null;
      }
      return matches(block.searchText, search) ? block : null;
    })
    .filter((block): block is SettingsBlock => block !== null);

  if (visible.length === 0) {
    return (
      <EmptyState
        title={
          search === ""
            ? "Nothing to configure here"
            : `No settings match "${search}"`
        }
        body={
          search === ""
            ? "This panel has no settings yet."
            : "Try another term, or clear the search to see this page."
        }
      />
    );
  }

  return (
    <>
      {visible.map((block) => (
        <section key={block.id} className="flex flex-none flex-col">
          <SectionHeader label={block.title} note={block.note} />
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
 * These tables are five columns of machine values — host, threads, transport,
 * role, actions — and the widest of them wants about 640px before its tracks
 * start lying about what they hold. Dropping columns would hide configuration
 * the panel exists to edit, so the table keeps them all and takes its own
 * scroller instead of pushing the page sideways. The scrollbar stays visible
 * here, unlike the tab strips: a table has no other cue that there is more.
 */
function SettingsTable({ block }: { block: SettingsTableModel }) {
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
        {block.rows.length === 0 ? (
          <div className="px-4 sm:px-6 py-5 text-[13px] text-wv-muted">
            {block.empty ?? "Nothing configured yet."}
          </div>
        ) : (
          block.rows.map((row) => {
            const onRowClick = block.onRowClick;
            const interactive = typeof onRowClick === "function";
            return (
              <div
                key={row.id}
                {...(interactive
                  ? {
                      role: "button",
                      tabIndex: 0,
                      onClick: () => onRowClick?.(row.id),
                      onKeyDown: (event: React.KeyboardEvent) => {
                        if (event.key === "Enter") {
                          event.preventDefault();
                          onRowClick?.(row.id);
                        }
                      },
                    }
                  : {})}
                className={`grid items-center gap-5 border-b border-wv-hairline px-4 sm:px-6 py-3 text-[12.5px] text-wv-fg hover:bg-wv-cell-hover${
                  interactive ? " cursor-pointer" : ""
                }`}
                style={{ gridTemplateColumns: block.columns }}
              >
                {row.cells.map((cell, index) => (
                  <div key={index} className="flex min-w-0 items-center">
                    {cell}
                  </div>
                ))}
              </div>
            );
          })
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
}

const ShellContext = createContext<ShellApi | null>(null);

export function SettingsShellProvider({
  search,
  actionsRef,
  setFlags,
  controlsHost,
  children,
}: ShellApi & { children: ReactNode }) {
  const value = useMemo<ShellApi>(
    () => ({ search, actionsRef, setFlags, controlsHost }),
    [actionsRef, controlsHost, search, setFlags],
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

/**
 * A panel's own controls, teleported into the top bar's left slot.
 *
 * A portal rather than shell state: the node is a new element on every render,
 * and storing it in state above would loop.
 */
export function PanelControls({ children }: { children: ReactNode }) {
  const { controlsHost } = useShell();
  return controlsHost ? createPortal(children, controlsHost) : null;
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

  useEffect(() => {
    shell.actionsRef.current = { save, revert };
  });

  useEffect(() => {
    shell.setFlags({ dirty, busy, status, failed });
  }, [busy, dirty, failed, shell, status]);

  useEffect(
    () => () => {
      shell.actionsRef.current = null;
      shell.setFlags({
        dirty: false,
        busy: false,
        status: null,
        failed: false,
      });
    },
    [shell],
  );
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

  useEffect(() => {
    shell.setFlags({ dirty: false, busy: false, status, failed });
  }, [failed, shell, status]);

  useEffect(
    () => () =>
      shell.setFlags({
        dirty: false,
        busy: false,
        status: null,
        failed: false,
      }),
    [shell],
  );
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
