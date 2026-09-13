import { useCallback, useEffect, useLayoutEffect, useRef, useState } from "react";
import { useVirtualizer } from "@tanstack/react-virtual";
import { useClient } from "urql";
import { BROWSE_DIRECTORIES_QUERY, CREATE_DIRECTORY_MUTATION } from "@/graphql/queries";
import { LoadingMark } from "@/lib/loading-mark";
import { cn } from "@/lib/utils";
import { Dialog } from "@/next/components/Dialog";
import { Cell, GridHeader, GridRow } from "@/next/components/rows";
import { PrimaryButton, SecondaryButton, TextField } from "@/next/components/controls";
import {
  describeEntryCount,
  filterEntries,
  parentPath as parentOf,
  pathCrumbs,
  type DirectoryEntry,
} from "@/next/data/directory-browser";

interface DirectoryListing {
  currentPath: string;
  parentPath: string | null;
  entries: DirectoryEntry[];
}

const ROW_HEIGHT = 34;
const COLUMNS = "minmax(0,1fr) auto";

function failureMessage(error: { graphQLErrors: { message: string }[]; message: string }): string {
  return error.graphQLErrors[0]?.message ?? error.message;
}

/**
 * Pick a folder on the daemon's filesystem.
 *
 * A library folder can hold thousands of entries, so the listing is a
 * virtualized table: only the rows in view exist in the DOM, however long the
 * directory is. Clicking a row opens it; "Use this folder" takes the folder
 * being shown. Browsing with no starting path opens the completed folder, the
 * daemon's own default.
 */
export function DirectoryBrowserDialog({
  open,
  initialPath,
  title = "Choose a folder",
  onClose,
  onChoose,
}: {
  open: boolean;
  initialPath: string | null;
  title?: string;
  onClose: () => void;
  onChoose: (path: string) => void;
}) {
  const client = useClient();
  const [listing, setListing] = useState<DirectoryListing | null>(null);
  const [typedPath, setTypedPath] = useState("");
  const [filter, setFilter] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [newFolder, setNewFolder] = useState("");
  const [creating, setCreating] = useState(false);
  const [createError, setCreateError] = useState<string | null>(null);
  // Only the newest request may land: a slow listing of a huge folder must
  // not replace the one the user has already moved on to.
  const requestRef = useRef(0);
  const scrollRef = useRef<HTMLDivElement | null>(null);
  const crumbsRef = useRef<HTMLDivElement | null>(null);

  const show = useCallback((next: DirectoryListing) => {
    setListing(next);
    setTypedPath(next.currentPath);
    setFilter("");
    setError(null);
  }, []);

  const browse = useCallback(
    async (path: string | null, fallback?: string | null) => {
      const request = ++requestRef.current;
      setLoading(true);
      setError(null);
      setCreateError(null);
      const result = await client
        .query<{ browseDirectories: DirectoryListing }>(
          BROWSE_DIRECTORIES_QUERY,
          { path },
          { requestPolicy: "network-only" },
        )
        .toPromise();
      if (request !== requestRef.current) {
        return;
      }
      if (result.error || !result.data?.browseDirectories) {
        if (fallback !== undefined) {
          void browse(fallback, fallback === null ? "/" : undefined);
          return;
        }
        setLoading(false);
        setError(result.error ? failureMessage(result.error) : "The folder could not be read.");
        return;
      }
      setLoading(false);
      show(result.data.browseDirectories);
    },
    [client, show],
  );

  useEffect(() => {
    if (!open) {
      requestRef.current += 1;
      setListing(null);
      setLoading(false);
      setError(null);
      setNewFolder("");
      setCreateError(null);
      return;
    }
    const start = initialPath?.trim() || null;
    // A saved path that no longer exists still opens somewhere useful.
    void browse(start, start === null ? "/" : null);
  }, [browse, initialPath, open]);

  const createFolder = async () => {
    const name = newFolder.trim();
    if (!listing || name === "") {
      return;
    }
    setCreating(true);
    setCreateError(null);
    const result = await client
      .mutation<{ createDirectory: DirectoryListing }>(CREATE_DIRECTORY_MUTATION, {
        path: listing.currentPath,
        name,
      })
      .toPromise();
    setCreating(false);
    if (result.error || !result.data?.createDirectory) {
      setCreateError(result.error ? failureMessage(result.error) : "The folder was not created.");
      return;
    }
    requestRef.current += 1;
    setNewFolder("");
    show(result.data.createDirectory);
  };

  const entries = listing?.entries ?? [];
  const shown = filterEntries(entries, filter);
  const currentPath = listing?.currentPath ?? "";
  const parent = listing ? (listing.parentPath ?? parentOf(listing.currentPath)) : null;
  const busy = loading || creating;

  const virtualizer = useVirtualizer({
    count: loading || error !== null ? 0 : shown.length,
    getScrollElement: () => scrollRef.current,
    getItemKey: (index) => shown[index]?.path ?? index,
    estimateSize: () => ROW_HEIGHT,
    overscan: 12,
    useFlushSync: false,
  });

  // A new folder or a new filter starts from the top of the list, and the
  // breadcrumb keeps its deepest folder in view.
  useLayoutEffect(() => {
    scrollRef.current?.scrollTo({ top: 0 });
    const crumbs = crumbsRef.current;
    if (crumbs) {
      crumbs.scrollLeft = crumbs.scrollWidth;
    }
  }, [currentPath, filter]);

  const crumbs = pathCrumbs(currentPath);

  return (
    <Dialog
      open={open}
      title={title}
      onDismiss={onClose}
      width={720}
      footer={
        <>
          <span
            title={currentPath}
            className="mr-auto min-w-0 flex-[1_1_200px] truncate font-wv-mono text-[11.5px] text-wv-muted"
          >
            {currentPath}
          </span>
          <SecondaryButton onClick={onClose}>Cancel</SecondaryButton>
          <PrimaryButton disabled={busy || !listing} onClick={() => listing && onChoose(listing.currentPath)}>
            Use this folder
          </PrimaryButton>
        </>
      }
    >
      <div className="flex min-h-0 flex-1 flex-col">
        <div
          ref={crumbsRef}
          className="flex h-10 flex-none items-center gap-[6px] overflow-x-auto border-b border-wv-hairline px-4 whitespace-nowrap sm:px-6"
        >
          {crumbs.map((crumb, index) => {
            const last = index === crumbs.length - 1;
            return (
              <span key={crumb.path} className="flex flex-none items-center gap-[6px]">
                {index === 0 ? null : <span className="font-wv-mono text-[11px] text-wv-faint">›</span>}
                <button
                  type="button"
                  title={crumb.path}
                  disabled={busy || last}
                  onClick={() => void browse(crumb.path)}
                  className={cn(
                    "max-w-[220px] truncate font-wv-mono text-[12px]",
                    last ? "cursor-default text-wv-fg" : "cursor-pointer text-wv-muted hover:text-wv-fg",
                  )}
                >
                  {crumb.label}
                </button>
              </span>
            );
          })}
        </div>

        <div className="flex flex-none flex-wrap items-center gap-2 border-b border-wv-hairline px-4 py-3 sm:px-6">
          <div className="flex min-w-0 flex-[1_1_320px] gap-2">
            <TextField
              label="Folder path"
              value={typedPath}
              onChange={setTypedPath}
              onKeyDown={(event) => {
                if (event.key === "Enter" && typedPath.trim() !== "") {
                  event.preventDefault();
                  void browse(typedPath.trim());
                }
              }}
              className="min-w-0 flex-1"
            />
            <SecondaryButton disabled={busy || typedPath.trim() === ""} onClick={() => void browse(typedPath.trim())}>
              Go
            </SecondaryButton>
          </div>
          <TextField
            label="Filter folders"
            placeholder="filter"
            value={filter}
            onChange={setFilter}
            onKeyDown={(event) => {
              // With the list narrowed to one folder, Enter opens it.
              if (event.key === "Enter" && shown.length === 1 && !busy) {
                event.preventDefault();
                void browse(shown[0]!.path);
              }
            }}
            className="w-[200px] max-w-full flex-[1_1_160px]"
          />
        </div>

        <GridHeader
          columns={COLUMNS}
          cells={["Folder", listing && !loading && error === null ? describeEntryCount(shown.length, entries.length) : ""]}
          cellClassNames={[undefined, "text-right"]}
        />
        {parent === null || loading ? null : (
          <GridRow
            columns={COLUMNS}
            title={parent}
            onClick={busy ? undefined : () => void browse(parent)}
            className="h-[34px] flex-none border-b border-wv-hairline px-4 sm:px-[22px]"
          >
            <Cell mono className="text-wv-muted">
              ..
            </Cell>
            <Cell mono className="text-right text-[11px] text-wv-faint">
              up one level
            </Cell>
          </GridRow>
        )}

        <div
          ref={scrollRef}
          className="relative h-[420px] min-h-[136px] flex-[0_1_420px] overflow-y-auto bg-wv-list"
        >
          {loading ? (
            <div role="status" className="flex items-center gap-3 px-4 py-4 text-[12.5px] text-wv-muted sm:px-6">
              <LoadingMark reveal />
              Reading folder…
            </div>
          ) : error !== null ? (
            <div className="px-4 py-4 text-[12.5px] text-wv-error-text sm:px-6">{error}</div>
          ) : shown.length === 0 ? (
            <div className="px-4 py-4 text-[12.5px] text-wv-muted sm:px-6">
              {entries.length === 0 ? "No folders in here." : "No folders match the filter."}
            </div>
          ) : (
            <div style={{ height: virtualizer.getTotalSize() }} className="relative w-full">
              {virtualizer.getVirtualItems().map((item) => {
                const entry = shown[item.index]!;
                return (
                  <div
                    key={item.key}
                    className="absolute top-0 left-0 w-full"
                    style={{ height: ROW_HEIGHT, transform: `translateY(${item.start}px)` }}
                  >
                    <GridRow
                      columns={COLUMNS}
                      title={entry.path}
                      onClick={busy ? undefined : () => void browse(entry.path)}
                      className="h-full border-b border-wv-hairline px-4 sm:px-[22px]"
                    >
                      <Cell className="text-wv-fg">{entry.name}</Cell>
                      <span />
                    </GridRow>
                  </div>
                );
              })}
            </div>
          )}
        </div>

        <div className="flex flex-none flex-wrap items-center gap-2 border-t border-wv-line-strong px-4 py-3 sm:px-6">
          <TextField
            label="New folder name"
            placeholder="new folder name"
            value={newFolder}
            mono={false}
            onChange={(next) => {
              setNewFolder(next);
              setCreateError(null);
            }}
            onKeyDown={(event) => {
              if (event.key === "Enter" && !busy && newFolder.trim() !== "") {
                event.preventDefault();
                void createFolder();
              }
            }}
            className="min-w-0 flex-[1_1_240px]"
          />
          <SecondaryButton disabled={busy || !listing || newFolder.trim() === ""} onClick={() => void createFolder()}>
            {creating ? "Creating…" : "Create folder"}
          </SecondaryButton>
          {createError === null ? null : (
            <div aria-live="polite" className="w-full text-[12px] text-wv-error-text">
              {createError}
            </div>
          )}
        </div>
      </div>
    </Dialog>
  );
}

/**
 * A path text field with a Browse button beside it: typing still works, and
 * the picker fills the field for anyone who would rather look.
 */
export function PathField({
  value,
  onChange,
  label,
  placeholder,
  compact = false,
  className,
}: {
  value: string;
  onChange: (next: string) => void;
  label: string;
  placeholder?: string;
  /** Table-cell height, for a path edited inside a row. */
  compact?: boolean;
  className?: string;
}) {
  const [browsing, setBrowsing] = useState(false);
  const [startPath, setStartPath] = useState<string | null>(null);
  return (
    <div className={cn("flex min-w-0 items-center gap-2", className ?? "w-[340px] max-w-full")}>
      <TextField
        label={label}
        value={value}
        placeholder={placeholder}
        onChange={onChange}
        className={cn("min-w-0 flex-1", compact && "h-7")}
      />
      <SecondaryButton
        size={compact ? "compact" : "default"}
        className={compact ? "h-7" : undefined}
        onClick={() => {
          setStartPath(value.trim() || null);
          setBrowsing(true);
        }}
      >
        Browse
      </SecondaryButton>
      <DirectoryBrowserDialog
        open={browsing}
        initialPath={startPath}
        title={label}
        onClose={() => setBrowsing(false)}
        onChoose={(path) => {
          onChange(path);
          setBrowsing(false);
        }}
      />
    </div>
  );
}
