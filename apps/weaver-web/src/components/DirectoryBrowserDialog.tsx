import { useCallback, useEffect, useRef, useState } from "react";
import { ArrowUp, ChevronRight, Folder, FolderOpen, FolderPlus, Loader2 } from "lucide-react";
import { useClient, useMutation } from "urql";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { BROWSE_DIRECTORIES_QUERY, CREATE_DIRECTORY_MUTATION } from "@/graphql/queries";
import { useTranslate } from "@/lib/context/translate-context";

type DirectoryBrowseResult = {
  currentPath: string;
  parentPath: string | null;
  entries: { name: string; path: string }[];
};

const ROOT_PATH = "/";

function normalizePath(path: string | null | undefined): string {
  return path?.trim() || ROOT_PATH;
}

function parentPathFor(path: string): string | null {
  return path === ROOT_PATH ? null : path.replace(/\/[^/]+\/?$/, "") || ROOT_PATH;
}

function errorMessage(error: { graphQLErrors: { message: string }[]; message: string }): string {
  return error.graphQLErrors[0]?.message ?? error.message;
}

export function DirectoryBrowserDialog({
  open,
  path,
  onPathChange,
  onClose,
  onChoose,
}: {
  open: boolean;
  path: string | null;
  onPathChange: (path: string | null) => void;
  onClose: () => void;
  onChoose: (path: string) => void;
}) {
  const t = useTranslate();
  const client = useClient();
  const [createState, createDirectory] = useMutation<{
    createDirectory: DirectoryBrowseResult;
  }>(CREATE_DIRECTORY_MUTATION);
  const [currentPath, setCurrentPath] = useState(normalizePath(path));
  const [parentPath, setParentPath] = useState<string | null>(parentPathFor(normalizePath(path)));
  const [entries, setEntries] = useState<DirectoryBrowseResult["entries"]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [newFolderName, setNewFolderName] = useState("");
  const [createError, setCreateError] = useState<string | null>(null);
  const breadcrumbRef = useRef<HTMLDivElement | null>(null);

  const applyListing = useCallback((browser: DirectoryBrowseResult | null | undefined, fallbackPath: string) => {
    const resolvedPath = browser?.currentPath?.trim() || fallbackPath;
    setCurrentPath(resolvedPath);
    setParentPath(browser?.parentPath ?? parentPathFor(resolvedPath));
    setEntries(browser?.entries ?? []);
    setError(null);
  }, []);

  const browse = useCallback(
    async function runBrowse(
      nextPath: string | null | undefined,
      options?: { fallbackToRootOnError?: boolean },
    ): Promise<void> {
      const requestedPath = normalizePath(nextPath);
      setCurrentPath(requestedPath);
      setLoading(true);
      setError(null);
      setCreateError(null);

      const result = await client
        .query<{ browseDirectories: DirectoryBrowseResult }>(BROWSE_DIRECTORIES_QUERY, {
          path: requestedPath,
        })
        .toPromise();

      setLoading(false);

      if (result.error) {
        if (options?.fallbackToRootOnError && requestedPath !== ROOT_PATH) {
          await runBrowse(ROOT_PATH, { fallbackToRootOnError: false });
          return;
        }
        setEntries([]);
        setParentPath(parentPathFor(requestedPath));
        setError(result.error.message);
        return;
      }

      applyListing(result.data?.browseDirectories, requestedPath);
    },
    [applyListing, client],
  );

  const handleCreateFolder = useCallback(async () => {
    const folderName = newFolderName.trim();
    if (!folderName) {
      return;
    }

    const basePath = normalizePath(currentPath);
    const result = await createDirectory({
      path: basePath,
      name: folderName,
    });

    if (result.error) {
      setCreateError(errorMessage(result.error));
      return;
    }

    applyListing(result.data?.createDirectory, basePath);
    setNewFolderName("");
    setCreateError(null);
  }, [applyListing, createDirectory, currentPath, newFolderName]);

  useEffect(() => {
    if (!open) {
      setError(null);
      setCreateError(null);
      setNewFolderName("");
      return;
    }

    void browse(path, { fallbackToRootOnError: true });
  }, [open, path, browse]);

  useEffect(() => {
    const breadcrumb = breadcrumbRef.current;
    if (!breadcrumb) {
      return;
    }

    breadcrumb.scrollLeft = breadcrumb.scrollWidth;
  }, [currentPath]);

  const isBusy = loading || createState.fetching;
  const pathSegments = currentPath.split("/").filter(Boolean);

  return (
    <Dialog open={open} onOpenChange={(next) => (!next ? onClose() : undefined)}>
      <DialogContent className="flex max-h-[85vh] w-[min(96vw,64rem)] flex-col overflow-hidden sm:max-w-4xl">
        <DialogHeader>
          <DialogTitle>{t("categories.directoryBrowserTitle")}</DialogTitle>
          <DialogDescription>{t("categories.directoryBrowserDesc")}</DialogDescription>
        </DialogHeader>

        <div className="min-w-0 space-y-4">
          <div
            ref={breadcrumbRef}
            className="flex items-center gap-1 overflow-x-auto whitespace-nowrap rounded-xl border border-border/70 bg-background/70 px-2 py-2 text-sm"
          >
            <button
              type="button"
              onClick={() => void browse(ROOT_PATH)}
              disabled={isBusy}
              title={ROOT_PATH}
              className="shrink-0 rounded px-1.5 py-0.5 text-muted-foreground transition-colors hover:bg-accent/50 hover:text-foreground"
            >
              /
            </button>
            {pathSegments.map((segment, index) => {
              const segmentPath = `${ROOT_PATH}${pathSegments.slice(0, index + 1).join("/")}`;
              const isLast = index === pathSegments.length - 1;

              return (
                <span key={segmentPath} className="flex items-center gap-1">
                  <ChevronRight className="size-3 shrink-0 text-muted-foreground" />
                  <button
                    type="button"
                    onClick={() => void browse(segmentPath)}
                    disabled={isBusy}
                    title={segmentPath}
                    className={
                      isLast
                        ? "max-w-[10rem] shrink-0 truncate rounded px-1.5 py-0.5 font-medium text-foreground sm:max-w-[14rem]"
                        : "max-w-[10rem] shrink-0 truncate rounded px-1.5 py-0.5 text-muted-foreground transition-colors hover:bg-accent/50 hover:text-foreground sm:max-w-[14rem]"
                    }
                  >
                    {segment}
                  </button>
                </span>
              );
            })}
          </div>

          <div className="flex min-w-0 flex-col gap-2 sm:flex-row">
            <Input
              id="directory-browser-path"
              aria-label={t("categories.directoryBrowserPath")}
              value={currentPath}
              onChange={(event) => setCurrentPath(event.target.value)}
              disabled={isBusy}
              title={currentPath}
              onKeyDown={(event) => {
                if (event.key === "Enter") {
                  event.preventDefault();
                  void browse(currentPath);
                }
              }}
              className="min-w-0 flex-1 font-mono text-sm"
            />
            <Button
              type="button"
              variant="outline"
              onClick={() => void browse(currentPath)}
              disabled={isBusy}
              className="shrink-0"
            >
              {t("categories.browse")}
            </Button>
          </div>

          <div className="space-y-2">
            <div className="flex min-w-0 flex-col gap-2 sm:flex-row">
              <Input
                value={newFolderName}
                placeholder={t("categories.newFolderPlaceholder")}
                disabled={isBusy}
                onChange={(event) => {
                  setNewFolderName(event.target.value);
                  setCreateError(null);
                }}
                onKeyDown={(event) => {
                  if (event.key !== "Enter" || isBusy || !newFolderName.trim()) {
                    return;
                  }
                  event.preventDefault();
                  void handleCreateFolder();
                }}
                className="min-w-0 flex-1"
              />
              <Button
                type="button"
                variant="outline"
                disabled={isBusy || !newFolderName.trim()}
                onClick={() => void handleCreateFolder()}
                className="shrink-0"
              >
                {createState.fetching ? (
                  <Loader2 className="size-4 animate-spin" />
                ) : (
                  <FolderPlus className="size-4" />
                )}
                {t("categories.createFolder")}
              </Button>
            </div>
            <div aria-live="polite" className="min-h-5 text-sm text-destructive">
              {createError}
            </div>
          </div>

          <div className="flex h-[24rem] flex-col overflow-hidden rounded-2xl border border-border/70 bg-background/70">
            {loading ? (
              <div className="flex flex-1 items-center gap-2 px-3 py-4 text-sm text-muted-foreground">
                <Loader2 className="size-4 animate-spin" />
                {t("categories.directoryBrowserLoading")}
              </div>
            ) : error ? (
              <div className="flex-1 overflow-y-auto px-4 py-4 text-sm text-destructive">{error}</div>
            ) : (
              <div className="flex-1 overflow-y-auto divide-y divide-border/70">
                {parentPath !== null ? (
                  <button
                    type="button"
                    onClick={() => void browse(parentPath)}
                    disabled={isBusy}
                    className="flex w-full items-center gap-2.5 px-3 py-2 text-left text-sm text-muted-foreground transition-colors hover:bg-accent/40 hover:text-foreground"
                  >
                    <ArrowUp className="size-4 shrink-0" />
                    <span>..</span>
                  </button>
                ) : null}

                {entries.length === 0 ? (
                  <div className="px-3 py-6 text-center text-sm text-muted-foreground">
                    {t("categories.directoryBrowserEmpty")}
                  </div>
                ) : (
                  entries.map((entry) => (
                    <button
                      key={entry.path}
                      type="button"
                      className="flex w-full items-center gap-2.5 px-3 py-2 text-left text-sm transition-colors hover:bg-accent/40"
                      onClick={() => void browse(entry.path)}
                      disabled={isBusy}
                    >
                      <Folder className="size-4 shrink-0 text-muted-foreground" />
                      <span className="truncate text-foreground">{entry.name}</span>
                    </button>
                  ))
                )}
              </div>
            )}
          </div>

          <DialogFooter className="min-w-0 gap-2 sm:flex-row sm:items-end sm:justify-between">
            <Button type="button" variant="outline" onClick={onClose}>
              {t("action.cancel")}
            </Button>
            <div className="flex min-w-0 flex-1 flex-col items-stretch gap-2 sm:items-end">
              <span
                className="w-full truncate text-left font-mono text-xs text-muted-foreground sm:text-right"
                title={currentPath}
              >
                {currentPath}
              </span>
              <Button
                type="button"
                onClick={() => {
                  const selectedPath = normalizePath(currentPath);
                  onPathChange(selectedPath);
                  onChoose(selectedPath);
                }}
                disabled={isBusy}
                className="w-full sm:w-auto"
              >
                <FolderOpen className="size-4" />
                <span>{t("categories.useCurrentFolder")}</span>
              </Button>
            </div>
          </DialogFooter>
        </div>
      </DialogContent>
    </Dialog>
  );
}
