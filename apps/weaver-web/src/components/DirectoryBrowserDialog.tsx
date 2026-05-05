import { useCallback, useEffect, useState } from "react";
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

  const isBusy = loading || createState.fetching;
  const pathSegments = currentPath.split("/").filter(Boolean);

  return (
    <Dialog open={open} onOpenChange={(next) => (!next ? onClose() : undefined)}>
      <DialogContent className="max-h-[85vh] overflow-hidden sm:max-w-2xl">
        <DialogHeader>
          <DialogTitle>{t("categories.directoryBrowserTitle")}</DialogTitle>
          <DialogDescription>{t("categories.directoryBrowserDesc")}</DialogDescription>
        </DialogHeader>

        <div className="space-y-4">
          <div className="flex items-center gap-1 overflow-x-auto rounded-xl border border-border/70 bg-background/70 px-2 py-2 text-sm">
            <button
              type="button"
              onClick={() => void browse(ROOT_PATH)}
              disabled={isBusy}
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
                    className={
                      isLast
                        ? "shrink-0 rounded px-1.5 py-0.5 font-medium text-foreground"
                        : "shrink-0 rounded px-1.5 py-0.5 text-muted-foreground transition-colors hover:bg-accent/50 hover:text-foreground"
                    }
                  >
                    {segment}
                  </button>
                </span>
              );
            })}
          </div>

          <div className="flex gap-2">
            <Input
              value={currentPath}
              onChange={(event) => setCurrentPath(event.target.value)}
              disabled={isBusy}
              onKeyDown={(event) => {
                if (event.key === "Enter") {
                  event.preventDefault();
                  void browse(currentPath);
                }
              }}
              className="font-mono text-sm"
            />
            <Button
              type="button"
              variant="outline"
              onClick={() => void browse(currentPath)}
              disabled={isBusy}
            >
              {t("categories.browse")}
            </Button>
          </div>

          <div className="space-y-2">
            <div className="flex gap-2">
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
              />
              <Button
                type="button"
                variant="outline"
                disabled={isBusy || !newFolderName.trim()}
                onClick={() => void handleCreateFolder()}
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

          <DialogFooter>
            <Button type="button" variant="outline" onClick={onClose}>
              {t("action.cancel")}
            </Button>
            <Button
              type="button"
              onClick={() => {
                const selectedPath = normalizePath(currentPath);
                onPathChange(selectedPath);
                onChoose(selectedPath);
              }}
              disabled={isBusy}
              className="max-w-full justify-start overflow-hidden"
            >
              <FolderOpen className="size-4" />
              <span>{t("categories.useCurrentFolder")}</span>
              <span className="min-w-0 truncate font-mono text-xs opacity-80">{currentPath}</span>
            </Button>
          </DialogFooter>
        </div>
      </DialogContent>
    </Dialog>
  );
}
