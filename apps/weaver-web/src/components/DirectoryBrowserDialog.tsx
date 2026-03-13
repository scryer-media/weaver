import { ChevronRight, FolderOpen, Loader2, MoveUp } from "lucide-react";
import { useQuery } from "urql";
import { Button } from "@/components/ui/button";
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { BROWSE_DIRECTORIES_QUERY } from "@/graphql/queries";
import { useTranslate } from "@/lib/context/translate-context";

type DirectoryBrowseResult = {
  currentPath: string;
  parentPath: string | null;
  entries: { name: string; path: string }[];
};

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
  const [{ data, fetching, error }] = useQuery<{ browseDirectories: DirectoryBrowseResult }>({
    query: BROWSE_DIRECTORIES_QUERY,
    variables: { path },
    pause: !open,
  });

  const browser = data?.browseDirectories;

  return (
    <Dialog open={open} onOpenChange={(next) => (!next ? onClose() : undefined)}>
      <DialogContent className="max-h-[85vh] overflow-hidden sm:max-w-2xl">
        <DialogHeader>
          <DialogTitle>{t("categories.directoryBrowserTitle")}</DialogTitle>
          <DialogDescription>{t("categories.directoryBrowserDesc")}</DialogDescription>
        </DialogHeader>

        <div className="space-y-4">
          <div className="rounded-xl border border-border/70 bg-background/70 px-3 py-2">
            <div className="text-[11px] uppercase tracking-[0.18em] text-muted-foreground">
              {t("categories.currentFolder")}
            </div>
            <div className="mt-1 break-all text-sm text-foreground">
              {browser?.currentPath ?? path ?? t("label.loading")}
            </div>
          </div>

          <div className="flex flex-wrap gap-2">
            <Button
              type="button"
              variant="outline"
              onClick={() => browser?.parentPath && onPathChange(browser.parentPath)}
              disabled={!browser?.parentPath || fetching}
            >
              <MoveUp className="size-4" />
              {t("categories.up")}
            </Button>
            <Button
              type="button"
              onClick={() => browser && onChoose(browser.currentPath)}
              disabled={!browser || fetching}
            >
              {t("categories.useCurrentFolder")}
            </Button>
          </div>

          <div className="max-h-[24rem] overflow-y-auto rounded-2xl border border-border/70 bg-background/70 p-2">
            {fetching ? (
              <div className="flex items-center gap-2 px-3 py-4 text-sm text-muted-foreground">
                <Loader2 className="size-4 animate-spin" />
                {t("categories.directoryBrowserLoading")}
              </div>
            ) : error ? (
              <div className="rounded-xl border border-destructive/30 bg-destructive/10 px-3 py-3 text-sm text-destructive">
                {error.message}
              </div>
            ) : browser && browser.entries.length > 0 ? (
              <div className="space-y-1">
                {browser.entries.map((entry) => (
                  <button
                    key={entry.path}
                    type="button"
                    className="flex w-full items-center justify-between rounded-xl px-3 py-2 text-left text-sm transition hover:bg-accent/40"
                    onClick={() => onPathChange(entry.path)}
                  >
                    <div className="flex min-w-0 items-center gap-3">
                      <FolderOpen className="size-4 shrink-0 text-primary" />
                      <span className="truncate text-foreground">{entry.name}</span>
                    </div>
                    <ChevronRight className="size-4 shrink-0 text-muted-foreground" />
                  </button>
                ))}
              </div>
            ) : (
              <div className="px-3 py-4 text-sm text-muted-foreground">
                {t("categories.directoryBrowserEmpty")}
              </div>
            )}
          </div>
        </div>
      </DialogContent>
    </Dialog>
  );
}
