import { RotateCcw, UploadCloud, X } from "lucide-react";
import type { Ref } from "react";
import { useTranslate } from "@/lib/context/translate-context";
import { useUploadNzb, type UploadNzbEntry } from "@/features/upload/hooks/use-upload-nzb";
import { formatNzbNameForDisplay } from "@/lib/format-nzb-name";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { cn } from "@/lib/utils";

const NO_CATEGORY_VALUE = "__none__";

function statusLabel(entry: UploadNzbEntry, t: ReturnType<typeof useTranslate>): string {
  switch (entry.status) {
    case "queued":
    case "staging":
      return t("upload.statusStaging");
    case "staged":
      return t("upload.statusReady");
    case "failed":
      return t("upload.statusFailed");
    case "submitting":
      return t("upload.statusSubmitting");
    case "submitted":
      return t("upload.statusSubmitted");
    default:
      return entry.status;
  }
}

function statusClassName(entry: UploadNzbEntry): string {
  switch (entry.status) {
    case "queued":
    case "staging":
      return "border-primary/30 bg-primary/10 text-primary";
    case "staged":
      return "border-emerald-500/30 bg-emerald-500/10 text-emerald-600 dark:text-emerald-300";
    case "submitting":
      return "border-sky-500/30 bg-sky-500/10 text-sky-600 dark:text-sky-300";
    case "failed":
      return "border-destructive/30 bg-destructive/10 text-destructive";
    case "submitted":
      return "border-muted bg-muted/30 text-muted-foreground";
    default:
      return "border-border bg-background text-foreground";
  }
}

export function UploadNzbForm({
  layout = "page",
  open,
  formRef,
  onSubmitted,
}: {
  layout?: "page" | "dialog";
  open?: boolean;
  formRef?: Ref<HTMLFormElement>;
  onSubmitted?: () => void;
}) {
  const t = useTranslate();
  const {
    categories,
    dragging,
    entries,
    error,
    fileInputRef,
    fetching,
    staging,
    readyCount,
    failedCount,
    totalBytes,
    password,
    priority,
    category,
    setCategory,
    setDragging,
    setPassword,
    setPriority,
    handleDrop,
    handleFiles,
    removeFile,
    retryFile,
    openPicker,
    submit,
    onFileInputChange,
  } = useUploadNzb({
    resetOnOpen: layout === "dialog",
    open,
    onSubmitted,
  });

  return (
    <Card className={cn(layout === "dialog" ? "border-0 bg-transparent shadow-none" : "")}>
      <CardHeader className={cn(layout === "dialog" ? "px-0 pt-0" : "")}>
        <CardTitle>{t("upload.title")}</CardTitle>
        <CardDescription>
          {layout === "page" ? t("upload.accepts") : t("upload.dropzone")}
        </CardDescription>
      </CardHeader>
      <CardContent className={cn(layout === "dialog" ? "px-0 pb-0" : "")}>
        <form
          ref={formRef}
          className="space-y-5"
          onSubmit={(event) => {
            event.preventDefault();
            if (readyCount === 0 || staging || fetching) {
              return;
            }
            void submit();
          }}
        >
          <button
            type="button"
            className={cn(
              "flex w-full min-w-0 cursor-pointer flex-col items-center justify-center rounded-2xl border-2 border-dashed px-6 py-10 text-center transition-colors",
              dragging
                ? "border-primary bg-primary/10"
                : "border-border bg-background/60 hover:border-primary/40 hover:bg-accent/20",
            )}
            onClick={openPicker}
            onDragOver={(event) => {
              event.preventDefault();
              setDragging(true);
            }}
            onDragLeave={() => setDragging(false)}
            onDrop={handleDrop}
          >
            <input
              ref={fileInputRef}
              type="file"
              accept=".nzb"
              multiple
              className="hidden"
              onChange={onFileInputChange}
            />
            <UploadCloud className="mb-3 size-10 text-primary" />
            {entries.length > 0 ? (
              <>
                <div className="text-base font-medium text-foreground">
                  {entries.length === 1
                    ? t("upload.selectedSingle")
                    : t("upload.selectedMultiple", { count: entries.length })}
                </div>
                <div className="mt-1 text-sm text-muted-foreground">
                  {t("upload.totalSizeLabel")} {(totalBytes / 1024).toFixed(1)} KB
                </div>
                <div className="mt-2 text-xs text-muted-foreground">
                  {t("upload.summary", {
                    ready: readyCount,
                    staging: entries.length - readyCount - failedCount,
                    failed: failedCount,
                  })}
                </div>
                <div className="mt-2 text-xs text-muted-foreground">{t("upload.replaceHint")}</div>
              </>
            ) : (
              <>
                <div className="text-base font-medium text-foreground">{t("upload.dropzone")}</div>
                <div className="mt-1 text-sm text-muted-foreground">{t("upload.accepts")}</div>
              </>
            )}
          </button>

          {entries.length > 0 ? (
            <div className="max-h-80 space-y-2 overflow-auto rounded-2xl border border-border/70 bg-background/70 p-3">
              {entries.map((entry) => (
                <div
                  key={entry.localId}
                  className="rounded-xl border border-border/50 bg-card px-3 py-3"
                >
                  <div className="flex items-start justify-between gap-3">
                    <div className="min-w-0">
                      <div
                        className="max-w-full text-sm font-medium text-foreground [overflow-wrap:anywhere]"
                        title={entry.file.name}
                      >
                        {entry.displayName ?? formatNzbNameForDisplay(entry.file.name)}
                      </div>
                      <div className="mt-1 max-w-full text-xs text-muted-foreground [overflow-wrap:anywhere]">
                        {entry.file.name}
                      </div>
                      {entry.error ? (
                        <div className="mt-2 text-xs text-destructive [overflow-wrap:anywhere]">
                          {entry.error}
                        </div>
                      ) : null}
                    </div>
                    <div className="flex shrink-0 items-start gap-2">
                      <div
                        className={cn(
                          "rounded-full border px-2 py-1 text-[11px] font-medium uppercase tracking-wide",
                          statusClassName(entry),
                        )}
                      >
                        {statusLabel(entry, t)}
                      </div>
                      <div className="pt-1 text-xs text-muted-foreground">
                        {(entry.file.size / 1024).toFixed(1)} KB
                      </div>
                    </div>
                  </div>

                  <div className="mt-3 flex items-center justify-end gap-2">
                    {entry.status === "failed" ? (
                      <Button
                        type="button"
                        variant="outline"
                        size="sm"
                        onClick={() => retryFile(entry.localId)}
                      >
                        <RotateCcw className="mr-2 size-4" />
                        {t("upload.retryFile")}
                      </Button>
                    ) : null}
                    <Button
                      type="button"
                      variant="ghost"
                      size="icon"
                      className="size-8 shrink-0"
                      onClick={() => removeFile(entry.localId)}
                      aria-label={t("upload.removeFile")}
                      title={t("upload.removeFile")}
                    >
                      <X className="size-4" />
                    </Button>
                  </div>
                </div>
              ))}
            </div>
          ) : null}

          <div className="grid gap-4 sm:grid-cols-3">
            <div className="space-y-2">
              <Label htmlFor={`upload-password-${layout}`}>{t("upload.passwordLabel")}</Label>
              <Input
                id={`upload-password-${layout}`}
                type="text"
                value={password}
                onChange={(event) => setPassword(event.target.value)}
                placeholder={t("upload.passwordPlaceholder")}
              />
            </div>

            <div className="space-y-2">
              <Label htmlFor={`upload-category-${layout}`}>{t("table.category")}</Label>
              <Select value={category} onValueChange={setCategory}>
                <SelectTrigger id={`upload-category-${layout}`}>
                  <SelectValue placeholder={t("upload.noCategory")} />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value={NO_CATEGORY_VALUE}>{t("upload.noCategory")}</SelectItem>
                  {categories.map((entry: { id: number; name: string }) => (
                    <SelectItem key={entry.id} value={entry.name}>
                      {entry.name}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>

            <div className="space-y-2">
              <Label htmlFor={`upload-priority-${layout}`}>{t("upload.priorityLabel")}</Label>
              <Select value={priority} onValueChange={setPriority}>
                <SelectTrigger id={`upload-priority-${layout}`}>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="LOW">{t("upload.priorityLow")}</SelectItem>
                  <SelectItem value="NORMAL">{t("upload.priorityNormal")}</SelectItem>
                  <SelectItem value="HIGH">{t("upload.priorityHigh")}</SelectItem>
                </SelectContent>
              </Select>
            </div>
          </div>

          {entries.length > 1 ? (
            <div className="text-sm text-muted-foreground">{t("upload.sharedSettingsHint")}</div>
          ) : null}

          {error ? (
            <div className="rounded-xl border border-destructive/30 bg-destructive/10 px-4 py-3 text-sm text-destructive">
              {error}
            </div>
          ) : null}

          {entries.length > 0 ? (
            <div className="flex justify-start">
              <Button type="button" variant="ghost" onClick={() => handleFiles([])}>
                {t("upload.clearSelection")}
              </Button>
            </div>
          ) : null}

          <div className="flex justify-end">
            <Button type="submit" disabled={readyCount === 0 || staging || fetching}>
              {fetching ? t("action.uploading") : t("action.submit")}
            </Button>
          </div>
        </form>
      </CardContent>
    </Card>
  );
}
