import { UploadCloud } from "lucide-react";
import { useTranslate } from "@/lib/context/translate-context";
import { useUploadNzb } from "@/features/upload/hooks/use-upload-nzb";
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

export function UploadNzbForm({
  layout = "page",
  open,
  onSubmitted,
}: {
  layout?: "page" | "dialog";
  open?: boolean;
  onSubmitted?: () => void;
}) {
  const t = useTranslate();
  const {
    categories,
    dragging,
    error,
    files,
    fileInputRef,
    fetching,
    password,
    priority,
    category,
    setCategory,
    setDragging,
    setPassword,
    setPriority,
    handleDrop,
    handleFiles,
    openPicker,
    submit,
    onFileInputChange,
  } = useUploadNzb({
    resetOnOpen: layout === "dialog",
    open,
    onSubmitted,
  });
  const totalBytes = files.reduce((sum, file) => sum + file.size, 0);

  return (
    <Card className={cn(layout === "dialog" ? "border-0 bg-transparent shadow-none" : "")}>
      <CardHeader className={cn(layout === "dialog" ? "px-0 pt-0" : "")}>
        <CardTitle>{t("upload.title")}</CardTitle>
        <CardDescription>
          {layout === "page" ? t("upload.accepts") : t("upload.dropzone")}
        </CardDescription>
      </CardHeader>
      <CardContent className={cn("space-y-5", layout === "dialog" ? "px-0 pb-0" : "")}>
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
          {files.length > 0 ? (
            <>
              <div className="text-base font-medium text-foreground">
                {files.length === 1
                  ? t("upload.selectedSingle")
                  : t("upload.selectedMultiple", { count: files.length })}
              </div>
              <div className="mt-1 text-sm text-muted-foreground">
                {t("upload.totalSizeLabel")} {(totalBytes / 1024).toFixed(1)} KB
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

        {files.length > 0 ? (
          <div className="max-h-64 space-y-2 overflow-auto rounded-2xl border border-border/70 bg-background/70 p-3">
            {files.map((file) => (
              <div
                key={`${file.name}:${file.size}:${file.lastModified}`}
                className="flex items-start justify-between gap-3 rounded-xl border border-border/50 bg-card px-3 py-2"
              >
                <div className="min-w-0">
                  <div
                    className="max-w-full text-sm font-medium text-foreground [overflow-wrap:anywhere]"
                    title={file.name}
                  >
                    {formatNzbNameForDisplay(file.name)}
                  </div>
                  <div className="mt-1 max-w-full text-xs text-muted-foreground [overflow-wrap:anywhere]">
                    {file.name}
                  </div>
                </div>
                <div className="shrink-0 pt-0.5 text-xs text-muted-foreground">
                  {(file.size / 1024).toFixed(1)} KB
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

        {files.length > 1 ? (
          <div className="text-sm text-muted-foreground">{t("upload.sharedSettingsHint")}</div>
        ) : null}

        {error ? (
          <div className="rounded-xl border border-destructive/30 bg-destructive/10 px-4 py-3 text-sm text-destructive">
            {error}
          </div>
        ) : null}

        {files.length > 0 ? (
          <div className="flex justify-start">
            <Button variant="ghost" onClick={() => handleFiles([])}>
              {t("upload.clearSelection")}
            </Button>
          </div>
        ) : null}

        <div className="flex justify-end">
          <Button disabled={files.length === 0 || fetching} onClick={() => void submit()}>
            {fetching ? t("action.uploading") : t("action.submit")}
          </Button>
        </div>
      </CardContent>
    </Card>
  );
}
