import { useEffect, useState, type ReactNode } from "react";
import { useMutation, useQuery } from "urql";
import { ChevronRight, FolderOpen, Loader2, MoveUp } from "lucide-react";
import { ConfirmDialog } from "@/components/ConfirmDialog";
import { EmptyState } from "@/components/EmptyState";
import { PageHeader } from "@/components/PageHeader";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import {
  ADD_CATEGORY_MUTATION,
  BROWSE_DIRECTORIES_QUERY,
  CATEGORIES_QUERY,
  REMOVE_CATEGORY_MUTATION,
  UPDATE_CATEGORY_MUTATION,
} from "@/graphql/queries";
import { useTranslate } from "@/lib/context/translate-context";

type Category = {
  id: number;
  name: string;
  destDir: string | null;
  aliases: string;
};

type CategoryFormValues = {
  name: string;
  destDir: string;
  aliases: string;
};

type DirectoryBrowseResult = {
  currentPath: string;
  parentPath: string | null;
  entries: Array<{
    name: string;
    path: string;
  }>;
};

const defaultForm: CategoryFormValues = {
  name: "",
  destDir: "",
  aliases: "",
};

export function Categories({ embedded = false }: { embedded?: boolean }) {
  const t = useTranslate();
  const [{ data }] = useQuery({ query: CATEGORIES_QUERY });
  const [, addCategory] = useMutation(ADD_CATEGORY_MUTATION);
  const [, updateCategory] = useMutation(UPDATE_CATEGORY_MUTATION);
  const [, removeCategory] = useMutation(REMOVE_CATEGORY_MUTATION);

  const [categories, setCategories] = useState<Category[]>([]);
  const [editingCategory, setEditingCategory] = useState<Category | null>(null);
  const [showForm, setShowForm] = useState(false);
  const [deleteConfirmId, setDeleteConfirmId] = useState<number | null>(null);

  useEffect(() => {
    if (data?.categories) {
      setCategories(data.categories);
    }
  }, [data?.categories]);

  const openAdd = () => {
    setEditingCategory(null);
    setShowForm(true);
  };

  const openEdit = (category: Category) => {
    setEditingCategory(category);
    setShowForm(true);
  };

  const closeForm = () => {
    setEditingCategory(null);
    setShowForm(false);
  };

  const handleSave = async (values: CategoryFormValues) => {
    const input = {
      name: values.name.trim(),
      destDir: values.destDir.trim() || null,
      aliases: values.aliases.trim(),
    };

    if (editingCategory) {
      const result = await updateCategory({ id: editingCategory.id, input });
      if (result.data?.updateCategory) {
        setCategories((current) =>
          current.map((category) =>
            category.id === editingCategory.id ? result.data.updateCategory : category,
          ),
        );
      }
    } else {
      const result = await addCategory({ input });
      if (result.data?.addCategory) {
        setCategories((current) =>
          [...current, result.data.addCategory].sort((left, right) =>
            left.name.localeCompare(right.name),
          ),
        );
      }
    }

    closeForm();
  };

  const handleDelete = async (id: number) => {
    const result = await removeCategory({ id });
    if (result.data?.removeCategory) {
      setCategories(result.data.removeCategory);
    }
    setDeleteConfirmId(null);
  };

  return (
    <div className={embedded ? "space-y-5" : "space-y-6"}>
      <PageHeader
        title={t("categories.title")}
        description={embedded ? t("settings.categoriesDesc") : t("categories.emptyHint")}
        actions={<Button onClick={openAdd}>{t("categories.addCategory")}</Button>}
      />

      {showForm ? (
        <CategoryFormCard
          initialValues={
            editingCategory
              ? {
                  name: editingCategory.name,
                  destDir: editingCategory.destDir ?? "",
                  aliases: editingCategory.aliases,
                }
              : defaultForm
          }
          editing={!!editingCategory}
          onCancel={closeForm}
          onSave={handleSave}
        />
      ) : null}

      {categories.length === 0 && !showForm ? (
        <EmptyState
          title={t("categories.empty")}
          description={t("categories.emptyHint")}
          actionLabel={t("categories.addCategory")}
          onAction={openAdd}
        />
      ) : (
        <>
          <Card className="hidden md:block">
            <CardContent className="px-0 pb-0">
              <Table>
                <TableHeader>
                  <TableRow className="hover:bg-transparent">
                    <TableHead>{t("categories.name")}</TableHead>
                    <TableHead>{t("categories.destDir")}</TableHead>
                    <TableHead>{t("categories.aliases")}</TableHead>
                    <TableHead>{t("table.actions")}</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {categories.map((category) => (
                    <TableRow key={category.id}>
                      <TableCell className="font-medium">{category.name}</TableCell>
                      <TableCell>{category.destDir || "\u2014"}</TableCell>
                      <TableCell>{category.aliases || "\u2014"}</TableCell>
                      <TableCell>
                        <div className="flex flex-wrap gap-2">
                          <Button variant="ghost" size="sm" onClick={() => openEdit(category)}>
                            {t("action.edit")}
                          </Button>
                          <Button variant="ghost" size="sm" onClick={() => setDeleteConfirmId(category.id)}>
                            {t("action.delete")}
                          </Button>
                        </div>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </CardContent>
          </Card>

          <div className="space-y-3 md:hidden">
            {categories.map((category) => (
              <Card key={category.id}>
                <CardContent className="space-y-3">
                  <div>
                    <div className="font-medium text-foreground">{category.name}</div>
                    <div className="mt-2 flex flex-wrap gap-2 text-xs text-muted-foreground">
                      <span>{category.destDir || "\u2014"}</span>
                      <span>{category.aliases || "\u2014"}</span>
                    </div>
                  </div>
                  <div className="flex justify-end gap-2">
                    <Button variant="ghost" size="sm" onClick={() => openEdit(category)}>
                      {t("action.edit")}
                    </Button>
                    <Button variant="ghost" size="sm" onClick={() => setDeleteConfirmId(category.id)}>
                      {t("action.delete")}
                    </Button>
                  </div>
                </CardContent>
              </Card>
            ))}
          </div>
        </>
      )}

      <ConfirmDialog
        open={deleteConfirmId != null}
        title={t("confirm.deleteCategory")}
        message={t("confirm.deleteCategoryMessage")}
        confirmLabel={t("confirm.deleteCategoryConfirm")}
        cancelLabel={t("confirm.deleteCategoryDismiss")}
        onConfirm={() => deleteConfirmId != null && void handleDelete(deleteConfirmId)}
        onCancel={() => setDeleteConfirmId(null)}
      />
    </div>
  );
}

function CategoryFormCard({
  initialValues,
  editing,
  onSave,
  onCancel,
}: {
  initialValues: CategoryFormValues;
  editing: boolean;
  onSave: (values: CategoryFormValues) => Promise<void>;
  onCancel: () => void;
}) {
  const t = useTranslate();
  const [values, setValues] = useState(initialValues);
  const [browserOpen, setBrowserOpen] = useState(false);
  const [browsePath, setBrowsePath] = useState<string | null>(null);

  useEffect(() => {
    setValues(initialValues);
  }, [initialValues]);

  return (
    <Card>
      <CardHeader>
        <CardTitle>{editing ? t("categories.editCategory") : t("categories.addCategory")}</CardTitle>
        <CardDescription>{t("settings.categoriesDesc")}</CardDescription>
      </CardHeader>
      <CardContent className="space-y-5">
        <div className="grid gap-4 xl:grid-cols-3">
          <Field label={t("categories.name")}>
            <Input
              value={values.name}
              placeholder="Movies"
              onChange={(event) => setValues((current) => ({ ...current, name: event.target.value }))}
            />
          </Field>
          <Field label={t("categories.destDir")} description={t("categories.destDirDesc")}>
            <div className="flex gap-2">
              <Input
                value={values.destDir}
                onChange={(event) =>
                  setValues((current) => ({ ...current, destDir: event.target.value }))
                }
              />
              <Button
                type="button"
                variant="outline"
                onClick={() => {
                  setBrowsePath(values.destDir.trim() || null);
                  setBrowserOpen(true);
                }}
              >
                <FolderOpen className="size-4" />
                {t("categories.browse")}
              </Button>
            </div>
          </Field>
          <Field label={t("categories.aliases")} description={t("categories.aliasesDesc")}>
            <Input
              value={values.aliases}
              placeholder="movie*, film*"
              onChange={(event) => setValues((current) => ({ ...current, aliases: event.target.value }))}
            />
          </Field>
        </div>

        <DirectoryBrowserDialog
          open={browserOpen}
          path={browsePath}
          onPathChange={setBrowsePath}
          onClose={() => setBrowserOpen(false)}
          onChoose={(nextPath) => {
            setValues((current) => ({ ...current, destDir: nextPath }));
            setBrowserOpen(false);
          }}
        />

        <div className="flex flex-wrap gap-3">
          <Button onClick={() => void onSave(values)} disabled={!values.name.trim()}>
            {editing ? t("settings.save") : t("categories.addCategory")}
          </Button>
          <Button variant="ghost" onClick={onCancel}>
            {t("action.cancel")}
          </Button>
        </div>
      </CardContent>
    </Card>
  );
}

function DirectoryBrowserDialog({
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

function Field({
  label,
  description,
  children,
}: {
  label: string;
  description?: string;
  children: ReactNode;
}) {
  return (
    <div className="space-y-2">
      <Label>{label}</Label>
      {children}
      {description ? <p className="text-xs text-muted-foreground">{description}</p> : null}
    </div>
  );
}
