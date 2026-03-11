import { useEffect, useState, type ReactNode } from "react";
import { useMutation, useQuery } from "urql";
import { ConfirmDialog } from "@/components/ConfirmDialog";
import { EmptyState } from "@/components/EmptyState";
import { PageHeader } from "@/components/PageHeader";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
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
            <Input
              value={values.destDir}
              onChange={(event) => setValues((current) => ({ ...current, destDir: event.target.value }))}
            />
          </Field>
          <Field label={t("categories.aliases")} description={t("categories.aliasesDesc")}>
            <Input
              value={values.aliases}
              placeholder="movie*, film*"
              onChange={(event) => setValues((current) => ({ ...current, aliases: event.target.value }))}
            />
          </Field>
        </div>

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
