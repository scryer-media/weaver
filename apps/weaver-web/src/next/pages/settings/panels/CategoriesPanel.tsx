import { useMemo, useState } from "react";
import { useMutation, useQuery } from "urql";
import {
  ADD_CATEGORY_MUTATION,
  CATEGORIES_QUERY,
  REMOVE_CATEGORY_MUTATION,
  SETTINGS_QUERY,
  UPDATE_CATEGORY_MUTATION,
} from "@/graphql/queries";
import { useTranslate } from "@/lib/context/translate-context";
import { Square } from "../../../components/chrome";
import { ConfirmDialog } from "../../../components/ConfirmDialog";
import { RecordEditor } from "../../../components/RecordEditor";
import { PrimaryButton } from "../../../components/controls";
import { Cell } from "../../../components/rows";
import { categoryColor } from "../../../data/palette";
import { PanelControls, SettingsBlocks, type SettingsBlock } from "../framework";

/**
 * Categories: where a release lands, and the names an indexer may call it.
 *
 * Destinations are shown as the daemon stores them — blank means "the
 * completed folder, under the category's own name" — so the table's note
 * carries that rule rather than every row repeating it.
 */

interface Category {
  id: number;
  name: string;
  destDir: string | null;
  aliases: string;
}

interface CategoryForm {
  name: string;
  destDir: string;
  aliases: string;
}

const NEW_CATEGORY: CategoryForm = { name: "", destDir: "", aliases: "" };

export function CategoriesPanel() {
  const t = useTranslate();
  const [{ data, fetching }, reexecute] = useQuery<{ categories: Category[] }>({ query: CATEGORIES_QUERY });
  const [{ data: settingsData }] = useQuery<{ settings: { completeDir: string; dataDir: string } }>({
    query: SETTINGS_QUERY,
  });
  const [, addCategory] = useMutation(ADD_CATEGORY_MUTATION);
  const [, updateCategory] = useMutation(UPDATE_CATEGORY_MUTATION);
  const [, removeCategory] = useMutation(REMOVE_CATEGORY_MUTATION);

  const [editingId, setEditingId] = useState<number | "new" | null>(null);
  const [form, setForm] = useState<CategoryForm>(NEW_CATEGORY);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [confirmRemove, setConfirmRemove] = useState<Category | null>(null);

  const categories = useMemo(
    () => [...(data?.categories ?? [])].sort((left, right) => left.name.localeCompare(right.name)),
    [data?.categories],
  );

  const completeDir = settingsData?.settings?.completeDir
    || (settingsData?.settings?.dataDir ? `${settingsData.settings.dataDir}/complete` : "");
  const editing = typeof editingId === "number" ? categories.find((c) => c.id === editingId) : null;

  const open = (category: Category | null) => {
    setError(null);
    setForm(
      category
        ? { name: category.name, destDir: category.destDir ?? "", aliases: category.aliases ?? "" }
        : NEW_CATEGORY,
    );
    setEditingId(category ? category.id : "new");
  };

  const save = async () => {
    const name = form.name.trim();
    if (!name) {
      setError(t("next.categories.nameRequired"));
      return;
    }
    setBusy(true);
    const input = { name, destDir: form.destDir.trim() || null, aliases: form.aliases.trim() };
    const result =
      editingId === "new"
        ? await addCategory({ input })
        : await updateCategory({ id: editingId, input });
    setBusy(false);
    if (result.error) {
      setError(result.error.graphQLErrors[0]?.message ?? result.error.message);
      return;
    }
    setEditingId(null);
    void reexecute({ requestPolicy: "network-only" });
  };

  const remove = async () => {
    if (!confirmRemove) {
      return;
    }
    setBusy(true);
    await removeCategory({ id: confirmRemove.id });
    setBusy(false);
    setConfirmRemove(null);
    setEditingId(null);
    void reexecute({ requestPolicy: "network-only" });
  };

  const blocks: SettingsBlock[] = [
    {
      kind: "table",
      id: "categories",
      title: t("next.settings.panel.categories"),
      note: t("next.categories.note"),
      columns: "minmax(0, 1fr) minmax(0, 1.4fr) minmax(0, 1fr)",
      headers: [t("next.categories.name"), t("next.categories.destination"), t("next.categories.aliases")],
      empty: t("next.categories.empty"),
      emptyAction: { label: t("next.categories.add"), onClick: () => open(null) },
      onRowClick: (id) => {
        const category = categories.find((entry) => String(entry.id) === id);
        if (category) {
          open(category);
        }
      },
      rows: categories.map((category) => ({
        id: String(category.id),
        searchText: `${category.name} ${category.destDir ?? ""} ${category.aliases ?? ""}`,
        cells: [
          <span key="name" className="flex min-w-0 items-center gap-[10px]">
            <Square color={categoryColor(category.name)} />
            <span className="min-w-0 truncate">{category.name}</span>
          </span>,
          <Cell key="dest" mono className="text-wv-secondary" title={category.destDir ?? ""}>
            {category.destDir || `${completeDir}/${category.name}`}
          </Cell>,
          <Cell key="aliases" mono className="text-wv-muted">
            {category.aliases || "—"}
          </Cell>,
        ],
      })),
    },
  ];

  return (
    <>
      <PanelControls>
        <PrimaryButton icon="add" onClick={() => open(null)}>{t("next.categories.add")}</PrimaryButton>
      </PanelControls>

      <SettingsBlocks blocks={blocks} loading={fetching && !data} />

      <RecordEditor
        open={editingId !== null}
        title={editingId === "new" ? t("next.categories.add") : (editing?.name ?? t("next.categories.category"))}
        note={editingId === "new" ? t("next.categories.newNote") : `#${editing?.id ?? ""}`}
        error={error}
        busy={busy}
        onSave={() => void save()}
        onDismiss={() => setEditingId(null)}
        onDelete={editing ? () => setConfirmRemove(editing) : undefined}
        deleteLabel={t("next.categories.remove")}
        sections={[
          {
            id: "category",
            title: t("next.categories.category"),
            fields: [
              {
                id: "name",
                label: t("next.categories.name"),
                help: t("next.categories.nameHelp"),
                control: {
                  kind: "text",
                  mono: false,
                  value: form.name,
                  onChange: (next) => setForm((current) => ({ ...current, name: next })),
                },
              },
              {
                id: "destDir",
                label: t("next.categories.destination"),
                help: t("next.categories.destinationHelp", {
                  path: `${completeDir || t("next.categories.completedFolder")}/${form.name || t("next.categories.namePlaceholder")}`,
                }),
                control: {
                  kind: "path",
                  value: form.destDir,
                  placeholder: completeDir ? `${completeDir}/${form.name || "name"}` : "",
                  onChange: (next) => setForm((current) => ({ ...current, destDir: next })),
                },
              },
              {
                id: "aliases",
                label: t("next.categories.aliases"),
                help: t("next.categories.aliasesHelp"),
                control: {
                  kind: "text",
                  value: form.aliases,
                  placeholder: "tv, series",
                  onChange: (next) => setForm((current) => ({ ...current, aliases: next })),
                },
              },
            ],
          },
        ]}
      />

      <ConfirmDialog
        open={confirmRemove !== null}
        title={t("next.categories.remove")}
        note={confirmRemove?.name}
        busy={busy}
        confirmLabel={t("next.categories.remove")}
        body={t("next.categories.removeBody", { name: confirmRemove?.name ?? "" })}
        onConfirm={() => void remove()}
        onDismiss={() => setConfirmRemove(null)}
      />
    </>
  );
}
