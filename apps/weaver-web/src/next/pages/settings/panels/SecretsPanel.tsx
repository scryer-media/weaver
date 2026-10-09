import { useMemo, useState } from "react";
import { useQuery } from "urql";
import { SECRETS_QUERY } from "@/graphql/queries";
import { useTranslate } from "@/lib/context/translate-context";
import { PrimaryButton } from "../../../components/controls";
import { Cell } from "../../../components/rows";
import { formatDate } from "../../../data/format";
import { sortedSecrets, usedByText, type Secret } from "../../../data/secrets";
import { PanelControls, SettingsBlocks, usePanelStatus, type SettingsBlock } from "../framework";
import { SecretEditor, type SecretEditorTarget } from "./SecretEditor";

/**
 * Secrets: values kept encrypted under a name, for script inputs to link. A
 * value goes in and is never shown again; what the table shows is who links
 * each one.
 */
export function SecretsPanel() {
  const t = useTranslate();
  const [{ data, fetching, error }, reexecute] = useQuery<{ secrets: Secret[] }>({ query: SECRETS_QUERY });
  const [target, setTarget] = useState<SecretEditorTarget | null>(null);
  const [status, setStatus] = useState<string | null>(null);
  // A list that could not be read says why in the status bar, as the other script screens do.
  const failure = error ? (error.graphQLErrors[0]?.message ?? error.message) : null;
  usePanelStatus(failure ?? status, failure !== null);

  const secrets = useMemo(() => sortedSecrets(data?.secrets ?? []), [data?.secrets]);
  const refresh = () => reexecute({ requestPolicy: "network-only" });

  const blocks: SettingsBlock[] = [
    {
      kind: "table",
      id: "secrets",
      title: t("next.settings.panel.secrets"),
      note: t("next.secrets.note"),
      columns: "minmax(0, 1fr) minmax(0, 1.4fr) minmax(0, 0.8fr)",
      headers: [t("next.secrets.name"), t("next.secrets.usedBy"), t("next.secrets.updated")],
      // The top bar's Add is the list's only one.
      empty: t("next.secrets.empty"),
      onRowClick: (id) => {
        const secret = secrets.find((entry) => entry.id === id);
        if (secret) {
          setTarget({ mode: "edit", secret });
        }
      },
      rows: secrets.map((secret) => ({
        id: secret.id,
        searchText: `${secret.name} ${usedByText(secret)}`,
        cells: [
          <Cell key="name" title={secret.name}>
            {secret.name}
          </Cell>,
          <Cell
            key="usedBy"
            className={secret.usedBy.length > 0 ? "text-wv-secondary" : "text-wv-muted"}
            title={usedByText(secret) || undefined}
          >
            {usedByText(secret) || t("next.secrets.unused")}
          </Cell>,
          <Cell key="updated" mono className="text-wv-muted">
            {formatDate(Date.parse(secret.updatedAt))}
          </Cell>,
        ],
      })),
    },
  ];

  return (
    <>
      <PanelControls>
        <PrimaryButton icon="add" onClick={() => setTarget({ mode: "new" })}>
          {t("next.secrets.add")}
        </PrimaryButton>
      </PanelControls>

      <SettingsBlocks blocks={blocks} loading={fetching && !data} />

      {target ? (
        <SecretEditor
          key={target.mode === "edit" ? target.secret.id : "new"}
          target={target}
          onSaved={(secret) => {
            setTarget(null);
            setStatus(t(target.mode === "edit" ? "next.secrets.saved" : "next.secrets.created", { name: secret.name }));
            refresh();
          }}
          onDeleted={(secret) => {
            setTarget(null);
            setStatus(t("next.secrets.deleted", { name: secret.name }));
            refresh();
          }}
          onDismiss={() => setTarget(null)}
        />
      ) : null}
    </>
  );
}
