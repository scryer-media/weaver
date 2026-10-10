import { useState } from "react";
import { useMutation, useQuery } from "urql";
import { useTranslate } from "@/lib/context/translate-context";
import { PrimaryButton, SecondaryButton } from "../../../components/controls";

interface Settings { hasPasswords: boolean; passwordFile: string | null }

export function ArchivePasswordSettings() {
  const t = useTranslate();
  const [{ data, fetching, error }, refresh] = useQuery<{ archivePasswordSettings: Settings }>({
    query: `query ArchivePasswordSettings { archivePasswordSettings { hasPasswords passwordFile } }`,
  });
  const [mutation, save] = useMutation(`mutation UpdateArchivePasswordSettings($passwords: [String!], $passwordFile: String) {
    updateArchivePasswordSettings(passwords: $passwords, passwordFile: $passwordFile) { hasPasswords passwordFile }
  }`);
  const [passwords, setPasswords] = useState<string | null>(null);
  const [file, setFile] = useState<string | null>(null);
  const [status, setStatus] = useState<string | null>(null);
  const [failed, setFailed] = useState(false);
  const settings = data?.archivePasswordSettings;
  const submit = async () => {
    setStatus(null);
    const result = await save({
      ...(passwords === null ? {} : { passwords: passwords.split(/\r?\n/) }),
      passwordFile: (file ?? settings?.passwordFile ?? "") || null,
    });
    setFailed(!!result.error);
    setStatus(t(result.error ? "next.settings.saveFailed" : "next.settings.saved"));
    if (!result.error) {
      setPasswords(null);
      setFile(null);
      refresh({ requestPolicy: "network-only" });
    }
  };
  return <div className="space-y-3 px-4 py-4 sm:px-6">
    <label className="block text-[13px]">{t("next.archivePasswords.list")}
      <textarea autoComplete="off" spellCheck={false} value={passwords ?? ""}
        placeholder={settings?.hasPasswords ? t("next.archivePasswords.saved") : ""}
        onChange={(event) => setPasswords(event.target.value)} rows={3}
        className="mt-2 w-full rounded border border-wv-control bg-wv-input p-2 font-wv-mono" />
    </label>
    <label className="block text-[13px]">{t("next.archivePasswords.file")}
      <input value={file ?? settings?.passwordFile ?? ""} autoComplete="off"
        onChange={(event) => setFile(event.target.value)} className="mt-2 w-full rounded border border-wv-control bg-wv-input p-2" />
    </label>
    <div className="flex gap-2">
      <SecondaryButton disabled={mutation.fetching || fetching || !settings} onClick={() => setPasswords("")}>{t("next.archivePasswords.clear")}</SecondaryButton>
      <PrimaryButton disabled={mutation.fetching || fetching || !settings || (passwords === null && file === null)} onClick={() => void submit()}>{t("action.save")}</PrimaryButton>
    </div>
    {error || status ? <p role="status" className={error || failed ? "text-wv-error-text" : "text-wv-secondary"}>{error ? t("next.archivePasswords.loadFailed") : status}</p> : null}
  </div>;
}
