import { useRef, useState } from "react";
import { CombinedError } from "urql";
import { authHeaders, fetchWithSessionRetry } from "@/graphql/client";
import { useTranslate, type Translate } from "@/lib/context/translate-context";
import { ConfirmDialog } from "../../../components/ConfirmDialog";

class BackupReauthenticationRequired extends Error {}

export async function createStoredBackup(password: string, t: Translate): Promise<void> {
  const response = await fetch(new URL("api/backup/create", document.baseURI), {
    method: "POST", headers: { "Content-Type": "application/json", ...authHeaders() },
    body: JSON.stringify({ password }),
  });
  if (!response.ok) {
    const payload = await response.json().catch(() => null) as { code?: string; error?: string } | null;
    if (response.status === 428 && payload?.code === "REAUTH_REQUIRED") throw new BackupReauthenticationRequired();
    throw new Error(payload?.error ?? t("next.backup.requestFailed", { status: response.status }));
  }
}

export function useBackupAdminAction(onSuccess?: () => void) {
  const t = useTranslate();
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const pendingAction = useRef<(() => Promise<void>) | null>(null);
  const [reauthenticating, setReauthenticating] = useState(false);
  const [accountPassword, setAccountPassword] = useState("");
  const [verificationError, setVerificationError] = useState<string | null>(null);
  const run = async (action: () => Promise<void>) => {
    setBusy(true); setError(null);
    try { await action(); onSuccess?.(); }
    catch (failure) {
      if (failure instanceof BackupReauthenticationRequired || (failure instanceof CombinedError && failure.graphQLErrors.some((item) => item.extensions?.code === "REAUTH_REQUIRED"))) {
        pendingAction.current = action;
        setVerificationError(null);
        setReauthenticating(true);
      } else {
        setError(failure instanceof Error ? failure.message : String(failure));
      }
    }
    finally { setBusy(false); }
  };
  const verifyAndContinue = async () => {
    setBusy(true); setVerificationError(null);
    try {
      const response = await fetchWithSessionRetry(new URL("api/auth/verify", document.baseURI), {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ password: accountPassword }),
      });
      if (!response.ok) {
        const payload = await response.json().catch(() => null) as { error?: string } | null;
        throw new Error(payload?.error ?? t("next.backup.requestFailed", { status: response.status }));
      }
      const action = pendingAction.current;
      pendingAction.current = null;
      setAccountPassword(""); setReauthenticating(false);
      if (action) await run(action);
    } catch (failure) {
      setVerificationError(failure instanceof Error ? failure.message : String(failure));
    } finally { setBusy(false); }
  };
  const reauthentication = (
    <ConfirmDialog open={reauthenticating} title={t("next.security.currentPassword")} destructive={false} busy={busy || !accountPassword} confirmLabel={t("next.firstRun.continue")} onConfirm={() => void verifyAndContinue()} onDismiss={() => {
      if (busy) return;
      pendingAction.current = null; setAccountPassword(""); setReauthenticating(false); setVerificationError(null);
    }} body={<>
      <p>{t("next.backup.verifyAccountPassword")}</p>
      <label className="mt-3 block">{t("next.security.currentPassword")}<input className="mt-1 w-full rounded border border-border bg-background p-2" type="password" autoComplete="current-password" value={accountPassword} onChange={(event) => setAccountPassword(event.target.value)} /></label>
      {verificationError ? <p role="alert" className="mt-2 text-wv-error-text">{verificationError}</p> : null}
    </>} />
  );
  return { run, busy, error, reauthentication };
}
