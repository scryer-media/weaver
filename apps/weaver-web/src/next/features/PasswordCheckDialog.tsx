import { useRef, useState, type ReactNode } from "react";
import type { CombinedError } from "urql";
import { authHeaders } from "@/graphql/client";
import { useTranslate } from "@/lib/context/translate-context";
import { Dialog } from "../components/Dialog";
import { PrimaryButton, SecondaryButton, TextField } from "../components/controls";

/**
 * What an attempt reports: finished (it handled its own success or failure),
 * or refused until this browser session proves its password again.
 */
export type CheckedOutcome = "done" | "password";

/** Whether a GraphQL failure is the server asking for a recent password check. */
export function needsPasswordCheck(error: CombinedError | undefined): boolean {
  return Boolean(error?.graphQLErrors.some((entry) => entry.extensions?.code === "REAUTH_REQUIRED"));
}

/**
 * Security changes on an install that requires sign-in need the password
 * checked within the last 15 minutes. Rather than asking for it up front, a
 * change runs through `run`: when the server refuses it for that reason, this
 * asks for the password, checks it, and runs the same change again.
 */
export function usePasswordCheck(): {
  run: (attempt: () => Promise<CheckedOutcome>) => Promise<void>;
  dialog: ReactNode;
} {
  const t = useTranslate();
  const [open, setOpen] = useState(false);
  const pending = useRef<(() => Promise<CheckedOutcome>) | null>(null);
  const [password, setPassword] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [checking, setChecking] = useState(false);

  const run = async (attempt: () => Promise<CheckedOutcome>) => {
    if ((await attempt()) === "password") {
      pending.current = attempt;
      setError(null);
      setOpen(true);
    }
  };

  const dismiss = () => {
    pending.current = null;
    setPassword("");
    setOpen(false);
  };

  const check = async () => {
    if (!password || checking) {
      return;
    }
    setChecking(true);
    setError(null);
    try {
      const response = await fetch(new URL("api/auth/verify", document.baseURI), {
        method: "POST",
        headers: { "Content-Type": "application/json", ...authHeaders() },
        credentials: "include",
        body: JSON.stringify({ password }),
      });
      if (!response.ok) {
        setError(
          response.status === 429
            ? t("next.security.checkBusy")
            : t("next.security.checkFailed"),
        );
        return;
      }
    } catch {
      setError(t("next.security.checkUnreachable"));
      return;
    } finally {
      setChecking(false);
    }
    const attempt = pending.current;
    dismiss();
    if (attempt) {
      // Checked once for this change: a second refusal is not asked about
      // again in a loop, the attempt reports it like any other failure.
      await attempt();
    }
  };

  const dialog = (
    <Dialog
      open={open}
      title={t("next.security.checkTitle")}
      note={t("next.security.checkNote")}
      width={520}
      onDismiss={dismiss}
      footer={
        <>
          <SecondaryButton onClick={dismiss}>{t("next.security.checkCancel")}</SecondaryButton>
          <PrimaryButton onClick={() => void check()} disabled={!password || checking}>
            {checking ? t("next.security.checking") : t("next.security.checkContinue")}
          </PrimaryButton>
        </>
      }
    >
      <form
        className="flex flex-col gap-3 px-4 py-5 sm:px-6"
        onSubmit={(event) => {
          event.preventDefault();
          void check();
        }}
      >
        <TextField
          label={t("next.security.password")}
          type="password"
          value={password}
          onChange={setPassword}
          secret
          autoFocus
          className="w-full"
        />
        {error ? (
          <div role="alert" className="text-[12.5px] leading-[1.45] text-wv-error-text">
            {error}
          </div>
        ) : null}
      </form>
    </Dialog>
  );

  return { run, dialog };
}
