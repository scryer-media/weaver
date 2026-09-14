import { useState } from "react";
import { BrandLockup } from "@/lib/brand";
import { useTranslate } from "@/lib/context/translate-context";
import { LOGIN_RESET_COMMANDS, useSignInForm } from "@/lib/login-required";
import { PrimaryButton, TextField } from "../components/controls";

/**
 * The Next interface's sign-in page: the lockup, two fields and a button.
 *
 * It is loaded on its own rather than with the rest of the Next tree, so a
 * signed-out browser downloads nothing it cannot use yet.
 */
export default function LoginPage() {
  const t = useTranslate();
  const form = useSignInForm(t);
  const [showReset, setShowReset] = useState(false);

  return (
    <div className="flex min-h-dvh items-center justify-center bg-wv-app p-6 text-wv-fg">
      <div className="flex w-full max-w-[340px] flex-col items-stretch gap-8">
        <BrandLockup className="h-[26px] w-auto self-center text-wv-strong" />

        <form
          onSubmit={form.submit}
          className="flex flex-col gap-4 border border-wv-control bg-wv-chrome p-6 shadow-wv-menu"
        >
          <Field label={t("next.login.username")} htmlFor="username">
            <TextField
              id="username"
              label={t("next.login.username")}
              value={form.username}
              onChange={form.setUsername}
              autoComplete="username"
              autoFocus
              className="w-full"
            />
          </Field>
          <Field label={t("next.login.password")} htmlFor="password">
            <TextField
              id="password"
              type="password"
              label={t("next.login.password")}
              value={form.password}
              onChange={form.setPassword}
              autoComplete="current-password"
              className="w-full"
            />
          </Field>
          {form.error ? (
            <div id="error" role="alert" className="text-[12.5px] leading-[1.45] text-wv-error-text">
              {form.error}
            </div>
          ) : null}
          <PrimaryButton type="submit" disabled={!form.canSubmit} className="mt-1 w-full justify-center">
            {form.busy ? t("next.login.signingIn") : t("next.login.submit")}
          </PrimaryButton>
        </form>

        <div className="flex flex-col items-center gap-3">
          <button
            type="button"
            aria-expanded={showReset}
            onClick={() => setShowReset((current) => !current)}
            className="cursor-pointer text-[12px] text-wv-muted hover:text-wv-fg"
          >
            {t("next.login.forgot")}
          </button>
          {showReset ? (
            <div className="flex w-full flex-col gap-3 border border-wv-hairline bg-wv-chrome p-4 text-[12px] leading-[1.5] text-wv-muted">
              <p>{t("next.login.resetHelp")}</p>
              <ResetCommand label={t("next.login.docker")} command={LOGIN_RESET_COMMANDS.docker} />
              <ResetCommand label={t("next.login.bareMetal")} command={LOGIN_RESET_COMMANDS.bareMetal} />
              <p>{t("next.login.trustedNetworks", { variable: LOGIN_RESET_COMMANDS.trustedNetworks })}</p>
            </div>
          ) : null}
        </div>
      </div>
    </div>
  );
}

function Field({ label, htmlFor, children }: { label: string; htmlFor: string; children: React.ReactNode }) {
  return (
    <div className="flex flex-col gap-[7px]">
      <label htmlFor={htmlFor} className="text-[12px] font-medium text-wv-secondary">
        {label}
      </label>
      {children}
    </div>
  );
}

function ResetCommand({ label, command }: { label: string; command: string }) {
  return (
    <div className="flex flex-col gap-1">
      <div className="font-medium text-wv-fg">{label}</div>
      <code className="block break-all bg-wv-input px-2 py-1.5 font-wv-mono text-[11px] text-wv-fg">{command}</code>
    </div>
  );
}
