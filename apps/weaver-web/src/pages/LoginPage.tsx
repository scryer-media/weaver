import { useState } from "react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { BrandLockup } from "@/lib/brand";
import { useTranslate } from "@/lib/context/translate-context";
import { LOGIN_RESET_COMMANDS, useSignInForm } from "@/lib/login-required";

/**
 * The classic interface's sign-in page, shown in place of everything else
 * once this browser needs a login it does not have.
 */
export function LoginPage() {
  const t = useTranslate();
  const form = useSignInForm(t);
  const [showReset, setShowReset] = useState(false);

  return (
    <div className="flex min-h-screen items-center justify-center bg-background p-6">
      <div className="w-full max-w-sm space-y-6 rounded-lg border border-border bg-card p-8">
        <BrandLockup className="mx-auto h-7 w-auto text-foreground" />

        <form className="space-y-4" onSubmit={form.submit}>
          <div className="space-y-2">
            <Label htmlFor="username">{t("next.login.username")}</Label>
            <Input
              id="username"
              value={form.username}
              onChange={(event) => form.setUsername(event.target.value)}
              autoComplete="username"
              autoFocus
            />
          </div>
          <div className="space-y-2">
            <Label htmlFor="password">{t("next.login.password")}</Label>
            <Input
              id="password"
              type="password"
              value={form.password}
              onChange={(event) => form.setPassword(event.target.value)}
              autoComplete="current-password"
            />
          </div>
          {form.error ? (
            <p id="error" role="alert" className="text-sm text-destructive">
              {form.error}
            </p>
          ) : null}
          <Button type="submit" className="w-full" disabled={!form.canSubmit}>
            {form.busy ? t("next.login.signingIn") : t("next.login.submit")}
          </Button>
        </form>

        <div className="space-y-3 text-center">
          <Button
            type="button"
            variant="link"
            size="sm"
            aria-expanded={showReset}
            onClick={() => setShowReset((current) => !current)}
          >
            {t("next.login.forgot")}
          </Button>
          {showReset ? (
            <div className="space-y-3 rounded-md border border-border bg-background p-3 text-left text-xs leading-relaxed text-muted-foreground">
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

function ResetCommand({ label, command }: { label: string; command: string }) {
  return (
    <div className="space-y-1">
      <div className="font-medium text-foreground">{label}</div>
      <code className="block break-all rounded bg-muted px-2 py-1.5 font-mono text-foreground">{command}</code>
    </div>
  );
}
