import { useState } from "react";
import { useMutation } from "urql";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  SET_ACCESS_POLICY_MUTATION,
  SET_HTTP_BIND_ADDRESS_MUTATION,
} from "@/graphql/queries";

type AccessMode = "login_required" | "login_except_local" | "no_login";

interface SetupResponse {
  ok?: boolean;
  restartRequiredForBind?: boolean;
  bindIgnoredBecauseEnvPinned?: boolean;
  error?: string;
}

// Deliberately neutral: three equal choices, none preselected. The wizard's
// job is to put the decision in front of the operator, not to make it.
const MODES: Array<{
  id: AccessMode;
  title: string;
  body: string;
}> = [
  {
    id: "login_required",
    title: "Require login",
    body: "Every browser signs in with a username and password, including on this machine.",
  },
  {
    id: "login_except_local",
    title: "Require login, except my local network",
    body: "Browsers on your local network get in without signing in. Anything else — including the internet, if you ever expose Weaver — needs the login.",
  },
  {
    id: "no_login",
    title: "No login",
    body: "No account at all. Weaver stays reachable only from this machine unless you widen its network access later.",
  },
];

function setupUrl(): string {
  return new URL("api/auth/setup", document.baseURI).href;
}

/// One mode card, shared by the fresh-install and upgrade flows so the three
/// choices read identically in both. A disabled card still renders: the
/// operator must see the option they cannot take, and why.
function ModeCard({
  candidate,
  groupName,
  checked,
  disabledReason,
  onSelect,
}: {
  candidate: (typeof MODES)[number];
  groupName: string;
  checked: boolean;
  disabledReason?: string | null;
  onSelect: () => void;
}) {
  const disabled = Boolean(disabledReason);
  const base = disabled
    ? "block cursor-not-allowed rounded-md border p-4 opacity-60 transition-colors"
    : "block cursor-pointer rounded-md border p-4 transition-colors";
  const state = disabled
    ? "border-border"
    : checked
      ? "border-primary bg-primary/5"
      : "border-border hover:border-muted-foreground/50";

  return (
    <label className={`${base} ${state}`}>
      <div className="flex items-start gap-3">
        <input
          type="radio"
          name={groupName}
          className="mt-1"
          checked={checked}
          disabled={disabled}
          onChange={onSelect}
        />
        <div>
          <div className="font-medium">{candidate.title}</div>
          <div className="text-sm text-muted-foreground">{candidate.body}</div>
          {disabledReason ? (
            <div className="mt-2 text-sm text-amber-500">{disabledReason}</div>
          ) : null}
        </div>
      </div>
    </label>
  );
}

export function SetupWizardPage() {
  const [mode, setMode] = useState<AccessMode | null>(null);
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [confirm, setConfirm] = useState("");
  const [bindWide, setBindWide] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const [restartNote, setRestartNote] = useState(false);

  const needsCredentials = mode === "login_required" || mode === "login_except_local";
  const credentialsValid =
    !needsCredentials ||
    (username.trim().length > 0 && password.length > 0 && password === confirm);
  const canSubmit = mode !== null && credentialsValid && !submitting;

  const submit = async () => {
    if (mode === null) {
      return;
    }
    setError(null);
    setSubmitting(true);
    try {
      const body: Record<string, unknown> = { mode };
      if (needsCredentials) {
        body.username = username.trim();
        body.password = password;
      }
      if (bindWide) {
        body.bindAddress = "0.0.0.0";
      }
      const response = await fetch(setupUrl(), {
        method: "POST",
        credentials: "include",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      const payload = (await response.json().catch(() => ({}))) as SetupResponse;
      if (!response.ok) {
        setError(payload.error ?? `setup failed (${response.status})`);
        setSubmitting(false);
        return;
      }
      if (payload.restartRequiredForBind) {
        setRestartNote(true);
        return;
      }
      window.location.reload();
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : String(cause));
      setSubmitting(false);
    }
  };

  if (restartNote) {
    return (
      <div className="flex min-h-screen items-center justify-center bg-background p-6">
        <div className="w-full max-w-lg space-y-4 rounded-lg border border-border bg-card p-8">
          <h1 className="text-xl font-semibold">Setup complete</h1>
          <p className="text-sm text-muted-foreground">
            Your choices are saved. Weaver is still listening only on this
            machine until it restarts — restart it now, then open it at its
            network address.
          </p>
          <Button onClick={() => window.location.reload()}>Continue on this machine</Button>
        </div>
      </div>
    );
  }

  return (
    <div className="flex min-h-screen items-center justify-center bg-background p-6">
      <div className="w-full max-w-2xl space-y-6 rounded-lg border border-border bg-card p-8">
        <div className="space-y-1">
          <h1 className="text-2xl font-semibold">Set up Weaver</h1>
          <p className="text-sm text-muted-foreground">
            Two decisions, changeable later in Settings → Security.
          </p>
        </div>

        <fieldset className="space-y-3">
          <legend className="text-sm font-medium">Who can open Weaver?</legend>
          {MODES.map((candidate) => (
            <ModeCard
              key={candidate.id}
              candidate={candidate}
              groupName="access-mode"
              checked={mode === candidate.id}
              onSelect={() => setMode(candidate.id)}
            />
          ))}
        </fieldset>

        {needsCredentials ? (
          <div className="grid gap-3 sm:grid-cols-3">
            <div className="space-y-2">
              <Label htmlFor="setup-username">Username</Label>
              <Input
                id="setup-username"
                value={username}
                onChange={(event) => setUsername(event.target.value)}
                autoComplete="username"
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="setup-password">Password</Label>
              <Input
                id="setup-password"
                type="password"
                value={password}
                onChange={(event) => setPassword(event.target.value)}
                autoComplete="new-password"
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="setup-confirm">Confirm</Label>
              <Input
                id="setup-confirm"
                type="password"
                value={confirm}
                onChange={(event) => setConfirm(event.target.value)}
                autoComplete="new-password"
              />
            </div>
            {password.length > 0 && confirm.length > 0 && password !== confirm ? (
              <p className="text-sm text-destructive sm:col-span-3">Passwords do not match.</p>
            ) : null}
          </div>
        ) : null}

        <fieldset className="space-y-3">
          <legend className="text-sm font-medium">Where should Weaver answer?</legend>
          <label className="flex cursor-pointer items-start gap-3">
            <input
              type="radio"
              name="bind"
              className="mt-1"
              checked={!bindWide}
              onChange={() => setBindWide(false)}
            />
            <span>
              <span className="font-medium">This machine only</span>
              <span className="block text-sm text-muted-foreground">
                Reachable at localhost. The safe default.
              </span>
            </span>
          </label>
          <label className="flex cursor-pointer items-start gap-3">
            <input
              type="radio"
              name="bind"
              className="mt-1"
              checked={bindWide}
              onChange={() => setBindWide(true)}
            />
            <span>
              <span className="font-medium">My network</span>
              <span className="block text-sm text-muted-foreground">
                Other machines on your network can reach Weaver. Takes effect
                after a restart.
              </span>
            </span>
          </label>
          {bindWide && mode === "no_login" ? (
            <p className="text-sm text-amber-500">
              With no login, anyone who can reach this machine's network
              address gets full control of Weaver. Only choose this on a
              network where you trust every device.
            </p>
          ) : null}
        </fieldset>

        {error ? <p className="text-sm text-destructive">{error}</p> : null}

        <Button onClick={submit} disabled={!canSubmit} className="w-full">
          {submitting ? "Setting up…" : "Finish setup"}
        </Button>
      </div>
    </div>
  );
}

export interface SecurityUpgradeState {
  loginEnabled: boolean;
  strictSecurity: boolean;
  bindEditable: boolean;
  /** The address the next restart uses: the stored setting, or what is running. */
  bindEffective: string;
}

/// Loopback judged on the spelling the server reports, including the
/// IPv4-mapped form a dual-stack listener produces.
function isLoopbackAddress(value: string): boolean {
  const normalized = value.trim().toLowerCase().replace(/^::ffff:/, "");
  return normalized === "::1" || normalized.startsWith("127.");
}

/// The same three choices, asked of an install that already has a login.
///
/// An upgrade adds settings the operator never saw, so the wizard runs once
/// more to put them in front of them — with a one-click exit that keeps the
/// pre-upgrade behaviour exactly, because an upgrade must never feel like a
/// demand.
export function SecurityUpgradeWizard({
  state,
  onDone,
}: {
  state: SecurityUpgradeState;
  onDone: () => void;
}) {
  const [mode, setMode] = useState<AccessMode | null>(null);
  const wideNow = !isLoopbackAddress(state.bindEffective);
  const [bindWide, setBindWide] = useState(wideNow);
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const [restartNote, setRestartNote] = useState(false);
  // Which mode is already stored. A policy write that succeeded must not be
  // replayed when the bind step is retried after failing — but a mode the
  // operator changed in between must still be sent.
  const [savedMode, setSavedMode] = useState<AccessMode | null>(null);
  const [, setPolicy] = useMutation(SET_ACCESS_POLICY_MUTATION);
  const [, setBindAddress] = useMutation(SET_HTTP_BIND_ADDRESS_MUTATION);

  const disabledReason = (candidate: AccessMode): string | null => {
    if (state.strictSecurity && candidate !== "login_required") {
      return "WEAVER_STRICT_SECURITY is set in this deployment's environment, which refuses trusting access modes.";
    }
    if (candidate === "no_login" && state.loginEnabled) {
      return "Your login stays. To remove it, disable login in Settings → Security first.";
    }
    return null;
  };

  // Only when the answer differs from what the next restart already does —
  // an unchanged choice must not produce a write or a restart notice.
  const bindChanges = state.bindEditable && bindWide !== wideNow;

  const applyPolicy = async (chosen: AccessMode): Promise<boolean> => {
    const result = await setPolicy({ mode: chosen });
    if (result.error) {
      setError(result.error.message.replace(/^\[GraphQL\]\s*/, ""));
      return false;
    }
    return true;
  };

  const finish = async () => {
    if (mode === null) {
      return;
    }
    setError(null);
    setSubmitting(true);
    // Policy first: it takes effect immediately, so it is the half worth
    // landing even if the bind change then fails.
    if (savedMode !== mode) {
      if (!(await applyPolicy(mode))) {
        setSubmitting(false);
        return;
      }
      setSavedMode(mode);
    }
    if (bindChanges) {
      const result = await setBindAddress({ address: bindWide ? "0.0.0.0" : "" });
      if (result.error) {
        setError(result.error.message.replace(/^\[GraphQL\]\s*/, ""));
        setSubmitting(false);
        return;
      }
      setRestartNote(true);
      return;
    }
    onDone();
  };

  const keepCurrent = async () => {
    setError(null);
    setSubmitting(true);
    // The upgrader's status quo: every browser signs in, exactly as before.
    // Storing it is what stops this wizard coming back.
    if (!(await applyPolicy("login_required"))) {
      setSubmitting(false);
      return;
    }
    onDone();
  };

  if (restartNote) {
    return (
      <div className="flex min-h-screen items-center justify-center bg-background p-6">
        <div className="w-full max-w-lg space-y-4 rounded-lg border border-border bg-card p-8">
          <h1 className="text-xl font-semibold">Settings saved</h1>
          <p className="text-sm text-muted-foreground">
            Your browser access choice applies now. The network address change
            waits for a restart — restart Weaver, then open it at its new
            address.
          </p>
          <Button onClick={onDone}>Continue on this machine</Button>
        </div>
      </div>
    );
  }

  return (
    <div className="flex min-h-screen items-center justify-center bg-background p-6">
      <div className="w-full max-w-2xl space-y-6 rounded-lg border border-border bg-card p-8">
        <div className="space-y-1">
          <h1 className="text-2xl font-semibold">Weaver added security options</h1>
          <p className="text-sm text-muted-foreground">
            This version lets you choose how browsers get in. Pick one, or keep
            what you have. Changeable later in Settings → Security.
          </p>
        </div>

        <fieldset className="space-y-3">
          <legend className="text-sm font-medium">Who can open Weaver?</legend>
          {MODES.map((candidate) => (
            <ModeCard
              key={candidate.id}
              candidate={candidate}
              groupName="upgrade-access-mode"
              checked={mode === candidate.id}
              disabledReason={disabledReason(candidate.id)}
              onSelect={() => setMode(candidate.id)}
            />
          ))}
        </fieldset>

        {state.bindEditable ? (
          <fieldset className="space-y-3">
            <legend className="text-sm font-medium">Where should Weaver answer?</legend>
            <label className="flex cursor-pointer items-start gap-3">
              <input
                type="radio"
                name="upgrade-bind"
                className="mt-1"
                checked={!bindWide}
                onChange={() => setBindWide(false)}
              />
              <span>
                <span className="font-medium">This machine only</span>
                <span className="block text-sm text-muted-foreground">
                  Reachable at localhost. The safe default.
                </span>
              </span>
            </label>
            <label className="flex cursor-pointer items-start gap-3">
              <input
                type="radio"
                name="upgrade-bind"
                className="mt-1"
                checked={bindWide}
                onChange={() => setBindWide(true)}
              />
              <span>
                <span className="font-medium">My network</span>
                <span className="block text-sm text-muted-foreground">
                  Other machines on your network can reach Weaver. Takes effect
                  after a restart.
                </span>
              </span>
            </label>
            {bindWide && mode === "no_login" ? (
              <p className="text-sm text-amber-500">
                With no login, anyone who can reach this machine's network
                address gets full control of Weaver. Only choose this on a
                network where you trust every device.
              </p>
            ) : null}
          </fieldset>
        ) : null}

        {error ? <p className="text-sm text-destructive">{error}</p> : null}

        <div className="space-y-3">
          <Button onClick={finish} disabled={mode === null || submitting} className="w-full">
            {submitting ? "Saving…" : "Save"}
          </Button>
          <Button
            variant="ghost"
            onClick={keepCurrent}
            disabled={submitting}
            className="w-full"
          >
            Keep my current setup
          </Button>
        </div>
      </div>
    </div>
  );
}
