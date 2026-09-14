import { useState } from "react";
import { useMutation } from "urql";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { SETUP_CODE_DISPLAY_LENGTH } from "@/lib/setup-code";
import {
  SET_ACCESS_POLICY_MUTATION,
  SET_HTTP_BIND_ADDRESS_MUTATION,
} from "@/graphql/queries";
import {
  BIND_CHOICES,
  CONTAINER_BIND_NOTE,
  ENV_PINNED_BIND_NOTE,
  MODES,
  NO_LOGIN_NETWORK_WARNING,
  RESTART_UNREACHABLE_NOTE,
  isContainerDeployment,
  useRestartAction,
  useSetupForm,
  type AccessMode,
  type SetupEnvironment,
} from "@/lib/setup-flow";

/// The two buttons on a restart-required screen.
///
/// The restart button only exists where restarting is genuinely safe — a
/// container that exits without a restart policy leaves the operator with
/// nothing — so an unsupported deployment gets exactly the screen it had
/// before: the manual instruction and a way back into the app.
function RestartNoteActions({
  restartSupported,
  restartUnsupportedReason,
  onContinue,
}: {
  restartSupported: boolean;
  /** The server's refusal, shown so the manual instruction says which kind of
   *  restart this deployment actually needs. */
  restartUnsupportedReason?: string | null;
  onContinue: () => void;
}) {
  const { phase, error, restart } = useRestartAction();

  if (!restartSupported) {
    return (
      <div className="space-y-3">
        {restartUnsupportedReason ? (
          <p className="text-sm text-muted-foreground">{restartUnsupportedReason}</p>
        ) : null}
        <Button onClick={onContinue}>Continue on this machine</Button>
      </div>
    );
  }

  if (phase === "restarting") {
    return (
      <p className="text-sm text-muted-foreground">
        Restarting Weaver. This page reloads by itself as soon as Weaver answers
        again.
      </p>
    );
  }

  return (
    <div className="space-y-3">
      {phase === "unreachable" ? (
        <p className="text-sm text-muted-foreground">{RESTART_UNREACHABLE_NOTE}</p>
      ) : null}
      {error ? <p className="text-sm text-destructive">{error}</p> : null}
      {phase === "idle" ? (
        <Button variant="destructive" onClick={restart} className="w-full">
          Restart Weaver
        </Button>
      ) : null}
      <Button onClick={onContinue} className="w-full">
        Continue on this machine
      </Button>
    </div>
  );
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

/// The two bind choices, shared by both flows.
function BindChoices({
  groupName,
  bindWide,
  onChange,
  warnNoLogin,
}: {
  groupName: string;
  bindWide: boolean;
  onChange: (wide: boolean) => void;
  warnNoLogin: boolean;
}) {
  return (
    <fieldset className="space-y-3">
      <legend className="text-sm font-medium">How can I access Weaver?</legend>
      {(["local", "network"] as const).map((choice) => (
        <label key={choice} className="flex cursor-pointer items-start gap-3">
          <input
            type="radio"
            name={groupName}
            className="mt-1"
            checked={bindWide === (choice === "network")}
            onChange={() => onChange(choice === "network")}
          />
          <span>
            <span className="font-medium">{BIND_CHOICES[choice].title}</span>
            <span className="block text-sm text-muted-foreground">
              {BIND_CHOICES[choice].body}
            </span>
          </span>
        </label>
      ))}
      {bindWide && warnNoLogin ? (
        <p className="text-sm text-amber-500">{NO_LOGIN_NETWORK_WARNING}</p>
      ) : null}
    </fieldset>
  );
}

export function SetupWizardPage({ environment }: { environment?: SetupEnvironment | null }) {
  const form = useSetupForm(environment);

  if (form.restart) {
    return (
      <div className="flex min-h-screen items-center justify-center bg-background p-6">
        <div className="w-full max-w-lg space-y-4 rounded-lg border border-border bg-card p-8">
          <h1 className="text-xl font-semibold">Setup complete</h1>
          <p className="text-sm text-muted-foreground">
            Your choices are saved. Weaver is still listening only on this
            machine until it restarts — restart it now, then open it at its
            network address.
          </p>
          <RestartNoteActions
            restartSupported={form.restart.supported}
            restartUnsupportedReason={form.restart.unsupportedReason}
            onContinue={() => window.location.reload()}
          />
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
            {form.authenticatedAccess
              ? "Create the administrator account for this protected Weaver."
              : "Two decisions, changeable later in Settings → Security."}
          </p>
        </div>

        {!form.authenticatedAccess ? (
          <fieldset className="space-y-3">
            <legend className="text-sm font-medium">Who can open Weaver?</legend>
            {MODES.map((candidate) => (
              <ModeCard
                key={candidate.id}
                candidate={candidate}
                groupName="access-mode"
                checked={form.mode === candidate.id}
                onSelect={() => form.setMode(candidate.id)}
              />
            ))}
          </fieldset>
        ) : null}

        {form.needsCredentials ? (
          <div className="grid gap-3 sm:grid-cols-3">
            <div className="space-y-2">
              <Label htmlFor="setup-username">Username</Label>
              <Input
                id="setup-username"
                value={form.username}
                onChange={(event) => form.setUsername(event.target.value)}
                autoComplete="username"
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="setup-password">Password</Label>
              <Input
                id="setup-password"
                type="password"
                value={form.password}
                onChange={(event) => form.setPassword(event.target.value)}
                autoComplete="new-password"
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="setup-confirm">Confirm</Label>
              <Input
                id="setup-confirm"
                type="password"
                value={form.confirm}
                onChange={(event) => form.setConfirm(event.target.value)}
                autoComplete="new-password"
              />
            </div>
            {form.passwordsDiffer ? (
              <p className="text-sm text-destructive sm:col-span-3">Passwords do not match.</p>
            ) : null}
          </div>
        ) : null}

        {form.codeRequired ? (
          <div className="space-y-2">
            <Label htmlFor="setup-code">One-time setup code</Label>
            <Input
              id="setup-code"
              value={form.setupCode}
              onChange={(event) => form.setSetupCode(event.target.value)}
              autoComplete="off"
              autoCapitalize="characters"
              spellCheck={false}
              maxLength={SETUP_CODE_DISPLAY_LENGTH}
              placeholder="···-···"
              className="font-mono tracking-[0.3em]"
            />
            <p className="text-sm text-muted-foreground">
              Enter the code shown when Weaver started. It is accepted only while setup is pending.
            </p>
          </div>
        ) : null}

        {form.bindQuestionApplies ? (
          <BindChoices
            groupName="bind"
            bindWide={form.bindWide}
            onChange={form.setBindWide}
            warnNoLogin={form.mode === "no_login"}
          />
        ) : (
          <p className="text-sm text-muted-foreground">
            {form.containerized ? CONTAINER_BIND_NOTE : ENV_PINNED_BIND_NOTE}
          </p>
        )}

        {form.error ? <p className="text-sm text-destructive">{form.error}</p> : null}

        <Button onClick={() => void form.submit()} disabled={!form.canSubmit} className="w-full">
          {form.submitting ? "Setting up…" : "Finish setup"}
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
  /** Whether this deployment can restart Weaver from the browser. */
  restartSupported: boolean;
  /** The server's refusal when it cannot, for the manual instruction. */
  restartUnsupportedReason: string | null;
  /** `native`, `docker`, or `container` — who decides network exposure. */
  deployment: string;
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

  // A container's exposure is its published ports, so the question is never
  // asked there — not even when nothing pins the address.
  const containerized = isContainerDeployment(state.deployment);
  // Only when the answer differs from what the next restart already does —
  // an unchanged choice must not produce a write or a restart notice.
  const bindChanges = state.bindEditable && !containerized && bindWide !== wideNow;

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
          <RestartNoteActions
            restartSupported={state.restartSupported}
            restartUnsupportedReason={state.restartUnsupportedReason}
            onContinue={onDone}
          />
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

        {containerized ? (
          <p className="text-sm text-muted-foreground">{CONTAINER_BIND_NOTE}</p>
        ) : state.bindEditable ? (
          <BindChoices
            groupName="upgrade-bind"
            bindWide={bindWide}
            onChange={setBindWide}
            warnNoLogin={mode === "no_login"}
          />
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
