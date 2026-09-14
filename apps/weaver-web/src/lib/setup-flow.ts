import { useEffect, useState } from "react";
import { refreshSessionCookie } from "@/graphql/client";
import { formatSetupCode } from "@/lib/setup-code";

/**
 * First-run setup, shared by both interfaces' setup pages: the same choices,
 * the same request and the same restart handling, drawn in each one's own
 * controls. It runs before either interface's translations load, so its copy
 * lives here in English.
 */

export type AccessMode = "login_required" | "login_except_local" | "no_login";

interface SetupResponse {
  ok?: boolean;
  restartRequiredForBind?: boolean;
  bindIgnoredBecauseEnvPinned?: boolean;
  restartSupported?: boolean;
  restartUnsupportedReason?: string | null;
  error?: string;
}

/// How this deployment answers the bind question, from `/api/auth/status`.
/// Absent on a server that predates it, which reads as "ask normally".
export interface SetupEnvironment {
  bindEditable: boolean;
  deployment: string;
  /** New installs use durable browser sessions and require a one-time code. */
  authenticatedAccess?: boolean;
  codeRequired?: boolean;
}

/// True for a deployment whose network exposure its runtime decides, not
/// Weaver: the bind address is namespace-local and published ports are what
/// the operator actually controls.
export function isContainerDeployment(deployment: string | undefined): boolean {
  return deployment === "docker" || deployment === "container";
}

export const CONTAINER_BIND_NOTE =
  "Network access is decided by the ports your container publishes.";
export const ENV_PINNED_BIND_NOTE =
  "This deployment pins Weaver's network address with WEAVER_HTTP_BIND_ADDRESS.";

// Deliberately neutral: three equal choices, none preselected. The wizard's
// job is to put the decision in front of the operator, not to make it.
export const MODES: Array<{
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

export const BIND_CHOICES = {
  local: { title: "This machine only", body: "Reachable at localhost." },
  network: {
    title: "My network",
    body: "Other machines on your network can reach Weaver. Takes effect after a restart.",
  },
} as const;

export const NO_LOGIN_NETWORK_WARNING =
  "No login limits browsers to this machine even when Weaver answers network-wide: other devices' browsers are turned away, only API clients with keys get in. If other machines should browse Weaver, choose \"Require login, except my local network\" instead.";

function setupUrl(): string {
  return new URL("api/auth/setup", document.baseURI).href;
}

function restartUrl(): string {
  return new URL("api/system/restart", document.baseURI).href;
}

function statusUrl(): string {
  return new URL("api/auth/status", document.baseURI).href;
}

// The accepted response is followed by a short grace period on the server, and
// the listener only stops once teardown finishes, so an immediate probe would
// answer from the process that is on its way out.
const RESTART_PROBE_DELAY_MS = 3_000;
const RESTART_PROBE_INTERVAL_MS = 1_500;
const RESTART_PROBE_TIMEOUT_MS = 45_000;

export const RESTART_UNREACHABLE_NOTE =
  "Weaver has not answered this page for 45 seconds. If the address change moved Weaver off the address this browser is using, this page cannot reach it — open Weaver at its new address. If it is not back at all, start it the way you normally start it.";

/// Restart Weaver from the browser and reload once it answers again.
export function useRestartAction() {
  const [phase, setPhase] = useState<"idle" | "restarting" | "unreachable">("idle");
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (phase !== "restarting") {
      return;
    }
    let cancelled = false;
    let timer: number | undefined;
    const deadline = Date.now() + RESTART_PROBE_TIMEOUT_MS;

    const probe = async () => {
      if (cancelled) {
        return;
      }
      try {
        const response = await fetch(statusUrl(), {
          credentials: "include",
          cache: "no-store",
        });
        if (!cancelled && response.ok) {
          window.location.reload();
          return;
        }
      } catch {
        // Expected for as long as Weaver is down.
      }
      if (cancelled) {
        return;
      }
      if (Date.now() >= deadline) {
        setPhase("unreachable");
        return;
      }
      timer = window.setTimeout(probe, RESTART_PROBE_INTERVAL_MS);
    };

    timer = window.setTimeout(probe, RESTART_PROBE_DELAY_MS);
    return () => {
      cancelled = true;
      if (timer !== undefined) {
        window.clearTimeout(timer);
      }
    };
  }, [phase]);

  const restart = async () => {
    setError(null);
    try {
      const response = await fetch(restartUrl(), {
        method: "POST",
        credentials: "include",
      });
      if (!response.ok) {
        const payload = (await response.json().catch(() => ({}))) as { error?: string };
        setError(payload.error ?? `restart failed (${response.status})`);
        return;
      }
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : String(cause));
      return;
    }
    setPhase("restarting");
  };

  return { phase, error, restart };
}

/// The fresh-install form: its state, what the deployment lets it ask, and
/// the request that finishes setup.
export function useSetupForm(environment?: SetupEnvironment | null) {
  const [mode, setMode] = useState<AccessMode | null>(null);
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [confirm, setConfirm] = useState("");
  const [setupCode, setSetupCode] = useState("");
  const [bindWide, setBindWide] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const [restart, setRestart] = useState<{
    supported: boolean;
    unsupportedReason: string | null;
  } | null>(null);

  // The bind question is only a question where this deployment leaves the
  // answer to Weaver. A container publishes ports, and an environment-pinned
  // address makes any answer here inert — asking either would collect a
  // decision the server then ignores.
  const containerized = isContainerDeployment(environment?.deployment);
  const bindPinned = environment ? !environment.bindEditable : false;
  const bindQuestionApplies = !containerized && !bindPinned;
  const authenticatedAccess = environment?.authenticatedAccess === true;
  const codeRequired = authenticatedAccess && environment?.codeRequired === true;
  const selectedMode = authenticatedAccess ? "login_required" : mode;

  const needsCredentials =
    authenticatedAccess || mode === "login_required" || mode === "login_except_local";
  const passwordsDiffer = password.length > 0 && confirm.length > 0 && password !== confirm;
  const credentialsValid =
    !needsCredentials ||
    (username.trim().length > 0 && password.length > 0 && password === confirm);
  const canSubmit =
    selectedMode !== null &&
    credentialsValid &&
    (!codeRequired || setupCode.trim().length > 0) &&
    !submitting;

  const submit = async () => {
    if (selectedMode === null) {
      return;
    }
    setError(null);
    setSubmitting(true);
    try {
      const body: Record<string, unknown> = { mode: selectedMode };
      if (needsCredentials) {
        body.username = username.trim();
        body.password = password;
      }
      if (codeRequired) {
        body.setupCode = setupCode.trim();
      }
      if (bindWide && bindQuestionApplies) {
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
      if (authenticatedAccess) {
        await refreshSessionCookie();
      }
      if (payload.restartRequiredForBind) {
        setRestart({
          supported: Boolean(payload.restartSupported),
          unsupportedReason: payload.restartUnsupportedReason ?? null,
        });
        return;
      }
      window.location.reload();
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : String(cause));
      setSubmitting(false);
    }
  };

  return {
    mode,
    setMode,
    username,
    setUsername,
    password,
    setPassword,
    confirm,
    setConfirm,
    setupCode,
    // Show the code the way Weaver prints it, however it is typed.
    setSetupCode: (next: string) => setSetupCode(formatSetupCode(next)),
    bindWide,
    setBindWide,
    error,
    submitting,
    restart,
    containerized,
    bindQuestionApplies,
    authenticatedAccess,
    codeRequired,
    needsCredentials,
    passwordsDiffer,
    canSubmit,
    submit,
  };
}
