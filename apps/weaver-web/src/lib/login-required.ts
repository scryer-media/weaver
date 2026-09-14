import { useState, useSyncExternalStore, type FormEvent } from "react";
import type { Translate } from "@/lib/context/translate-context";

/**
 * Whether this browser has to sign in before the interface can do anything.
 *
 * Set at startup from `/api/auth/status`, and again whenever one of Weaver's
 * own requests comes back 401 — which is how a login turned on from this very
 * tab, or a login cookie that expired mid-session, reaches the sign-in page.
 * A 401 alone never shows the page: the status endpoint has the final word,
 * and only a login this browser lacks counts. Nothing clears it: signing in
 * reloads the page.
 */
let required = false;
const listeners = new Set<() => void>();

export interface AuthStatus {
  enabled?: boolean;
  authenticated?: boolean;
  setupRequired?: boolean;
}

export function authStatusUrl(): string {
  return new URL("api/auth/status", document.baseURI).href;
}

function loginUrl(): string {
  return new URL("api/login", document.baseURI).href;
}

/** Record a status answer; only a login this browser lacks changes anything. */
export function noteAuthStatus(status: AuthStatus) {
  if (typeof status.enabled === "boolean") {
    noteLoginEnabled(status.enabled);
  }
  if (required || !status.enabled || status.authenticated) {
    return;
  }
  required = true;
  for (const listener of listeners) {
    listener();
  }
}

let checking: Promise<void> | null = null;

/** Ask the server whether a refused request means this browser must sign in. */
export function recheckLoginRequired(): Promise<void> {
  checking ??= fetch(authStatusUrl(), { credentials: "include" })
    .then((response) => (response.ok ? (response.json() as Promise<AuthStatus>) : {}))
    .then(noteAuthStatus)
    .catch(() => {
      // Unreachable: the connection banner already says so.
    })
    .finally(() => {
      checking = null;
    });
  return checking;
}

/**
 * Watch every request the page makes for a refusal from Weaver itself.
 *
 * Requests to any other origin are ignored; the daemon's own calls out to
 * GitHub or SRRDB happen server-side and reach the page as ordinary errors.
 */
export function watchForSignOut() {
  const origin = new URL(document.baseURI).origin;
  const ownEndpoints = new Set([authStatusUrl(), loginUrl()]);
  const nativeFetch = window.fetch.bind(window);
  window.fetch = async (input, init) => {
    const response = await nativeFetch(input, init);
    if (response.status === 401) {
      const url = new URL(input instanceof Request ? input.url : String(input), document.baseURI);
      if (url.origin === origin && !ownEndpoints.has(url.href)) {
        void recheckLoginRequired();
      }
    }
    return response;
  };
}

function subscribe(listener: () => void) {
  listeners.add(listener);
  return () => {
    listeners.delete(listener);
  };
}

export function useLoginRequired(): boolean {
  return useSyncExternalStore(
    subscribe,
    () => required,
    () => required,
  );
}

/**
 * Whether this server has a login at all, which is when there is something to
 * sign out of. Known before the interface mounts, from the same status answer
 * as `required`, and kept current by the security settings, which turn the
 * login on and off from inside the running page.
 */
let loginEnabled = false;
const loginEnabledListeners = new Set<() => void>();

export function noteLoginEnabled(enabled: boolean) {
  if (loginEnabled === enabled) {
    return;
  }
  loginEnabled = enabled;
  for (const listener of loginEnabledListeners) {
    listener();
  }
}

function subscribeLoginEnabled(listener: () => void) {
  loginEnabledListeners.add(listener);
  return () => {
    loginEnabledListeners.delete(listener);
  };
}

export function useLoginEnabled(): boolean {
  return useSyncExternalStore(
    subscribeLoginEnabled,
    () => loginEnabled,
    () => loginEnabled,
  );
}

type SignInOutcome = { ok: true } | { ok: false; status: number; message: string };

/** Post the credentials; rejects only when Weaver cannot be reached at all. */
async function signIn(username: string, password: string, remember: boolean): Promise<SignInOutcome> {
  const response = await fetch(loginUrl(), {
    method: "POST",
    credentials: "include",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ username, password, remember }),
  });
  if (response.ok) {
    return { ok: true };
  }
  const payload = (await response.json().catch(() => ({}))) as { error?: string };
  return { ok: false, status: response.status, message: payload.error ?? `HTTP ${response.status}` };
}

/**
 * Both interfaces' sign-in pages: the same form state and the same outcome,
 * drawn in each one's own controls.
 *
 * A successful sign-in reloads the page rather than mounting the interface in
 * place, so every gate above it — and every query it had already refused —
 * starts again with the new cookie. The form stays busy until that reload.
 */
export function useSignInForm(t: Translate) {
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [remember, setRemember] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const submit = (event: FormEvent) => {
    event.preventDefault();
    if (busy) {
      return;
    }
    setBusy(true);
    setError(null);
    void signIn(username, password, remember)
      .catch(() => null)
      .then((outcome) => {
        if (outcome?.ok) {
          window.location.reload();
          return;
        }
        setBusy(false);
        setError(
          outcome === null
            ? t("next.login.unreachable")
            : outcome.status === 401
              ? t("next.login.invalid")
              : outcome.status === 429
                ? t("next.login.tooMany")
                : t("next.login.failed", { error: outcome.message }),
        );
      });
  };

  return {
    username,
    setUsername,
    password,
    setPassword,
    remember,
    setRemember,
    busy,
    error,
    canSubmit: !busy && username.length > 0 && password.length > 0,
    submit,
  };
}

/** What the "forgot password" help shows; the variable names are the daemon's own. */
export const LOGIN_RESET_COMMANDS = {
  docker:
    "docker run -e WEAVER_RESET_LOGIN=1 -e WEAVER_BOOTSTRAP_LOGIN_USERNAME=admin -e WEAVER_BOOTSTRAP_LOGIN_PASSWORD_FILE=/run/secrets/weaver-login -v /host/password:/run/secrets/weaver-login:ro ...",
  bareMetal:
    "WEAVER_RESET_LOGIN=1 WEAVER_BOOTSTRAP_LOGIN_USERNAME=admin WEAVER_BOOTSTRAP_LOGIN_PASSWORD_FILE=/path/to/password weaver serve",
} as const;
