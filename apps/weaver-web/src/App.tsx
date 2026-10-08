import { Suspense, lazy, useEffect, useMemo, useRef, useState } from "react";
import type { SetupEnvironment } from "@/lib/setup-flow";
import type { SecurityUpgradeState } from "@/lib/security-upgrade";
import { Provider, useQuery } from "urql";
import { SECURITY_SETUP_STATE_QUERY } from "@/graphql/queries";
import { requestGraphqlClientRestart, useGraphqlClient } from "./graphql/client";
import { useLanguage } from "@/lib/hooks/use-language";
import { TranslateContext, type TranslateContextValue } from "@/lib/context/translate-context";
import { PwaProvider } from "@/lib/context/pwa-context";
import { LoadingMark } from "@/lib/loading-mark";
import { noteAuthStatus, useLoginRequired, type AuthStatus } from "@/lib/login-required";
import { AppRoot } from "./next/AppRoot";

/// The pages a browser sees before the app itself are loaded on their own, so
/// a signed-out or not-yet-set-up browser downloads nothing it cannot use yet.
const LoginPage = lazy(() => import("./next/pages/LoginPage"));
const SetupPage = lazy(() => import("./next/pages/SetupPage"));
const SecurityUpgradePage = lazy(() => import("./next/pages/SecurityUpgradePage"));

/// Gates render this while they decide, on the app's own background. The
/// loading mark waits before it appears, so a gate that decides at once shows
/// a plain background rather than a flicker.
function GatePlaceholder() {
  return (
    <div className="flex h-dvh items-center justify-center bg-wv-app" aria-hidden="true">
      <LoadingMark className="h-10" reveal />
    </div>
  );
}

function AppProviders() {
  const { isReady, t, uiLanguage, setLanguagePreference, selectedLanguage } = useLanguage();
  const client = useGraphqlClient();
  const loginRequired = useLoginRequired();
  const wasBackgroundedRef = useRef(false);

  useEffect(() => {
    const markBackgrounded = () => {
      wasBackgroundedRef.current = true;
    };
    const reconnectOnForeground = () => {
      if (document.visibilityState !== "visible" || !wasBackgroundedRef.current) {
        return;
      }

      wasBackgroundedRef.current = false;
      void requestGraphqlClientRestart();
    };
    const handleVisibilityChange = () => {
      if (document.visibilityState === "hidden") {
        markBackgrounded();
        return;
      }

      reconnectOnForeground();
    };

    window.addEventListener("blur", markBackgrounded);
    window.addEventListener("focus", reconnectOnForeground);
    window.addEventListener("pagehide", markBackgrounded);
    window.addEventListener("pageshow", reconnectOnForeground);
    document.addEventListener("visibilitychange", handleVisibilityChange);

    return () => {
      window.removeEventListener("blur", markBackgrounded);
      window.removeEventListener("focus", reconnectOnForeground);
      window.removeEventListener("pagehide", markBackgrounded);
      window.removeEventListener("pageshow", reconnectOnForeground);
      document.removeEventListener("visibilitychange", handleVisibilityChange);
    };
  }, []);

  const contextValue = useMemo<TranslateContextValue>(
    () => ({ t, uiLanguage, setLanguagePreference, selectedLanguage }),
    [t, uiLanguage, setLanguagePreference, selectedLanguage],
  );

  if (!isReady) {
    return <GatePlaceholder />;
  }

  // Nothing below can load for a browser that has to sign in first, so the
  // sign-in page replaces the whole tree rather than sitting inside it.
  if (loginRequired) {
    return (
      <TranslateContext.Provider value={contextValue}>
        <Suspense fallback={<GatePlaceholder />}>
          <LoginPage />
        </Suspense>
      </TranslateContext.Provider>
    );
  }

  return (
    <TranslateContext.Provider value={contextValue}>
      <Provider value={client}>
        <SecurityUpgradeGate>
          <AppRoot />
        </SecurityUpgradeGate>
      </Provider>
    </TranslateContext.Provider>
  );
}

interface SecuritySetupState {
  adminLoginStatus: { enabled: boolean };
  accessPolicy: {
    editable: boolean;
    configured: boolean;
    strictSecurity: boolean;
  };
  httpBindAddress: {
    address: string;
    storedAddress: string | null;
    editable: boolean;
  };
  serverRestart: { supported: boolean; reason: string | null; deployment: string };
}

/// Offer the security wizard once to an install that predates these settings.
///
/// Lives inside the urql provider because the answer is a GraphQL query, and
/// separate from [`SetupGate`], which handles the credential-less case before
/// any authenticated query could succeed. It fails OPEN on every ambiguity —
/// a query error, an unauthenticated or non-admin browser, an environment-
/// managed deployment — because nagging is the worse failure here: the app
/// renders and Settings → Security still holds every one of these controls.
///
/// The decision is latched after the first resolution. The urql client is
/// recreated on tab refocus, which re-runs this query; without the latch the
/// app would blank mid-session every time. Nothing re-latches after a login
/// either: signing in reloads the page, so the gate is re-evaluated by the
/// fresh document load.
function SecurityUpgradeGate({ children }: { children: React.ReactNode }) {
  const [decision, setDecision] = useState<"pending" | "wizard" | "app">("pending");
  const [{ data, error, fetching }] = useQuery<SecuritySetupState>({
    query: SECURITY_SETUP_STATE_QUERY,
    requestPolicy: "network-only",
  });

  useEffect(() => {
    if (fetching || (!data && !error)) {
      return;
    }
    setDecision((current) => {
      if (current !== "pending") {
        return current;
      }
      const policy = data?.accessPolicy;
      if (error || !policy || !data?.httpBindAddress) {
        return "app";
      }
      return !policy.configured && policy.editable ? "wizard" : "app";
    });
  }, [data, error, fetching]);

  if (decision === "pending") {
    return <GatePlaceholder />;
  }
  if (decision === "wizard" && data) {
    const state: SecurityUpgradeState = {
      loginEnabled: data.adminLoginStatus.enabled,
      strictSecurity: data.accessPolicy.strictSecurity,
      bindEditable: data.httpBindAddress.editable,
      bindEffective: data.httpBindAddress.storedAddress ?? data.httpBindAddress.address,
      restartSupported: Boolean(data.serverRestart?.supported),
      restartUnsupportedReason: data.serverRestart?.reason ?? null,
      // The GraphQL enum arrives upper-cased; the wizard compares against
      // the same lower-case spellings the REST status surface uses.
      deployment: (data.serverRestart?.deployment ?? "").toLowerCase(),
    };
    const onDone = () => setDecision("app");
    return (
      <Suspense fallback={<GatePlaceholder />}>
        <SecurityUpgradePage state={state} onDone={onDone} />
      </Suspense>
    );
  }
  return <>{children}</>;
}

/// Gate the app behind first-run setup. Checked before the GraphQL-driven
/// tree mounts, because a pre-setup browser has no credentials and every
/// authenticated query would land as a 401 — the wizard is the only thing it
/// can usefully see.
function SetupGate({ children }: { children: React.ReactNode }) {
  const [setupRequired, setSetupRequired] = useState<boolean | null>(null);
  // Only sent to a browser that is about to run the wizard, so it is absent
  // whenever `setupRequired` is false — and absent entirely on a server that
  // predates it, which the wizard reads as "ask the bind question normally".
  const [setupEnvironment, setSetupEnvironment] = useState<SetupEnvironment | null>(null);

  useEffect(() => {
    let cancelled = false;
    const statusUrl = new URL("api/auth/status", document.baseURI).href;
    fetch(statusUrl, { credentials: "include" })
      .then((response) => (response.ok ? response.json() : { setupRequired: false }))
      .then((payload: AuthStatus & { authenticatedAccess?: boolean; setup?: SetupEnvironment }) => {
        if (!cancelled) {
          // Before the tree mounts, so a signed-out browser goes straight to
          // the sign-in page instead of firing queries that are refused.
          noteAuthStatus(payload);
          setSetupRequired(Boolean(payload.setupRequired));
          setSetupEnvironment(
            payload.setup
              ? {
                  ...payload.setup,
                  authenticatedAccess: payload.authenticatedAccess === true,
                }
              : null,
          );
        }
        }
      )
      .catch(() => {
        // Unreachable status endpoint: let the app render and surface its own
        // errors rather than trapping the user on a blank gate.
        if (!cancelled) {
          setSetupRequired(false);
        }
      });
    return () => {
      cancelled = true;
    };
  }, []);

  if (setupRequired === null) {
    return <GatePlaceholder />;
  }
  if (setupRequired) {
    return (
      <Suspense fallback={<GatePlaceholder />}>
        <SetupPage environment={setupEnvironment} />
      </Suspense>
    );
  }
  return <>{children}</>;
}

export function App() {
  return (
    <PwaProvider>
      <SetupGate>
        <AppProviders />
      </SetupGate>
    </PwaProvider>
  );
}
