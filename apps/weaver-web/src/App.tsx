import { useEffect, useMemo, useRef, useState } from "react";
import { SecurityUpgradeWizard, SetupWizardPage } from "@/pages/SetupWizardPage";
import { Provider, useQuery } from "urql";
import { RouterProvider } from "react-router/dom";
import { ThemeProvider } from "next-themes";
import { SECURITY_SETUP_STATE_QUERY } from "@/graphql/queries";
import { requestGraphqlClientRestart, useGraphqlClient } from "./graphql/client";
import { router } from "./router";
import { useLanguage } from "@/lib/hooks/use-language";
import { TranslateContext, type TranslateContextValue } from "@/lib/context/translate-context";
import { PwaProvider } from "@/lib/context/pwa-context";
import { Toaster } from "@/components/ui/sonner";

function AppProviders() {
  const { isReady, t, uiLanguage, setLanguagePreference, selectedLanguage } = useLanguage();
  const client = useGraphqlClient();
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
    return <div className="min-h-screen bg-background" aria-hidden="true" />;
  }

  return (
    <TranslateContext.Provider value={contextValue}>
      <Provider value={client}>
        <SecurityUpgradeGate>
          <RouterProvider router={router} />
        </SecurityUpgradeGate>
        <Toaster />
      </Provider>
    </TranslateContext.Provider>
  );
}

interface SecuritySetupState {
  adminLoginStatus: { enabled: boolean };
  accessPolicy: {
    mode: string;
    trustedNetworks: string[];
    editable: boolean;
    envPinned: boolean;
    configured: boolean;
    strictSecurity: boolean;
  };
  httpBindAddress: {
    address: string;
    storedAddress: string | null;
    editable: boolean;
  };
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
/// either: the sign-in page is server-rendered and navigates to `/`, so the
/// gate is re-evaluated by the fresh document load.
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
    return <div className="min-h-screen bg-background" aria-hidden="true" />;
  }
  if (decision === "wizard" && data) {
    return (
      <SecurityUpgradeWizard
        state={{
          loginEnabled: data.adminLoginStatus.enabled,
          strictSecurity: data.accessPolicy.strictSecurity,
          bindEditable: data.httpBindAddress.editable,
          bindEffective: data.httpBindAddress.storedAddress ?? data.httpBindAddress.address,
        }}
        onDone={() => setDecision("app")}
      />
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

  useEffect(() => {
    let cancelled = false;
    const statusUrl = new URL("api/auth/status", document.baseURI).href;
    fetch(statusUrl, { credentials: "include" })
      .then((response) => (response.ok ? response.json() : { setupRequired: false }))
      .then((payload: { setupRequired?: boolean }) => {
        if (!cancelled) {
          setSetupRequired(Boolean(payload.setupRequired));
        }
      })
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
    return <div className="min-h-screen bg-background" aria-hidden="true" />;
  }
  if (setupRequired) {
    return <SetupWizardPage />;
  }
  return <>{children}</>;
}

export function App() {
  return (
    <ThemeProvider attribute="class" defaultTheme="dark" enableSystem>
      <PwaProvider>
        <SetupGate>
          <AppProviders />
        </SetupGate>
      </PwaProvider>
    </ThemeProvider>
  );
}
