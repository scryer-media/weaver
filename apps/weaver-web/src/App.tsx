import { useEffect, useMemo, useRef } from "react";
import { Provider } from "urql";
import { RouterProvider } from "react-router";
import { ThemeProvider } from "next-themes";
import { requestGraphqlClientRestart, useGraphqlClient } from "./graphql/client";
import { router } from "./router";
import { useLanguage } from "@/lib/hooks/use-language";
import { TranslateContext, type TranslateContextValue } from "@/lib/context/translate-context";
import { PwaProvider } from "@/lib/context/pwa-context";

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
        <RouterProvider router={router} />
      </Provider>
    </TranslateContext.Provider>
  );
}

export function App() {
  return (
    <ThemeProvider attribute="class" defaultTheme="dark" enableSystem>
      <PwaProvider>
        <AppProviders />
      </PwaProvider>
    </ThemeProvider>
  );
}
