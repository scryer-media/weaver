import { useMemo } from "react";
import { Provider } from "urql";
import { RouterProvider } from "react-router";
import { ThemeProvider } from "next-themes";
import { useGraphqlClient } from "./graphql/client";
import { router } from "./router";
import { useLanguage } from "@/lib/hooks/use-language";
import { TranslateContext, type TranslateContextValue } from "@/lib/context/translate-context";
import { PwaProvider } from "@/lib/context/pwa-context";

function AppProviders() {
  const { t, uiLanguage, setLanguagePreference, selectedLanguage } = useLanguage();
  const client = useGraphqlClient();

  const contextValue = useMemo<TranslateContextValue>(
    () => ({ t, uiLanguage, setLanguagePreference, selectedLanguage }),
    [t, uiLanguage, setLanguagePreference, selectedLanguage],
  );

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
