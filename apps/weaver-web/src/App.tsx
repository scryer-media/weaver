import { Provider } from "urql";
import { RouterProvider } from "react-router";
import { ThemeProvider } from "next-themes";
import { useGraphqlClient } from "./graphql/client";
import { router } from "./router";
import { useLanguage } from "@/lib/hooks/use-language";
import { TranslateContext } from "@/lib/context/translate-context";
import { PwaProvider } from "@/lib/context/pwa-context";

function AppProviders() {
  const { t } = useLanguage();
  const client = useGraphqlClient();

  return (
    <TranslateContext.Provider value={t}>
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
