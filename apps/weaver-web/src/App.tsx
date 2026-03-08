import { Provider } from "urql";
import { RouterProvider } from "react-router";
import { ThemeProvider } from "next-themes";
import { client } from "./graphql/client";
import { router } from "./router";
import { useLanguage } from "@/lib/hooks/use-language";
import { TranslateContext } from "@/lib/context/translate-context";

function AppProviders() {
  const { t } = useLanguage();

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
      <AppProviders />
    </ThemeProvider>
  );
}
