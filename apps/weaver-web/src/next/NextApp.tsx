import { RouterProvider } from "react-router/dom";
import { NextDataProvider } from "./data/next-data";
import { nextRouter } from "./router";

/**
 * Root of the Next interface.
 *
 * `App` loads this lazily so a browser running the classic UI never downloads
 * the Next chunk — the two trees share the urql client and the translation
 * context above this point and nothing below it.
 */
export function NextApp() {
  return (
    <NextDataProvider>
      <RouterProvider router={nextRouter} useTransitions={false} />
    </NextDataProvider>
  );
}

export default NextApp;
