import { lazy, Suspense } from "react";
import { RouterProvider } from "react-router/dom";
import { NextDataProvider } from "./data/next-data";
import { nextRouter } from "./router";

/**
 * The dev server's font trial (`dev/font-trial.tsx`). A production build reads
 * `import.meta.env.DEV` as `false` and drops the import with it. The import
 * starts as this module loads, ahead of the first screen's own lazy chunk, so a
 * stored choice is normally on before anything paints.
 */
const fontTrial = import.meta.env.DEV ? import("./dev/font-trial") : null;
const FontTrial = fontTrial ? lazy(() => fontTrial) : null;

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
      {FontTrial ? (
        <Suspense fallback={null}>
          <FontTrial />
        </Suspense>
      ) : null}
    </NextDataProvider>
  );
}

export default NextApp;
