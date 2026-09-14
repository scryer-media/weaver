import { RouterProvider } from "react-router/dom";
import { NextDataProvider } from "./data/next-data";
import { FirstRunGate } from "./features/FirstRunSetup";
import { nextRouter } from "./router";

/**
 * Root of the Next interface.
 *
 * `App` loads this lazily so a browser running the classic UI never downloads
 * the Next chunk — the two trees share the urql client and the translation
 * context above this point and nothing below it. A new install is walked
 * through first-run setup before any of the interface mounts.
 */
export function NextApp() {
  return (
    <FirstRunGate>
      <NextDataProvider>
        <RouterProvider router={nextRouter} useTransitions={false} />
      </NextDataProvider>
    </FirstRunGate>
  );
}

export default NextApp;
