import { RouterProvider } from "react-router/dom";
import { NextDataProvider } from "./data/next-data";
import { FirstRunGate } from "./features/FirstRunSetup";
import { SecurityUpgradeNotice } from "./features/SecurityUpgradeNotice";
import { nextRouter } from "./router";

/**
 * Root of the Next interface.
 *
 * `App` loads this lazily so a browser running the classic UI never downloads
 * the Next chunk — the two trees share the urql client and the translation
 * context above this point and nothing below it. A new install is walked
 * through first-run setup before any of the interface mounts, and an install
 * still on the access settings from before 0.12.0 is told how to move once.
 */
export function NextApp() {
  return (
    <FirstRunGate>
      <NextDataProvider>
        <RouterProvider router={nextRouter} useTransitions={false} />
      </NextDataProvider>
      <SecurityUpgradeNotice />
    </FirstRunGate>
  );
}

export default NextApp;
