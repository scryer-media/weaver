import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { App } from "./App";
import "./fonts.css";
import "./globals.css";

if (import.meta.env.DEV) {
  // React 19's dev builds emit a `performance.measure` per component per
  // commit for the DevTools performance tracks, and the browser retains User
  // Timing entries without limit — in renderer memory the JS heap profiler
  // never shows. On the queue page, which commits its whole tree several
  // times a second under load, that is megabytes per second: measured at
  // ~150 MB/min of renderer RSS with ~500k retained entries, crashing the
  // tab inside 15 minutes. The tracks stream to an attached profiler live,
  // so periodically dropping the retained buffer costs the tooling nothing.
  window.setInterval(() => {
    performance.clearMeasures();
    performance.clearMarks();
  }, 30_000);
}

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <App />
  </StrictMode>,
);
