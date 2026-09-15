import { LoadingMark } from "@/lib/loading-mark";
import { NextShell } from "./NextShell";

/**
 * Hydration fallback for the lazy next routes.
 *
 * React Router renders nothing while a matched lazy module is pending unless
 * the route declares a fallback, which would leave the window empty for the
 * whole first load. The shell itself is cheap and already has its data, so the
 * rail, top bar and status bar paint immediately and only the content region
 * waits, showing the loading mark only if the module is slow to arrive.
 */
export function NextRouteFallback() {
  return (
    <NextShell title="Weaver">
      <div role="status" aria-busy="true" className="flex flex-1 items-center justify-center bg-wv-list">
        <LoadingMark className="h-10" reveal />
      </div>
    </NextShell>
  );
}
