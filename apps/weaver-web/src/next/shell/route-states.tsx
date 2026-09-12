import { isRouteErrorResponse, useNavigate, useRouteError } from "react-router";
import { NextShell } from "./NextShell";
import { EmptyState } from "../components/chrome";
import { PrimaryButton, SecondaryButton } from "../components/controls";

/**
 * Hydration fallback for the lazy next routes.
 *
 * React Router renders nothing while a matched lazy module is pending unless
 * the route declares a fallback, which would leave the window empty for the
 * whole first load. The shell itself is cheap and already has its data, so the
 * rail, top bar and status bar paint immediately and only the content region
 * waits.
 */
export function NextRouteFallback() {
  return (
    <NextShell title="Weaver">
      <div role="status" aria-busy="true" className="flex-1 bg-wv-list" />
    </NextShell>
  );
}

function describe(error: unknown): { title: string; body: string } {
  if (isRouteErrorResponse(error)) {
    return {
      title: error.status === 404 ? "Page not found" : `Request failed (${error.status})`,
      body:
        typeof error.statusText === "string" && error.statusText.trim() !== ""
          ? error.statusText
          : "Weaver hit a routing error before the page could finish loading.",
    };
  }
  if (error instanceof Error) {
    return {
      title: "Something went wrong",
      body: error.message || "Weaver hit an unexpected error while rendering this screen.",
    };
  }
  return {
    title: "Something went wrong",
    body: "Weaver hit an unexpected error while rendering this screen.",
  };
}

export function NextRouteError() {
  const error = useRouteError();
  const navigate = useNavigate();
  const { title, body } = describe(error);
  const detail = error instanceof Error ? error.stack : null;

  return (
    <div className="flex h-screen flex-col overflow-hidden bg-wv-app text-wv-fg">
      <header className="flex h-14 flex-none items-center gap-[9px] border-b border-wv-line-strong bg-wv-chrome px-4 sm:px-6">
        <span aria-hidden="true" className="size-[18px] flex-none bg-wv-accent" />
        <span className="text-[14px] font-semibold tracking-[-0.01em]">Weaver</span>
      </header>
      <div className="flex min-h-0 flex-1 flex-col overflow-y-auto bg-wv-list">
        <EmptyState title={title} body={body} />
        <div className="flex gap-[10px] px-4 sm:px-6">
          <SecondaryButton onClick={() => void navigate(-1)}>Go back</SecondaryButton>
          <PrimaryButton onClick={() => window.location.reload()}>Reload</PrimaryButton>
        </div>
        {detail === null ? null : (
          <pre className="mt-6 overflow-x-auto border-t border-wv-hairline px-4 sm:px-6 py-5 font-wv-mono text-[11.5px] leading-[1.55] whitespace-pre-wrap text-wv-faint">
            {detail}
          </pre>
        )}
      </div>
    </div>
  );
}
