import { isRouteErrorResponse, useNavigate, useRouteError } from "react-router";
import { BrandLockup } from "@/lib/brand";
import { useTranslate, type Translate } from "@/lib/context/translate-context";
import { LoadingMark } from "@/lib/loading-mark";
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

function describe(error: unknown, t: Translate): { title: string; body: string } {
  if (isRouteErrorResponse(error)) {
    return {
      title:
        error.status === 404
          ? t("next.routeError.notFound")
          : t("next.routeError.requestFailed", { status: error.status }),
      body:
        typeof error.statusText === "string" && error.statusText.trim() !== ""
          ? error.statusText
          : t("next.routeError.routingBody"),
    };
  }
  if (error instanceof Error) {
    return {
      title: t("next.routeError.title"),
      body: error.message || t("next.routeError.renderBody"),
    };
  }
  return {
    title: t("next.routeError.title"),
    body: t("next.routeError.renderBody"),
  };
}

export function NextRouteError() {
  const t = useTranslate();
  const error = useRouteError();
  const navigate = useNavigate();
  const { title, body } = describe(error, t);
  const detail = error instanceof Error ? error.stack : null;

  return (
    <div className="flex h-screen flex-col overflow-hidden bg-wv-app text-wv-fg">
      <header className="flex h-14 flex-none items-center gap-[9px] border-b border-wv-line-strong bg-wv-chrome px-4 sm:px-6">
        <BrandLockup className="h-[16px] w-auto flex-none" />
      </header>
      <div className="flex min-h-0 flex-1 flex-col overflow-y-auto bg-wv-list">
        <EmptyState title={title} body={body} />
        <div className="flex gap-[10px] px-4 sm:px-6">
          <SecondaryButton icon="back" onClick={() => void navigate(-1)}>
            {t("next.routeError.goBack")}
          </SecondaryButton>
          <PrimaryButton icon="refresh" onClick={() => window.location.reload()}>
            {t("pwa.reload")}
          </PrimaryButton>
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
