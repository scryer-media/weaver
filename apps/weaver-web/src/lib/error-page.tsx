import { Component, useContext, useEffect, useRef, useState, type CSSProperties, type ReactNode } from "react";
import { isRouteErrorResponse, useNavigate, useRouteError } from "react-router";
import { BrandLockup } from "@/lib/brand";
import { TranslateContext, type Translate } from "@/lib/context/translate-context";
import { interpolate } from "@/lib/i18n/types";

/**
 * The page either interface shows when it cannot show anything else.
 *
 * It lives beside the brand lockup rather than under either interface because
 * both mount it, and it draws its own palette -- the Next UI's -- rather than
 * either one's theme tokens: the classic tree has no Next tokens, and a crash can
 * land before any theme has been applied at all. The server's startup holding
 * page draws the same design in plain HTML, so a Weaver that is failing looks
 * the same whether the browser or the server found out first.
 *
 * Retry asks Weaver whether it is answering before reloading, so a Weaver that
 * has stopped leaves the operator on this page, still checking, rather than on
 * the browser's own "cannot connect".
 */

/**
 * English, for a crash above the language provider. The same keys live in the
 * Next dictionaries, which every locale carries whichever interface is in use.
 */
const ENGLISH: Record<string, string> = {
  "next.errorPage.title": "Something went wrong",
  "next.errorPage.renderBody": "Weaver hit an unexpected error while rendering this screen.",
  "next.errorPage.notFound": "Page not found",
  "next.errorPage.requestFailed": "Request failed ({{status}})",
  "next.errorPage.routingBody": "Weaver hit a routing error before the page could finish loading.",
  "next.errorPage.retry": "Retry",
  "next.errorPage.checking": "Checking…",
  "next.errorPage.unreachable": "Weaver isn't responding. This page opens it as soon as it answers.",
  "next.errorPage.goBack": "Go back",
  "next.errorPage.details": "Technical details",
};

const CHECK_TIMEOUT_MS = 5_000;
const RECHECK_INTERVAL_MS = 3_000;

const TITLE_FONT: CSSProperties = {
  fontFamily: '"Sora Variable", ui-sans-serif, system-ui, sans-serif',
};
const UI_FONT: CSSProperties = {
  fontFamily: '"Fira Code Variable", ui-monospace, "SFMono-Regular", Menlo, monospace',
};

function useErrorTranslate(): Translate {
  const context = useContext(TranslateContext);
  return context?.t ?? ((key, values) => interpolate(ENGLISH[key] ?? key, values));
}

async function weaverAnswers(): Promise<boolean> {
  const base = (window.__WEAVER_BASE__ || "/").replace(/\/+$/, "");
  try {
    const response = await fetch(`${base}/healthz`, {
      cache: "no-store",
      credentials: "same-origin",
      signal: AbortSignal.timeout(CHECK_TIMEOUT_MS),
    });
    return response.ok;
  } catch {
    return false;
  }
}

/**
 * Reload once Weaver answers. A pressed Retry that finds nothing keeps checking
 * in the background, so the page opens Weaver by itself when it comes back.
 */
function useRetry() {
  const [state, setState] = useState<"idle" | "checking" | "unreachable">("idle");
  const timer = useRef<number | null>(null);
  const mounted = useRef(true);
  // Only the latest check may schedule the next, so pressing Retry during a
  // check does not start a second round of them.
  const latest = useRef(0);

  useEffect(() => {
    mounted.current = true;
    return () => {
      mounted.current = false;
      if (timer.current !== null) {
        window.clearTimeout(timer.current);
      }
    };
  }, []);

  const check = async (pressed: boolean) => {
    const mine = ++latest.current;
    if (timer.current !== null) {
      window.clearTimeout(timer.current);
      timer.current = null;
    }
    if (pressed) {
      setState("checking");
    }
    if (await weaverAnswers()) {
      window.location.reload();
      return;
    }
    if (!mounted.current || mine !== latest.current) {
      return;
    }
    setState("unreachable");
    timer.current = window.setTimeout(() => void check(false), RECHECK_INTERVAL_MS);
  };

  return { state, retry: () => void check(true) };
}

export function ErrorPage({
  title,
  body,
  detail,
  onBack,
}: {
  title: string;
  body: string;
  /** Shown folded away, for a bug report. */
  detail?: string | null;
  onBack?: () => void;
}) {
  const t = useErrorTranslate();
  const { state, retry } = useRetry();
  const focusRing =
    "focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[#3fb39c]";

  return (
    <div
      className="flex min-h-dvh items-center justify-center bg-[#1a1b1e] px-6 py-12 text-[#ecebe6]"
      style={UI_FONT}
    >
      <main className="flex w-full max-w-[460px] flex-col items-center text-center">
        <BrandLockup className="h-7 w-auto flex-none" />
        <h1
          className="mt-9 text-[22px] leading-[1.3] font-semibold tracking-[-0.01em] text-[#ecebe6]"
          style={TITLE_FONT}
        >
          {title}
        </h1>
        <p className="mt-2 text-[13px] leading-[1.6] break-words text-[#a09d96]">{body}</p>
        <div className="mt-6 flex flex-wrap justify-center gap-[10px]">
          <button
            type="button"
            onClick={retry}
            disabled={state === "checking"}
            className={`h-[34px] cursor-pointer bg-[#3fb39c] px-4 text-[13px] font-medium text-[#10201c] hover:bg-[#52c4ad] disabled:cursor-default disabled:bg-[#2f8a78] ${focusRing}`}
          >
            {state === "checking" ? t("next.errorPage.checking") : t("next.errorPage.retry")}
          </button>
          {onBack ? (
            <button
              type="button"
              onClick={onBack}
              className={`h-[34px] cursor-pointer border border-[#3a3b41] bg-[#23242a] px-[14px] text-[13px] font-medium text-[#ecebe6] hover:border-[#4a4b52] hover:bg-[#2a2b31] ${focusRing}`}
            >
              {t("next.errorPage.goBack")}
            </button>
          ) : null}
        </div>
        <p role="status" aria-live="polite" className="mt-3 min-h-[1.6em] text-[12px] leading-[1.6] text-[#84817a]">
          {state === "unreachable" ? t("next.errorPage.unreachable") : ""}
        </p>
        {detail ? (
          <details className="mt-5 w-full text-left">
            <summary className={`cursor-pointer text-center text-[12px] text-[#84817a] hover:text-[#a09d96] ${focusRing}`}>
              {t("next.errorPage.details")}
            </summary>
            <pre className="mt-3 max-h-[240px] overflow-auto border border-[#2a2b30] bg-[#1f2024] px-3 py-2 text-[11.5px] leading-[1.55] break-words whitespace-pre-wrap text-[#a09d96]">
              {detail}
            </pre>
          </details>
        ) : null}
      </main>
    </div>
  );
}

function detailOf(error: unknown): string | null {
  if (error instanceof Error) {
    return error.stack || error.message || null;
  }
  if (isRouteErrorResponse(error)) {
    if (typeof error.data === "string") {
      return error.data || null;
    }
    return error.data == null ? null : JSON.stringify(error.data, null, 2);
  }
  return error == null ? null : String(error);
}

/** Either router's `errorElement`. */
export function RouteErrorPage() {
  const t = useErrorTranslate();
  const error = useRouteError();
  const navigate = useNavigate();

  let title = t("next.errorPage.title");
  let body = t("next.errorPage.renderBody");
  if (isRouteErrorResponse(error)) {
    title =
      error.status === 404
        ? t("next.errorPage.notFound")
        : t("next.errorPage.requestFailed", { status: error.status });
    body = error.statusText?.trim() ? error.statusText : t("next.errorPage.routingBody");
  } else if (error instanceof Error && error.message) {
    body = error.message;
  }

  return (
    <ErrorPage
      title={title}
      body={body}
      detail={detailOf(error)}
      onBack={() => void (window.history.length > 1 ? navigate(-1) : navigate("/"))}
    />
  );
}

/**
 * Catches whatever no router caught: a crash in a gate, the sign-in page, or
 * anything else outside a route.
 */
export class AppErrorBoundary extends Component<{ children: ReactNode }, { failed: boolean; error: unknown }> {
  state = { failed: false, error: null as unknown };

  static getDerivedStateFromError(error: unknown) {
    return { failed: true, error };
  }

  render() {
    if (!this.state.failed) {
      return this.props.children;
    }
    return <CrashPage error={this.state.error} />;
  }
}

function CrashPage({ error }: { error: unknown }) {
  const t = useErrorTranslate();
  return (
    <ErrorPage
      title={t("next.errorPage.title")}
      body={error instanceof Error && error.message ? error.message : t("next.errorPage.renderBody")}
      detail={detailOf(error)}
    />
  );
}
