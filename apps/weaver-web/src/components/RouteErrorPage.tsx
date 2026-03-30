import { AlertTriangle, Home, RefreshCw, RotateCcw } from "lucide-react";
import { Link, isRouteErrorResponse, useNavigate, useRouteError } from "react-router";
import { Button } from "@/components/ui/button";

function describeRouteError(error: unknown) {
  if (isRouteErrorResponse(error)) {
    return {
      title: error.status === 404 ? "Page not found" : `Request failed (${error.status})`,
      summary:
        typeof error.statusText === "string" && error.statusText.trim().length > 0
          ? error.statusText
          : "Weaver hit a routing error before the page could finish loading.",
      detail:
        typeof error.data === "string"
          ? error.data
          : error.data != null
            ? JSON.stringify(error.data, null, 2)
            : null,
    };
  }

  if (error instanceof Error) {
    return {
      title: "Something went wrong",
      summary:
        error.message || "Weaver hit an unexpected error while rendering this screen.",
      detail: error.stack ?? null,
    };
  }

  return {
    title: "Something went wrong",
    summary: "Weaver hit an unexpected error while rendering this screen.",
    detail: error != null ? String(error) : null,
  };
}

export function RouteErrorPage() {
  const navigate = useNavigate();
  const error = useRouteError();
  const description = describeRouteError(error);

  return (
    <div className="min-h-screen bg-[radial-gradient(circle_at_top,_rgba(14,165,233,0.16),_transparent_38%),linear-gradient(180deg,_rgba(15,23,42,0.96),_rgba(2,6,23,1))] text-foreground">
      <div className="mx-auto flex min-h-screen w-full max-w-5xl items-center px-6 py-12 sm:px-8">
        <div className="grid w-full gap-8 lg:grid-cols-[1.15fr_0.85fr] lg:items-center">
          <section className="space-y-6">
            <div className="inline-flex items-center gap-2 rounded-full border border-amber-400/30 bg-amber-400/10 px-3 py-1 text-[11px] font-semibold uppercase tracking-[0.22em] text-amber-100/90">
              <AlertTriangle className="size-3.5" />
              Weaver Error Boundary
            </div>

            <div className="space-y-4">
              <h1 className="max-w-2xl text-4xl font-semibold tracking-tight text-white sm:text-5xl">
                {description.title}
              </h1>
              <p className="max-w-2xl text-base leading-7 text-slate-300 sm:text-lg">
                {description.summary}
              </p>
              <p className="text-sm text-slate-400">
                Path: <span className="font-mono text-slate-200">{window.location.pathname}</span>
              </p>
            </div>

            <div className="flex flex-wrap gap-3">
              <Button
                size="lg"
                onClick={() => window.location.reload()}
                className="rounded-full px-6"
              >
                <RefreshCw className="size-4" />
                Reload Weaver
              </Button>
              <Button
                variant="outline"
                size="lg"
                onClick={() => {
                  if (window.history.length > 1) {
                    navigate(-1);
                    return;
                  }
                  navigate("/");
                }}
                className="rounded-full border-white/15 bg-white/5 px-6 text-white hover:bg-white/10 hover:text-white"
              >
                <RotateCcw className="size-4" />
                Go Back
              </Button>
              <Button
                variant="outline"
                size="lg"
                asChild
                className="rounded-full border-white/15 bg-white/5 px-6 text-white hover:bg-white/10 hover:text-white"
              >
                <Link to="/">
                  <Home className="size-4" />
                  Open Dashboard
                </Link>
              </Button>
            </div>
          </section>

          <section className="rounded-[28px] border border-white/10 bg-white/6 p-5 shadow-[0_24px_80px_rgba(2,6,23,0.45)] backdrop-blur-xl">
            <div className="rounded-[22px] border border-white/8 bg-slate-950/75 p-5">
              <div className="mb-4 flex items-center justify-between gap-3">
                <div>
                  <div className="text-sm font-medium text-slate-100">Crash report</div>
                  <div className="mt-1 text-xs uppercase tracking-[0.18em] text-slate-500">
                    Route diagnostics
                  </div>
                </div>
                <div className="rounded-full border border-white/10 bg-white/5 px-3 py-1 text-xs text-slate-300">
                  {new Date().toLocaleTimeString([], {
                    hour: "numeric",
                    minute: "2-digit",
                  })}
                </div>
              </div>

              <div className="space-y-3 text-sm">
                <div className="rounded-2xl border border-white/8 bg-white/3 p-4">
                  <div className="text-[11px] uppercase tracking-[0.18em] text-slate-500">
                    Summary
                  </div>
                  <div className="mt-2 break-words text-slate-100">{description.summary}</div>
                </div>

                {description.detail ? (
                  <details className="rounded-2xl border border-white/8 bg-white/3 p-4">
                    <summary className="cursor-pointer list-none text-[11px] uppercase tracking-[0.18em] text-slate-400">
                      Technical details
                    </summary>
                    <pre className="mt-3 max-h-80 overflow-auto whitespace-pre-wrap break-words rounded-xl bg-black/30 p-3 font-mono text-xs leading-6 text-slate-300">
                      {description.detail}
                    </pre>
                  </details>
                ) : null}
              </div>
            </div>
          </section>
        </div>
      </div>
    </div>
  );
}
