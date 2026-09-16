import { LoadingMark } from "@/lib/loading-mark";

/**
 * The turning mark over a surface that is waiting on something slow, such as
 * a provider connection test. It covers its nearest positioned ancestor, so
 * nothing under it can be pressed while the answer is out, and fades in so a
 * quick answer barely registers.
 */
export function WorkingOverlay({ label }: { label: string }) {
  return (
    <div
      role="status"
      aria-live="polite"
      className="absolute inset-0 z-10 flex flex-col items-center justify-center gap-5 bg-wv-chrome/85"
      style={{ animation: "weaver-loading-reveal 160ms ease-out both" }}
    >
      <LoadingMark className="h-20" />
      <span className="font-wv-mono text-[12px] text-wv-secondary">{label}</span>
    </div>
  );
}
