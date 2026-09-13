import loadingMark from "@/assets/weaver-loading.webp";
import loadingMarkStill from "@/assets/weaver-loading-still.webp";
import { cn } from "@/lib/utils";

/**
 * Weaver's loading indicator: the W mark turning on its axis.
 *
 * It stands wherever a spinner would, in both interfaces, so it lives beside the
 * brand lockup rather than under either one's components. It is an animated
 * WebP rather than a GIF because a GIF's one-bit transparency leaves the mark's
 * edges stepped once it is scaled down this far.
 *
 * Size it by height alone (`h-4`, `h-8`); the width follows the artwork. The
 * mark is decoration: every caller already says "loading" in text or through a
 * `role="status"` region, so it is hidden from assistive technology. Under
 * reduced motion it holds still on its first frame.
 *
 * `reveal` holds it back briefly before fading in, for a placeholder that most
 * loads replace before anyone would notice it.
 */
export function LoadingMark({ className, reveal = false }: { className?: string; reveal?: boolean }) {
  return (
    <picture className="contents">
      <source media="(prefers-reduced-motion: reduce)" srcSet={loadingMarkStill} />
      <img
        src={loadingMark}
        width={120}
        height={82}
        alt=""
        aria-hidden="true"
        draggable={false}
        className={cn("h-4 w-auto flex-none select-none", reveal && "animate-loading-reveal", className)}
      />
    </picture>
  );
}
