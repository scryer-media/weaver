import { useId } from "react";

/**
 * The Weaver brand lockup, split so its two halves can be coloured apart.
 *
 * The mark keeps the brand gradient, which reads against any background. The
 * wordmark is filled with currentColor rather than the artwork's near-black, so
 * a single copy serves a light theme, a dark theme and both interfaces -- a
 * second, recoloured copy would be a second thing to keep in step.
 *
 * It lives here rather than under either interface's components directory
 * because both of them draw it and neither owns it.
 *
 * The geometry is lifted from packaging/brand/weaver-lockup-color.svg, and each
 * viewBox below is that file's ink bounds for the shapes it holds, so these
 * render with no slack around them and a caller can size one by giving it a
 * height alone.
 */

const LOCKUP_VIEW_BOX = "0 118.5 2500 363";
const MARK_VIEW_BOX = "0 118.5 553 363";
const WORDMARK_VIEW_BOX = "694.5 172 1805.5 247.5";

const MARK_PATH =
  "M264.67,390.8l-55.59,73.77c-5.87,7.79-13.08,16.09-23.48,16.2l-30.57.34c-14.4.16-22.97-9.62-28.66-21.4l-46.63-96.62-31.12-63.09-9.92-19.78L2.25,206.8c-1.45-2.92-4.01-9.35-.41-11.19l57.43-.35c5.07-.03,6.28,3.35,8.3,7l84,151.86c2.83,5.12,4.46,9.08,7.11,14.04l13.6,25.57,19.35-22.08,32.02-37.8c14.66,19.46,27.83,37.25,41.03,56.94ZM494.14,195.77c-4.79.01-7.09,2.89-9.22,6.68l-61.79,110.02-41.06,73.93c-.04.54-.21,1.55-.83,1.88-.15.24-.12.66-.12.94l-3.74,4.6-15.96-18.16-63.88-81.8-21.71-27.79-41.83,52.15,62.02,85.76,27.3,37.1,17.84,25.42c6.83,9.71,16.75,15.56,28.99,14.7,10.54-.75,21.25,1.31,30.84-1.26,9.82-2.62,16.93-10.66,21.35-19.75l6.59-13.59,70.95-141.3,51.29-100.72c1.43-2.81,1.01-6.07,1.37-8.98l-58.42.19ZM140.72,268.94l37.71-49.29-56.72-86.47c-5.09-7.76-13.19-14.42-22.53-14.42l-42.1-.03c-7.46,0-15.58,4.66-19.8,10.75L3.71,177.95l61.23.67c5.29.06,9.45,3.92,11.76,8.18l53.96,70.11,10.06,12.04ZM518.89,133c-5.16-7.58-12.38-12.65-21.52-14.16l-42.9.37c-7.04.06-15.03,4.33-19.05,10.12l-29.84,42.97-32.12,47.69,32.36,41.87c2.02,2.6,3.23,5.57,6.07,7.5l65.3-84.45c3.33-4.3,7.25-6.91,12.96-6.95l59.12-.33-30.38-44.62ZM396.66,279.36l-32.88-43.16-43.8-56.48c-5.69-7.34-13.05-15.14-22.95-15.2l-40.77-.22c-8.09-.05-15.28,4.79-20.55,10.24l-44.01,56.3-51.06,66.88,35.08,65.33,13.47-17.34,25.37-31.44,22.25-28.51,30.86-39.58c2.75-3.51,3.75-8.13,8.63-10.13l66.18,85.59,32.08,40.73,21.52-38.19,14.52-25.9-13.95-18.9ZM381.25,388.28c.62-.33.79-1.34.83-1.88l-.83,1.88Z";

const WORDMARK_PATHS = [
  "M2500,418.58l-75.02.85-59.74-74.41-56.26.02-.12,74.04-68.19.21v-246.99s176.89,0,176.89,0c15.66,0,29.67,1.61,43.09,9.49,18.42,10.43,29.52,29.51,30.04,50.88l-.25,50.56c-.15,29.76-20.82,52.81-51.08,58.44l60.64,76.92ZM2424.3,272.76l-.38-28.38c-.09-7.16-9.38-11.83-15.94-11.83h-99.1s.07,55.11.07,55.11l99.03-.13c7.59-.72,16.43-5.74,16.31-14.77Z",
  "M949.38,410.24l-50.09-122.5-55.74,121.27c-2.82,6.14-5.42,10.24-13.08,10.19l-38.09-.27c-2.82-.02-8.2-4.55-9.22-7.35l-10.14-27.58,35.94-67.49,59.2-108.02c1.6-2.92,9.39-5.99,12.92-5.99h37.18c9.15,0,12.85,5.4,16.4,12.29l70.56,136.96,15.6,32.29-10.74,25.93c-2.3,5.55-7,9.39-13.41,9.36l-38.69-.2c-3.16-.02-7.55-4.92-8.6-8.88Z",
  "M810.47,280.83l-45.04,83.22-35.95-95.85-34.52-94.48,71.93-.05c6.04-.82,11.11,2.99,11.82,9.09l31.76,98.08Z",
  "M1028.92,363.92 L988.77,284.03 L1034.62,172.34 L1112.51,172.56 L1028.92,363.92 Z",
  "M1482.12,379.44l31.48-60.01,62.38-.18-41.8-83.32-96.5,183.38-71.8-.41,120.93-225.92c3.77-12.09,14.23-20.69,26.89-20.63l44.54.21c11.2.05,18.88,8.71,23.49,17.89l50.86,101.15,64.57,127.65-72.77.06-20.03-39.66-122.25-.2Z",
  "M1804.69,419.43l-34.99-.07c-4.23,0-11.97-4.61-13.9-8.26l-12.16-23.03,39.77-66.27,90.07-149.2,76.06.18-57.8,103.93-74.83,136.15c-1.56,2.83-8.7,6.58-12.21,6.58Z",
  "M1779.57,294.39 L1734.49,369.29 L1696.5,299.13 L1628.74,172.43 L1704.72,172.34 L1779.57,294.39 Z",
  "M1145.53,172.58c-13.81.02-28.06,12.33-28.06,25.89v66.83h-.02l-.02,58.11h.03v34.73h-.03v34.31c-.02,15.7,14.54,25.6,28.97,26.84l198.27.02v-61.18l-137.55-.02v.03h-10.17v-.03h-3.49c-5.41,0-9.8-4.37-9.8-9.78v-15.11c0-5.41,4.39-9.8,9.8-9.8h.95l3.59-.08,133-.07v-57.92l-133.2-.05h-4.34c-5.41,0-9.8-4.39-9.8-9.8v-13.21c0-5.41,4.39-9.8,9.8-9.8h151.28l.21-60.17-199.43.24Z",
  "M1993.89,172.58c-13.81.02-28.06,12.33-28.06,25.89v66.83h-.02l-.02,58.11h.03v34.73h-.03v34.31c-.02,15.7,14.54,25.6,28.97,26.84l198.27.02v-61.18l-137.55-.02v.03h-10.17v-.03h-3.49c-5.41,0-9.8-4.37-9.8-9.78v-15.11c0-5.41,4.39-9.8,9.8-9.8h.95l3.59-.08,133-.07v-57.92l-133.2-.05h-4.34c-5.41,0-9.8-4.39-9.8-9.8v-13.21c0-5.41,4.39-9.8,9.8-9.8h151.28l.21-60.17-199.43.24Z",
] as const;

/**
 * Artwork drawn on its own still names the product, so it is labelled by
 * default. `decorative` is for the places that already carry the name in text
 * beside it, where a second announcement is only noise.
 */
interface BrandProps {
  className?: string;
  decorative?: boolean;
}

function naming(decorative: boolean) {
  return decorative
    ? ({ "aria-hidden": true } as const)
    : ({ role: "img", "aria-label": "Weaver" } as const);
}

/**
 * Declared per instance: an SVG gradient is addressed by a document-wide id, so
 * two marks on one page would otherwise fight over one definition.
 */
function MarkGradient({ id }: { id: string }) {
  return (
    <linearGradient
      id={id}
      x1="0"
      y1="300"
      x2="552.56"
      y2="300"
      gradientUnits="userSpaceOnUse"
    >
      <stop offset="0" stopColor="#4566b0" />
      <stop offset="1" stopColor="#ee3b85" />
    </linearGradient>
  );
}

export function BrandMark({ className, decorative = false }: BrandProps) {
  const gradient = useId();
  return (
    <svg viewBox={MARK_VIEW_BOX} className={className} {...naming(decorative)}>
      <defs>
        <MarkGradient id={gradient} />
      </defs>
      <path d={MARK_PATH} fill={"url(#" + gradient + ")"} />
    </svg>
  );
}

export function BrandWordmark({ className, decorative = false }: BrandProps) {
  return (
    <svg viewBox={WORDMARK_VIEW_BOX} className={className} {...naming(decorative)}>
      {WORDMARK_PATHS.map((path) => (
        <path key={path} d={path} fill="currentColor" />
      ))}
    </svg>
  );
}

export function BrandLockup({ className, decorative = false }: BrandProps) {
  const gradient = useId();
  return (
    <svg viewBox={LOCKUP_VIEW_BOX} className={className} {...naming(decorative)}>
      <defs>
        <MarkGradient id={gradient} />
      </defs>
      <path d={MARK_PATH} fill={"url(#" + gradient + ")"} />
      {WORDMARK_PATHS.map((path) => (
        <path key={path} d={path} fill="currentColor" />
      ))}
    </svg>
  );
}
