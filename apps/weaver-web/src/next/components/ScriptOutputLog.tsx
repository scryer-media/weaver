import { useMemo, useRef } from "react";
import { useVirtualizer } from "@tanstack/react-virtual";
import { parseLogLine } from "../data/service-log-buffer";
import { LogRow } from "./LogRow";

/**
 * A script's output in the log viewer: numbered lines, wrapped, with the
 * `key=value` tint wherever a line is a log record the parser recognises.
 *
 * The daemon keeps stdout and stderr interleaved in one text, so a line says
 * nothing about which stream wrote it. The box has a height of its own and
 * scrolls inside it, so one long run does not push the list off the screen;
 * only the lines in view are drawn, as the service log does.
 */

/** A line that does not wrap. Wrapped lines are measured once they render. */
const ROW_ESTIMATE_PX = 29;

/** The lines of an output, without the empty one a trailing newline leaves. */
export function outputLines(output: string): string[] {
  const lines = output.replace(/\r\n?/g, "\n").split("\n");
  if (lines.length > 1 && lines[lines.length - 1] === "") {
    lines.pop();
  }
  return lines;
}

export function ScriptOutputLog({ output, label }: { output: string; label: string }) {
  const lines = useMemo(
    () => outputLines(output).map((raw, index) => parseLogLine(index, raw)),
    [output],
  );
  const scrollerRef = useRef<HTMLDivElement>(null);
  const virtualizer = useVirtualizer({
    count: lines.length,
    getScrollElement: () => scrollerRef.current,
    estimateSize: () => ROW_ESTIMATE_PX,
    overscan: 20,
    useFlushSync: false,
  });
  return (
    <div
      ref={scrollerRef}
      role="region"
      aria-label={label}
      tabIndex={0}
      className="max-h-[420px] min-h-0 overflow-y-auto border border-wv-hairline bg-wv-list"
    >
      <div className="relative w-full" style={{ height: virtualizer.getTotalSize() }}>
        {virtualizer.getVirtualItems().map((item) => {
          const line = lines[item.index]!;
          return (
            <div
              key={item.key}
              ref={virtualizer.measureElement}
              data-index={item.index}
              className="absolute top-0 left-0 w-full"
              style={{ transform: `translateY(${item.start}px)` }}
            >
              {/* A line the parser did not take for a log record has no stamp or level to show. */}
              <LogRow line={line} bare={line.time === "" && line.target === ""} />
            </div>
          );
        })}
      </div>
    </div>
  );
}
