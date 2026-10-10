import { memo } from "react";
import { LOG_LEVEL_COLORS } from "../data/palette";
import type { LogKeyValue, LogLine } from "../data/service-log-buffer";

/** Split the message so its `key=value` tail can be tinted separately. */
function messageFragments(line: LogLine) {
  const fragments: { text: string; kv: LogKeyValue | null }[] = [];
  let cursor = 0;
  for (const pair of line.kvPairs) {
    if (pair.start > cursor) {
      fragments.push({ text: line.message.slice(cursor, pair.start), kv: null });
    }
    fragments.push({ text: line.message.slice(pair.start, pair.end), kv: pair });
    cursor = pair.end;
  }
  if (cursor < line.message.length) {
    fragments.push({ text: line.message.slice(cursor), kv: null });
  }
  return fragments;
}

/**
 * One line of a log. A line is never edited, so a row renders once for as
 * long as its line stays in view.
 *
 * `bare` drops the stamp, level and target columns, for a line that is plain
 * text rather than a log record: a guessed level would only mislabel it.
 */
export const LogRow = memo(function LogRow({ line, bare = false }: { line: LogLine; bare?: boolean }) {
  return (
    <div className="flex flex-wrap gap-x-[14px] gap-y-0.5 border-b border-wv-log-line px-4 sm:px-6 py-1.5 font-wv-mono text-[11.5px] leading-[1.55] sm:flex-nowrap">
      <span className="w-[34px] flex-none text-right text-wv-dim">{line.id + 1}</span>
      {bare ? null : (
        <>
          <span className="flex-none text-wv-faint">{line.time}</span>
          <span
            className="w-[44px] flex-none font-semibold uppercase"
            style={{ color: LOG_LEVEL_COLORS[line.level] }}
          >
            {line.level}
          </span>
          <span className="min-w-0 flex-1 truncate text-wv-slate sm:flex-none">{line.target}</span>
        </>
      )}
      <span
        className={
          bare
            ? "min-w-0 flex-1 break-words whitespace-pre-wrap text-wv-secondary"
            : "w-full min-w-0 break-words text-wv-secondary sm:w-auto sm:flex-1"
        }
      >
        {messageFragments(line).map((fragment, index) => (
          <span key={index} className={fragment.kv ? "text-wv-info" : undefined}>
            {fragment.text}
          </span>
        ))}
      </span>
    </div>
  );
});
