// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { memo, type KeyboardEvent } from "react";
import { LOG_LEVEL_COLORS } from "../data/palette";
import { recordJson, type LogKeyValue, type LogLine } from "../data/service-log-buffer";

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
 *
 * With `onToggle`, clicking the line selects it, and a selected line shows
 * its record's JSON beneath it.
 */
export const LogRow = memo(function LogRow({
  line,
  bare = false,
  selected = false,
  onToggle,
}: {
  line: LogLine;
  bare?: boolean;
  selected?: boolean;
  onToggle?: (line: LogLine) => void;
}) {
  const row = (
    <div
      className="flex flex-wrap gap-x-[14px] gap-y-0.5 border-b border-wv-log-line px-4 sm:px-6 py-1.5 font-wv-mono text-[11.5px] leading-[1.55] sm:flex-nowrap"
      {...(onToggle
        ? {
            role: "button",
            tabIndex: 0,
            "aria-expanded": selected,
            onClick: () => {
              // Selecting text to copy it is not selecting the line.
              if (window.getSelection()?.toString()) return;
              onToggle(line);
            },
            onKeyDown: (event: KeyboardEvent) => {
              if (event.key === "Enter" || event.key === " ") {
                event.preventDefault();
                onToggle(line);
              }
            },
          }
        : {})}
    >
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
  if (!selected) return row;
  return (
    <div className="bg-wv-selected">
      {row}
      <pre className="overflow-x-auto border-b border-wv-log-line px-4 py-2 font-wv-mono text-[11.5px] leading-[1.55] text-wv-secondary select-text sm:px-6">
        {recordJson(line)}
      </pre>
    </div>
  );
});
