import { useTranslate } from "@/lib/context/translate-context";
import { cn } from "@/lib/utils";
import { Eyebrow } from "./chrome";
import { Segmented } from "./controls";
import { Icon, type IconName } from "./icons";
import { formatCount } from "../data/format";

/**
 * The 44px bar under a paginated list: how many rows a page holds, which slice
 * of the set is on screen, and the way to the next one.
 *
 * It sits below the scrolling region rather than inside it, so the controls
 * never scroll away from the rows they page through. Page numbers are windowed
 * — a history of ten thousand entries would otherwise draw four hundred
 * buttons — and the arrows stay in place when they can no longer move, because
 * a control that disappears at the end of a list moves everything else with it.
 */

const PAGE_WINDOW = 5;

export function Pagination({
  pageIndex,
  pageCount,
  onPage,
  pageSize,
  pageSizes,
  onPageSize,
  total,
}: {
  pageIndex: number;
  pageCount: number;
  onPage: (next: number) => void;
  pageSize: number;
  pageSizes: readonly number[];
  onPageSize: (next: number) => void;
  total: number;
}) {
  const t = useTranslate();
  const from = total === 0 ? 0 : pageIndex * pageSize + 1;
  const to = Math.min(total, (pageIndex + 1) * pageSize);
  const windowStart = Math.max(0, Math.min(pageIndex - 2, pageCount - PAGE_WINDOW));
  const pages = Array.from(
    { length: Math.min(PAGE_WINDOW, pageCount) },
    (_, index) => windowStart + index,
  );

  return (
    <div className="flex min-h-[44px] flex-none flex-wrap items-center gap-x-[14px] gap-y-1 border-t border-wv-line-strong bg-wv-list px-4 py-1 sm:px-[22px] sm:py-0">
      <Eyebrow tone="rail" className="tracking-[0.12em] whitespace-nowrap">
        {t("next.pagination.rows")}
      </Eyebrow>
      <Segmented
        size="compact"
        label={t("table.rowsPerPage")}
        value={String(pageSize)}
        options={pageSizes.map((size) => ({ value: String(size), label: String(size) }))}
        onChange={(next) => onPageSize(Number(next))}
      />
      <span className="font-wv-mono text-[11.5px] whitespace-nowrap text-wv-muted">
        {total === 0
          ? t("next.pagination.empty")
          : t("next.pagination.range", {
              from: formatCount(from),
              to: formatCount(to),
              total: formatCount(total),
            })}
      </span>

      <div className="ml-auto flex items-center gap-2">
        <Step label={t("action.previous")} before="previous" disabled={pageIndex === 0} onClick={() => onPage(pageIndex - 1)} />
        <div className="flex items-center gap-1">
          {pages.map((page) => (
            <button
              key={page}
              type="button"
              aria-current={page === pageIndex ? "page" : undefined}
              onClick={() => onPage(page)}
              className={cn(
                "flex h-[28px] w-[26px] cursor-pointer items-center justify-center border font-wv-mono text-[11.5px]",
                page === pageIndex
                  ? "border-wv-accent bg-wv-page-current text-wv-fg"
                  : "border-wv-control bg-wv-button text-wv-muted hover:border-wv-control-hover",
              )}
            >
              {page + 1}
            </button>
          ))}
        </div>
        <Step
          label={t("action.next")}
          after="next"
          disabled={pageIndex + 1 >= pageCount}
          onClick={() => onPage(pageIndex + 1)}
        />
      </div>
    </div>
  );
}

function Step({
  label,
  before,
  after,
  disabled,
  onClick,
}: {
  label: string;
  before?: IconName;
  after?: IconName;
  disabled: boolean;
  onClick: () => void;
}) {
  return (
    <button
      type="button"
      disabled={disabled}
      onClick={onClick}
      className={cn(
        "flex h-[28px] items-center border bg-wv-button px-3 text-[12.5px] whitespace-nowrap",
        disabled
          ? "cursor-default border-wv-control-off text-wv-dim"
          : "cursor-pointer border-wv-control text-wv-fg hover:border-wv-control-hover",
      )}
    >
      {before === undefined ? null : <Icon name={before} size={13} className="-ml-[3px] mr-[4px] flex-none" />}
      {label}
      {after === undefined ? null : <Icon name={after} size={13} className="-mr-[3px] ml-[4px] flex-none" />}
    </button>
  );
}
