import { useId, useState, type ReactNode } from "react";
import { cn } from "@/lib/utils";
import { Menu, MenuItem } from "./Menu";

/* ------------------------------------------------------------------ buttons */

/**
 * The top bar's right-hand cluster is always
 * `[contextual control] [secondary] [primary]`, and these three are the whole
 * button vocabulary — there is no icon button anywhere in the redesign.
 */
export function PrimaryButton({
  children,
  onClick,
  disabled,
  className,
  title,
}: {
  children: ReactNode;
  onClick?: () => void;
  disabled?: boolean;
  className?: string;
  title?: string;
}) {
  return (
    <button
      type="button"
      title={title}
      onClick={onClick}
      disabled={disabled}
      className={cn(
        "flex h-[34px] cursor-pointer items-center whitespace-nowrap px-4 text-[13px] font-semibold",
        disabled
          ? "cursor-default bg-wv-button text-wv-disabled"
          : "bg-wv-accent text-wv-on-accent hover:bg-wv-accent-hover",
        className,
      )}
    >
      {children}
    </button>
  );
}

export function SecondaryButton({
  children,
  onClick,
  disabled,
  className,
  title,
  size = "default",
}: {
  children: ReactNode;
  onClick?: () => void;
  disabled?: boolean;
  className?: string;
  title?: string;
  /** `compact` is the 32px action a detail header carries four of. */
  size?: "default" | "compact";
}) {
  return (
    <button
      type="button"
      title={title}
      onClick={onClick}
      disabled={disabled}
      className={cn(
        "flex cursor-pointer items-center justify-center whitespace-nowrap border border-wv-control bg-wv-button font-medium",
        size === "compact" ? "h-8 px-3 text-[12.5px]" : "h-[34px] px-[14px] text-[13px]",
        disabled
          ? "cursor-default text-wv-disabled"
          : "text-wv-fg hover:border-wv-control-hover hover:bg-wv-button-hover",
        className,
      )}
    >
      {children}
    </button>
  );
}

export function DangerButton({
  children,
  onClick,
  disabled,
  className,
  size = "default",
}: {
  children: ReactNode;
  onClick?: () => void;
  disabled?: boolean;
  className?: string;
  size?: "default" | "compact";
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      disabled={disabled}
      className={cn(
        "flex cursor-pointer items-center justify-center border border-wv-danger-border bg-wv-button font-medium text-wv-error-text",
        size === "compact" ? "h-8 px-3 text-[12.5px]" : "h-[34px] text-[13px]",
        disabled
          ? "cursor-default opacity-50"
          : "hover:border-wv-danger-border-hover hover:bg-wv-danger-bg-hover",
        className,
      )}
    >
      {children}
    </button>
  );
}

/* ------------------------------------------------------------------- toggle */

/**
 * Square switch with a mono ON/OFF caption.
 *
 * Two sizes: forms use 38×20 with a 14px knob, table cells 34×18 with a 12px
 * knob and no caption — the column header already says what the switch means.
 */
export function Toggle({
  checked,
  onChange,
  label,
  size = "form",
  disabled,
}: {
  checked: boolean;
  onChange: (next: boolean) => void;
  /** Accessible name; the visible caption is always ON/OFF. */
  label: string;
  size?: "form" | "table";
  disabled?: boolean;
}) {
  const table = size === "table";
  return (
    <div className="flex items-center gap-[11px]">
      {table ? null : (
        <span
          aria-hidden="true"
          className={cn(
            "font-wv-mono text-[11px] uppercase tracking-[0.12em]",
            checked ? "text-wv-fg" : "text-wv-muted",
          )}
        >
          {checked ? "On" : "Off"}
        </span>
      )}
      <button
        type="button"
        role="switch"
        aria-checked={checked}
        aria-label={label}
        disabled={disabled}
        onClick={() => onChange(!checked)}
        className={cn(
          "flex flex-none cursor-pointer items-center border p-[2px]",
          table ? "h-[18px] w-[34px]" : "h-5 w-[38px]",
          checked
            ? "justify-end border-wv-accent bg-[rgb(63_179_156_/_0.18)]"
            : "justify-start border-wv-control bg-wv-button",
          disabled && "cursor-default opacity-50",
        )}
      >
        <span
          className={cn(
            table ? "size-3" : "size-[14px]",
            checked ? "bg-wv-accent" : "bg-wv-control-focus",
          )}
        />
      </button>
    </div>
  );
}

/* ----------------------------------------------------------------- checkbox */

/**
 * The 12px selection square.
 *
 * Rows own the click that opens them, so the box stops propagation itself —
 * every caller would otherwise have to remember to, and one that forgot would
 * open a job when you meant to tick it.
 */
export function CheckBox({
  checked,
  onChange,
  label,
  className,
}: {
  checked: boolean;
  onChange: (next: boolean) => void;
  /** Accessible name; the box itself carries no visible text. */
  label: string;
  className?: string;
}) {
  return (
    <button
      type="button"
      role="checkbox"
      aria-checked={checked}
      aria-label={label}
      onClick={(event) => {
        event.stopPropagation();
        onChange(!checked);
      }}
      className={cn("flex cursor-pointer items-center", className)}
    >
      <span
        aria-hidden="true"
        className={cn(
          "size-3 flex-none border",
          checked ? "border-wv-accent bg-wv-accent" : "border-wv-inert",
        )}
      />
    </button>
  );
}

/* ---------------------------------------------------------------- segmented */

export function Segmented<T extends string>({
  value,
  options,
  onChange,
  label,
  size = "form",
  className,
}: {
  value: T;
  options: readonly { value: T; label: string }[];
  onChange: (next: T) => void;
  label: string;
  /** `compact` is the 28px mono variant the pagination bar's row count takes. */
  size?: "form" | "compact";
  className?: string;
}) {
  const compact = size === "compact";
  return (
    <div
      role="radiogroup"
      aria-label={label}
      className={cn(
        "flex border border-wv-control",
        compact ? "bg-wv-button" : "bg-wv-input",
        className,
      )}
    >
      {options.map((option, index) => {
        const active = option.value === value;
        return (
          <button
            key={option.value}
            type="button"
            role="radio"
            aria-checked={active}
            onClick={() => onChange(option.value)}
            className={cn(
              "flex cursor-pointer items-center whitespace-nowrap hover:text-wv-strong",
              compact
                ? "h-[28px] px-[11px] font-wv-mono text-[11.5px]"
                : "h-8 px-[13px] text-[12.5px]",
              index > 0 && "border-l border-wv-control",
              active ? "bg-wv-segment-active font-semibold text-wv-strong" : "text-wv-muted",
            )}
          >
            {option.label}
          </button>
        );
      })}
    </div>
  );
}

/* ------------------------------------------------------------------- select */

/**
 * A real dropdown, styled like the Downloads menus — the prototype cycled
 * options on click, which the handoff explicitly calls out as prototype-only.
 */
export function Select<T extends string>({
  value,
  options,
  onChange,
  label,
  className,
  menuClassName,
}: {
  value: T;
  options: readonly { value: T; label: string }[];
  onChange: (next: T) => void;
  label: string;
  className?: string;
  menuClassName?: string;
}) {
  const [open, setOpen] = useState(false);
  const current = options.find((option) => option.value === value);

  return (
    <div className="relative">
      <button
        type="button"
        data-wv-menu-trigger=""
        aria-haspopup="listbox"
        aria-expanded={open}
        aria-label={label}
        onClick={() => setOpen((previous) => !previous)}
        className={cn(
          "flex h-[34px] min-w-[208px] cursor-pointer items-center justify-between gap-3 border border-wv-control bg-wv-input px-3 text-[13px] text-wv-fg hover:border-wv-control-hover-strong",
          className,
        )}
      >
        <span className="truncate">{current?.label ?? value}</span>
        <span aria-hidden="true" className="flex-none text-[8px] text-wv-muted">
          &#9660;
        </span>
      </button>
      <Menu
        open={open}
        onDismiss={() => setOpen(false)}
        label={label}
        className={cn("top-[36px] right-0 max-h-[280px] min-w-full overflow-y-auto", menuClassName)}
      >
        {options.map((option) => (
          <MenuItem
            key={option.value}
            selected={option.value === value}
            onSelect={() => {
              onChange(option.value);
              setOpen(false);
            }}
          >
            <span className="truncate">{option.label}</span>
          </MenuItem>
        ))}
      </Menu>
    </div>
  );
}

/* --------------------------------------------------------------- text input */

export function TextField({
  value,
  onChange,
  label,
  placeholder,
  className,
  mono = true,
  type = "text",
  onBlur,
  onKeyDown,
}: {
  value: string;
  onChange: (next: string) => void;
  label: string;
  placeholder?: string;
  className?: string;
  mono?: boolean;
  type?: "text" | "password" | "url";
  onBlur?: () => void;
  onKeyDown?: (event: React.KeyboardEvent<HTMLInputElement>) => void;
}) {
  return (
    <input
      type={type}
      aria-label={label}
      placeholder={placeholder}
      value={value}
      onChange={(event) => onChange(event.target.value)}
      onBlur={onBlur}
      onKeyDown={onKeyDown}
      className={cn(
        "h-[34px] border border-wv-control bg-wv-input px-3 text-wv-fg outline-none focus:border-wv-control-focus",
        mono ? "font-wv-mono text-[12px]" : "text-[13px]",
        className,
      )}
    />
  );
}

/* ------------------------------------------------------------------- slider */

export function Slider({
  value,
  min,
  max,
  step,
  onChange,
  label,
  display,
}: {
  value: number;
  min: number;
  max: number;
  step: number;
  onChange: (next: number) => void;
  label: string;
  /** Right-hand readout; `0` conventionally reads "Unlimited". */
  display: string;
}) {
  const id = useId();
  return (
    <div className="flex w-full max-w-[300px] items-center gap-[14px]">
      <input
        id={id}
        type="range"
        aria-label={label}
        min={min}
        max={max}
        step={step}
        value={value}
        onChange={(event) => onChange(Number(event.target.value))}
        className="h-5 min-w-0 flex-1 cursor-pointer"
      />
      <span className="w-[92px] whitespace-nowrap text-right font-wv-mono text-[12.5px] text-wv-fg">
        {display}
      </span>
    </div>
  );
}

/* --------------------------------------------------------------- number box */

/**
 * A whole-number box.
 *
 * Kept as text while focused so a field can be cleared and retyped — a number
 * input that snaps an empty string back to 0 is unusable — and clamped on blur.
 */
export function NumberField({
  value,
  onChange,
  label,
  min,
  max,
  step = 1,
  suffix,
  className,
  disabled,
}: {
  value: number;
  onChange: (next: number) => void;
  label: string;
  min?: number;
  max?: number;
  step?: number;
  suffix?: string;
  className?: string;
  disabled?: boolean;
}) {
  const [draft, setDraft] = useState<string | null>(null);

  const commit = (raw: string) => {
    const parsed = Number(raw);
    if (raw.trim() === "" || !Number.isFinite(parsed)) {
      setDraft(null);
      return;
    }
    let next = parsed;
    if (min !== undefined) next = Math.max(min, next);
    if (max !== undefined) next = Math.min(max, next);
    onChange(next);
    setDraft(null);
  };

  return (
    <div className="flex items-center gap-[10px]">
      <input
        type="number"
        aria-label={label}
        min={min}
        max={max}
        step={step}
        disabled={disabled}
        value={draft ?? String(value)}
        onChange={(event) => setDraft(event.target.value)}
        onBlur={(event) => commit(event.target.value)}
        className={cn(
          "h-[34px] w-[110px] border border-wv-control bg-wv-input px-3 font-wv-mono text-[12px] text-wv-fg outline-none focus:border-wv-control-focus",
          disabled && "text-wv-disabled",
          className,
        )}
      />
      {suffix === undefined ? null : (
        <span className="flex-none font-wv-mono text-[11.5px] text-wv-muted">{suffix}</span>
      )}
    </div>
  );
}

/* ----------------------------------------------------------------- textarea */

/** Multi-line mono input — key material, address lists, script arguments. */
export function TextArea({
  value,
  onChange,
  label,
  placeholder,
  rows = 3,
  className,
}: {
  value: string;
  onChange: (next: string) => void;
  label: string;
  placeholder?: string;
  rows?: number;
  className?: string;
}) {
  return (
    <textarea
      aria-label={label}
      placeholder={placeholder}
      rows={rows}
      value={value}
      onChange={(event) => onChange(event.target.value)}
      className={cn(
        "w-[268px] max-w-full resize-y border border-wv-control bg-wv-input px-3 py-2 font-wv-mono text-[12px] leading-[1.5] text-wv-fg outline-none focus:border-wv-control-focus",
        className,
      )}
    />
  );
}

/* --------------------------------------------------------------------- time */

/** A 24-hour clock field — cap windows, quota resets, schedule edges. */
export function TimeField({
  value,
  onChange,
  label,
  className,
}: {
  value: string;
  onChange: (next: string) => void;
  label: string;
  className?: string;
}) {
  return (
    <input
      type="time"
      aria-label={label}
      value={value}
      onChange={(event) => onChange(event.target.value)}
      className={cn(
        "h-[34px] w-[120px] border border-wv-control bg-wv-input px-3 font-wv-mono text-[12px] text-wv-fg outline-none focus:border-wv-control-focus",
        className,
      )}
    />
  );
}
