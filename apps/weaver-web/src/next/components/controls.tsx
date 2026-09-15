import { useEffect, useId, useRef, useState, type ReactNode, type Ref } from "react";
import { useTranslate } from "@/lib/context/translate-context";
import { ignoredByPasswordManagers, PASSWORD_MANAGER_IGNORE } from "@/lib/password-manager";
import { cn } from "@/lib/utils";
import { Icon, type IconName } from "./icons";
import { Menu, MenuItem } from "./Menu";

/* ------------------------------------------------------------------ buttons */

/**
 * The top bar's right-hand cluster is always
 * `[contextual control] [secondary] [primary]`, and these three are the whole
 * button vocabulary. A button may lead with an icon from `ICONS`, named by the
 * action it performs; the label always stays.
 */
export function PrimaryButton({
  children,
  icon,
  onClick,
  disabled,
  className,
  title,
  type = "button",
}: {
  children: ReactNode;
  /** A leading icon, named by the action. */
  icon?: IconName;
  onClick?: () => void;
  disabled?: boolean;
  className?: string;
  title?: string;
  /** `submit` only inside a real form, where Enter should press it. */
  type?: "button" | "submit";
}) {
  return (
    <button
      type={type}
      title={title}
      onClick={onClick}
      disabled={disabled}
      className={cn(
        "flex h-[34px] cursor-pointer items-center whitespace-nowrap px-4 text-[13px] font-medium",
        disabled
          ? "cursor-default bg-wv-button text-wv-disabled"
          : "bg-wv-accent text-wv-on-accent hover:bg-wv-accent-hover",
        className,
      )}
    >
      {icon === undefined ? null : (
        <Icon name={icon} size={14} className="-ml-[2px] mr-[7px] flex-none" />
      )}
      {children}
    </button>
  );
}

export function SecondaryButton({
  children,
  icon,
  onClick,
  disabled,
  className,
  title,
  size = "default",
}: {
  children: ReactNode;
  /** A leading icon, named by the action. */
  icon?: IconName;
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
      {icon === undefined ? null : (
        <Icon name={icon} size={size === "compact" ? 13 : 14} className="-ml-[2px] mr-[7px] flex-none" />
      )}
      {children}
    </button>
  );
}

export function DangerButton({
  children,
  icon,
  onClick,
  disabled,
  className,
  size = "default",
  solid = false,
}: {
  children: ReactNode;
  /** A leading icon, named by the action. */
  icon?: IconName;
  onClick?: () => void;
  disabled?: boolean;
  className?: string;
  size?: "default" | "compact";
  /** Filled red, for the most destructive of two destructive choices side by side. */
  solid?: boolean;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      disabled={disabled}
      className={cn(
        "flex cursor-pointer items-center justify-center border font-medium whitespace-nowrap",
        solid
          ? "!border-wv-error bg-wv-error text-wv-on-accent"
          : "!border-wv-danger-border bg-wv-button text-wv-error-text",
        size === "compact" ? "h-8 px-3 text-[12.5px]" : "h-[34px] text-[13px]",
        disabled
          ? "cursor-default opacity-50"
          : solid
            ? "hover:!border-wv-error-text hover:bg-wv-error-text"
            : "hover:!border-wv-danger-border-hover hover:bg-wv-danger-bg-hover",
        className,
      )}
    >
      {icon === undefined ? null : (
        <Icon name={icon} size={size === "compact" ? 13 : 14} className="-ml-[2px] mr-[7px] flex-none" />
      )}
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
  const t = useTranslate();
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
          {checked ? t("next.common.on") : t("next.common.off")}
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
 * The 14px selection square. Its empty border is the faint text colour, about
 * 4:1 against the list ground, so the box does not fade into the rows.
 *
 * Rows own the click that opens them, so the box stops propagation itself —
 * every caller would otherwise have to remember to, and one that forgot would
 * open a job when you meant to tick it.
 */
export function CheckBox({
  checked,
  onChange,
  label,
  disabled = false,
  className,
}: {
  checked: boolean;
  onChange: (next: boolean) => void;
  /** Accessible name; the box itself carries no visible text. */
  label: string;
  disabled?: boolean;
  className?: string;
}) {
  return (
    <button
      type="button"
      role="checkbox"
      aria-checked={checked}
      aria-label={label}
      disabled={disabled}
      onClick={(event) => {
        event.stopPropagation();
        if (!disabled) {
          onChange(!checked);
        }
      }}
      className={cn("group flex items-center", disabled ? "cursor-default opacity-40" : "cursor-pointer", className)}
    >
      <span
        aria-hidden="true"
        className={cn(
          "size-3.5 flex-none border",
          checked
            ? "!border-wv-accent bg-wv-accent"
            : cn("!border-wv-faint", !disabled && "group-hover:!border-wv-muted"),
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
              active ? "bg-wv-segment-active font-medium text-wv-strong" : "text-wv-muted",
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
 *
 * The menu is pinned to the viewport under its trigger rather than placed
 * inside the trigger's box: a Select near the foot of a scrolling panel — a
 * dialog's body, a settings list — would otherwise have its options cut off at
 * the panel's edge.
 */
export function Select<T extends string>({
  value,
  options,
  onChange,
  label,
  icon,
  className,
  menuClassName,
}: {
  value: T;
  options: readonly { value: T; label: string }[];
  onChange: (next: T) => void;
  label: string;
  /** Shown before the current choice, for a control without a visible label. */
  icon?: IconName;
  className?: string;
  menuClassName?: string;
}) {
  const [placement, setPlacement] = useState<MenuPlacement | null>(null);
  const triggerRef = useRef<HTMLButtonElement>(null);
  const open = placement !== null;
  const current = options.find((option) => option.value === value);

  useEffect(() => {
    if (!open) {
      return;
    }
    // Pinned to the viewport, the menu would drift off its trigger as anything
    // under it scrolls, so it follows — except when the scroll is its own list.
    const follow = (event: Event) => {
      if ((event.target as Element | null)?.closest?.('[role="menu"]')) {
        return;
      }
      if (triggerRef.current) {
        setPlacement(placeMenu(triggerRef.current));
      }
    };
    window.addEventListener("scroll", follow, true);
    window.addEventListener("resize", follow);
    return () => {
      window.removeEventListener("scroll", follow, true);
      window.removeEventListener("resize", follow);
    };
  }, [open]);

  return (
    <div className="relative">
      <button
        ref={triggerRef}
        type="button"
        data-wv-menu-trigger=""
        aria-haspopup="listbox"
        aria-expanded={open}
        aria-label={label}
        onClick={(event) => setPlacement(open ? null : placeMenu(event.currentTarget))}
        className={cn(
          "flex h-[34px] min-w-[208px] cursor-pointer items-center justify-between gap-3 border border-wv-control bg-wv-input px-3 text-[13px] text-wv-fg hover:border-wv-control-hover-strong",
          className,
        )}
      >
        {icon ? <Icon name={icon} size={13} className="-mr-1 flex-none text-wv-muted" /> : null}
        <span className="min-w-0 flex-1 truncate text-left">{current?.label ?? value}</span>
        <Icon name="dropdown" size={13} className="flex-none text-wv-muted" />
      </button>
      <Menu
        open={open}
        onDismiss={() => setPlacement(null)}
        label={label}
        className={cn("fixed overflow-y-auto", menuClassName)}
        style={placement ?? undefined}
      >
        {options.map((option) => (
          <MenuItem
            key={option.value}
            selected={option.value === value}
            onSelect={() => {
              onChange(option.value);
              setPlacement(null);
            }}
          >
            <span className="truncate">{option.label}</span>
          </MenuItem>
        ))}
      </Menu>
    </div>
  );
}

interface MenuPlacement {
  top?: number;
  bottom?: number;
  right: number;
  minWidth: number;
  maxHeight: number;
}

const MENU_GAP = 2;
const MENU_MAX_HEIGHT = 280;
/** Room kept between an open menu and the edge of the window. */
const MENU_MARGIN = 8;

/**
 * Where a Select's menu goes: right-aligned under its trigger and at least as
 * wide, or above it when the window runs out below and there is more room up.
 */
function placeMenu(trigger: HTMLElement): MenuPlacement {
  const rect = trigger.getBoundingClientRect();
  const viewport = document.documentElement;
  const right = viewport.clientWidth - rect.right;
  const below = viewport.clientHeight - rect.bottom - MENU_GAP - MENU_MARGIN;
  const above = rect.top - MENU_GAP - MENU_MARGIN;
  if (below < 160 && above > below) {
    return {
      bottom: viewport.clientHeight - rect.top + MENU_GAP,
      right,
      minWidth: rect.width,
      maxHeight: Math.min(MENU_MAX_HEIGHT, above),
    };
  }
  return {
    top: rect.bottom + MENU_GAP,
    right,
    minWidth: rect.width,
    maxHeight: Math.min(MENU_MAX_HEIGHT, below),
  };
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
  onFocus,
  onKeyDown,
  ref,
  id,
  autoComplete,
  autoFocus,
  secret,
}: {
  value: string;
  onChange: (next: string) => void;
  label: string;
  placeholder?: string;
  className?: string;
  mono?: boolean;
  type?: "text" | "password" | "url";
  /** Keep password managers out. Defaults to on for a password that is not the Weaver login. */
  secret?: boolean;
  onBlur?: () => void;
  onFocus?: () => void;
  onKeyDown?: (event: React.KeyboardEvent<HTMLInputElement>) => void;
  ref?: Ref<HTMLInputElement>;
  id?: string;
  autoComplete?: string;
  autoFocus?: boolean;
}) {
  return (
    <input
      ref={ref}
      id={id}
      autoComplete={autoComplete}
      {...(ignoredByPasswordManagers({ secret, type, autoComplete }) ? PASSWORD_MANAGER_IGNORE : null)}
      autoFocus={autoFocus}
      type={type}
      aria-label={label}
      placeholder={placeholder}
      value={value}
      onChange={(event) => onChange(event.target.value)}
      onBlur={onBlur}
      onFocus={onFocus}
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
 *
 * A unit ("days", "seconds") is set inside the box at its right edge. The
 * border belongs to the label wrapping both, so a click on the unit still
 * lands in the field, and `className` sizes that box rather than the bare input.
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
    let next = Math.round(parsed);
    if (min !== undefined) next = Math.max(min, next);
    if (max !== undefined) next = Math.min(max, next);
    onChange(next);
    setDraft(null);
  };

  return (
    <label
      className={cn(
        "flex h-[34px] cursor-text items-center border border-wv-control bg-wv-input focus-within:border-wv-control-focus",
        suffix === undefined ? "w-[110px]" : "w-fit",
        disabled && "cursor-default",
        className,
      )}
    >
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
          "h-full min-w-0 bg-transparent pl-3 font-wv-mono text-[12px] text-wv-fg outline-none",
          suffix === undefined ? "flex-1 pr-3" : "w-[86px] flex-none",
          disabled && "text-wv-disabled",
        )}
      />
      {suffix === undefined ? null : (
        <span className="flex-none pr-3 pl-2 font-wv-mono text-[11.5px] text-wv-muted">
          {suffix}
        </span>
      )}
    </label>
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
  secret = false,
}: {
  value: string;
  onChange: (next: string) => void;
  label: string;
  placeholder?: string;
  rows?: number;
  className?: string;
  /** Keep password managers out, for key material. */
  secret?: boolean;
}) {
  return (
    <textarea
      {...(secret ? PASSWORD_MANAGER_IGNORE : null)}
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

/**
 * A clock field — cap windows, quota resets, schedule edges. The browser draws it
 * in the reader's own 12- or 24-hour style; the value is always `HH:MM`.
 */
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
