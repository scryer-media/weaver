import type { ReactNode } from "react";
import type { Translate } from "@/lib/context/translate-context";
import type { Egress, RouteProblem } from "@/lib/networking";
import { cn } from "@/lib/utils";
import { Square } from "../../components/chrome";
import { CheckBox } from "../../components/controls";
import { Icon, type IconName } from "../../components/icons";
import { WV } from "../../data/palette";

/**
 * How the networking screens say what state something is in.
 *
 * Egresses, route legs, ladder rungs and pool members each report their own
 * vocabulary of states, but a reader only needs five answers: carrying
 * traffic, finding out, holding back, broken, or doing nothing. Every state
 * maps to one of those tones, and every tone is a square plus a word, so no
 * state is told by colour or by a glyph alone.
 */
export type NetworkTone = "ok" | "busy" | "warn" | "bad" | "idle";

export const TONE_COLOR: Record<NetworkTone, string> = {
  ok: WV.accent,
  busy: WV.info,
  warn: WV.warn,
  bad: WV.error,
  idle: WV.idle,
};

export function stateTone(state: string): NetworkTone {
  switch (state.toUpperCase()) {
    case "UP":
    case "ACTIVE":
    case "PINNED":
    case "READY":
      return "ok";
    case "PROBING":
    case "CHALLENGER":
      return "busy";
    case "COOLDOWN":
    case "SUSPECT":
    case "HOLD":
      return "warn";
    case "DOWN":
    case "FAILING":
    case "BLOCKED":
      return "bad";
    default:
      return "idle";
  }
}

export function stateColor(state: string): string {
  return TONE_COLOR[stateTone(state)];
}

/** The word for a state; one the daemon adds later still reads as itself. */
export function stateLabel(t: Translate, state: string): string {
  switch (state.toUpperCase()) {
    case "UP":
      return t("next.networking.state.up");
    case "DOWN":
      return t("next.networking.state.down");
    case "PROBING":
      return t("next.networking.state.probing");
    case "BLOCKED":
      return t("next.networking.state.blocked");
    case "IDLE":
      return t("next.networking.state.idle");
    case "UNKNOWN":
      return t("next.networking.state.unknown");
    case "ACTIVE":
      return t("next.networking.state.active");
    case "COOLDOWN":
      return t("next.networking.state.cooldown");
    case "FAILING":
      return t("next.networking.state.failing");
    case "STANDBY":
      return t("next.networking.state.standby");
    case "PINNED":
      return t("next.networking.state.pinned");
    case "SUSPECT":
      return t("next.networking.state.suspect");
    case "CHALLENGER":
      return t("next.networking.state.challenger");
    case "READY":
      return t("next.networking.state.ready");
    case "UNMEASURED":
      return t("next.networking.state.unmeasured");
    default: {
      const words = state.toLowerCase().replace(/_/g, " ");
      return words.charAt(0).toUpperCase() + words.slice(1);
    }
  }
}

export function bindingLabel(t: Translate, kind: Egress["bindingKind"]): string {
  switch (kind) {
    case "SYSTEM":
      return t("next.networking.binding.system");
    case "INTERFACE":
      return t("next.networking.binding.interface");
    case "SOURCE_ADDRESS":
      return t("next.networking.binding.sourceAddress");
  }
}

export function routeProblemText(t: Translate, problem: RouteProblem): string {
  switch (problem) {
    case "legCount":
      return t("next.networking.problem.legCount");
    case "weights":
      return t("next.networking.problem.weights");
    case "rungCount":
      return t("next.networking.problem.rungCount");
    case "rungTarget":
      return t("next.networking.problem.rungTarget");
    case "chain":
      return t("next.networking.problem.chain");
  }
}

export const MIB = 1024 * 1024;

export function formatRate(bytesPerSecond = 0): string {
  return `${(bytesPerSecond / MIB).toFixed(2)} MiB/s`;
}

export function formatLimit(t: Translate, bytesPerSecond: number): string {
  return bytesPerSecond > 0
    ? `${(bytesPerSecond / MIB).toFixed(1)} MiB/s`
    : t("next.networking.unlimited");
}

/** A state as the system draws one: the square, then the word. */
export function StatusMark({
  state,
  label,
  tone,
  detail,
  wrap = false,
  className,
}: {
  state: string;
  /** The word, when the caller already has it. */
  label: string;
  tone?: NetworkTone;
  /** Quieter text after the word: a count, a reason. */
  detail?: ReactNode;
  /** Let a long detail run onto more lines instead of cutting it off. */
  wrap?: boolean;
  className?: string;
}) {
  return (
    <span className={cn("inline-flex min-w-0 gap-2", wrap ? "items-start" : "items-center", className)}>
      <Square color={TONE_COLOR[tone ?? stateTone(state)]} className={wrap ? "mt-[6px]" : undefined} />
      <span className="flex-none font-wv-mono text-[12px] leading-[1.5] text-wv-fg">{label}</span>
      {detail === undefined ? null : (
        <span
          className={cn(
            "min-w-0 font-wv-mono text-[11.5px] leading-[1.5] text-wv-muted",
            wrap ? "break-words" : "truncate",
          )}
        >
          {detail}
        </span>
      )}
    </span>
  );
}

/** A sentence of guidance under a section header. */
export function Hint({ children, className }: { children: ReactNode; className?: string }) {
  return (
    <p
      className={cn(
        "border-b border-wv-hairline px-4 py-3 text-[12.5px] leading-[1.5] text-pretty text-wv-muted sm:px-6",
        className,
      )}
    >
      {children}
    </p>
  );
}

/** A full-width band for something the reader should know before acting. */
export function NoticeBand({
  tone,
  children,
  role,
}: {
  tone: NetworkTone;
  children: ReactNode;
  role?: "status" | "alert";
}) {
  return (
    <div
      role={role}
      className={cn(
        "flex flex-none items-start gap-[10px] border-b border-wv-hairline px-4 py-3 text-[12.5px] leading-[1.5] sm:px-6",
        tone === "bad" ? "bg-wv-danger-bg text-wv-error-text" : "bg-wv-cell-hover text-wv-secondary",
      )}
    >
      <Square color={TONE_COLOR[tone]} className="mt-[5px]" />
      <span className="min-w-0">{children}</span>
    </div>
  );
}

/** A square icon button for the small moves inside an editor: reorder, remove. */
export function IconButton({
  icon,
  label,
  onClick,
  disabled = false,
}: {
  icon: IconName;
  label: string;
  onClick: () => void;
  disabled?: boolean;
}) {
  return (
    <button
      type="button"
      aria-label={label}
      title={label}
      disabled={disabled}
      onClick={onClick}
      className={cn(
        "flex size-8 flex-none items-center justify-center border border-wv-control bg-wv-button",
        disabled
          ? "cursor-default text-wv-disabled"
          : "cursor-pointer text-wv-fg hover:border-wv-control-hover hover:bg-wv-button-hover",
      )}
    >
      <Icon name={icon} size={13} />
    </button>
  );
}

/** A checkbox with its words beside it; the words toggle it too. */
export function CheckRow({
  checked,
  onChange,
  label,
  detail,
  disabled,
}: {
  checked: boolean;
  onChange: (next: boolean) => void;
  label: string;
  detail?: ReactNode;
  disabled?: boolean;
}) {
  return (
    <label
      className={cn(
        "flex min-w-0 items-center gap-[10px] text-[12.5px] text-wv-fg",
        disabled ? "cursor-default" : "cursor-pointer",
      )}
    >
      <CheckBox checked={checked} onChange={onChange} label={label} disabled={disabled} />
      <span className="min-w-0 truncate">{label}</span>
      {detail === undefined ? null : (
        <span className="flex-none font-wv-mono text-[11px] text-wv-muted">{detail}</span>
      )}
    </label>
  );
}

/** A small caption over a control that has no form row of its own. */
export function ControlLabel({ label, children, className }: { label: string; children: ReactNode; className?: string }) {
  return (
    <div className={cn("flex min-w-0 flex-col gap-[6px]", className)}>
      <span aria-hidden="true" className="text-[11.5px] text-wv-muted">
        {label}
      </span>
      {children}
    </div>
  );
}
