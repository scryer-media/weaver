import { useTranslate } from "@/lib/context/translate-context";
import { cn } from "@/lib/utils";
import {
  detectedHardware,
  profileBody,
  profileFacts,
  profileName,
  type HardwareProfileName,
  type HardwareProfileSettings,
} from "../data/hardware-profiles";

/**
 * The three named profiles as cards, shared by first-run setup and Settings.
 *
 * Only profiles this machine can honour are drawn, so a pick is never one the
 * hardware has to refuse. The caller decides what choosing means: the setup
 * walk saves on Continue, the settings panel saves at once.
 */
export function HardwareProfilePicker({
  settings,
  value,
  onChange,
  disabled = false,
}: {
  settings: HardwareProfileSettings;
  value: HardwareProfileName;
  onChange: (next: HardwareProfileName) => void;
  disabled?: boolean;
}) {
  const t = useTranslate();
  return (
    <div className="flex flex-col gap-3">
      <div
        role="radiogroup"
        aria-label={t("next.performance.title")}
        className="grid grid-cols-1 gap-2 sm:grid-cols-3"
      >
        {settings.options.map((option) => {
          const active = option.profile === value;
          const recommended = option.profile === settings.recommended;
          return (
            <button
              key={option.profile}
              type="button"
              role="radio"
              aria-checked={active}
              disabled={disabled}
              onClick={() => onChange(option.profile)}
              className={cn(
                "flex cursor-pointer flex-col gap-2 border p-3 text-left disabled:cursor-default",
                active
                  ? "border-wv-accent bg-wv-segment-active"
                  : "border-wv-control bg-wv-input hover:border-wv-control-focus",
              )}
            >
              <div className="flex items-baseline gap-2">
                <span
                  className={cn(
                    "font-wv-title text-[13.5px] font-semibold",
                    active ? "text-wv-strong" : "text-wv-fg",
                  )}
                >
                  {profileName(t, option.profile)}
                </span>
                {recommended ? (
                  <span className="font-wv-mono text-[10.5px] whitespace-nowrap text-wv-accent">
                    {t("next.performance.recommendedTag")}
                  </span>
                ) : null}
              </div>
              <p className="text-[12px] leading-[1.45] text-wv-muted">
                {profileBody(t, option.profile)}
              </p>
              <ul className="flex flex-col gap-0.5">
                {profileFacts(t, option).map((fact) => (
                  <li key={fact} className="font-wv-mono text-[11px] text-wv-dim">
                    {fact}
                  </li>
                ))}
              </ul>
            </button>
          );
        })}
      </div>
      <p className="text-[11.5px] leading-[1.45] text-wv-muted">
        {detectedHardware(t, settings)}
      </p>
      <p className="text-[11.5px] leading-[1.45] text-wv-muted">{t("next.performance.timing")}</p>
    </div>
  );
}
