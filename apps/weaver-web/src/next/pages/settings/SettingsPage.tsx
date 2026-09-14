import { useCallback, useMemo, useRef, useState } from "react";
import { useParams } from "react-router";
import { useTranslate } from "@/lib/context/translate-context";
import { NextShell } from "../../shell/NextShell";
import { PanelListBlock } from "../../shell/rail-blocks";
import { useNextData } from "../../data/next-data";
import { BetaTag, EmptyState } from "../../components/chrome";
import { PrimaryButton, SecondaryButton, TextField } from "../../components/controls";
import { SettingsShellProvider, type PanelFlags } from "./framework";
import { SETTINGS_PANELS, findPanel } from "./panels";

/**
 * The settings shell.
 *
 * One panel is mounted at a time; the top bar's Search, Revert and Save and
 * the status bar's dirty line belong to the shell, and the panel publishes
 * what they act on through `usePanelState`. Panels that write immediately —
 * the list-shaped ones — simply never publish, and the two buttons stay in
 * their resting state, which is exactly what the handoff draws.
 */

const CLEAN: PanelFlags = { dirty: false, busy: false, status: null, failed: false };

export function SettingsPage() {
  const t = useTranslate();
  const { panel: slug } = useParams();
  const panel = findPanel(slug);
  const { providers } = useNextData();

  const [search, setSearch] = useState("");
  const [flags, setFlagsState] = useState<PanelFlags>(CLEAN);
  const [controlsHost, setControlsHost] = useState<HTMLElement | null>(null);
  const actionsRef = useRef<{ save: () => void; revert: () => void } | null>(null);

  // Panels publish flags from an effect, so this must be stable or every panel
  // would re-register on each of the shell's own renders.
  const setFlags = useCallback((next: PanelFlags) => {
    setFlagsState((current) =>
      current.dirty === next.dirty
      && current.busy === next.busy
      && current.status === next.status
      && current.failed === next.failed
        ? current
        : next,
    );
  }, []);

  const railItems = useMemo(
    () =>
      SETTINGS_PANELS.map((entry) => ({
        to: `/settings/${entry.slug}`,
        label: t(entry.label),
        icon: entry.icon,
        tag:
          entry.tag === "beta"
            ? t("next.settings.beta")
            : entry.tag === "count:providers"
              ? providers.length > 0
                ? String(providers.length)
                : undefined
              : undefined,
      })),
    [providers.length, t],
  );

  const statusRight =
    flags.status ?? (flags.dirty ? t("next.settings.unsaved") : t("next.settings.allSaved"));
  const statusTone = flags.failed
    ? "text-wv-error-text"
    : flags.dirty
      ? "text-wv-warn"
      : undefined;

  const Panel = panel?.Component;

  return (
    <NextShell
      title={panel ? t(panel.label) : t("nav.settings")}
      titleTag={panel?.tag === "beta" ? <BetaTag /> : undefined}
      note={panel ? t(panel.note) : undefined}
      railMiddle={<PanelListBlock eyebrow={t("nav.settings")} items={railItems} />}
      statusRight={<span className={statusTone}>{statusRight}</span>}
      controls={
        <>
          {/*
            A measured field, not a greedy one: `w-full` here asked for the
            whole controls row, which pushed the panel's own button and the
            Revert/Save pair onto lines of their own. Same widths the other
            top bars use, and the same min-width, so the field yields before
            the title does.
          */}
          <TextField
            label={t("next.settings.search")}
            placeholder={t("next.settings.search")}
            mono={false}
            value={search}
            onChange={setSearch}
            className="w-[132px] min-w-[88px] sm:w-[172px] sm:min-w-[96px]"
          />
          {/*
            The panel's own actions sit between the search and the commit
            pair. `empty:hidden` keeps a panel that publishes none — most of
            them — from spending a gap on an empty slot.
          */}
          <div ref={setControlsHost} className="flex items-center gap-[10px] empty:hidden" />
          <SecondaryButton
            icon="revert"
            disabled={!flags.dirty || flags.busy}
            onClick={() => actionsRef.current?.revert()}
          >
            {t("next.settings.revert")}
          </SecondaryButton>
          <PrimaryButton
            icon="save"
            disabled={!flags.dirty || flags.busy}
            onClick={() => actionsRef.current?.save()}
          >
            {flags.busy
              ? t("settings.saving")
              : flags.dirty
                ? t("next.settings.saveChanges")
                : t("next.settings.saved")}
          </PrimaryButton>
        </>
      }
    >
      <div className="flex min-h-0 flex-1 flex-col overflow-y-auto bg-wv-list">
        <SettingsShellProvider
          search={search}
          actionsRef={actionsRef}
          setFlags={setFlags}
          controlsHost={controlsHost}
        >
          {Panel ? (
            <Panel key={panel?.slug} />
          ) : (
            <EmptyState
              title={t("next.settings.noPanel")}
              body={t("next.settings.noPanelBody")}
            />
          )}
        </SettingsShellProvider>
      </div>
    </NextShell>
  );
}
