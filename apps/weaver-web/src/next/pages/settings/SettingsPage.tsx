import { useCallback, useMemo, useRef, useState, useSyncExternalStore } from "react";
import { useNavigate, useParams } from "react-router";
import { useTranslate } from "@/lib/context/translate-context";
import { NextShell } from "../../shell/NextShell";
import { PanelListBlock } from "../../shell/rail-blocks";
import { useNextData } from "../../data/next-data";
import { BetaTag, EmptyState } from "../../components/chrome";
import { PrimaryButton, SecondaryButton, TextField } from "../../components/controls";
import { SettingsPanelScope, SettingsShellProvider, type PanelFlags } from "./framework";
import { findPanel, panelSearchTitle, SETTINGS_PANELS, settingsRail } from "./panels";
import { SearchRegistry, searchOutcome } from "./search";

const PANEL_SLUGS = SETTINGS_PANELS.map((entry) => entry.slug);

/**
 * The search's answer when no panel has a match: still loading, or nothing.
 * The matches themselves draw under each panel's heading.
 */
function SearchOutcomeState({ search, registry }: { search: string; registry: SearchRegistry }) {
  const t = useTranslate();
  const state = useSyncExternalStore(
    registry.subscribe,
    () => searchOutcome(PANEL_SLUGS, (slug) => registry.panel(slug)).state,
  );
  if (state === "loading") {
    return <EmptyState loading title={t("next.common.loading")} body={t("next.settings.searchLoadingBody")} />;
  }
  if (state === "empty") {
    return <EmptyState title={t("next.settings.noMatch", { search })} body={t("next.settings.noMatchBody")} />;
  }
  return null;
}

/**
 * The settings shell.
 *
 * One panel is mounted at a time; the top bar's Search, Revert and Save and
 * the status bar's dirty line belong to the shell, and the panel publishes
 * what they act on through `usePanelState`. Panels that write immediately —
 * the list-shaped ones — simply never publish, and the two buttons stay in
 * their resting state, which is exactly what the handoff draws.
 *
 * The search box searches every panel, not only the open one: while it holds
 * a query, each panel's matches show under that panel's heading, and clearing
 * it returns to the open panel as it was left.
 */

const CLEAN: PanelFlags = { dirty: false, busy: false, status: null, failed: false };

export function SettingsPage() {
  const t = useTranslate();
  const { panel: root, "*": nested } = useParams();
  const slug = nested ? `${root}/${nested}` : root;
  const panel = findPanel(slug);
  const { providers } = useNextData();

  const navigate = useNavigate();
  const [search, setSearch] = useState("");
  const [registry] = useState(() => new SearchRegistry());
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

  const railItems = useMemo(() => settingsRail(t, providers.length), [providers.length, t]);

  const statusRight =
    flags.status ?? (flags.dirty ? t("next.settings.unsaved") : t("next.settings.allSaved"));
  const statusTone = flags.failed
    ? "text-wv-error-text"
    : flags.dirty
      ? "text-wv-warn"
      : undefined;

  // A query searches every panel, so every panel mounts to answer it; without
  // one only the open panel does.
  const searching = search.trim() !== "";
  const mounted = searching ? SETTINGS_PANELS : panel ? [panel] : [];

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
          registry={registry}
        >
          {/*
            One list keyed by slug in both modes, so the open panel is the
            same instance with or without a query and keeps its unsaved edits.
          */}
          {mounted.map((entry) => (
            <SettingsPanelScope
              key={entry.slug}
              slug={entry.slug}
              active={entry.slug === panel?.slug}
              title={panelSearchTitle(t, entry)}
              onOpen={() => navigate(`/settings/${entry.slug}`)}
            >
              <entry.Component />
            </SettingsPanelScope>
          ))}
          {searching ? <SearchOutcomeState search={search.trim()} registry={registry} /> : null}
          {!searching && !panel ? (
            <EmptyState
              title={t("next.settings.noPanel")}
              body={t("next.settings.noPanelBody")}
            />
          ) : null}
        </SettingsShellProvider>
      </div>
    </NextShell>
  );
}
