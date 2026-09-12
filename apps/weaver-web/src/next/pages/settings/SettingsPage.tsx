import { useCallback, useMemo, useRef, useState } from "react";
import { useParams } from "react-router";
import { useQuery } from "urql";
import { SETTINGS_QUERY } from "@/graphql/queries";
import { NextShell } from "../../shell/NextShell";
import { PanelListBlock, PathBlock } from "../../shell/rail-blocks";
import { useNextData } from "../../data/next-data";
import { EmptyState } from "../../components/chrome";
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

  const [{ data: settingsData }] = useQuery<{ settings: { dataDir: string } }>({
    query: SETTINGS_QUERY,
  });

  const railItems = useMemo(
    () =>
      SETTINGS_PANELS.map((entry) => ({
        to: `/settings/${entry.slug}`,
        label: entry.label,
        tag:
          entry.tag === "beta"
            ? "Beta"
            : entry.tag === "count:providers"
              ? providers.length > 0
                ? String(providers.length)
                : undefined
              : undefined,
      })),
    [providers.length],
  );

  const statusRight = flags.status ?? (flags.dirty ? "Unsaved changes" : "All changes saved");
  const statusTone = flags.failed
    ? "text-wv-error-text"
    : flags.dirty
      ? "text-wv-warn"
      : undefined;

  const Panel = panel?.Component;

  return (
    <NextShell
      title={panel?.label ?? "Settings"}
      note={panel?.note}
      railMiddle={<PanelListBlock eyebrow="Settings" items={railItems} />}
      railFooter={<PathBlock eyebrow="Data directory" path={settingsData?.settings?.dataDir} />}
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
            label="Search settings"
            placeholder="Search settings"
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
            disabled={!flags.dirty || flags.busy}
            onClick={() => actionsRef.current?.revert()}
          >
            Revert
          </SecondaryButton>
          <PrimaryButton
            disabled={!flags.dirty || flags.busy}
            onClick={() => actionsRef.current?.save()}
          >
            {flags.busy ? "Saving…" : flags.dirty ? "Save changes" : "Saved"}
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
              title="No such settings panel"
              body="Pick one from the settings list in the navigation."
            />
          )}
        </SettingsShellProvider>
      </div>
    </NextShell>
  );
}
