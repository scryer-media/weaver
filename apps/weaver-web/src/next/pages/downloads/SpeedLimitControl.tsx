import { useState } from "react";
import { useMutation, useQuery } from "urql";
import { SETTINGS_QUERY, UPDATE_SETTINGS_MUTATION } from "@/graphql/queries";
import { NumberField, PrimaryButton, SecondaryButton, Toggle } from "@/next/components/controls";
import { Dialog } from "@/next/components/Dialog";
import { FormRow } from "@/next/components/rows";
import { formatRate } from "@/next/data/format";
import { useNextData } from "@/next/data/next-data";

const MIB = 1024 * 1024;

/**
 * The download ceiling, from the queue.
 *
 * It writes the same `maxDownloadSpeed` setting as Settings → Bandwidth, so the
 * two never disagree and a limit set here outlives a restart. A schedule can
 * impose its own limit for a while; the button shows that one while it holds,
 * because it is the one actually in force.
 */
export function SpeedLimitControl() {
  const { downloadBlock } = useNextData();
  const [{ data }, reexecute] = useQuery<{ settings: { maxDownloadSpeed: number } }>({
    query: SETTINGS_QUERY,
  });
  const [saveState, updateSettings] = useMutation(UPDATE_SETTINGS_MUTATION);
  const [open, setOpen] = useState(false);
  const [limited, setLimited] = useState(false);
  const [megabytes, setMegabytes] = useState(10);
  const [error, setError] = useState<string | null>(null);

  const ceiling = data?.settings?.maxDownloadSpeed ?? 0;
  const scheduled = downloadBlock.scheduledSpeedLimit;
  const inForce = scheduled > 0 ? scheduled : ceiling;

  const openDialog = () => {
    setLimited(ceiling > 0);
    setMegabytes(ceiling > 0 ? Math.round((ceiling / MIB) * 10) / 10 : 10);
    setError(null);
    setOpen(true);
  };

  const save = () => {
    const bytes = limited ? Math.round(Math.max(0, megabytes) * MIB) : 0;
    setError(null);
    void updateSettings({ input: { maxDownloadSpeed: bytes } }).then((result) => {
      if (result.error || !result.data?.updateSettings) {
        setError(result.error?.message ?? "Could not save the speed limit.");
        return;
      }
      setOpen(false);
      void reexecute({ requestPolicy: "network-only" });
    });
  };

  return (
    <>
      <SecondaryButton
        icon="bandwidth"
        title={scheduled > 0 ? "Speed limit set by a schedule" : "Download speed limit"}
        onClick={openDialog}
      >
        {inForce > 0 ? formatRate(inForce) : "Unlimited"}
      </SecondaryButton>
      <Dialog
        open={open}
        title="Speed limit"
        width={580}
        note="persists across restarts"
        onDismiss={() => setOpen(false)}
        footer={
          <>
            <SecondaryButton onClick={() => setOpen(false)}>Cancel</SecondaryButton>
            <PrimaryButton
              disabled={saveState.fetching || (limited && megabytes <= 0)}
              onClick={save}
            >
              {saveState.fetching ? "Saving" : "Apply"}
            </PrimaryButton>
          </>
        }
      >
        <FormRow label="Limit download speed" help="Off lets every download run as fast as the providers allow.">
          <Toggle checked={limited} onChange={setLimited} label="Limit download speed" />
        </FormRow>
        <FormRow label="Ceiling" help="Shared by every download, applied immediately.">
          <NumberField
            label="Speed limit in megabytes per second"
            value={megabytes}
            onChange={setMegabytes}
            min={0.1}
            step={0.1}
            suffix="MB/s"
            disabled={!limited}
          />
        </FormRow>
        {scheduled > 0 ? (
          <div className="border-b border-wv-hairline px-4 py-3 text-[12.5px] text-wv-warn sm:px-6">
            A schedule is holding downloads to {formatRate(scheduled)} right now.
          </div>
        ) : null}
        {error === null ? null : (
          <div className="px-4 py-3 text-[12.5px] text-wv-error-text sm:px-6">{error}</div>
        )}
      </Dialog>
    </>
  );
}
