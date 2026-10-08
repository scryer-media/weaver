import { gql, useQuery } from "urql";
import { useTranslate } from "@/lib/context/translate-context";
import { isOneShot, type ScheduleOptionsForm, type ScheduleTargets } from "../data/schedule-options";

const TARGETS = gql`query ScheduleTargets { servers { id host } rssFeeds { id name } }`;
const controlClass = "w-full border border-wv-control bg-wv-input px-2 py-1.5 text-sm";

export function useScheduleTargets() {
  const [{ data }] = useQuery<ScheduleTargets>({ query: TARGETS });
  return data;
}

export function ScheduleOptionsFields({ action, value, onChange }: {
  action: string; value: ScheduleOptionsForm; onChange: (value: ScheduleOptionsForm) => void;
}) {
  const t = useTranslate();
  const data = useScheduleTargets();
  const set = (patch: Partial<ScheduleOptionsForm>) => onChange({ ...value, ...patch });
  return <div className="w-full space-y-3 text-sm">
    {isOneShot(action) && <label className="flex items-center gap-2">
      <input type="checkbox" checked={value.everyHourAtMinute !== null} onChange={(event) => set({ everyHourAtMinute: event.target.checked ? 0 : null })} />
      {t("next.schedules.hourly")}
    </label>}
    {isOneShot(action) && value.everyHourAtMinute !== null
      ? <label className="block">{t("next.schedules.minute")}<input className={controlClass} type="number" min={0} max={59} value={value.everyHourAtMinute} onChange={(event) => set({ everyHourAtMinute: Number(event.target.value) })} /></label>
      : <label className="block">{t("next.schedules.multipleTimes")}<input className={controlClass} placeholder="08:00, 18:00" value={value.timesText} onChange={(event) => set({ timesText: event.target.value })} /></label>}
    <p className="text-wv-muted">{t(isOneShot(action) ? "next.schedules.oneShotHelp" : "next.schedules.multipleTimesHelp")}</p>
    {action === "pause_all" && <p>{t("next.schedules.pauseAllHelp")}</p>}
    {(action === "pause_post_processing" || action === "resume_post_processing") && <p>{t("next.schedules.postHelp")}</p>}
    {action === "set_server_active" && <>
      <label className="block">{t("next.schedules.server")}<select aria-label={t("next.schedules.server")} className={controlClass} value={value.serverId ?? ""} onChange={(event) => set({ serverId: event.target.value ? Number(event.target.value) : null })}>
        <option value="">{t("next.schedules.chooseServer")}</option>
        {data?.servers.map((server) => <option key={server.id} value={server.id}>{server.host}</option>)}
      </select></label>
      <label className="flex items-center gap-2"><input type="checkbox" checked={value.serverActive ?? true} onChange={(event) => set({ serverActive: event.target.checked })} />{t("next.schedules.serverEnabled")}</label>
      <p className="text-wv-muted">{t("next.schedules.serverHelp")}</p>
    </>}
    {action === "set_quota_metering" && <>
      <label className="flex items-center gap-2"><input type="checkbox" checked={value.quotaMeteringEnabled ?? true} onChange={(event) => set({ quotaMeteringEnabled: event.target.checked })} />{t("next.schedules.meterBytes")}</label>
      <p className="text-wv-muted">{t("next.schedules.quotaHelp")}</p>
    </>}
    {action === "fetch_rss" && <label className="block">{t("next.schedules.feed")}<select aria-label={t("next.schedules.feed")} className={controlClass} value={value.feedId ?? ""} onChange={(event) => set({ feedId: event.target.value ? Number(event.target.value) : null })}>
      <option value="">{t("next.schedules.allFeeds")}</option>
      {data?.rssFeeds.map((feed) => <option key={feed.id} value={feed.id}>{feed.name}</option>)}
    </select></label>}
    {action === "prune_history" && <>
      {(["pruneFailed", "pruneCompleted", "pruneCancelled"] as const).map((key) => <div key={key} className="space-y-1">
        <label className="flex items-center gap-2"><input type="checkbox" checked={value[key] !== null} onChange={(event) => set({ [key]: event.target.checked ? { deleteFiles: key !== "pruneCompleted" } : null })} />{t(`next.schedules.${key}`)}</label>
        {value[key] && <label className="flex items-center gap-2 pl-5"><input type="checkbox" checked={value[key].deleteFiles} onChange={(event) => set({ [key]: { deleteFiles: event.target.checked } })} />{t("next.schedules.deleteFiles")}</label>}
      </div>)}
      <p className="text-wv-muted">{t("next.schedules.pruneWarning")}</p>
    </>}
  </div>;
}
