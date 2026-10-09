import { useTranslate } from "@/lib/context/translate-context";
import { SCRIPT_KIND_LABELS, type ScriptKind } from "../data/script-instances";
import { Tag } from "./chrome";

export interface ScriptDeclarations {
  kinds: ScriptKind[];
  queueEvents: string[];
  taskTimes: string[];
}

const DETAIL = "font-wv-mono text-[11px] leading-[1.45] break-words whitespace-normal text-wv-muted";

/** What a script declares it runs for: its kinds, then the events and times it names. */
export function ScriptKinds({ script }: { script: ScriptDeclarations }) {
  const t = useTranslate();
  return (
    <div className="flex min-w-0 flex-col gap-[6px]">
      {script.kinds.length > 0 ? (
        <div className="flex flex-wrap gap-1.5">
          {script.kinds.map((kind) => (
            <Tag key={kind}>{t(SCRIPT_KIND_LABELS[kind])}</Tag>
          ))}
        </div>
      ) : null}
      {script.kinds.includes("QUEUE") ? (
        <span className={DETAIL}>
          {t("next.postProcessing.declaredEvents", {
            events: script.queueEvents.join(", ") || t("next.postProcessing.noEvents"),
          })}
        </span>
      ) : null}
      {script.taskTimes.length > 0 ? (
        <span className={DETAIL}>
          {t("next.postProcessing.taskTimes", { times: script.taskTimes.join(", ") })}
        </span>
      ) : null}
    </div>
  );
}
