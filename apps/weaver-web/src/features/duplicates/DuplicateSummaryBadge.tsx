import { Badge } from "@/components/ui/badge";
import {
  duplicateActionI18nKey,
  duplicateFingerprintKindI18nKey,
  duplicateLifecycleI18nKey,
  semanticStateI18nKey,
} from "@/features/duplicates/duplicate-presentation";
import { useTranslate } from "@/lib/context/translate-context";
import type { DuplicateSummaryData } from "@/lib/job-types";

/** Compact, non-sensitive duplicate state for queue and history rows. */
export function DuplicateSummaryBadge({ summary }: { summary?: DuplicateSummaryData | null }) {
  const t = useTranslate();
  if (!summary) {
    return null;
  }

  const action = t(duplicateActionI18nKey(summary.action));
  const reason = summary.primaryReason
    ? t("duplicate.summaryMatch", {
        kind: t(duplicateFingerprintKindI18nKey(summary.primaryReason)),
        lifecycle: t(duplicateLifecycleI18nKey(summary.lifecycle)),
      })
    : summary.semantic
      ? t("duplicate.summarySemantic", {
          state: t(semanticStateI18nKey(summary.semantic.state)),
        })
      : t(duplicateLifecycleI18nKey(summary.lifecycle));

  return (
    <Badge
      variant="secondary"
      className="mt-1 max-w-full truncate text-[10px] font-medium"
      title={t("duplicate.summaryTitle", { action, reason })}
    >
      {t("duplicate.summary", { action, reason })}
    </Badge>
  );
}
