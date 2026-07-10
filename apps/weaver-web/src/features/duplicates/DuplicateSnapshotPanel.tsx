import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import {
  duplicateLifecycleI18nKey,
  duplicatePromotionStateI18nKey,
  duplicateTerminalCauseI18nKey,
  semanticStateI18nKey,
} from "@/features/duplicates/duplicate-presentation";
import { useTranslate } from "@/lib/context/translate-context";

export type DuplicateSnapshot = {
  jobId: number;
  lifecycle: string;
  normalizedName: string;
  semantic?: {
    groupId: number;
    normalizedKey: string;
    score: number;
    state: string;
    terminalCause?: string | null;
    promotionState: string;
  } | null;
};

export function DuplicateSnapshotPanel({
  snapshot,
  busy,
  error,
  onMarkGood,
  onMarkBad,
  onPromote,
  onForget,
}: {
  snapshot: DuplicateSnapshot;
  busy: boolean;
  error?: string | null;
  onMarkGood: () => void;
  onMarkBad: () => void;
  onPromote: () => void;
  onForget: () => void;
}) {
  const t = useTranslate();
  const semantic = snapshot.semantic;

  return (
    <Card className="border-primary/25 bg-primary/5">
      <CardHeader className="pb-3">
        <CardTitle className="text-base">{t("duplicate.panelTitle")}</CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="grid gap-3 text-sm sm:grid-cols-2">
          <Detail label={t("duplicate.lifecycle")} value={t(duplicateLifecycleI18nKey(snapshot.lifecycle))} />
          <Detail label={t("duplicate.normalizedName")} value={snapshot.normalizedName} />
          {semantic ? (
            <>
              <Detail label={t("duplicate.groupId")} value={String(semantic.groupId)} />
              <Detail label={t("duplicate.score")} value={String(semantic.score)} />
              <Detail label={t("duplicate.candidateState")} value={t(semanticStateI18nKey(semantic.state))} />
              <Detail
                label={t("duplicate.promotionState")}
                value={t(duplicatePromotionStateI18nKey(semantic.promotionState))}
              />
              {semantic.terminalCause ? (
                <Detail
                  label={t("duplicate.terminalCause")}
                  value={t(duplicateTerminalCauseI18nKey(semantic.terminalCause))}
                />
              ) : null}
            </>
          ) : null}
        </div>

        {semantic ? (
          <div className="flex flex-wrap gap-2">
            <Button type="button" variant="outline" size="sm" disabled={busy} onClick={onMarkGood}>
              {t("duplicate.markGood")}
            </Button>
            <Button type="button" variant="outline" size="sm" disabled={busy} onClick={onMarkBad}>
              {t("duplicate.markBad")}
            </Button>
            <Button type="button" variant="outline" size="sm" disabled={busy} onClick={onPromote}>
              {t("duplicate.promote")}
            </Button>
          </div>
        ) : null}
        <Button type="button" variant="destructive" size="sm" disabled={busy} onClick={onForget}>
          {t("duplicate.forget")}
        </Button>
        {error ? <p className="text-sm text-destructive">{error}</p> : null}
      </CardContent>
    </Card>
  );
}

function Detail({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-inner border border-border bg-background/50 px-3 py-2">
      <div className="text-[10.5px] font-semibold uppercase tracking-[0.12em] text-muted-foreground">
        {label}
      </div>
      <div className="mt-1 break-words text-sm text-foreground">{value}</div>
    </div>
  );
}
