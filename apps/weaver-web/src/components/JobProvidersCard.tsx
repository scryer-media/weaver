import { useMemo } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Progress } from "@/components/ui/progress";
import { formatBytes } from "@/components/SpeedDisplay";
import { useTranslate } from "@/lib/context/translate-context";
import { cn } from "@/lib/utils";

export interface JobServerContributionData {
  serverId: number;
  serverHost?: string | null;
  articles: number;
  wireBytes: number;
}

/**
 * Bar colors by rank. Kept to theme tokens plus the same fixed hues the phase
 * bars use, so a provider's color is stable for as long as its rank is.
 */
const RANK_COLOR = [
  "bg-primary",
  "bg-cyan-500",
  "bg-violet-500",
  "bg-orange-500",
  "bg-emerald-500",
];

/**
 * Which servers served this job's articles.
 *
 * Shares are of the articles that could be attributed, not of the job's whole
 * article count: a server is credited only when the landing article could be
 * traced back to it, so the counts understate rather than misattribute. Servers
 * that contributed nothing are absent rather than listed at zero.
 */
export function JobProvidersCard({
  contributions,
}: {
  contributions?: JobServerContributionData[] | null;
}) {
  const t = useTranslate();
  const rows = useMemo(() => {
    const credited = (contributions ?? []).filter((entry) => entry.articles > 0);
    const attributed = credited.reduce((sum, entry) => sum + entry.articles, 0);
    return credited
      .slice()
      .sort(
        (left, right) =>
          right.articles - left.articles || left.serverId - right.serverId,
      )
      .map((entry) => ({
        ...entry,
        share: attributed > 0 ? (entry.articles / attributed) * 100 : 0,
      }));
  }, [contributions]);

  if (rows.length === 0) {
    return null;
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle>{t("job.providersUsed")}</CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        {rows.map((row, index) => (
          <div key={row.serverId} className="w-full min-w-0 space-y-1.5">
            <div className="flex items-center justify-between gap-3 text-xs">
              <span className="min-w-0 truncate font-medium">
                {row.serverHost ?? t("job.providerUnnamed", { id: row.serverId })}
              </span>
              <span className="shrink-0 tabular-nums text-muted-foreground">
                {t("job.providerArticles", { count: row.articles })}
                {" · "}
                {formatBytes(row.wireBytes)}
              </span>
              <span className="w-[5ch] shrink-0 text-right font-medium tabular-nums">
                {row.share.toFixed(0)}%
              </span>
            </div>
            <Progress
              value={row.share}
              className="h-2 rounded-pill bg-secondary"
              indicatorClassName={cn(
                "rounded-pill",
                RANK_COLOR[index % RANK_COLOR.length],
              )}
            />
          </div>
        ))}
        <p className="text-xs text-muted-foreground">{t("job.providersUsedNote")}</p>
      </CardContent>
    </Card>
  );
}
