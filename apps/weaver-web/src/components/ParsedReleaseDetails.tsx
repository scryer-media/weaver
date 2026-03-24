import { Badge } from "@/components/ui/badge";
import type { ParsedReleaseData } from "@/lib/job-types";
import { cn } from "@/lib/utils";

function DetailItem({
  label,
  value,
}: {
  label: string;
  value: string;
}) {
  return (
    <div className="space-y-1">
      <div className="text-[10px] uppercase tracking-[0.16em] text-muted-foreground">{label}</div>
      <div className="text-sm text-foreground">{value}</div>
    </div>
  );
}

export function ParsedReleaseDetails({
  originalTitle,
  parsedRelease,
  category,
  compact = false,
}: {
  originalTitle: string;
  parsedRelease: ParsedReleaseData;
  category?: string | null;
  compact?: boolean;
}) {
  const fields = [
    parsedRelease.year ? { label: "Year", value: String(parsedRelease.year) } : null,
    parsedRelease.quality ? { label: "Quality", value: parsedRelease.quality } : null,
    parsedRelease.source ? { label: "Source", value: parsedRelease.source } : null,
    parsedRelease.videoCodec ? { label: "Video", value: parsedRelease.videoCodec } : null,
    parsedRelease.videoEncoding ? { label: "Encoding", value: parsedRelease.videoEncoding } : null,
    parsedRelease.audio ? { label: "Audio", value: parsedRelease.audio } : null,
    parsedRelease.audioChannels ? { label: "Channels", value: parsedRelease.audioChannels } : null,
    parsedRelease.releaseGroup ? { label: "Group", value: parsedRelease.releaseGroup } : null,
    parsedRelease.streamingService
      ? { label: "Service", value: parsedRelease.streamingService }
      : null,
    parsedRelease.edition ? { label: "Edition", value: parsedRelease.edition } : null,
    parsedRelease.episode?.raw && category !== "movies"
      ? { label: "Episode", value: parsedRelease.episode.raw }
      : null,
    parsedRelease.languagesAudio.length > 0
      ? { label: "Audio Lang", value: parsedRelease.languagesAudio.join(", ") }
      : null,
    parsedRelease.languagesSubtitles.length > 0
      ? { label: "Subtitle Lang", value: parsedRelease.languagesSubtitles.join(", ") }
      : null,
    parsedRelease.parseConfidence > 0
      ? { label: "Parse", value: `${Math.round(parsedRelease.parseConfidence * 100)}%` }
      : null,
  ].filter(Boolean) as { label: string; value: string }[];

  const flags = [
    parsedRelease.isDualAudio ? "Dual Audio" : null,
    parsedRelease.isAtmos ? "Atmos" : null,
    parsedRelease.isDolbyVision ? "Dolby Vision" : null,
    parsedRelease.detectedHdr ? "HDR" : null,
    parsedRelease.isHdr10Plus ? "HDR10+" : null,
    parsedRelease.isHlg ? "HLG" : null,
    parsedRelease.isProperUpload ? "Proper" : null,
    parsedRelease.isRepack ? "Repack" : null,
    parsedRelease.isRemux ? "Remux" : null,
    parsedRelease.isBdDisk ? "Full Disc" : null,
    parsedRelease.isAiEnhanced ? "AI Enhanced" : null,
    parsedRelease.isHardcodedSubs ? "Hardcoded Subs" : null,
  ].filter(Boolean) as string[];

  return (
    <div className={cn("space-y-4", compact && "space-y-3")}>
      <div className="space-y-1">
        <div className="text-[10px] uppercase tracking-[0.16em] text-muted-foreground">
          Original NZB Title
        </div>
        <div className={cn("break-words text-sm text-foreground", compact && "text-[13px]")}>
          {originalTitle}
        </div>
      </div>

      {fields.length > 0 ? (
        <div
          className={cn(
            "grid gap-4 sm:grid-cols-2 lg:grid-cols-4",
            compact && "gap-3 sm:grid-cols-2 lg:grid-cols-3",
          )}
        >
          {fields.map((field) => (
            <DetailItem key={field.label} label={field.label} value={field.value} />
          ))}
        </div>
      ) : null}

      {flags.length > 0 ? (
        <div className="flex flex-wrap gap-2">
          {flags.map((flag) => (
            <Badge key={flag} variant="outline" className="text-[10px] uppercase tracking-[0.12em]">
              {flag}
            </Badge>
          ))}
        </div>
      ) : null}
    </div>
  );
}
