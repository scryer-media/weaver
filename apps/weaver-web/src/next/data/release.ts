import type { ParsedReleaseData } from "@/lib/job-types";

/**
 * What weaver understood about a release name.
 *
 * The vocabulary is the classic UI's, deliberately: the redesign is a new
 * interface, not a new opinion about what a parsed release is called. Empty
 * fields are dropped rather than dashed — a parse grid full of em dashes says
 * nothing except that the grid was fixed-size.
 */

export interface ReleaseField {
  label: string;
  value: string;
}

export function releaseFields(
  parsed: ParsedReleaseData,
  category: string | null | undefined,
): ReleaseField[] {
  const fields: (ReleaseField | null)[] = [
    parsed.year ? { label: "Year", value: String(parsed.year) } : null,
    parsed.quality ? { label: "Quality", value: parsed.quality } : null,
    parsed.source ? { label: "Source", value: parsed.source } : null,
    parsed.videoCodec ? { label: "Video", value: parsed.videoCodec } : null,
    parsed.videoEncoding ? { label: "Encoding", value: parsed.videoEncoding } : null,
    parsed.audio ? { label: "Audio", value: parsed.audio } : null,
    parsed.audioChannels ? { label: "Channels", value: parsed.audioChannels } : null,
    parsed.releaseGroup ? { label: "Group", value: parsed.releaseGroup } : null,
    parsed.streamingService ? { label: "Service", value: parsed.streamingService } : null,
    parsed.edition ? { label: "Edition", value: parsed.edition } : null,
    parsed.episode?.raw && category !== "movies"
      ? { label: "Episode", value: parsed.episode.raw }
      : null,
    parsed.languagesAudio.length > 0
      ? { label: "Audio lang", value: parsed.languagesAudio.join(", ") }
      : null,
    parsed.languagesSubtitles.length > 0
      ? { label: "Subtitle lang", value: parsed.languagesSubtitles.join(", ") }
      : null,
    parsed.parseConfidence > 0
      ? { label: "Parse", value: `${Math.round(parsed.parseConfidence * 100)}% confident` }
      : null,
  ];
  return fields.filter((field): field is ReleaseField => field !== null);
}

export function releaseFlags(parsed: ParsedReleaseData): string[] {
  const flags = [
    parsed.isDualAudio ? "Dual audio" : null,
    parsed.isAtmos ? "Atmos" : null,
    parsed.isDolbyVision ? "Dolby Vision" : null,
    parsed.detectedHdr ? "HDR" : null,
    parsed.isHdr10Plus ? "HDR10+" : null,
    parsed.isHlg ? "HLG" : null,
    parsed.isProperUpload ? "Proper" : null,
    parsed.isRepack ? "Repack" : null,
    parsed.isRemux ? "Remux" : null,
    parsed.isBdDisk ? "Full disc" : null,
    parsed.isAiEnhanced ? "AI enhanced" : null,
    parsed.isHardcodedSubs ? "Hardcoded subs" : null,
    parsed.animeVersion ? `v${parsed.animeVersion}` : null,
  ];
  return flags.filter((flag): flag is string => flag !== null);
}
