import type { Translate } from "@/lib/context/translate-context";
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
  t: Translate,
  parsed: ParsedReleaseData,
  category: string | null | undefined,
): ReleaseField[] {
  const fields: (ReleaseField | null)[] = [
    parsed.year ? { label: t("next.release.year"), value: String(parsed.year) } : null,
    parsed.quality ? { label: t("next.release.quality"), value: parsed.quality } : null,
    parsed.source ? { label: t("next.release.source"), value: parsed.source } : null,
    parsed.videoCodec ? { label: t("next.release.video"), value: parsed.videoCodec } : null,
    parsed.videoEncoding ? { label: t("next.release.encoding"), value: parsed.videoEncoding } : null,
    parsed.audio ? { label: t("next.release.audio"), value: parsed.audio } : null,
    parsed.audioChannels ? { label: t("next.release.channels"), value: parsed.audioChannels } : null,
    parsed.releaseGroup ? { label: t("next.release.group"), value: parsed.releaseGroup } : null,
    parsed.streamingService ? { label: t("next.release.service"), value: parsed.streamingService } : null,
    parsed.edition ? { label: t("next.release.edition"), value: parsed.edition } : null,
    parsed.episode?.raw && category !== "movies"
      ? { label: t("next.release.episode"), value: parsed.episode.raw }
      : null,
    parsed.languagesAudio.length > 0
      ? { label: t("next.release.audioLanguages"), value: parsed.languagesAudio.join(", ") }
      : null,
    parsed.languagesSubtitles.length > 0
      ? { label: t("next.release.subtitleLanguages"), value: parsed.languagesSubtitles.join(", ") }
      : null,
    parsed.parseConfidence > 0
      ? { label: t("next.release.parse"), value: t("next.release.confidence", { percent: Math.round(parsed.parseConfidence * 100) }) }
      : null,
  ];
  return fields.filter((field): field is ReleaseField => field !== null);
}

/** Format names such as Atmos, HDR10+ or Remux read the same in every language; the rest translate. */
export function releaseFlags(t: Translate, parsed: ParsedReleaseData): string[] {
  const flags = [
    parsed.isDualAudio ? t("next.release.dualAudio") : null,
    parsed.isAtmos ? "Atmos" : null,
    parsed.isDolbyVision ? "Dolby Vision" : null,
    parsed.detectedHdr ? "HDR" : null,
    parsed.isHdr10Plus ? "HDR10+" : null,
    parsed.isHlg ? "HLG" : null,
    parsed.isProperUpload ? "Proper" : null,
    parsed.isRepack ? "Repack" : null,
    parsed.isRemux ? "Remux" : null,
    parsed.isBdDisk ? t("next.release.fullDisc") : null,
    parsed.isAiEnhanced ? t("next.release.aiEnhanced") : null,
    parsed.isHardcodedSubs ? t("next.release.hardcodedSubs") : null,
    parsed.animeVersion ? `v${parsed.animeVersion}` : null,
  ];
  return flags.filter((flag): flag is string => flag !== null);
}
