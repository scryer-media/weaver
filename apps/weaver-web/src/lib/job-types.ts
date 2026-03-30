export interface ParsedEpisodeData {
  season: number | null;
  episodeNumbers: number[];
  absoluteEpisode: number | null;
  raw: string | null;
}

export interface ParsedReleaseData {
  normalizedTitle: string;
  releaseGroup: string | null;
  languagesAudio: string[];
  languagesSubtitles: string[];
  year: number | null;
  quality: string | null;
  source: string | null;
  videoCodec: string | null;
  videoEncoding: string | null;
  audio: string | null;
  audioCodecs: string[];
  audioChannels: string | null;
  isDualAudio: boolean;
  isAtmos: boolean;
  isDolbyVision: boolean;
  detectedHdr: boolean;
  isHdr10Plus: boolean;
  isHlg: boolean;
  fps: number | null;
  isProperUpload: boolean;
  isRepack: boolean;
  isRemux: boolean;
  isBdDisk: boolean;
  isAiEnhanced: boolean;
  isHardcodedSubs: boolean;
  streamingService: string | null;
  edition: string | null;
  animeVersion: number | null;
  parseConfidence: number;
  episode: ParsedEpisodeData | null;
}

export interface JobData {
  id: number;
  name: string;
  displayTitle: string;
  originalTitle: string;
  parsedRelease: ParsedReleaseData;
  status: string;
  progress: number;
  progressPercent?: number | null;
  totalBytes: number;
  downloadedBytes: number;
  optionalRecoveryBytes: number;
  optionalRecoveryDownloadedBytes: number;
  failedBytes: number;
  health: number;
  hasPassword: boolean;
  category: string | null;
  createdAt?: number | null;
  completedAt?: number | null;
  error?: string | null;
  outputDir?: string | null;
  metadata: { key: string; value: string }[];
}

export interface GraphqlJobData extends Omit<JobData, "progress" | "createdAt" | "completedAt" | "metadata"> {
  progress?: number | null;
  progressPercent?: number | null;
  createdAt?: string | number | null;
  completedAt?: string | number | null;
  metadata?: { key: string; value: string }[];
  attributes?: { key: string; value: string }[];
}

export function normalizeFacadeJobStatus(status: string): string {
  switch (status) {
    case "COMPLETED":
      return "COMPLETE";
    case "FINALIZING":
      return "MOVING";
    default:
      return status;
  }
}

export function normalizeFacadeJobProgress(
  progressPercent?: number | null,
  progress?: number | null,
): number {
  if (typeof progressPercent === "number" && Number.isFinite(progressPercent)) {
    return progressPercent / 100;
  }
  if (typeof progress === "number" && Number.isFinite(progress)) {
    return progress;
  }
  return 0;
}

export function normalizeGraphqlTimestamp(value?: string | number | null): number | null {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string" && value.trim()) {
    const epochMs = Date.parse(value);
    return Number.isFinite(epochMs) ? epochMs : null;
  }
  return null;
}

export function normalizeJobData<T extends GraphqlJobData>(job: T): T & JobData {
  return {
    ...job,
    status: normalizeFacadeJobStatus(job.status),
    progress: normalizeFacadeJobProgress(job.progressPercent, job.progress),
    createdAt: normalizeGraphqlTimestamp(job.createdAt),
    completedAt: normalizeGraphqlTimestamp(job.completedAt),
    metadata: job.metadata ?? job.attributes ?? [],
  };
}
