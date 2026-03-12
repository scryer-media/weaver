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
  totalBytes: number;
  downloadedBytes: number;
  optionalRecoveryBytes: number;
  optionalRecoveryDownloadedBytes: number;
  failedBytes: number;
  health: number;
  hasPassword: boolean;
  category: string | null;
  createdAt?: number | null;
  metadata: { key: string; value: string }[];
}
