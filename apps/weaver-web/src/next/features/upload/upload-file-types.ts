export const NZB_UPLOAD_ACCEPT = ".nzb,.xz,.nzb.xz";

export function isSupportedNzbUploadFilename(filename: string): boolean {
  const normalized = filename.toLowerCase();
  return normalized.endsWith(".nzb") || normalized.endsWith(".nzb.xz");
}
