export function formatNzbNameForDisplay(name: string): string {
  return name.replace(/\.nzb$/i, "").replace(/[._]+/g, " ").replace(/\s+/g, " ").trim();
}
