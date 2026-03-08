export function formatBytes(bytes: number): string {
  if (bytes === 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  const i = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1);
  const value = bytes / Math.pow(1024, i);
  return `${value.toFixed(i === 0 ? 0 : 1)} ${units[i]}`;
}

export function formatSpeed(bytesPerSec: number): string {
  return `${formatBytes(bytesPerSec)}/s`;
}

interface SpeedDisplayProps {
  bytesPerSec: number;
  className?: string;
}

export function SpeedDisplay({ bytesPerSec, className = "" }: SpeedDisplayProps) {
  return (
    <span className={`font-mono text-sm ${className}`}>
      {formatSpeed(bytesPerSec)}
    </span>
  );
}
