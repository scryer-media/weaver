import { createContext, useContext } from "react";
import type { JobData } from "@/lib/job-types";
import type { GraphqlConnectionStatus } from "@/graphql/client";

export interface LiveConnectionState {
  status: GraphqlConnectionStatus;
  isDisconnected: boolean;
  isPolling: boolean;
}

export interface DownloadBlockState {
  kind: "NONE" | "MANUAL_PAUSE" | "SCHEDULED" | "ISP_CAP";
  capEnabled: boolean;
  period?: "DAILY" | "WEEKLY" | "MONTHLY" | null;
  usedBytes: number;
  limitBytes: number;
  remainingBytes: number;
  reservedBytes: number;
  windowStartsAtEpochMs?: number | null;
  windowEndsAtEpochMs?: number | null;
  timezoneName: string;
  scheduledSpeedLimit: number;
}

export interface LiveData {
  jobs: JobData[];
  speed: number;
  isPaused: boolean;
  downloadBlock: DownloadBlockState;
  connection: LiveConnectionState;
}

export const LiveDataContext = createContext<LiveData>({
  jobs: [],
  speed: 0,
  isPaused: false,
  downloadBlock: {
    kind: "NONE",
    capEnabled: false,
    period: null,
    usedBytes: 0,
    limitBytes: 0,
    remainingBytes: 0,
    reservedBytes: 0,
    windowStartsAtEpochMs: null,
    windowEndsAtEpochMs: null,
    timezoneName: "",
    scheduledSpeedLimit: 0,
  },
  connection: {
    status: "connecting",
    isDisconnected: false,
    isPolling: false,
  },
});

export function useLiveData(): LiveData {
  return useContext(LiveDataContext);
}
