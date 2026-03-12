import { createContext, useContext } from "react";
import type { JobData } from "@/lib/job-types";
import type { GraphqlConnectionStatus } from "@/graphql/client";

export interface LiveConnectionState {
  status: GraphqlConnectionStatus;
  isDisconnected: boolean;
  isPolling: boolean;
}

export interface LiveData {
  jobs: JobData[];
  speed: number;
  isPaused: boolean;
  connection: LiveConnectionState;
}

export const LiveDataContext = createContext<LiveData>({
  jobs: [],
  speed: 0,
  isPaused: false,
  connection: {
    status: "connecting",
    isDisconnected: false,
    isPolling: false,
  },
});

export function useLiveData(): LiveData {
  return useContext(LiveDataContext);
}
