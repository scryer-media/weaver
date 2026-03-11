import { createContext, useContext } from "react";
import type { JobData } from "@/lib/job-types";

export interface LiveData {
  jobs: JobData[];
  speed: number;
  isPaused: boolean;
}

export const LiveDataContext = createContext<LiveData>({
  jobs: [],
  speed: 0,
  isPaused: false,
});

export function useLiveData(): LiveData {
  return useContext(LiveDataContext);
}
