import { createContext, useContext } from "react";

interface JobData {
  id: number;
  name: string;
  status: string;
  progress: number;
  totalBytes: number;
  downloadedBytes: number;
  failedBytes: number;
  health: number;
  hasPassword: boolean;
  category: string | null;
}

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
