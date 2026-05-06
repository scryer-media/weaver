import {
  createContext,
  useContext,
  useLayoutEffect,
  useMemo,
  useState,
  useSyncExternalStore,
  type ReactNode,
} from "react";
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

interface LiveJobsStore {
  getJob: (jobId: number) => JobData | null;
  setJobs: (jobs: JobData[]) => void;
  subscribe: (listener: () => void) => () => void;
}

const EMPTY_JOBS: JobData[] = [];
const DEFAULT_DOWNLOAD_BLOCK: DownloadBlockState = {
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
};
const DEFAULT_CONNECTION: LiveConnectionState = {
  status: "connecting",
  isDisconnected: false,
  isPolling: false,
};

function createLiveJobsStore(initialJobs: JobData[] = EMPTY_JOBS): LiveJobsStore {
  let jobs = initialJobs;
  let jobsById = new Map(initialJobs.map((job) => [job.id, job] as const));
  const listeners = new Set<() => void>();

  return {
    getJob: (jobId) => jobsById.get(jobId) ?? null,
    setJobs: (nextJobs) => {
      if (jobs === nextJobs) {
        return;
      }

      jobs = nextJobs;
      jobsById = new Map(nextJobs.map((job) => [job.id, job] as const));

      for (const listener of listeners) {
        listener();
      }
    },
    subscribe: (listener) => {
      listeners.add(listener);
      return () => listeners.delete(listener);
    },
  };
}

const LiveJobsStoreContext = createContext<LiveJobsStore>(createLiveJobsStore());
const LiveJobsContext = createContext<JobData[]>(EMPTY_JOBS);
const LiveSpeedContext = createContext(0);
const LivePauseStateContext = createContext(false);
const LiveDownloadBlockContext = createContext<DownloadBlockState>(DEFAULT_DOWNLOAD_BLOCK);
const LiveConnectionContext = createContext<LiveConnectionState>(DEFAULT_CONNECTION);

export function LiveDataProvider({
  jobs,
  speed,
  isPaused,
  downloadBlock,
  connection,
  children,
}: LiveData & { children: ReactNode }) {
  const [jobsStore] = useState(() => createLiveJobsStore(jobs));

  useLayoutEffect(() => {
    jobsStore.setJobs(jobs);
  }, [jobs, jobsStore]);

  return (
    <LiveJobsStoreContext.Provider value={jobsStore}>
      <LiveJobsContext.Provider value={jobs}>
        <LiveSpeedContext.Provider value={speed}>
          <LivePauseStateContext.Provider value={isPaused}>
            <LiveDownloadBlockContext.Provider value={downloadBlock}>
              <LiveConnectionContext.Provider value={connection}>
                {children}
              </LiveConnectionContext.Provider>
            </LiveDownloadBlockContext.Provider>
          </LivePauseStateContext.Provider>
        </LiveSpeedContext.Provider>
      </LiveJobsContext.Provider>
    </LiveJobsStoreContext.Provider>
  );
}

export function useLiveJobs(): JobData[] {
  return useContext(LiveJobsContext);
}

export function useLiveJob(jobId: number | null | undefined): JobData | null {
  const jobsStore = useContext(LiveJobsStoreContext);

  return useSyncExternalStore(
    jobsStore.subscribe,
    () => (Number.isFinite(jobId) ? jobsStore.getJob(jobId as number) : null),
    () => null,
  );
}

export function useLiveSpeed(): number {
  return useContext(LiveSpeedContext);
}

export function useLivePaused(): boolean {
  return useContext(LivePauseStateContext);
}

export function useLiveDownloadBlock(): DownloadBlockState {
  return useContext(LiveDownloadBlockContext);
}

export function useLiveConnection(): LiveConnectionState {
  return useContext(LiveConnectionContext);
}

export function useLiveData(): LiveData {
  const jobs = useLiveJobs();
  const speed = useLiveSpeed();
  const isPaused = useLivePaused();
  const downloadBlock = useLiveDownloadBlock();
  const connection = useLiveConnection();

  return useMemo(
    () => ({
      jobs,
      speed,
      isPaused,
      downloadBlock,
      connection,
    }),
    [connection, downloadBlock, isPaused, jobs, speed],
  );
}
