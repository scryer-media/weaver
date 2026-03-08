import { gql } from "urql";

export const JOBS_QUERY = gql`
  query Jobs {
    jobs {
      id
      name
      status
      progress
      totalBytes
      downloadedBytes
      failedBytes
      health
      hasPassword
      category
    }
  }
`;

export const JOBS_PAGE_QUERY = gql`
  query JobsPage {
    jobs {
      id
      name
      status
      progress
      totalBytes
      downloadedBytes
      failedBytes
      health
      hasPassword
      category
    }
    metrics {
      currentDownloadSpeed
    }
    isPaused
  }
`;

export const JOB_QUERY = gql`
  query Job($id: Int!) {
    job(id: $id) {
      id
      name
      status
      progress
      totalBytes
      downloadedBytes
      failedBytes
      health
      hasPassword
    }
    jobEvents(jobId: $id) {
      kind
      jobId
      fileId
      message
      timestamp
    }
  }
`;

export const METRICS_QUERY = gql`
  query Metrics {
    metrics {
      bytesDownloaded
      bytesDecoded
      bytesCommitted
      downloadQueueDepth
      decodePending
      commitPending
      segmentsDownloaded
      segmentsDecoded
      segmentsCommitted
      articlesNotFound
      decodeErrors
      verifyActive
      repairActive
      extractActive
      segmentsRetried
      segmentsFailedPermanent
      currentDownloadSpeed
    }
  }
`;

export const IS_PAUSED_QUERY = gql`
  query IsPaused {
    isPaused
  }
`;

export const SETTINGS_PAGE_QUERY = gql`
  query SettingsPage {
    metrics {
      bytesDownloaded
      bytesDecoded
      bytesCommitted
      downloadQueueDepth
      decodePending
      commitPending
      segmentsDownloaded
      segmentsDecoded
      segmentsCommitted
      articlesNotFound
      decodeErrors
      verifyActive
      repairActive
      extractActive
      segmentsRetried
      segmentsFailedPermanent
      currentDownloadSpeed
    }
    isPaused
  }
`;

export const SUBMIT_NZB_MUTATION = gql`
  mutation SubmitNzb($nzbBase64: String!, $filename: String, $password: String) {
    submitNzb(nzbBase64: $nzbBase64, filename: $filename, password: $password) {
      id
      name
      status
    }
  }
`;

export const PAUSE_JOB_MUTATION = gql`
  mutation PauseJob($id: Int!) {
    pauseJob(id: $id)
  }
`;

export const RESUME_JOB_MUTATION = gql`
  mutation ResumeJob($id: Int!) {
    resumeJob(id: $id)
  }
`;

export const CANCEL_JOB_MUTATION = gql`
  mutation CancelJob($id: Int!) {
    cancelJob(id: $id)
  }
`;

export const REPROCESS_JOB_MUTATION = gql`
  mutation ReprocessJob($id: Int!) {
    reprocessJob(id: $id)
  }
`;

export const PAUSE_ALL_MUTATION = gql`
  mutation PauseAll {
    pauseAll
  }
`;

export const RESUME_ALL_MUTATION = gql`
  mutation ResumeAll {
    resumeAll
  }
`;

export const SET_SPEED_LIMIT_MUTATION = gql`
  mutation SetSpeedLimit($bytesPerSec: Int!) {
    setSpeedLimit(bytesPerSec: $bytesPerSec)
  }
`;

export const EVENTS_SUBSCRIPTION = gql`
  subscription Events {
    events {
      kind
      jobId
      fileId
      message
    }
  }
`;

export const JOB_UPDATES_SUBSCRIPTION = gql`
  subscription JobUpdates {
    jobUpdates {
      jobs {
        id
        name
        status
        progress
        totalBytes
        downloadedBytes
        failedBytes
        health
        hasPassword
        category
      }
      metrics {
        currentDownloadSpeed
      }
      isPaused
    }
  }
`;

export const HISTORY_JOBS_QUERY = gql`
  query HistoryJobs {
    jobs(status: [COMPLETE, FAILED]) {
      id
      name
      status
      progress
      totalBytes
      downloadedBytes
      failedBytes
      health
      hasPassword
      category
      outputDir
    }
  }
`;

// --- Server management ---

export const SERVERS_QUERY = gql`
  query Servers {
    servers {
      id
      host
      port
      tls
      username
      connections
      active
      supportsPipelining
    }
  }
`;

export const ADD_SERVER_MUTATION = gql`
  mutation AddServer($input: ServerInput!) {
    addServer(input: $input) {
      id
      host
      port
      tls
      username
      connections
      active
      supportsPipelining
    }
  }
`;

export const UPDATE_SERVER_MUTATION = gql`
  mutation UpdateServer($id: Int!, $input: ServerInput!) {
    updateServer(id: $id, input: $input) {
      id
      host
      port
      tls
      username
      connections
      active
      supportsPipelining
    }
  }
`;

export const REMOVE_SERVER_MUTATION = gql`
  mutation RemoveServer($id: Int!) {
    removeServer(id: $id)
  }
`;

export const TEST_CONNECTION_MUTATION = gql`
  mutation TestConnection($input: ServerInput!) {
    testConnection(input: $input) {
      success
      message
      latencyMs
      supportsPipelining
    }
  }
`;

// --- General settings ---

export const SETTINGS_QUERY = gql`
  query Settings {
    settings {
      dataDir
      intermediateDir
      completeDir
      cleanupAfterExtract
      maxDownloadSpeed
      maxRetries
    }
  }
`;

export const UPDATE_SETTINGS_MUTATION = gql`
  mutation UpdateSettings($input: GeneralSettingsInput!) {
    updateSettings(input: $input) {
      dataDir
      intermediateDir
      completeDir
      cleanupAfterExtract
      maxDownloadSpeed
      maxRetries
    }
  }
`;
