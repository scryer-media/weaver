import { gql } from "urql";

const FACADE_QUEUE_ITEM_FIELDS = `
  fragment FacadeQueueItemFields on QueueItem {
    id
    name
    displayTitle
    originalTitle
    parsedRelease {
      ...ParsedReleaseFields
    }
    status: state
    progressPercent
    totalBytes
    downloadedBytes
    optionalRecoveryBytes
    optionalRecoveryDownloadedBytes
    failedBytes
    health
    hasPassword
    category
    metadata: attributes {
      key
      value
    }
  }
`;

const FACADE_HISTORY_ITEM_FIELDS = `
  fragment FacadeHistoryItemFields on HistoryItem {
    id
    name
    displayTitle
    originalTitle
    parsedRelease {
      ...ParsedReleaseFields
    }
    status: state
    progressPercent
    totalBytes
    downloadedBytes
    optionalRecoveryBytes
    optionalRecoveryDownloadedBytes
    failedBytes
    health
    hasPassword
    category
    outputDir
    createdAt
    completedAt
    metadata: attributes {
      key
      value
    }
  }
`;

const PARSED_RELEASE_FIELDS = `
  fragment ParsedReleaseFields on ParsedRelease {
    normalizedTitle
    releaseGroup
    languagesAudio
    languagesSubtitles
    year
    quality
    source
    videoCodec
    videoEncoding
    audio
    audioCodecs
    audioChannels
    isDualAudio
    isAtmos
    isDolbyVision
    detectedHdr
    isHdr10Plus
    isHlg
    fps
    isProperUpload
    isRepack
    isRemux
    isBdDisk
    isAiEnhanced
    isHardcodedSubs
    streamingService
    edition
    animeVersion
    parseConfidence
    episode {
      season
      episodeNumbers
      absoluteEpisode
      raw
    }
  }
`;

const SERVER_FIELDS = `
  fragment ServerFields on Server {
    id
    host
    port
    tls
    username
    connections
    active
    supportsPipelining
    priority
  }
`;

const CATEGORY_FIELDS = `
  fragment CategoryFields on Category {
    id
    name
    destDir
    aliases
  }
`;

export const VERSION_QUERY = gql`
  query Version {
    version
  }
`;

export const BROWSE_DIRECTORIES_QUERY = gql`
  query BrowseDirectories($path: String) {
    browseDirectories(path: $path) {
      currentPath
      parentPath
      entries {
        name
        path
      }
    }
  }
`;

export const JOB_OUTPUT_FILES_QUERY = gql`
  query JobOutputFiles($jobId: Int!) {
    jobOutputFiles(jobId: $jobId) {
      outputDir
      files {
        name
        path
        sizeBytes
      }
      totalBytes
    }
  }
`;

const GENERAL_SETTINGS_FIELDS = `
  fragment GeneralSettingsFields on GeneralSettings {
    dataDir
    intermediateDir
    completeDir
    cleanupAfterExtract
    maxDownloadSpeed
    maxRetries
    ispBandwidthCap {
      ...IspBandwidthCapFields
    }
  }
`;

const ISP_BANDWIDTH_CAP_FIELDS = `
  fragment IspBandwidthCapFields on IspBandwidthCapSettings {
    enabled
    period
    limitBytes
    resetTimeMinutesLocal
    weeklyResetWeekday
    monthlyResetDay
  }
`;

const DOWNLOAD_BLOCK_FIELDS = `
  fragment DownloadBlockFields on DownloadBlock {
    kind
    capEnabled
    period
    usedBytes
    limitBytes
    remainingBytes
    reservedBytes
    windowStartsAtEpochMs
    windowEndsAtEpochMs
    timezoneName
    scheduledSpeedLimit
  }
`;

const METRICS_FIELDS = `
  fragment MetricsFields on Metrics {
    bytesDownloaded
    bytesDecoded
    bytesCommitted
    downloadQueueDepth
    decodePending
    commitPending
    writeBufferedBytes
    writeBufferedSegments
    directWriteEvictions
    segmentsDownloaded
    segmentsDecoded
    segmentsCommitted
    articlesNotFound
    decodeErrors
    verifyActive
    repairActive
    extractActive
    diskWriteLatencyUs
    segmentsRetried
    segmentsFailedPermanent
    currentDownloadSpeed
    crcErrors
    recoveryQueueDepth
    articlesPerSec
    decodeRateMbps
  }
`;

const JOB_TIMELINE_FIELDS = `
  fragment JobTimelineFields on JobTimeline {
    startedAt
    endedAt
    outcome
    lanes {
      stage
      spans {
        startedAt
        endedAt
        state
        label
      }
    }
    extractionGroups {
      setName
      members {
        member
        state
        error
        spans {
          kind
          startedAt
          endedAt
          state
          label
        }
      }
    }
  }
`;

const API_KEY_FIELDS = `
  fragment ApiKeyFields on ApiKey {
    id
    name
    scope
    createdAt
    lastUsedAt
  }
`;

const RSS_RULE_FIELDS = `
  fragment RssRuleFields on RssRule {
    id
    feedId
    sortOrder
    enabled
    action
    titleRegex
    itemCategories
    minSizeBytes
    maxSizeBytes
    categoryOverride
    metadata {
      key
      value
    }
  }
`;

const RSS_FEED_FIELDS = `
  fragment RssFeedFields on RssFeed {
    id
    name
    url
    enabled
    pollIntervalSecs
    username
    hasPassword
    defaultCategory
    defaultMetadata {
      key
      value
    }
    etag
    lastModified
    lastPolledAt
    lastSuccessAt
    lastError
    consecutiveFailures
    rules {
      ...RssRuleFields
    }
  }
`;

const RSS_SEEN_ITEM_FIELDS = `
  fragment RssSeenItemFields on RssSeenItem {
    feedId
    itemId
    itemTitle
    publishedAt
    sizeBytes
    decision
    seenAt
    jobId
    itemUrl
    error
  }
`;

export const JOBS_PAGE_QUERY = gql`
  query JobsPage {
    jobs: queueItems {
      ...FacadeQueueItemFields
    }
    metrics: queueSummary {
      currentDownloadSpeed
    }
    globalState: globalQueueState {
      isPaused
      downloadBlock {
        ...DownloadBlockFields
      }
    }
  }
  ${PARSED_RELEASE_FIELDS}
  ${FACADE_QUEUE_ITEM_FIELDS}
  ${DOWNLOAD_BLOCK_FIELDS}
`;

export const JOB_QUERY = gql`
  query Job($id: Int!) {
    queueItem(id: $id) {
      ...FacadeQueueItemFields
      error
      outputDir
      createdAt
    }
    historyItem(id: $id) {
      ...FacadeHistoryItemFields
      error
    }
    jobTimeline(jobId: $id) {
      ...JobTimelineFields
    }
    jobEvents(jobId: $id) {
      kind
      jobId
      fileId
      message
      timestamp
    }
  }
  ${JOB_TIMELINE_FIELDS}
  ${PARSED_RELEASE_FIELDS}
  ${FACADE_QUEUE_ITEM_FIELDS}
  ${FACADE_HISTORY_ITEM_FIELDS}
`;

export const METRICS_QUERY = gql`
  query Metrics {
    metrics {
      ...MetricsFields
    }
  }
  ${METRICS_FIELDS}
`;

export const METRICS_PAGE_QUERY = gql`
  query MetricsPage {
    metrics: systemMetrics {
      ...MetricsFields
    }
    globalState: globalQueueState {
      isPaused
      downloadBlock {
        ...DownloadBlockFields
      }
    }
  }
  ${METRICS_FIELDS}
  ${DOWNLOAD_BLOCK_FIELDS}
`;

export const METRICS_HISTORY_QUERY = gql`
  query MetricsHistory($minutes: Int!, $metrics: [String!]!) {
    metricsHistory(minutes: $minutes, metrics: $metrics) {
      timestamps
      series {
        metric
        labels {
          key
          value
        }
        values
      }
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

export const REDOWNLOAD_JOB_MUTATION = gql`
  mutation RedownloadJob($id: Int!) {
    redownloadJob(id: $id)
  }
`;

export const DELETE_HISTORY_MUTATION = gql`
  mutation DeleteHistory($id: Int!, $deleteFiles: Boolean) {
    deleteHistory(id: $id, deleteFiles: $deleteFiles) {
      ...FacadeHistoryItemFields
    }
  }
  ${PARSED_RELEASE_FIELDS}
  ${FACADE_HISTORY_ITEM_FIELDS}
`;

export const DELETE_HISTORY_BATCH_MUTATION = gql`
  mutation DeleteHistoryBatch($ids: [Int!]!, $deleteFiles: Boolean) {
    deleteHistoryBatch(ids: $ids, deleteFiles: $deleteFiles) {
      ...FacadeHistoryItemFields
    }
  }
  ${PARSED_RELEASE_FIELDS}
  ${FACADE_HISTORY_ITEM_FIELDS}
`;

export const DELETE_ALL_HISTORY_MUTATION = gql`
  mutation DeleteAllHistory($deleteFiles: Boolean) {
    deleteAllHistory(deleteFiles: $deleteFiles) {
      ...FacadeHistoryItemFields
    }
  }
  ${PARSED_RELEASE_FIELDS}
  ${FACADE_HISTORY_ITEM_FIELDS}
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

export const UPDATE_JOBS_MUTATION = gql`
  mutation UpdateJobs($ids: [Int!]!, $category: String, $priority: String) {
    updateJobs(ids: $ids, category: $category, priority: $priority)
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
    queueSnapshots {
      items {
        ...FacadeQueueItemFields
      }
      summary {
        currentDownloadSpeed
      }
      globalState {
        isPaused
        downloadBlock {
          ...DownloadBlockFields
        }
      }
    }
  }
  ${PARSED_RELEASE_FIELDS}
  ${FACADE_QUEUE_ITEM_FIELDS}
  ${DOWNLOAD_BLOCK_FIELDS}
`;

export const METRICS_PAGE_SUBSCRIPTION = gql`
  subscription MetricsPageUpdates {
    queueSnapshots {
      metrics {
        ...MetricsFields
      }
      globalState {
        isPaused
        downloadBlock {
          ...DownloadBlockFields
        }
      }
    }
  }
  ${METRICS_FIELDS}
  ${DOWNLOAD_BLOCK_FIELDS}
`;

export const HISTORY_FACADE_EVENTS_SUBSCRIPTION = gql`
  subscription HistoryFacadeEvents {
    queueEvents {
      kind
      itemId
      state
    }
  }
`;

export const SERVICE_LOGS_QUERY = gql`
  query ServiceLogs($limit: Int) {
    serviceLogs(limit: $limit) {
      lines
      count
    }
  }
`;

export const SERVICE_LOG_LINES_SUBSCRIPTION = gql`
  subscription ServiceLogLines {
    serviceLogLines
  }
`;

export const HISTORY_JOBS_QUERY = gql`
  query HistoryJobs($first: Int, $after: String, $filter: QueueFilterInput) {
    historyItems(first: $first, after: $after, filter: $filter) {
      ...FacadeHistoryItemFields
    }
  }
  ${PARSED_RELEASE_FIELDS}
  ${FACADE_HISTORY_ITEM_FIELDS}
`;

export const HISTORY_JOBS_COUNT_QUERY = gql`
  query HistoryJobsCount {
    all: historyItemsCount
    success: historyItemsCount(filter: { states: [COMPLETED] })
    failure: historyItemsCount(filter: { states: [FAILED] })
  }
`;

// --- Server management ---

export const SERVERS_QUERY = gql`
  query Servers {
    servers {
      ...ServerFields
    }
  }
  ${SERVER_FIELDS}
`;

export const ADD_SERVER_MUTATION = gql`
  mutation AddServer($input: ServerInput!) {
    addServer(input: $input) {
      ...ServerFields
    }
  }
  ${SERVER_FIELDS}
`;

export const UPDATE_SERVER_MUTATION = gql`
  mutation UpdateServer($id: Int!, $input: ServerInput!) {
    updateServer(id: $id, input: $input) {
      ...ServerFields
    }
  }
  ${SERVER_FIELDS}
`;

export const REMOVE_SERVER_MUTATION = gql`
  mutation RemoveServer($id: Int!) {
    removeServer(id: $id) {
      ...ServerFields
    }
  }
  ${SERVER_FIELDS}
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

// --- Categories ---

export const CATEGORIES_QUERY = gql`
  query Categories {
    categories {
      ...CategoryFields
    }
  }
  ${CATEGORY_FIELDS}
`;

export const ADD_CATEGORY_MUTATION = gql`
  mutation AddCategory($input: CategoryInput!) {
    addCategory(input: $input) {
      ...CategoryFields
    }
  }
  ${CATEGORY_FIELDS}
`;

export const UPDATE_CATEGORY_MUTATION = gql`
  mutation UpdateCategory($id: Int!, $input: CategoryInput!) {
    updateCategory(id: $id, input: $input) {
      ...CategoryFields
    }
  }
  ${CATEGORY_FIELDS}
`;

export const REMOVE_CATEGORY_MUTATION = gql`
  mutation RemoveCategory($id: Int!) {
    removeCategory(id: $id) {
      ...CategoryFields
    }
  }
  ${CATEGORY_FIELDS}
`;

// --- General settings ---

export const SETTINGS_QUERY = gql`
  query Settings {
    settings {
      ...GeneralSettingsFields
    }
    globalState: globalQueueState {
      downloadBlock {
        ...DownloadBlockFields
      }
    }
  }
  ${GENERAL_SETTINGS_FIELDS}
  ${ISP_BANDWIDTH_CAP_FIELDS}
  ${DOWNLOAD_BLOCK_FIELDS}
`;

export const UPDATE_SETTINGS_MUTATION = gql`
  mutation UpdateSettings($input: GeneralSettingsInput!) {
    updateSettings(input: $input) {
      ...GeneralSettingsFields
    }
  }
  ${GENERAL_SETTINGS_FIELDS}
  ${ISP_BANDWIDTH_CAP_FIELDS}
`;

// --- API Keys ---

export const API_KEYS_QUERY = gql`
  query ApiKeys {
    apiKeys {
      ...ApiKeyFields
    }
  }
  ${API_KEY_FIELDS}
`;

export const CREATE_API_KEY_MUTATION = gql`
  mutation CreateApiKey($name: String!, $scope: ApiKeyScope!) {
    createApiKey(name: $name, scope: $scope) {
      key {
        ...ApiKeyFields
      }
      rawKey
    }
  }
  ${API_KEY_FIELDS}
`;

export const DELETE_API_KEY_MUTATION = gql`
  mutation DeleteApiKey($id: Int!) {
    deleteApiKey(id: $id) {
      ...ApiKeyFields
    }
  }
  ${API_KEY_FIELDS}
`;

// --- Login Protection ---

export const LOGIN_STATUS_QUERY = gql`
  query LoginStatus {
    adminLoginStatus {
      enabled
      username
    }
  }
`;

export const ENABLE_LOGIN_MUTATION = gql`
  mutation EnableLogin($username: String!, $password: String!) {
    enableLogin(username: $username, password: $password)
  }
`;

export const DISABLE_LOGIN_MUTATION = gql`
  mutation DisableLogin {
    disableLogin
  }
`;

export const CHANGE_PASSWORD_MUTATION = gql`
  mutation ChangePassword($currentPassword: String!, $newPassword: String!) {
    changePassword(currentPassword: $currentPassword, newPassword: $newPassword)
  }
`;

// --- RSS ---

export const RSS_SETTINGS_QUERY = gql`
  query RssSettings {
    rssFeeds {
      ...RssFeedFields
    }
    rssSeenItems(limit: 200) {
      ...RssSeenItemFields
    }
    categories {
      ...CategoryFields
    }
  }
  ${RSS_RULE_FIELDS}
  ${RSS_FEED_FIELDS}
  ${RSS_SEEN_ITEM_FIELDS}
  ${CATEGORY_FIELDS}
`;

export const ADD_RSS_FEED_MUTATION = gql`
  mutation AddRssFeed($input: RssFeedInput!) {
    addRssFeed(input: $input) {
      ...RssFeedFields
    }
  }
  ${RSS_RULE_FIELDS}
  ${RSS_FEED_FIELDS}
`;

export const UPDATE_RSS_FEED_MUTATION = gql`
  mutation UpdateRssFeed($id: Int!, $input: RssFeedInput!) {
    updateRssFeed(id: $id, input: $input) {
      ...RssFeedFields
    }
  }
  ${RSS_RULE_FIELDS}
  ${RSS_FEED_FIELDS}
`;

export const DELETE_RSS_FEED_MUTATION = gql`
  mutation DeleteRssFeed($id: Int!) {
    deleteRssFeed(id: $id)
  }
`;

export const ADD_RSS_RULE_MUTATION = gql`
  mutation AddRssRule($feedId: Int!, $input: RssRuleInput!) {
    addRssRule(feedId: $feedId, input: $input) {
      ...RssRuleFields
    }
  }
  ${RSS_RULE_FIELDS}
`;

export const UPDATE_RSS_RULE_MUTATION = gql`
  mutation UpdateRssRule($id: Int!, $input: RssRuleInput!) {
    updateRssRule(id: $id, input: $input) {
      ...RssRuleFields
    }
  }
  ${RSS_RULE_FIELDS}
`;

export const DELETE_RSS_RULE_MUTATION = gql`
  mutation DeleteRssRule($id: Int!) {
    deleteRssRule(id: $id)
  }
`;

export const RUN_RSS_SYNC_MUTATION = gql`
  mutation RunRssSync($feedId: Int) {
    runRssSync(feedId: $feedId) {
      feedsPolled
      itemsFetched
      itemsNew
      itemsAccepted
      itemsSubmitted
      itemsIgnored
      errors
      feedResults {
        feedId
        feedName
        itemsFetched
        itemsNew
        itemsAccepted
        itemsSubmitted
        itemsIgnored
        errors
      }
    }
  }
`;

export const DELETE_RSS_SEEN_ITEM_MUTATION = gql`
  mutation DeleteRssSeenItem($feedId: Int!, $itemId: String!) {
    deleteRssSeenItem(feedId: $feedId, itemId: $itemId)
  }
`;

export const CLEAR_RSS_SEEN_ITEMS_MUTATION = gql`
  mutation ClearRssSeenItems($feedId: Int) {
    clearRssSeenItems(feedId: $feedId)
  }
`;

// ── Schedules ───────────────────────────────────────────────────────────────

export const SCHEDULES_QUERY = gql`
  query Schedules {
    schedules {
      id
      enabled
      label
      days
      time
      actionType
      speedLimitBytes
    }
  }
`;

export const CREATE_SCHEDULE_MUTATION = gql`
  mutation CreateSchedule($input: ScheduleInput!) {
    createSchedule(input: $input) {
      id
      enabled
      label
      days
      time
      actionType
      speedLimitBytes
    }
  }
`;

export const UPDATE_SCHEDULE_MUTATION = gql`
  mutation UpdateSchedule($id: String!, $input: ScheduleInput!) {
    updateSchedule(id: $id, input: $input) {
      id
      enabled
      label
      days
      time
      actionType
      speedLimitBytes
    }
  }
`;

export const DELETE_SCHEDULE_MUTATION = gql`
  mutation DeleteSchedule($id: String!) {
    deleteSchedule(id: $id) {
      id
      enabled
      label
      days
      time
      actionType
      speedLimitBytes
    }
  }
`;

export const TOGGLE_SCHEDULE_MUTATION = gql`
  mutation ToggleSchedule($id: String!, $enabled: Boolean!) {
    toggleSchedule(id: $id, enabled: $enabled) {
      id
      enabled
      label
      days
      time
      actionType
      speedLimitBytes
    }
  }
`;
