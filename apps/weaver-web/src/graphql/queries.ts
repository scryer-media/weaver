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
    phaseProgress {
      phase
      completedBytes
      totalBytes
      progressPercent
      rateBps
      estimatedRemainingMs
      startedAtEpochMs
      updatedAtEpochMs
    }
    failedBytes
    health
    hasPassword
    category
    duplicateSummary {
      lifecycle
      action
      primaryReason
      semantic {
        score
        state
      }
    }
    metadata: attributes {
      key
      value
    }
  }
`;

export const FACADE_HISTORY_ITEM_FIELDS = `
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
    duplicateSummary {
      lifecycle
      action
      primaryReason
      semantic {
        score
        state
      }
    }
    deleteOperation {
      operationId
      state
      locked
      deleteFiles
      errorMessage
    }
    metadata: attributes {
      key
      value
    }
  }
`;

export const HISTORY_TABLE_ITEM_FIELDS = `
  fragment HistoryTableItemFields on HistoryItem {
    id
    name
    displayTitle
    originalTitle
    status: state
    totalBytes
    health
    category
    completedAt
    duplicateSummary {
      lifecycle
      action
      primaryReason
      semantic {
        score
        state
      }
    }
    deleteOperation {
      operationId
      state
      locked
      deleteFiles
      errorMessage
    }
  }
`;

export const PARSED_RELEASE_FIELDS = `
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
    connections
    active
    supportsPipelining
    priority
    backfill
    retentionDays
    maxDownloadSpeed
    downloadQuota {
      enabled
      period
      limitBytes
      resetTimeMinutesLocal
      weeklyResetWeekday
      monthlyResetDay
      usedBytes
      reservedBytes
      remainingBytes
      blocked
      windowStartsAtEpochMs
      windowEndsAtEpochMs
      timezoneName
    }
  }
`;

const SERVER_DETAILS_FIELDS = `
  fragment ServerDetailsFields on ServerDetails {
    id
    host
    port
    tls
    username
    connections
    active
    supportsPipelining
    priority
    backfill
    retentionDays
    maxDownloadSpeed
    downloadQuota {
      enabled
      period
      limitBytes
      resetTimeMinutesLocal
      weeklyResetWeekday
      monthlyResetDay
      usedBytes
      reservedBytes
      remainingBytes
      blocked
      windowStartsAtEpochMs
      windowEndsAtEpochMs
      timezoneName
    }
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

export const CREATE_DIRECTORY_MUTATION = gql`
  mutation CreateDirectory($path: String!, $name: String!) {
    createDirectory(path: $path, name: $name) {
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
    ipReplacementTrialExtraConnections
    duplicatePolicy {
      strictActiveOrSuccess
      strictFailedOrCancelled
      articleLayoutActiveOrSuccess
      articleLayoutFailedOrCancelled
      articleSet
      normalizedName
    }
    ispBandwidthCap {
      ...IspBandwidthCapFields
    }
    watchFolder {
      mode
      path
      pollIntervalSecs
      stabilitySecs
      categoryFromSubfolders
      scanningPaused
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
    activeDownloads
    activeDecodes
    decodePending
    decodePendingBytes
    decodeActiveBytes
    commitPending
    writeBufferedBytes
    writeBufferedSegments
    directWriteEvictions
    decodePressureSoftLimitBytes
    decodePressureHardLimitBytes
    writePressureSoftLimitBytes
    writePressureHardLimitBytes
    downloadPressureState
    downloadPressureReason
    downloadPressureStallsTotal
    downloadPressureStallDurationMs
    downloadPressureCurrentStallMs
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
    downloadFailuresArticleNotFound
    downloadFailuresCapacityUnavailable
    downloadFailuresTransient
    downloadFailuresAuth
    downloadFailuresPermanent
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
    queueSnapshot {
      items {
        ...FacadeQueueItemFields
      }
      latestCursor
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

export const JOB_QUERY = gql`
  query Job($id: Int!) {
    jobDetailSnapshot(jobId: $id) {
      queueItem {
        ...FacadeQueueItemFields
        error
        outputDir
        createdAt
      }
      historyItem {
        ...FacadeHistoryItemFields
        error
      }
      jobTimeline {
        ...JobTimelineFields
      }
      jobEvents {
        kind
        jobId
        fileId
        message
        timestamp
      }
    }
  }
  ${JOB_TIMELINE_FIELDS}
  ${PARSED_RELEASE_FIELDS}
  ${FACADE_QUEUE_ITEM_FIELDS}
  ${FACADE_HISTORY_ITEM_FIELDS}
`;

export const DUPLICATE_SNAPSHOT_QUERY = gql`
  query DuplicateSnapshot($id: Int!) {
    duplicateSnapshot(id: $id) {
      jobId
      lifecycle
      normalizedName
      semantic {
        groupId
        normalizedKey
        score
        state
        terminalCause
        promotionState
      }
    }
  }
`;

export const MARK_DUPLICATE_GOOD_MUTATION = gql`
  mutation MarkDuplicateGood($id: Int!) {
    markDuplicateGood(id: $id)
  }
`;

export const MARK_DUPLICATE_BAD_MUTATION = gql`
  mutation MarkDuplicateBad($id: Int!) {
    markDuplicateBad(id: $id) {
      accepted
      jobId
      message
    }
  }
`;

export const PROMOTE_DUPLICATE_CANDIDATE_MUTATION = gql`
  mutation PromoteDuplicateCandidate($id: Int!) {
    promoteDuplicateCandidate(id: $id) {
      accepted
      jobId
      message
    }
  }
`;

export const FORGET_DUPLICATE_IDENTITY_MUTATION = gql`
  mutation ForgetDuplicateIdentity($id: Int!) {
    forgetDuplicateIdentity(id: $id)
  }
`;

export const JOB_DETAIL_UPDATES_SUBSCRIPTION = gql`
  subscription JobDetailUpdates($id: Int!) {
    jobDetailUpdates(jobId: $id) {
      queueItem {
        ...FacadeQueueItemFields
        error
        outputDir
        createdAt
      }
      historyItem {
        ...FacadeHistoryItemFields
        error
      }
      jobTimeline {
        ...JobTimelineFields
      }
      jobEvents {
        kind
        jobId
        fileId
        message
        timestamp
      }
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

export const LIVE_METRICS_QUERY = gql`
  query LiveMetrics {
    metrics: systemMetrics {
      currentDownloadSpeed
    }
    globalState: globalQueueState {
      isPaused
      downloadBlock {
        ...DownloadBlockFields
      }
    }
  }
  ${DOWNLOAD_BLOCK_FIELDS}
`;

export const DISK_USAGE_QUERY = gql`
  query DiskUsage {
    diskUsage {
      label
      path
      totalBytes
      usedBytes
      freeBytes
    }
  }
`;

export const SERVER_HEALTH_QUERY = gql`
  query ServerHealth {
    serverHealth {
      host
      port
      label
      tier
      state
      connectionsActive
      connectionsMax
      connectionsConfigured
      connectionsEffective
      capacityPenaltyUntilEpochMs
      runtimeGeneration
      latencyMs
      successCount
      failureCount
      consecutiveFailures
      prematureDeaths
    }
  }
`;

export const METRICS_HISTORY_QUERY = gql`
  query MetricsHistory($range: MetricsHistoryRangeGql!) {
    metricsHistory(range: $range) {
      timestamps
      resolutionSec
      series {
        metric
        variant
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

export const ACCEPT_HISTORY_DELETE_MUTATION = gql`
  mutation AcceptHistoryDelete($input: AcceptHistoryDeleteInput!) {
    acceptHistoryDelete(input: $input) {
      operationId
      totalTargets
    }
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

export const QUEUE_EVENTS_SUBSCRIPTION = gql`
  subscription QueueEvents($after: String) {
    queueEvents(after: $after) {
      cursor
      kind
      itemId
      state
      previousState
      item {
        ...FacadeQueueItemFields
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

export const LIVE_METRICS_SUBSCRIPTION = gql`
  subscription LiveMetricsUpdates {
    systemMetricsUpdates {
      metrics {
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
  ${DOWNLOAD_BLOCK_FIELDS}
`;

export const METRICS_PAGE_SUBSCRIPTION = gql`
  subscription MetricsPageUpdates {
    systemMetricsUpdates {
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

export const HISTORY_PAGE_QUERY = gql`
  query HistoryPage($input: HistoryPageInput!) {
    historyPage(input: $input) {
      items {
        ...HistoryTableItemFields
      }
      totalCount
      counts {
        all
        success
        failure
      }
    }
  }
  ${HISTORY_TABLE_ITEM_FIELDS}
`;

export const HISTORY_DELETE_OPERATIONS_QUERY = gql`
  query HistoryDeleteOperations($activeOnly: Boolean) {
    historyDeleteOperations(activeOnly: $activeOnly) {
      id
      state
      deleteFiles
      totalTargets
      queuedTargets
      runningTargets
      completedTargets
      failedTargets
      requestedAt
    }
  }
`;

export const HISTORY_JOBS_COUNT_QUERY = gql`
  query HistoryJobsCount {
    all: historyItemsCount
    success: historyItemsCount(filter: { states: [COMPLETED] })
    failure: historyItemsCount(filter: { states: [FAILED] })
  }
`;

// --- Server management ---

export const HAS_CONFIGURED_SERVERS_QUERY = gql`
  query HasConfiguredServers {
    hasConfiguredServers
  }
`;

export const SERVERS_QUERY = gql`
  query Servers {
    servers {
      ...ServerFields
    }
  }
  ${SERVER_FIELDS}
`;

export const SERVER_QUERY = gql`
  query Server($id: Int!) {
    server(id: $id) {
      ...ServerDetailsFields
    }
  }
  ${SERVER_DETAILS_FIELDS}
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

export const RESET_SERVER_DOWNLOAD_QUOTA_USAGE_MUTATION = gql`
  mutation ResetServerDownloadQuotaUsage($id: Int!) {
    resetServerDownloadQuotaUsage(id: $id) {
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

export const WATCH_FOLDER_SETTINGS_QUERY = gql`
  query WatchFolderSettings {
    settings {
      watchFolder {
        mode
        path
        pollIntervalSecs
        stabilitySecs
        categoryFromSubfolders
        scanningPaused
      }
    }
  }
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

export const SCAN_WATCH_FOLDER_MUTATION = gql`
  mutation ScanWatchFolder {
    scanWatchFolder {
      discoveredFiles
      queuedNzbs
      skippedInputs {
        path
        reason
      }
      permanentErrors {
        path
        reason
      }
      transientErrors {
        path
        reason
      }
      markerRenamedSources {
        from
        to
        marker
      }
    }
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

export const POST_PROCESSING_SETTINGS_QUERY = gql`
  query PostProcessingSettings {
    postProcessingSettings {
      discoveryEnabled
      executionEnabled
      concurrency
      terminationGraceSeconds
      pythonInterpreter
      powershellInterpreter
      batchInterpreter
      webhooksEnabled
      allowedRoots
    }
    postProcessingRevisions {
      extensionId
      revisionId
      declaredVersion
      digest
      adapter
      displayName
      trustState
      managed
      sourcePath
      discoveredAtEpochMs
      approvedAtEpochMs
      manifest
    }
    postProcessingProfiles {
      profileId
      name
      enabled
      createdAtEpochMs
      updatedAtEpochMs
      definition
    }
  }
`;

export const UPDATE_POST_PROCESSING_SETTINGS_MUTATION = gql`
  mutation UpdatePostProcessingSettings($input: PostProcessingSettingsInput!) {
    updatePostProcessingSettings(input: $input) {
      discoveryEnabled
      executionEnabled
      concurrency
      terminationGraceSeconds
      pythonInterpreter
      powershellInterpreter
      batchInterpreter
      allowedRoots
    }
  }
`;

export const DISCOVER_POST_PROCESSING_EXTENSIONS_MUTATION = gql`
  mutation DiscoverPostProcessingExtensions {
    discoverPostProcessingExtensions {
      extensionId
      revisionId
      declaredVersion
      digest
      adapter
      displayName
      trustState
      managed
      sourcePath
      discoveredAtEpochMs
      approvedAtEpochMs
      manifest
    }
  }
`;

export const APPROVE_POST_PROCESSING_REVISION_MUTATION = gql`
  mutation ApprovePostProcessingRevision($extensionId: String!, $revisionId: String!) {
    approvePostProcessingRevision(extensionId: $extensionId, revisionId: $revisionId) {
      extensionId
      revisionId
      trustState
      managed
    }
  }
`;

export const DISABLE_POST_PROCESSING_REVISION_MUTATION = gql`
  mutation DisablePostProcessingRevision($extensionId: String!, $revisionId: String!) {
    disablePostProcessingRevision(extensionId: $extensionId, revisionId: $revisionId)
  }
`;

export const REVOKE_POST_PROCESSING_REVISION_MUTATION = gql`
  mutation RevokePostProcessingRevision($extensionId: String!, $revisionId: String!) {
    revokePostProcessingRevision(extensionId: $extensionId, revisionId: $revisionId)
  }
`;

export const SAVE_POST_PROCESSING_PROFILE_MUTATION = gql`
  mutation SavePostProcessingProfile($input: PostProcessingProfileInput!) {
    savePostProcessingProfile(input: $input) {
      profileId
      name
      enabled
      updatedAtEpochMs
      definition
    }
  }
`;

export const DELETE_POST_PROCESSING_PROFILE_MUTATION = gql`
  mutation DeletePostProcessingProfile($profileId: String!) {
    deletePostProcessingProfile(profileId: $profileId)
  }
`;

export const ASSIGN_GLOBAL_POST_PROCESSING_PROFILE_MUTATION = gql`
  mutation AssignGlobalPostProcessingProfile($profileId: String) {
    assignGlobalPostProcessingProfile(profileId: $profileId)
  }
`;

export const ASSIGN_CATEGORY_POST_PROCESSING_PROFILE_MUTATION = gql`
  mutation AssignCategoryPostProcessingProfile($category: String!, $profileId: String) {
    assignCategoryPostProcessingProfile(category: $category, profileId: $profileId)
  }
`;

const POST_PROCESSING_RUN_FIELDS = gql`
  fragment PostProcessingRunFields on PostProcessingRun {
    runId
    jobId
    status
    pipelineOutcome
    summary
    terminalIntent
    plan
    rerunOfRunId
    queuedAtEpochMs
    queuePosition
    startedAtEpochMs
    finishedAtEpochMs
  }
`;

export const POST_PROCESSING_QUEUE_QUERY = gql`
  query PostProcessingQueue {
    postProcessingQueue {
      ...PostProcessingRunFields
    }
  }
  ${POST_PROCESSING_RUN_FIELDS}
`;

export const POST_PROCESSING_JOB_QUERY = gql`
  query PostProcessingJob($jobId: Int!) {
    postProcessingJobPlan(jobId: $jobId) {
      jobId
      definition
    }
    postProcessingRuns(jobId: $jobId, limit: 100) {
      ...PostProcessingRunFields
    }
  }
  ${POST_PROCESSING_RUN_FIELDS}
`;

export const POST_PROCESSING_ATTEMPTS_QUERY = gql`
  query PostProcessingAttempts($runId: String!) {
    postProcessingAttempts(runId: $runId) {
      attemptId
      runId
      stepIndex
      status
      extensionId
      revisionId
      adapter
      workingDirectory
      exitCode
      errorMessage
      progress
      outputTruncated
      queuedAtEpochMs
      startedAtEpochMs
      finishedAtEpochMs
    }
  }
`;

export const POST_PROCESSING_ARTIFACTS_QUERY = gql`
  query PostProcessingArtifacts($runId: String!) {
    postProcessingArtifacts(runId: $runId) {
      attemptId
      stepIndex
      path
      exists
      isFile
      isDirectory
      isSymlink
      sizeBytes
    }
  }
`;

export const POST_PROCESSING_LOGS_QUERY = gql`
  query PostProcessingLogs($attemptId: String!, $cursor: Int, $limit: Int!) {
    postProcessingLogs(attemptId: $attemptId, cursor: $cursor, limit: $limit) {
      chunks {
        sequence
        stream
        text
        createdAtEpochMs
      }
      nextCursor
      truncated
    }
  }
`;

export const PAUSE_POST_PROCESSING_QUEUE_MUTATION = gql`
  mutation PausePostProcessingQueue {
    pausePostProcessingQueue
  }
`;

export const RESUME_POST_PROCESSING_QUEUE_MUTATION = gql`
  mutation ResumePostProcessingQueue {
    resumePostProcessingQueue
  }
`;

export const REORDER_POST_PROCESSING_QUEUE_MUTATION = gql`
  mutation ReorderPostProcessingQueue($runIds: [String!]!) {
    reorderPostProcessingQueue(runIds: $runIds)
  }
`;

export const CANCEL_JOB_POST_PROCESSING_MUTATION = gql`
  mutation CancelJobPostProcessing($jobId: Int!) {
    cancelJobPostProcessing(jobId: $jobId)
  }
`;

export const RERUN_POST_PROCESSING_MUTATION = gql`
  mutation RerunPostProcessing($input: PostProcessingRerunInput!) {
    rerunPostProcessing(input: $input) {
      ...PostProcessingRunFields
    }
  }
  ${POST_PROCESSING_RUN_FIELDS}
`;

export const SET_JOB_POST_PROCESSING_SELECTION_MUTATION = gql`
  mutation SetJobPostProcessingSelection($jobId: Int!, $selection: PostProcessingSelectionInput!) {
    setJobPostProcessingSelection(jobId: $jobId, selection: $selection) {
      jobId
      definition
    }
  }
`;
