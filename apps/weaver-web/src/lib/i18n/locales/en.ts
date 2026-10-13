import type { LocaleDictionary } from "../types";
import { duplicateLocaleEntries } from "../duplicate-locales";
import { nextEn } from "@/next/i18n/en";

const en: LocaleDictionary = {
  "settings.networking": "Networking",
  "settings.networkOverview": "Overview",
  "settings.networkEgress": "Egress",
  "settings.networkRoutes": "Routes",
  "settings.networkingDesc": "Egress interfaces, proxy pools and consumer routes",
  "settings.proxies": "Proxies",

  // Navigation
  "nav.settings": "Settings",
  "nav.sponsor": "Sponsor",
  // In-application upgrade
  "applicationUpgrade.title": "Application update",
  "applicationUpgrade.upToDate": "Weaver v{{version}} is up to date.",
  "applicationUpgrade.available": "Weaver v{{version}} is available.",
  "applicationUpgrade.install": "Install v{{version}}",
  "applicationUpgrade.notEligible": "Weaver cannot update this installation in-app ({{reason}}).",
  "applicationUpgrade.failed": "The update to v{{version}} failed: {{error}}",
  "applicationUpgrade.completed": "Updated to v{{version}}.",
  "applicationUpgrade.phase.checking": "Checking the release…",
  "applicationUpgrade.phase.downloading": "Downloading v{{version}}…",
  "applicationUpgrade.phase.verifying": "Verifying the download…",
  "applicationUpgrade.phase.staging": "Unpacking the update…",
  "applicationUpgrade.phase.applying": "Installing v{{version}}…",
  "applicationUpgrade.phase.awaiting_elevation": "Waiting for permission to install…",
  "applicationUpgrade.phase.restarting": "Restarting Weaver…",
  "applicationUpgrade.phase.reboot_required": "Restart this computer to finish updating.",
  "update.newVersionAria": "Open Weaver v{{version}} on GitHub in a new tab",

  // System information
  "systemInfo.diagnosticsFailed": "The diagnostics package could not be created.",

  // Status labels
  "status.queued": "Queued",
  "status.downloading": "Downloading",
  "status.propagating": "Propagating",
  "status.propagationUntil": "Waiting for propagation. Download starts at {{time}}.",
  "status.fetchingRepairData": "Fetching repair data",
  "status.verifying": "Verifying",
  "status.repairing": "Repairing",
  "status.extracting": "Extracting",
  "status.postProcessing": "Post-processing",
  "status.awaitingQueueScripts": "Waiting for queue scripts",
  "status.moving": "Moving",
  "status.complete": "Complete",
  "status.failed": "Failed",
  "status.paused": "Paused",
  "status.finalizing": "Finalizing",
  "status.cancelled": "Cancelled",
  "phase.downloading": "Downloading",
  "phase.repairing": "Repairing",
  "phase.extracting": "Extracting",
  "phase.moving": "Moving",

  // Actions
  "action.pause": "Pause",
  "action.resume": "Resume",
  "action.cancel": "Cancel",
  "action.pauseAll": "Pause All",
  "action.resumeAll": "Resume All",
  "action.apply": "Apply",
  "action.submit": "Submit",
  "action.uploading": "Uploading...",
  "action.backToJobs": "Back to queue",
  "action.delete": "Delete",
  "action.reprocess": "Reprocess",
  "action.rerunPostProcessing": "Re-run scripts",
  "action.redownload": "Re-download",
  "action.refresh": "Refresh",
  "action.clearFilters": "Clear Filters",
  "action.previous": "Previous",
  "action.next": "Next",

  // Confirmation dialogs
  "confirm.cancelSelectedMessage": "These downloads will be stopped and cannot be resumed.",
  "bulk.selected": "{{count}} selected",
  "bulk.editSelected": "Edit Selected",
  "bulk.pauseSelected": "Pause Selected",
  "bulk.cancelSelected": "Cancel Selected",
  "bulk.editTitle": "Edit Selected Jobs",
  "bulk.noChange": "— No change —",
  "action.deleteAll": "Delete All",

  // Upload page
  "upload.invalidFiles": "Please select only .nzb or .nzb.xz files.",
  "upload.noCategory": "No category",
  "upload.priorityLow": "Low",
  "upload.priorityNormal": "Normal",
  "upload.priorityHigh": "High",
  "upload.removeFile": "Remove File",
  "upload.rejected": "Upload was rejected by the server.",
  "upload.stageExpired": "Staged upload expired. Re-add the file.",

  // Jobs page
  "jobs.egressQuotaBadge": "Egress quota",
  "jobs.egressQuotaEta": "Until {{resetAt}}",
  "jobs.serverQuotaBadge": "Server Quota",
  "jobs.serverQuotaEta": "Waiting for server quota",

  // Table headers
  "table.name": "Name",
  "table.status": "Status",
  "table.priority": "Priority",
  "table.progress": "Progress",
  "table.size": "Size",

  // Timeline
  "timeline.finalizingDownload": "Finalizing Download",
  "timeline.totalDuration": "Total",

  // Settings page
  "settings.unlimited": "Unlimited",

  // History page
  "history.filterAll": "All",
  "table.category": "Category",
  "table.rowsPerPage": "Rows per page",

  // Servers page
  "servers.password": "Password",

  // Categories
  "categories.title": "Categories",

  // General settings
  "settings.saving": "Saving…",

  // Metrics labels
  "metrics.downloaded": "Downloaded",
  "metrics.decoded": "Decoded",
  "metrics.committed": "Committed",
  "metrics.downloadSpeed": "Download Speed",
  "metrics.downloadQueue": "Download Queue",
  "metrics.decodePending": "Decode Pending",
  "metrics.commitPending": "Commit Pending",
  "metrics.writeBufferedBytes": "Write Buffered Bytes",
  "metrics.writeBufferedSegments": "Write Buffered Segments",
  "metrics.diskWriteLatency": "Disk Write Latency",
  "metrics.segmentsDownloaded": "Segments Downloaded",
  "metrics.segmentsDecoded": "Segments Decoded",
  "metrics.segmentsCommitted": "Segments Committed",
  "metrics.articlesNotFound": "Articles Not Found",
  "metrics.decodeErrors": "Decode Errors",
  "metrics.crcErrors": "CRC Errors",
  "metrics.recoveryQueue": "Recovery Queue",
  "metrics.articlesPerSec": "Articles / Sec",
  "metrics.decodeRate": "Decode Rate",
  "metrics.verifyActive": "Verify Active",
  "metrics.repairActive": "Repair Active",
  "metrics.extractActive": "Extract Active",
  "metrics.segmentsRetried": "Segments Retried",
  "metrics.failedPermanent": "Failed Permanent",
  "metrics.serverBodyDepth": "BODY depth",
  "metrics.serverBodyDepthSequential": "seq",
  "metrics.serverLatencyBand.good": "good",
  "metrics.serverLatencyBand.moderate": "moderate",
  "metrics.serverLatencyBand.slow": "slow",
  "metrics.range10m": "10m",
  "metrics.range1h": "1h",
  "metrics.range6h": "6h",
  "metrics.range24h": "24h",
  "metrics.range7d": "7d",
  "metrics.range30d": "30d",
  "metrics.downloadThroughputChart": "Download Throughput",
  "metrics.downloadThroughputDesc": "Per-second byte flow derived from stored download, decode, and commit counters.",
  "metrics.segmentsChart": "Segments",
  "metrics.segmentsDesc": "How quickly the pipeline is downloading, decoding, committing, retrying, and failing segments.",
  "metrics.errorsChart": "Errors",
  "metrics.errorsDesc": "Counter-derived failure rates that reveal article gaps, decode failures, and CRC trouble.",
  "metrics.downloadSpeedChart": "Download Speed",
  "metrics.downloadSpeedDesc": "Raw sampled download speed as reported by the live pipeline metrics exporter.",
  "metrics.queueDepthsChart": "Queue Depths",
  "metrics.queueDepthsDesc": "Backlog growth across download, decode, commit, and recovery stages.",
  "metrics.activeWorkersChart": "Active Workers",
  "metrics.activeWorkersDesc": "Live worker pressure across verify, repair, and extraction stages.",
  "metrics.writeBufferChart": "Write Buffer",
  "metrics.writeBufferDesc": "Buffered bytes and buffered segment counts waiting to hit disk.",
  "metrics.diskWriteLatencyChart": "Disk Write Latency",
  "metrics.diskWriteLatencyDesc": "Raw write latency from the pipeline's disk path, stored as microseconds.",
  "metrics.throughputRatesChart": "Throughput Rates",
  "metrics.throughputRatesDesc": "Current articles per second beside decode throughput for quick pacing checks.",

  // API Keys
  "action.edit": "Edit",
  "action.save": "Save",

  // Duplicate handling
  "upload.forceDesc": "Accept this submission despite semantic and article-fingerprint duplicate policy. Idempotency conflicts still apply.",
  ...duplicateLocaleEntries.eng,
  ...nextEn,
};

export default en;
