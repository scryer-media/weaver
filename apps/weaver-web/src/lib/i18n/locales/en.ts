import type { LocaleDictionary } from "../types";

const en: LocaleDictionary = {
  // Brand
  "brand": "weaver",

  // Navigation
  "nav.jobs": "Jobs",
  "nav.upload": "Upload",
  "nav.settings": "Settings",

  // Status labels
  "status.queued": "Queued",
  "status.downloading": "Downloading",
  "status.verifying": "Verifying",
  "status.repairing": "Repairing",
  "status.extracting": "Extracting",
  "status.complete": "Complete",
  "status.failed": "Failed",
  "status.paused": "Paused",
  "status.checking": "Checking",
  "status.propagating": "Propagating",

  // Actions
  "action.pause": "Pause",
  "action.resume": "Resume",
  "action.cancel": "Cancel",
  "action.pauseAll": "Pause All",
  "action.resumeAll": "Resume All",
  "action.apply": "Apply",
  "action.submit": "Submit",
  "action.uploading": "Uploading...",
  "action.backToJobs": "Back to jobs",
  "action.delete": "Delete",
  "action.reprocess": "Reprocess",

  // Confirmation dialogs
  "confirm.cancelJob": "Cancel this job?",
  "confirm.cancelJobMessage": "The download will be stopped and cannot be resumed.",
  "confirm.cancelJobConfirm": "Cancel Download",
  "confirm.cancelJobDismiss": "Keep Downloading",
  "confirm.deleteServer": "Delete this server?",
  "confirm.deleteServerMessage": "This server configuration will be permanently removed.",

  // Upload page
  "upload.title": "Upload NZB",
  "upload.dropzone": "Drop an NZB file here or click to browse",
  "upload.accepts": "Accepts .nzb files",
  "upload.replaceHint": "Click or drag to replace",
  "upload.passwordLabel": "Password (optional)",
  "upload.passwordPlaceholder": "Enter password if required",
  "upload.invalidFile": "Please select an NZB file.",

  // Jobs page
  "jobs.title": "Jobs",
  "jobs.empty": "No jobs.",
  "jobs.emptyAction": "Upload an NZB",
  "jobs.emptyHint": "to get started.",
  "jobs.passwordProtected": "[PW]",

  // Table headers
  "table.name": "Name",
  "table.status": "Status",
  "table.progress": "Progress",
  "table.size": "Size",
  "table.eta": "ETA",
  "table.actions": "Actions",
  "table.time": "Time",
  "table.kind": "Kind",
  "table.message": "Message",

  // Job detail
  "job.notFound": "Job not found.",
  "job.passwordProtected": "Password protected",
  "job.eventLog": "Event Log",
  "job.waitingForEvents": "Waiting for events...",

  // Labels
  "label.loading": "Loading...",
  "label.downloaded": "Downloaded",
  "label.totalSize": "Total Size",
  "label.progress": "Progress",
  "label.downloadSpeed": "Download Speed",

  // Settings page
  "settings.title": "Settings",
  "settings.downloadControl": "Download Control",
  "settings.globalPause": "Global Pause",
  "settings.pausedDesc": "All downloads are paused",
  "settings.activeDesc": "Downloads are active",
  "settings.speedLimit": "Speed Limit",
  "settings.unlimited": "Unlimited",
  "settings.metrics": "Metrics",
  "settings.theme": "Theme",
  "settings.themeLight": "Light",
  "settings.themeDark": "Dark",
  "settings.themeSystem": "System",

  // History page
  "nav.history": "History",
  "history.title": "History",
  "history.empty": "No completed jobs yet.",
  "table.health": "Health",
  "table.category": "Category",

  // Servers page
  "nav.servers": "Servers",
  "servers.title": "Servers",
  "servers.empty": "No servers configured.",
  "servers.emptyHint": "Add an NNTP server to start downloading.",
  "servers.addServer": "Add Server",
  "servers.editServer": "Edit Server",
  "servers.host": "Host",
  "servers.port": "Port",
  "servers.tls": "TLS",
  "servers.username": "Username",
  "servers.password": "Password",
  "servers.connections": "Connections",
  "servers.active": "Active",
  "servers.testConnection": "Test Connection",
  "servers.testing": "Testing...",
  "servers.testSuccess": "Connected",
  "servers.testFailed": "Connection failed",
  "servers.deleteConfirm": "Delete this server?",

  // General settings
  "settings.general": "General",
  "settings.intermediateDir": "Intermediate Directory",
  "settings.intermediateDirDesc": "Active downloads are stored here in per-job subdirectories",
  "settings.completeDir": "Complete Directory",
  "settings.completeDirDesc": "Finished downloads are moved here, organized by category",
  "settings.dataDir": "Data Directory",
  "settings.cleanupAfterExtract": "Cleanup After Extract",
  "settings.cleanupDesc": "Delete intermediate files after successful extraction",
  "settings.maxRetries": "Max Retries",
  "settings.save": "Save",
  "settings.saved": "Saved",

  // Metrics labels
  "metrics.downloaded": "Downloaded",
  "metrics.decoded": "Decoded",
  "metrics.committed": "Committed",
  "metrics.downloadSpeed": "Download Speed",
  "metrics.downloadQueue": "Download Queue",
  "metrics.decodePending": "Decode Pending",
  "metrics.commitPending": "Commit Pending",
  "metrics.segmentsDownloaded": "Segments Downloaded",
  "metrics.segmentsDecoded": "Segments Decoded",
  "metrics.segmentsCommitted": "Segments Committed",
  "metrics.articlesNotFound": "Articles Not Found",
  "metrics.decodeErrors": "Decode Errors",
  "metrics.verifyActive": "Verify Active",
  "metrics.repairActive": "Repair Active",
  "metrics.extractActive": "Extract Active",
  "metrics.segmentsRetried": "Segments Retried",
  "metrics.failedPermanent": "Failed Permanent",
};

export default en;
