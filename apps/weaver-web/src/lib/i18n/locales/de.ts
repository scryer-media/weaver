import type { LocaleDictionary } from "../types";
import { duplicateLocaleEntries } from "../duplicate-locales";
import { nextDe } from "@/next/i18n/de";

const de: LocaleDictionary = {

  // Navigation
  "nav.settings": "Einstellungen",
  "nav.sponsor": "Sponsern",
  // In-application upgrade
  "applicationUpgrade.title": "Anwendungsupdate",
  "applicationUpgrade.upToDate": "Weaver v{{version}} ist aktuell.",
  "applicationUpgrade.available": "Weaver v{{version}} ist verfügbar.",
  "applicationUpgrade.install": "v{{version}} installieren",
  "applicationUpgrade.notEligible": "Weaver kann diese Installation nicht innerhalb der Anwendung aktualisieren ({{reason}}).",
  "applicationUpgrade.failed": "Das Update auf v{{version}} ist fehlgeschlagen: {{error}}",
  "applicationUpgrade.completed": "Auf v{{version}} aktualisiert.",
  "applicationUpgrade.phase.checking": "Release wird geprüft …",
  "applicationUpgrade.phase.downloading": "v{{version}} wird heruntergeladen …",
  "applicationUpgrade.phase.verifying": "Download wird überprüft …",
  "applicationUpgrade.phase.staging": "Update wird entpackt …",
  "applicationUpgrade.phase.applying": "v{{version}} wird installiert …",
  "applicationUpgrade.phase.awaiting_elevation": "Warten auf Installationsberechtigung …",
  "applicationUpgrade.phase.restarting": "Weaver wird neu gestartet …",
  "applicationUpgrade.phase.reboot_required": "Starten Sie diesen Computer neu, um das Update abzuschließen.",
  "update.newVersionAria": "Weaver v{{version}} auf GitHub in einem neuen Tab öffnen",

  // Status labels
  "status.queued": "Eingereiht",
  "status.downloading": "Wird heruntergeladen",
  "status.fetchingRepairData": "Reparaturdaten werden abgerufen",
  "status.verifying": "Wird verifiziert",
  "status.repairing": "Wird repariert",
  "status.extracting": "Wird entpackt",
  "status.postProcessing": "Nachbearbeitung",
  "status.moving": "Wird verschoben",
  "status.complete": "Abgeschlossen",
  "status.failed": "Fehlgeschlagen",
  "status.paused": "Pausiert",
  "phase.downloading": "Wird heruntergeladen",
  "phase.repairing": "Wird repariert",
  "phase.extracting": "Wird entpackt",
  "phase.moving": "Wird verschoben",

  // Actions
  "action.pause": "Pause",
  "action.resume": "Fortsetzen",
  "action.cancel": "Abbrechen",
  "action.pauseAll": "Alle pausieren",
  "action.resumeAll": "Alle fortsetzen",
  "action.apply": "Übernehmen",
  "action.submit": "Absenden",
  "action.uploading": "Wird hochgeladen...",
  "action.backToJobs": "Zurück zur Warteschlange",
  "action.delete": "Löschen",
  "action.reprocess": "Erneut verarbeiten",
  "action.rerunPostProcessing": "Re-run scripts",
  "action.refresh": "Aktualisieren",
  "action.clearFilters": "Filter zurücksetzen",
  "action.previous": "Vorherige",
  "action.next": "Nächste",

  // Confirmation dialogs
  "confirm.cancelSelectedMessage": "Diese Downloads werden gestoppt und können nicht fortgesetzt werden.",
  "bulk.selected": "{{count}} ausgewählt",
  "bulk.editSelected": "Ausgewählte bearbeiten",
  "bulk.pauseSelected": "Ausgewählte pausieren",
  "bulk.cancelSelected": "Ausgewählte abbrechen",
  "bulk.editTitle": "Ausgewählte Aufträge bearbeiten",
  "bulk.noChange": "— Keine Änderung —",
  "action.deleteAll": "Alle löschen",

  // Upload page
  "upload.invalidFiles": "Bitte nur .nzb- oder .nzb.xz-Dateien auswählen.",
  "upload.noCategory": "Keine Kategorie",
  "upload.priorityLow": "Niedrig",
  "upload.priorityNormal": "Normal",
  "upload.priorityHigh": "Hoch",
  "upload.removeFile": "Datei entfernen",
  "upload.rejected": "Der Upload wurde vom Server abgelehnt.",
  "upload.stageExpired": "Staged upload expired. Re-add the file.",

  // Jobs page
  "jobs.bandwidthCapShort": "ISP-Limit",
  "jobs.bandwidthCapEta": "Bis {{resetAt}}",
  "jobs.serverQuotaBadge": "Serverkontingent",
  "jobs.serverQuotaEta": "Warten auf Serverkontingent",

  // Table headers
  "table.name": "Name",
  "table.status": "Status",
  "table.priority": "Priorität",
  "table.progress": "Fortschritt",
  "table.size": "Größe",

  // Timeline
  "timeline.finalizingDownload": "Download wird finalisiert",
  "timeline.totalDuration": "Gesamt",

  // Settings page
  "settings.unlimited": "Unbegrenzt",

  // History page
  "history.filterAll": "Alle",
  "table.category": "Kategorie",
  "table.rowsPerPage": "Zeilen pro Seite",

  // Servers page
  "servers.password": "Passwort",

  // Categories
  "categories.title": "Kategorien",

  // General settings
  "settings.saving": "Speichert…",

  // Metrics labels
  "metrics.downloaded": "Heruntergeladen",
  "metrics.decoded": "Dekodiert",
  "metrics.committed": "Geschrieben",
  "metrics.downloadSpeed": "Downloadgeschwindigkeit",
  "metrics.downloadQueue": "Download-Warteschlange",
  "metrics.decodePending": "Dekodierung ausstehend",
  "metrics.commitPending": "Schreibvorgang ausstehend",
  "metrics.writeBufferedBytes": "Gepufferte Bytes",
  "metrics.writeBufferedSegments": "Gepufferte Segmente",
  "metrics.diskWriteLatency": "Festplatte-Schreiblatenz",
  "metrics.segmentsDownloaded": "Segmente heruntergeladen",
  "metrics.segmentsDecoded": "Segmente dekodiert",
  "metrics.segmentsCommitted": "Segmente geschrieben",
  "metrics.articlesNotFound": "Artikel nicht gefunden",
  "metrics.decodeErrors": "Dekodierfehler",
  "metrics.crcErrors": "CRC-Fehler",
  "metrics.recoveryQueue": "Wiederherstellungswarteschlange",
  "metrics.articlesPerSec": "Artikel / Sek.",
  "metrics.decodeRate": "Dekodierrate",
  "metrics.verifyActive": "Verifizierung aktiv",
  "metrics.repairActive": "Reparatur aktiv",
  "metrics.extractActive": "Entpacken aktiv",
  "metrics.segmentsRetried": "Segmente wiederholt",
  "metrics.failedPermanent": "Dauerhaft fehlgeschlagen",

  // API Keys
  "action.edit": "Bearbeiten",
  "action.save": "Speichern",

  // Doppelte NZBs
  "upload.forceDesc": "Akzeptiert diese Übermittlung trotz semantischer und Artikel-Duplikatrichtlinie. Idempotenzkonflikte bleiben bestehen.",

  // System info
  "systemInfo.diagnosticsFailed": "Das Diagnosepaket konnte nicht erstellt werden.",
  ...duplicateLocaleEntries.deu,
  ...nextDe,
};

export default de;
