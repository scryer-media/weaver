import type { LocaleDictionary } from "../types";
import { duplicateLocaleEntries } from "../duplicate-locales";
import { nextIt } from "@/next/i18n/it";

const it: LocaleDictionary = {

  // Navigation
  "nav.settings": "Impostazioni",
  "nav.sponsor": "Sostieni",
  // In-application upgrade
  "applicationUpgrade.title": "Aggiornamento dell'applicazione",
  "applicationUpgrade.upToDate": "Weaver v{{version}} è aggiornato.",
  "applicationUpgrade.available": "Weaver v{{version}} è disponibile.",
  "applicationUpgrade.install": "Installa la v{{version}}",
  "applicationUpgrade.notEligible": "Weaver non può aggiornare questa installazione dall’applicazione ({{reason}}).",
  "applicationUpgrade.failed": "L'aggiornamento alla v{{version}} è fallito: {{error}}",
  "applicationUpgrade.completed": "Aggiornato alla v{{version}}.",
  "applicationUpgrade.phase.checking": "Controllo della release…",
  "applicationUpgrade.phase.downloading": "Download della v{{version}}…",
  "applicationUpgrade.phase.verifying": "Verifica del download…",
  "applicationUpgrade.phase.staging": "Estrazione dell'aggiornamento…",
  "applicationUpgrade.phase.applying": "Installazione della v{{version}}…",
  "applicationUpgrade.phase.awaiting_elevation": "In attesa dell'autorizzazione a installare…",
  "applicationUpgrade.phase.restarting": "Riavvio di Weaver…",
  "applicationUpgrade.phase.reboot_required": "Riavvia questo computer per completare l'aggiornamento.",
  "update.newVersionAria": "Apri Weaver v{{version}} su GitHub in una nuova scheda",

  // Status labels
  "status.queued": "In coda",
  "status.downloading": "Download in corso",
  "status.fetchingRepairData": "Recupero dei dati di riparazione",
  "status.verifying": "Verifica in corso",
  "status.repairing": "Riparazione in corso",
  "status.extracting": "Estrazione in corso",
  "status.postProcessing": "Post-elaborazione",
  "status.moving": "Spostamento in corso",
  "status.complete": "Completato",
  "status.failed": "Fallito",
  "status.paused": "In pausa",
  "phase.downloading": "Download in corso",
  "phase.repairing": "Riparazione in corso",
  "phase.extracting": "Estrazione in corso",
  "phase.moving": "Spostamento in corso",

  // Actions
  "action.pause": "Pausa",
  "action.resume": "Riprendi",
  "action.cancel": "Annulla",
  "action.pauseAll": "Pausa Tutto",
  "action.resumeAll": "Riprendi Tutto",
  "action.apply": "Applica",
  "action.submit": "Invia",
  "action.uploading": "Caricamento...",
  "action.backToJobs": "Torna alla coda",
  "action.delete": "Elimina",
  "action.reprocess": "Rielabora",
  "action.rerunPostProcessing": "Re-run scripts",
  "action.refresh": "Aggiorna",
  "action.clearFilters": "Cancella Filtri",
  "action.previous": "Precedente",
  "action.next": "Successivo",

  // Confirmation dialogs
  "confirm.cancelSelectedMessage": "Questi download verranno interrotti e non potranno essere ripresi.",
  "bulk.selected": "{{count}} selezionati",
  "bulk.editSelected": "Modifica Selezionati",
  "bulk.pauseSelected": "Pausa Selezionati",
  "bulk.cancelSelected": "Annulla Selezionati",
  "bulk.editTitle": "Modifica Job Selezionati",
  "bulk.noChange": "— Nessuna modifica —",
  "action.deleteAll": "Elimina Tutto",

  // Upload page
  "upload.invalidFiles": "Seleziona solo file .nzb o .nzb.xz.",
  "upload.noCategory": "Nessuna categoria",
  "upload.priorityLow": "Bassa",
  "upload.priorityNormal": "Normale",
  "upload.priorityHigh": "Alta",
  "upload.removeFile": "Rimuovi File",
  "upload.rejected": "Il caricamento è stato rifiutato dal server.",
  "upload.stageExpired": "Staged upload expired. Re-add the file.",

  // Jobs page
  "jobs.egressQuotaBadge": "Quota di uscita",
  "jobs.egressQuotaEta": "Fino a {{resetAt}}",
  "jobs.serverQuotaBadge": "Quota server",
  "jobs.serverQuotaEta": "In attesa della quota server",

  // Table headers
  "table.name": "Nome",
  "table.status": "Stato",
  "table.priority": "Priorità",
  "table.progress": "Avanzamento",
  "table.size": "Dimensione",

  // Timeline
  "timeline.finalizingDownload": "Finalizzazione del Download",
  "timeline.totalDuration": "Totale",

  // Settings page
  "settings.unlimited": "Illimitato",

  // History page
  "history.filterAll": "Tutti",
  "table.category": "Categoria",
  "table.rowsPerPage": "Righe per pagina",

  // Servers page
  "servers.password": "Password",

  // Categories
  "categories.title": "Categorie",

  // General settings
  "settings.saving": "Salvataggio…",

  // Metrics labels
  "metrics.downloaded": "Scaricato",
  "metrics.decoded": "Decodificato",
  "metrics.committed": "Confermato",
  "metrics.downloadSpeed": "Velocità di Download",
  "metrics.downloadQueue": "Coda Download",
  "metrics.decodePending": "Decodifica in Attesa",
  "metrics.commitPending": "Conferma in Attesa",
  "metrics.writeBufferedBytes": "Byte in Buffer di Scrittura",
  "metrics.writeBufferedSegments": "Segmenti in Buffer di Scrittura",
  "metrics.diskWriteLatency": "Latenza Scrittura su Disco",
  "metrics.segmentsDownloaded": "Segmenti Scaricati",
  "metrics.segmentsDecoded": "Segmenti Decodificati",
  "metrics.segmentsCommitted": "Segmenti Confermati",
  "metrics.articlesNotFound": "Articoli Non Trovati",
  "metrics.decodeErrors": "Errori di Decodifica",
  "metrics.crcErrors": "Errori CRC",
  "metrics.recoveryQueue": "Coda di Recupero",
  "metrics.articlesPerSec": "Articoli / Sec",
  "metrics.decodeRate": "Velocità di Decodifica",
  "metrics.verifyActive": "Verifica Attiva",
  "metrics.repairActive": "Riparazione Attiva",
  "metrics.extractActive": "Estrazione Attiva",
  "metrics.segmentsRetried": "Segmenti Ritentati",
  "metrics.failedPermanent": "Fallimento Permanente",

  // API Keys
  "action.edit": "Modifica",
  "action.save": "Salva",

  "upload.forceDesc": "Accetta questo invio nonostante la politica di duplicati semantica e degli articoli. I conflitti di idempotenza restano applicabili.",

  // System info
  "systemInfo.diagnosticsFailed": "Non è stato possibile creare il pacchetto diagnostico.",
  ...duplicateLocaleEntries.ita,
  ...nextIt,
};

export default it;
