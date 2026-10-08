import type { LocaleDictionary } from "../types";
import { duplicateLocaleEntries } from "../duplicate-locales";
import { nextFr } from "@/next/i18n/fr";

const fr: LocaleDictionary = {

  // Navigation
  "nav.settings": "Paramètres",
  "nav.sponsor": "Sponsoriser",
  // In-application upgrade
  "applicationUpgrade.title": "Mise à jour de l'application",
  "applicationUpgrade.upToDate": "Weaver v{{version}} est à jour.",
  "applicationUpgrade.available": "Weaver v{{version}} est disponible.",
  "applicationUpgrade.install": "Installer la v{{version}}",
  "applicationUpgrade.notEligible": "Weaver ne peut pas mettre à jour cette installation depuis l’application ({{reason}}).",
  "applicationUpgrade.failed": "La mise à jour vers la v{{version}} a échoué : {{error}}",
  "applicationUpgrade.completed": "Mis à jour vers la v{{version}}.",
  "applicationUpgrade.phase.checking": "Vérification de la version…",
  "applicationUpgrade.phase.downloading": "Téléchargement de la v{{version}}…",
  "applicationUpgrade.phase.verifying": "Vérification du téléchargement…",
  "applicationUpgrade.phase.staging": "Décompression de la mise à jour…",
  "applicationUpgrade.phase.applying": "Installation de la v{{version}}…",
  "applicationUpgrade.phase.awaiting_elevation": "Attente de l'autorisation d'installer…",
  "applicationUpgrade.phase.restarting": "Redémarrage de Weaver…",
  "applicationUpgrade.phase.reboot_required": "Redémarrez cet ordinateur pour terminer la mise à jour.",
  "update.newVersionAria": "Ouvrir Weaver v{{version}} sur GitHub dans un nouvel onglet",

  // Status labels
  "status.queued": "En attente",
  "status.downloading": "Téléchargement",
  "status.fetchingRepairData": "Récupération des données de réparation",
  "status.verifying": "Vérification",
  "status.repairing": "Réparation",
  "status.extracting": "Extraction",
  "status.postProcessing": "Post-traitement",
  "status.moving": "Déplacement",
  "status.complete": "Terminé",
  "status.failed": "Échoué",
  "status.paused": "En pause",
  "phase.downloading": "Téléchargement",
  "phase.repairing": "Réparation",
  "phase.extracting": "Extraction",
  "phase.moving": "Déplacement",

  // Actions
  "action.pause": "Pause",
  "action.resume": "Reprendre",
  "action.cancel": "Annuler",
  "action.pauseAll": "Tout mettre en pause",
  "action.resumeAll": "Tout reprendre",
  "action.apply": "Appliquer",
  "action.submit": "Soumettre",
  "action.uploading": "Téléversement...",
  "action.backToJobs": "Retour à la file",
  "action.delete": "Supprimer",
  "action.reprocess": "Retraiter",
  "action.rerunPostProcessing": "Re-run scripts",
  "action.refresh": "Actualiser",
  "action.clearFilters": "Effacer les filtres",
  "action.previous": "Précédent",
  "action.next": "Suivant",

  // Confirmation dialogs
  "confirm.cancelSelectedMessage": "Ces téléchargements seront arrêtés et ne pourront pas être repris.",
  "bulk.selected": "{{count}} sélectionnés",
  "bulk.editSelected": "Modifier la sélection",
  "bulk.pauseSelected": "Mettre en pause la sélection",
  "bulk.cancelSelected": "Annuler la sélection",
  "bulk.editTitle": "Modifier les tâches sélectionnées",
  "bulk.noChange": "— Aucun changement —",
  "action.deleteAll": "Tout supprimer",

  // Upload page
  "upload.invalidFiles": "Veuillez sélectionner uniquement des fichiers .nzb ou .nzb.xz.",
  "upload.noCategory": "Aucune catégorie",
  "upload.priorityLow": "Basse",
  "upload.priorityNormal": "Normale",
  "upload.priorityHigh": "Haute",
  "upload.removeFile": "Retirer le fichier",
  "upload.rejected": "Le téléversement a été refusé par le serveur.",
  "upload.stageExpired": "Staged upload expired. Re-add the file.",

  // Jobs page
  "jobs.bandwidthCapShort": "Quota FAI",
  "jobs.bandwidthCapEta": "Jusqu'au {{resetAt}}",
  "jobs.serverQuotaBadge": "Quota serveur",
  "jobs.serverQuotaEta": "En attente d'un quota serveur",

  // Table headers
  "table.name": "Nom",
  "table.status": "Statut",
  "table.priority": "Priorité",
  "table.progress": "Progression",
  "table.size": "Taille",

  // Timeline
  "timeline.finalizingDownload": "Finalisation du téléchargement",
  "timeline.totalDuration": "Total",

  // Settings page
  "settings.unlimited": "Illimité",

  // History page
  "history.filterAll": "Tout",
  "table.category": "Catégorie",
  "table.rowsPerPage": "Lignes par page",

  // Servers page
  "servers.password": "Mot de passe",

  // Categories
  "categories.title": "Catégories",

  // General settings
  "settings.saving": "Enregistrement…",

  // Metrics labels
  "metrics.downloaded": "Téléchargé",
  "metrics.decoded": "Décodé",
  "metrics.committed": "Validé",
  "metrics.downloadSpeed": "Vitesse de téléchargement",
  "metrics.downloadQueue": "File de téléchargement",
  "metrics.decodePending": "Décodage en attente",
  "metrics.commitPending": "Validation en attente",
  "metrics.writeBufferedBytes": "Octets en tampon d'écriture",
  "metrics.writeBufferedSegments": "Segments en tampon d'écriture",
  "metrics.diskWriteLatency": "Latence d'écriture disque",
  "metrics.segmentsDownloaded": "Segments téléchargés",
  "metrics.segmentsDecoded": "Segments décodés",
  "metrics.segmentsCommitted": "Segments validés",
  "metrics.articlesNotFound": "Articles introuvables",
  "metrics.decodeErrors": "Erreurs de décodage",
  "metrics.crcErrors": "Erreurs CRC",
  "metrics.recoveryQueue": "File de récupération",
  "metrics.articlesPerSec": "Articles / sec",
  "metrics.decodeRate": "Taux de décodage",
  "metrics.verifyActive": "Vérification active",
  "metrics.repairActive": "Réparation active",
  "metrics.extractActive": "Extraction active",
  "metrics.segmentsRetried": "Segments réessayés",
  "metrics.failedPermanent": "Échecs permanents",

  // API Keys
  "action.edit": "Modifier",
  "action.save": "Enregistrer",

  "upload.forceDesc": "Accepte cet envoi malgré la politique de doublons sémantique et d'articles. Les conflits d'idempotence restent applicables.",

  // System info
  "systemInfo.diagnosticsFailed": "Le paquet de diagnostic n'a pas pu être créé.",
  ...duplicateLocaleEntries.fra,
  ...nextFr,
};

export default fr;
