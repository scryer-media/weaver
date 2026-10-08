import type { LocaleDictionary } from "../types";
import { duplicateLocaleEntries } from "../duplicate-locales";
import { nextEs } from "@/next/i18n/es";

const es: LocaleDictionary = {

  // Navigation
  "nav.settings": "Ajustes",
  "nav.sponsor": "Patrocinar",
  // In-application upgrade
  "applicationUpgrade.title": "Actualización de la aplicación",
  "applicationUpgrade.upToDate": "Weaver v{{version}} está actualizado.",
  "applicationUpgrade.available": "Weaver v{{version}} está disponible.",
  "applicationUpgrade.install": "Instalar v{{version}}",
  "applicationUpgrade.notEligible": "Weaver no puede actualizar esta instalación desde la aplicación ({{reason}}).",
  "applicationUpgrade.failed": "La actualización a v{{version}} falló: {{error}}",
  "applicationUpgrade.completed": "Actualizado a v{{version}}.",
  "applicationUpgrade.phase.checking": "Comprobando la versión…",
  "applicationUpgrade.phase.downloading": "Descargando v{{version}}…",
  "applicationUpgrade.phase.verifying": "Verificando la descarga…",
  "applicationUpgrade.phase.staging": "Descomprimiendo la actualización…",
  "applicationUpgrade.phase.applying": "Instalando v{{version}}…",
  "applicationUpgrade.phase.awaiting_elevation": "Esperando permiso para instalar…",
  "applicationUpgrade.phase.restarting": "Reiniciando Weaver…",
  "applicationUpgrade.phase.reboot_required": "Reinicia este equipo para terminar la actualización.",
  "update.newVersionAria": "Abrir Weaver v{{version}} en GitHub en una pestaña nueva",

  // Status labels
  "status.queued": "En cola",
  "status.downloading": "Descargando",
  "status.fetchingRepairData": "Obteniendo datos de reparación",
  "status.verifying": "Verificando",
  "status.repairing": "Reparando",
  "status.extracting": "Extrayendo",
  "status.postProcessing": "Posprocesamiento",
  "status.moving": "Moviendo",
  "status.complete": "Completado",
  "status.failed": "Fallido",
  "status.paused": "Pausado",
  "phase.downloading": "Descargando",
  "phase.repairing": "Reparando",
  "phase.extracting": "Extrayendo",
  "phase.moving": "Moviendo",

  // Actions
  "action.pause": "Pausar",
  "action.resume": "Reanudar",
  "action.cancel": "Cancelar",
  "action.pauseAll": "Pausar todo",
  "action.resumeAll": "Reanudar todo",
  "action.apply": "Aplicar",
  "action.submit": "Enviar",
  "action.uploading": "Subiendo...",
  "action.backToJobs": "Volver a la cola",
  "action.delete": "Eliminar",
  "action.reprocess": "Reprocesar",
  "action.rerunPostProcessing": "Re-run scripts",
  "action.refresh": "Actualizar",
  "action.clearFilters": "Limpiar filtros",
  "action.previous": "Anterior",
  "action.next": "Siguiente",

  // Confirmation dialogs
  "confirm.cancelSelectedMessage": "Estas descargas se detendrán y no se podrán reanudar.",
  "bulk.selected": "{{count}} seleccionadas",
  "bulk.editSelected": "Editar selección",
  "bulk.pauseSelected": "Pausar selección",
  "bulk.cancelSelected": "Cancelar selección",
  "bulk.editTitle": "Editar tareas seleccionadas",
  "bulk.noChange": "— Sin cambios —",
  "action.deleteAll": "Eliminar todo",

  // Upload page
  "upload.invalidFiles": "Por favor selecciona solo archivos .nzb o .nzb.xz.",
  "upload.noCategory": "Sin categoría",
  "upload.priorityLow": "Baja",
  "upload.priorityNormal": "Normal",
  "upload.priorityHigh": "Alta",
  "upload.removeFile": "Quitar archivo",
  "upload.rejected": "La subida fue rechazada por el servidor.",
  "upload.stageExpired": "Staged upload expired. Re-add the file.",

  // Jobs page
  "jobs.bandwidthCapShort": "Límite ISP",
  "jobs.bandwidthCapEta": "Hasta {{resetAt}}",
  "jobs.serverQuotaBadge": "Cuota del servidor",
  "jobs.serverQuotaEta": "Esperando cuota del servidor",

  // Table headers
  "table.name": "Nombre",
  "table.status": "Estado",
  "table.priority": "Prioridad",
  "table.progress": "Progreso",
  "table.size": "Tamaño",

  // Timeline
  "timeline.finalizingDownload": "Finalizando descarga",
  "timeline.totalDuration": "Total",

  // Settings page
  "settings.unlimited": "Ilimitado",

  // History page
  "history.filterAll": "Todas",
  "table.category": "Categoría",
  "table.rowsPerPage": "Filas por página",

  // Servers page
  "servers.password": "Contraseña",

  // Categories
  "categories.title": "Categorías",

  // General settings
  "settings.saving": "Guardando…",

  // Metrics labels
  "metrics.downloaded": "Descargado",
  "metrics.decoded": "Decodificado",
  "metrics.committed": "Confirmado",
  "metrics.downloadSpeed": "Velocidad de descarga",
  "metrics.downloadQueue": "Cola de descarga",
  "metrics.decodePending": "Decodificación pendiente",
  "metrics.commitPending": "Confirmación pendiente",
  "metrics.writeBufferedBytes": "Bytes en búfer de escritura",
  "metrics.writeBufferedSegments": "Segmentos en búfer de escritura",
  "metrics.diskWriteLatency": "Latencia de escritura en disco",
  "metrics.segmentsDownloaded": "Segmentos descargados",
  "metrics.segmentsDecoded": "Segmentos decodificados",
  "metrics.segmentsCommitted": "Segmentos confirmados",
  "metrics.articlesNotFound": "Artículos no encontrados",
  "metrics.decodeErrors": "Errores de decodificación",
  "metrics.crcErrors": "Errores CRC",
  "metrics.recoveryQueue": "Cola de recuperación",
  "metrics.articlesPerSec": "Artículos / seg",
  "metrics.decodeRate": "Tasa de decodificación",
  "metrics.verifyActive": "Verificación activa",
  "metrics.repairActive": "Reparación activa",
  "metrics.extractActive": "Extracción activa",
  "metrics.segmentsRetried": "Segmentos reintentados",
  "metrics.failedPermanent": "Fallos permanentes",

  // API Keys
  "action.edit": "Editar",
  "action.save": "Guardar",

  // Duplicados
  "upload.forceDesc": "Acepta este envío pese a la política de duplicados semántica y de artículos. Los conflictos de idempotencia se mantienen.",

  // System info
  "systemInfo.diagnosticsFailed": "No se pudo crear el paquete de diagnóstico.",
  ...duplicateLocaleEntries.spa,
  ...nextEs,
};

export default es;
