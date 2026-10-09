import type { LocaleDictionary } from "../types";
import { duplicateLocaleEntries } from "../duplicate-locales";
import { nextPt } from "@/next/i18n/pt";

const pt: LocaleDictionary = {

  // Navigation
  "nav.settings": "Configurações",
  "nav.sponsor": "Patrocinar",
  // In-application upgrade
  "applicationUpgrade.title": "Atualização do aplicativo",
  "applicationUpgrade.upToDate": "O Weaver v{{version}} está atualizado.",
  "applicationUpgrade.available": "O Weaver v{{version}} está disponível.",
  "applicationUpgrade.install": "Instalar a v{{version}}",
  "applicationUpgrade.notEligible": "O Weaver não pode atualizar esta instalação pelo aplicativo ({{reason}}).",
  "applicationUpgrade.failed": "A atualização para a v{{version}} falhou: {{error}}",
  "applicationUpgrade.completed": "Atualizado para a v{{version}}.",
  "applicationUpgrade.phase.checking": "Verificando a versão…",
  "applicationUpgrade.phase.downloading": "Baixando a v{{version}}…",
  "applicationUpgrade.phase.verifying": "Verificando o download…",
  "applicationUpgrade.phase.staging": "Descompactando a atualização…",
  "applicationUpgrade.phase.applying": "Instalando a v{{version}}…",
  "applicationUpgrade.phase.awaiting_elevation": "Aguardando permissão para instalar…",
  "applicationUpgrade.phase.restarting": "Reiniciando o Weaver…",
  "applicationUpgrade.phase.reboot_required": "Reinicie este computador para concluir a atualização.",
  "update.newVersionAria": "Abrir o Weaver v{{version}} no GitHub em uma nova aba",

  // Status labels
  "status.queued": "Na fila",
  "status.downloading": "Baixando",
  "status.fetchingRepairData": "Obtendo dados de reparo",
  "status.verifying": "Verificando",
  "status.repairing": "Reparando",
  "status.extracting": "Extraindo",
  "status.postProcessing": "Pós-processamento",
  "status.moving": "Movendo",
  "status.complete": "Concluído",
  "status.failed": "Falhou",
  "status.paused": "Pausado",
  "phase.downloading": "Baixando",
  "phase.repairing": "Reparando",
  "phase.extracting": "Extraindo",
  "phase.moving": "Movendo",

  // Actions
  "action.pause": "Pausar",
  "action.resume": "Retomar",
  "action.cancel": "Cancelar",
  "action.pauseAll": "Pausar Tudo",
  "action.resumeAll": "Retomar Tudo",
  "action.apply": "Aplicar",
  "action.submit": "Enviar",
  "action.uploading": "Enviando...",
  "action.backToJobs": "Voltar para a fila",
  "action.delete": "Excluir",
  "action.reprocess": "Reprocessar",
  "action.rerunPostProcessing": "Re-run scripts",
  "action.refresh": "Atualizar",
  "action.clearFilters": "Limpar Filtros",
  "action.previous": "Anterior",
  "action.next": "Próximo",

  // Confirmation dialogs
  "confirm.cancelSelectedMessage": "Esses downloads serão interrompidos e não poderão ser retomados.",
  "bulk.selected": "{{count}} selecionados",
  "bulk.editSelected": "Editar Selecionados",
  "bulk.pauseSelected": "Pausar Selecionados",
  "bulk.cancelSelected": "Cancelar Selecionados",
  "bulk.editTitle": "Editar Jobs Selecionados",
  "bulk.noChange": "— Sem alteração —",
  "action.deleteAll": "Excluir Tudo",

  // Upload page
  "upload.invalidFiles": "Por favor, selecione apenas arquivos .nzb ou .nzb.xz.",
  "upload.noCategory": "Sem categoria",
  "upload.priorityLow": "Baixa",
  "upload.priorityNormal": "Normal",
  "upload.priorityHigh": "Alta",
  "upload.removeFile": "Remover Arquivo",
  "upload.rejected": "O envio foi rejeitado pelo servidor.",
  "upload.stageExpired": "Staged upload expired. Re-add the file.",

  // Jobs page
  "jobs.egressQuotaBadge": "Cota de saída",
  "jobs.egressQuotaEta": "Até {{resetAt}}",
  "jobs.serverQuotaBadge": "Cota do servidor",
  "jobs.serverQuotaEta": "Aguardando cota do servidor",

  // Table headers
  "table.name": "Nome",
  "table.status": "Status",
  "table.priority": "Prioridade",
  "table.progress": "Progresso",
  "table.size": "Tamanho",

  // Timeline
  "timeline.finalizingDownload": "Finalizando Download",
  "timeline.totalDuration": "Total",

  // Settings page
  "settings.unlimited": "Ilimitado",

  // History page
  "history.filterAll": "Todos",
  "table.category": "Categoria",
  "table.rowsPerPage": "Linhas por página",

  // Servers page
  "servers.password": "Senha",

  // Categories
  "categories.title": "Categorias",

  // General settings
  "settings.saving": "Salvando…",

  // Metrics labels
  "metrics.downloaded": "Baixado",
  "metrics.decoded": "Decodificado",
  "metrics.committed": "Confirmado",
  "metrics.downloadSpeed": "Velocidade de Download",
  "metrics.downloadQueue": "Fila de Download",
  "metrics.decodePending": "Decodificação Pendente",
  "metrics.commitPending": "Confirmação Pendente",
  "metrics.writeBufferedBytes": "Bytes em Buffer de Escrita",
  "metrics.writeBufferedSegments": "Segmentos em Buffer de Escrita",
  "metrics.diskWriteLatency": "Latência de Escrita em Disco",
  "metrics.segmentsDownloaded": "Segmentos Baixados",
  "metrics.segmentsDecoded": "Segmentos Decodificados",
  "metrics.segmentsCommitted": "Segmentos Confirmados",
  "metrics.articlesNotFound": "Artigos Não Encontrados",
  "metrics.decodeErrors": "Erros de Decodificação",
  "metrics.crcErrors": "Erros de CRC",
  "metrics.recoveryQueue": "Fila de Recuperação",
  "metrics.articlesPerSec": "Artigos / Seg",
  "metrics.decodeRate": "Taxa de Decodificação",
  "metrics.verifyActive": "Verificação Ativa",
  "metrics.repairActive": "Reparo Ativo",
  "metrics.extractActive": "Extração Ativa",
  "metrics.segmentsRetried": "Segmentos Retentados",
  "metrics.failedPermanent": "Falha Permanente",

  // API Keys
  "action.edit": "Editar",
  "action.save": "Salvar",

  "upload.forceDesc": "Aceita este envio apesar da política de duplicados semântica e de artigos. Conflitos de idempotência continuam aplicáveis.",

  // System info
  "systemInfo.diagnosticsFailed": "Não foi possível criar o pacote de diagnóstico.",
  ...duplicateLocaleEntries.por,
  ...nextPt,
};

export default pt;
