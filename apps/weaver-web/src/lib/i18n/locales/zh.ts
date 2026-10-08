import type { LocaleDictionary } from "../types";
import { duplicateLocaleEntries } from "../duplicate-locales";
import { nextZh } from "@/next/i18n/zh";

const zh: LocaleDictionary = {

  // Navigation
  "nav.settings": "设置",
  "nav.sponsor": "赞助",
  // In-application upgrade
  "applicationUpgrade.title": "应用更新",
  "applicationUpgrade.upToDate": "Weaver v{{version}} 已是最新版本。",
  "applicationUpgrade.available": "Weaver v{{version}} 可用。",
  "applicationUpgrade.install": "安装 v{{version}}",
  "applicationUpgrade.notEligible": "Weaver 无法在应用内更新此安装（{{reason}}）。",
  "applicationUpgrade.failed": "更新到 v{{version}} 失败：{{error}}",
  "applicationUpgrade.completed": "已更新到 v{{version}}。",
  "applicationUpgrade.phase.checking": "正在检查版本…",
  "applicationUpgrade.phase.downloading": "正在下载 v{{version}}…",
  "applicationUpgrade.phase.verifying": "正在校验下载内容…",
  "applicationUpgrade.phase.staging": "正在解包更新…",
  "applicationUpgrade.phase.applying": "正在安装 v{{version}}…",
  "applicationUpgrade.phase.awaiting_elevation": "正在等待安装授权…",
  "applicationUpgrade.phase.restarting": "正在重启 Weaver…",
  "applicationUpgrade.phase.reboot_required": "请重启这台计算机以完成更新。",
  "update.newVersionAria": "在新标签页中打开 GitHub 上的 Weaver v{{version}}",

  // Status labels
  "status.queued": "排队中",
  "status.downloading": "下载中",
  "status.fetchingRepairData": "正在获取修复数据",
  "status.verifying": "校验中",
  "status.repairing": "修复中",
  "status.extracting": "解压中",
  "status.postProcessing": "后处理中",
  "status.moving": "移动中",
  "status.complete": "已完成",
  "status.failed": "失败",
  "status.paused": "已暂停",
  "phase.downloading": "下载中",
  "phase.repairing": "修复中",
  "phase.extracting": "解压中",
  "phase.moving": "移动中",

  // Actions
  "action.pause": "暂停",
  "action.resume": "恢复",
  "action.cancel": "取消",
  "action.pauseAll": "全部暂停",
  "action.resumeAll": "全部恢复",
  "action.apply": "应用",
  "action.submit": "提交",
  "action.uploading": "上传中...",
  "action.backToJobs": "返回队列",
  "action.delete": "删除",
  "action.reprocess": "重新处理",
  "action.rerunPostProcessing": "Re-run scripts",
  "action.refresh": "刷新",
  "action.clearFilters": "清除筛选",
  "action.previous": "上一页",
  "action.next": "下一页",

  // Confirmation dialogs
  "confirm.cancelSelectedMessage": "这些下载将被停止且无法恢复。",
  "bulk.selected": "已选 {{count}} 项",
  "bulk.editSelected": "编辑所选",
  "bulk.pauseSelected": "暂停所选",
  "bulk.cancelSelected": "取消所选",
  "bulk.editTitle": "编辑所选任务",
  "bulk.noChange": "— 不更改 —",
  "action.deleteAll": "全部删除",

  // Upload page
  "upload.invalidFiles": "请仅选择 .nzb 或 .nzb.xz 文件。",
  "upload.noCategory": "无分类",
  "upload.priorityLow": "低",
  "upload.priorityNormal": "普通",
  "upload.priorityHigh": "高",
  "upload.removeFile": "移除文件",
  "upload.rejected": "上传被服务器拒绝。",
  "upload.stageExpired": "Staged upload expired. Re-add the file.",

  // Jobs page
  "jobs.bandwidthCapShort": "ISP 限额",
  "jobs.bandwidthCapEta": "直到 {{resetAt}}",
  "jobs.serverQuotaBadge": "服务器配额",
  "jobs.serverQuotaEta": "等待服务器配额",

  // Table headers
  "table.name": "名称",
  "table.status": "状态",
  "table.priority": "优先级",
  "table.progress": "进度",
  "table.size": "大小",

  // Timeline
  "timeline.finalizingDownload": "正在完成下载收尾",
  "timeline.totalDuration": "总计",

  // Settings page
  "settings.unlimited": "无限制",

  // History page
  "history.filterAll": "全部",
  "table.category": "分类",
  "table.rowsPerPage": "每页行数",

  // Servers page
  "servers.password": "密码",

  // Categories
  "categories.title": "分类",

  // General settings
  "settings.saving": "保存中…",

  // Metrics labels
  "metrics.downloaded": "已下载",
  "metrics.decoded": "已解码",
  "metrics.committed": "已提交",
  "metrics.downloadSpeed": "下载速度",
  "metrics.downloadQueue": "下载队列",
  "metrics.decodePending": "待解码",
  "metrics.commitPending": "待提交",
  "metrics.writeBufferedBytes": "写缓冲字节",
  "metrics.writeBufferedSegments": "写缓冲分段",
  "metrics.diskWriteLatency": "磁盘写入延迟",
  "metrics.segmentsDownloaded": "已下载分段",
  "metrics.segmentsDecoded": "已解码分段",
  "metrics.segmentsCommitted": "已提交分段",
  "metrics.articlesNotFound": "未找到文章",
  "metrics.decodeErrors": "解码错误",
  "metrics.crcErrors": "CRC 错误",
  "metrics.recoveryQueue": "恢复队列",
  "metrics.articlesPerSec": "文章/秒",
  "metrics.decodeRate": "解码速率",
  "metrics.verifyActive": "校验中",
  "metrics.repairActive": "修复中",
  "metrics.extractActive": "解压中",
  "metrics.segmentsRetried": "已重试分段",
  "metrics.failedPermanent": "永久失败",

  // API Keys
  "action.edit": "编辑",
  "action.save": "保存",

  "upload.forceDesc": "即使语义和文章指纹重复策略匹配，也接受此提交。幂等性冲突仍然有效。",

  // System info
  "systemInfo.diagnosticsFailed": "无法创建诊断包。",
  ...duplicateLocaleEntries.zho,
  ...nextZh,
};

export default zh;
