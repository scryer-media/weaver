import type { LocaleDictionary } from "../types";
import { duplicateLocaleEntries } from "../duplicate-locales";
import { nextJa } from "@/next/i18n/ja";

const ja: LocaleDictionary = {

  // Navigation
  "nav.settings": "設定",
  "nav.sponsor": "スポンサー",
  // In-application upgrade
  "applicationUpgrade.title": "アプリケーションの更新",
  "applicationUpgrade.upToDate": "Weaver v{{version}} は最新です。",
  "applicationUpgrade.available": "Weaver v{{version}} が利用できます。",
  "applicationUpgrade.install": "v{{version}} をインストール",
  "applicationUpgrade.notEligible": "このインストールは Weaver 内から更新できません（{{reason}}）。",
  "applicationUpgrade.failed": "v{{version}} への更新に失敗しました: {{error}}",
  "applicationUpgrade.completed": "v{{version}} に更新しました。",
  "applicationUpgrade.phase.checking": "リリースを確認しています…",
  "applicationUpgrade.phase.downloading": "v{{version}} をダウンロードしています…",
  "applicationUpgrade.phase.verifying": "ダウンロードを検証しています…",
  "applicationUpgrade.phase.staging": "更新を展開しています…",
  "applicationUpgrade.phase.applying": "v{{version}} をインストールしています…",
  "applicationUpgrade.phase.awaiting_elevation": "インストールの許可を待っています…",
  "applicationUpgrade.phase.restarting": "Weaver を再起動しています…",
  "applicationUpgrade.phase.reboot_required": "更新を完了するにはこのコンピューターを再起動してください。",
  "update.newVersionAria": "GitHub で Weaver v{{version}} を新しいタブで開く",

  // Status labels
  "status.queued": "待機中",
  "status.downloading": "ダウンロード中",
  "status.verifying": "検証中",
  "status.repairing": "修復中",
  "status.extracting": "展開中",
  "status.postProcessing": "後処理中",
  "status.moving": "移動中",
  "status.complete": "完了",
  "status.failed": "失敗",
  "status.paused": "一時停止",
  "phase.downloading": "ダウンロード中",
  "phase.repairing": "修復中",
  "phase.extracting": "展開中",
  "phase.moving": "移動中",

  // Actions
  "action.pause": "一時停止",
  "action.resume": "再開",
  "action.cancel": "キャンセル",
  "action.pauseAll": "すべて一時停止",
  "action.resumeAll": "すべて再開",
  "action.apply": "適用",
  "action.submit": "送信",
  "action.uploading": "アップロード中...",
  "action.backToJobs": "キューに戻る",
  "action.delete": "削除",
  "action.reprocess": "再処理",
  "action.rerunPostProcessing": "Re-run scripts",
  "action.refresh": "更新",
  "action.clearFilters": "フィルターをクリア",
  "action.previous": "前へ",
  "action.next": "次へ",

  // Confirmation dialogs
  "confirm.cancelSelectedMessage": "これらのダウンロードは停止され、再開できません。",
  "bulk.selected": "{{count}} 件選択中",
  "bulk.editSelected": "選択項目を編集",
  "bulk.pauseSelected": "選択項目を一時停止",
  "bulk.cancelSelected": "選択項目をキャンセル",
  "bulk.editTitle": "選択したジョブを編集",
  "bulk.noChange": "— 変更なし —",
  "action.deleteAll": "すべて削除",

  // Upload page
  "upload.invalidFiles": ".nzb または .nzb.xz ファイルのみ選択してください。",
  "upload.noCategory": "カテゴリなし",
  "upload.priorityLow": "低",
  "upload.priorityNormal": "通常",
  "upload.priorityHigh": "高",
  "upload.removeFile": "ファイルを除去",
  "upload.rejected": "アップロードはサーバーに拒否されました。",
  "upload.stageExpired": "Staged upload expired. Re-add the file.",

  // Jobs page
  "jobs.bandwidthCapShort": "ISP 制限",
  "jobs.bandwidthCapEta": "{{resetAt}} まで",
  "jobs.serverQuotaBadge": "サーバークォータ",
  "jobs.serverQuotaEta": "サーバークォータ待ち",

  // Table headers
  "table.name": "名前",
  "table.status": "ステータス",
  "table.priority": "優先度",
  "table.progress": "進捗",
  "table.size": "サイズ",

  // Timeline
  "timeline.finalizingDownload": "ダウンロードを最終処理中",
  "timeline.totalDuration": "合計",

  // Settings page
  "settings.unlimited": "無制限",

  // History page
  "history.filterAll": "すべて",
  "table.category": "カテゴリ",
  "table.rowsPerPage": "1ページあたりの行数",

  // Servers page
  "servers.password": "パスワード",

  // Categories
  "categories.title": "カテゴリ",

  // General settings
  "settings.saving": "保存中…",

  // Metrics labels
  "metrics.downloaded": "ダウンロード済み",
  "metrics.decoded": "デコード済み",
  "metrics.committed": "コミット済み",
  "metrics.downloadSpeed": "ダウンロード速度",
  "metrics.downloadQueue": "ダウンロードキュー",
  "metrics.decodePending": "デコード待ち",
  "metrics.commitPending": "コミット待ち",
  "metrics.writeBufferedBytes": "書き込みバッファバイト数",
  "metrics.writeBufferedSegments": "書き込みバッファセグメント数",
  "metrics.diskWriteLatency": "ディスク書き込みレイテンシ",
  "metrics.segmentsDownloaded": "ダウンロード済みセグメント",
  "metrics.segmentsDecoded": "デコード済みセグメント",
  "metrics.segmentsCommitted": "コミット済みセグメント",
  "metrics.articlesNotFound": "記事未検出",
  "metrics.decodeErrors": "デコードエラー",
  "metrics.crcErrors": "CRC エラー",
  "metrics.recoveryQueue": "リカバリキュー",
  "metrics.articlesPerSec": "記事数 / 秒",
  "metrics.decodeRate": "デコードレート",
  "metrics.verifyActive": "検証中",
  "metrics.repairActive": "修復中",
  "metrics.extractActive": "展開中",
  "metrics.segmentsRetried": "リトライ済みセグメント",
  "metrics.failedPermanent": "恒久的失敗",

  // API Keys
  "action.edit": "編集",
  "action.save": "保存",

  "upload.forceDesc": "意味的および記事フィンガープリントの重複ポリシーを無視して送信を受理します。冪等性の競合は引き続き適用されます。",

  // System info
  "systemInfo.diagnosticsFailed": "診断パッケージを作成できませんでした。",
  ...duplicateLocaleEntries.jpn,
  ...nextJa,
};

export default ja;
