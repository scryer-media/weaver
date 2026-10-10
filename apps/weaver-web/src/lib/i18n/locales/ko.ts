import type { LocaleDictionary } from "../types";
import { duplicateLocaleEntries } from "../duplicate-locales";
import { nextKo } from "@/next/i18n/ko";

const ko: LocaleDictionary = {

  // Navigation
  "nav.settings": "설정",
  "nav.sponsor": "후원하기",
  // In-application upgrade
  "applicationUpgrade.title": "애플리케이션 업데이트",
  "applicationUpgrade.upToDate": "Weaver v{{version}}은 최신입니다.",
  "applicationUpgrade.available": "Weaver v{{version}}을 사용할 수 있습니다.",
  "applicationUpgrade.install": "v{{version}} 설치",
  "applicationUpgrade.notEligible": "Weaver 앱에서 이 설치를 업데이트할 수 없습니다 ({{reason}}).",
  "applicationUpgrade.failed": "v{{version}} 업데이트가 실패했습니다: {{error}}",
  "applicationUpgrade.completed": "v{{version}}으로 업데이트했습니다.",
  "applicationUpgrade.phase.checking": "릴리스를 확인하고 있습니다…",
  "applicationUpgrade.phase.downloading": "v{{version}}을 다운로드하고 있습니다…",
  "applicationUpgrade.phase.verifying": "다운로드를 검증하고 있습니다…",
  "applicationUpgrade.phase.staging": "업데이트를 압축 해제하고 있습니다…",
  "applicationUpgrade.phase.applying": "v{{version}}을 설치하고 있습니다…",
  "applicationUpgrade.phase.awaiting_elevation": "설치 권한을 기다리고 있습니다…",
  "applicationUpgrade.phase.restarting": "Weaver를 다시 시작하고 있습니다…",
  "applicationUpgrade.phase.reboot_required": "업데이트를 마치려면 이 컴퓨터를 다시 시작하세요.",
  "update.newVersionAria": "GitHub에서 Weaver v{{version}}을 새 탭으로 열기",

  // Status labels
  "status.queued": "대기 중",
  "status.downloading": "다운로드 중",
  "status.fetchingRepairData": "복구 데이터 가져오는 중",
  "status.verifying": "검증 중",
  "status.repairing": "복구 중",
  "status.extracting": "추출 중",
  "status.postProcessing": "후처리 중",
  "status.moving": "이동 중",
  "status.complete": "완료",
  "status.failed": "실패",
  "status.paused": "일시정지",
  "phase.downloading": "다운로드 중",
  "phase.repairing": "복구 중",
  "phase.extracting": "추출 중",
  "phase.moving": "이동 중",

  // Actions
  "action.pause": "일시정지",
  "action.resume": "재개",
  "action.cancel": "취소",
  "action.pauseAll": "모두 일시정지",
  "action.resumeAll": "모두 재개",
  "action.apply": "적용",
  "action.submit": "제출",
  "action.uploading": "업로드 중...",
  "action.backToJobs": "대기열로 돌아가기",
  "action.delete": "삭제",
  "action.reprocess": "재처리",
  "action.rerunPostProcessing": "Re-run scripts",
  "action.refresh": "새로고침",
  "action.clearFilters": "필터 초기화",
  "action.previous": "이전",
  "action.next": "다음",

  // Confirmation dialogs
  "confirm.cancelSelectedMessage": "이 다운로드들이 중단되며 재개할 수 없습니다.",
  "bulk.selected": "{{count}}개 선택됨",
  "bulk.editSelected": "선택 항목 편집",
  "bulk.pauseSelected": "선택 항목 일시정지",
  "bulk.cancelSelected": "선택 항목 취소",
  "bulk.editTitle": "선택한 작업 편집",
  "bulk.noChange": "— 변경 없음 —",
  "action.deleteAll": "모두 삭제",

  // Upload page
  "upload.invalidFiles": ".nzb 또는 .nzb.xz 파일만 선택해 주세요.",
  "upload.noCategory": "카테고리 없음",
  "upload.priorityLow": "낮음",
  "upload.priorityNormal": "보통",
  "upload.priorityHigh": "높음",
  "upload.removeFile": "파일 제거",
  "upload.rejected": "업로드가 서버에서 거부되었습니다.",
  "upload.stageExpired": "Staged upload expired. Re-add the file.",

  // Jobs page
  "jobs.egressQuotaBadge": "송신 할당량",
  "jobs.egressQuotaEta": "{{resetAt}}까지",
  "jobs.serverQuotaBadge": "서버 할당량",
  "jobs.serverQuotaEta": "서버 할당량 대기 중",

  // Table headers
  "table.name": "이름",
  "table.status": "상태",
  "table.priority": "우선순위",
  "table.progress": "진행률",
  "table.size": "크기",

  // Timeline
  "timeline.finalizingDownload": "다운로드 마무리 중",
  "timeline.totalDuration": "총 시간",

  // Settings page
  "settings.unlimited": "무제한",

  // History page
  "history.filterAll": "전체",
  "table.category": "카테고리",
  "table.rowsPerPage": "페이지당 행 수",

  // Servers page
  "servers.password": "비밀번호",

  // Categories
  "categories.title": "카테고리",

  // General settings
  "settings.saving": "저장 중…",

  // Metrics labels
  "metrics.downloaded": "다운로드됨",
  "metrics.decoded": "디코딩됨",
  "metrics.committed": "커밋됨",
  "metrics.downloadSpeed": "다운로드 속도",
  "metrics.downloadQueue": "다운로드 대기열",
  "metrics.decodePending": "디코딩 대기",
  "metrics.commitPending": "커밋 대기",
  "metrics.writeBufferedBytes": "버퍼링된 쓰기 바이트",
  "metrics.writeBufferedSegments": "버퍼링된 쓰기 세그먼트",
  "metrics.diskWriteLatency": "디스크 쓰기 지연",
  "metrics.segmentsDownloaded": "다운로드된 세그먼트",
  "metrics.segmentsDecoded": "디코딩된 세그먼트",
  "metrics.segmentsCommitted": "커밋된 세그먼트",
  "metrics.articlesNotFound": "아티클 미발견",
  "metrics.decodeErrors": "디코드 오류",
  "metrics.crcErrors": "CRC 오류",
  "metrics.recoveryQueue": "복구 대기열",
  "metrics.articlesPerSec": "아티클 / 초",
  "metrics.decodeRate": "디코드 속도",
  "metrics.verifyActive": "검증 활성",
  "metrics.repairActive": "복구 활성",
  "metrics.extractActive": "추출 활성",
  "metrics.segmentsRetried": "재시도된 세그먼트",
  "metrics.failedPermanent": "영구 실패",

  // API Keys
  "action.edit": "편집",
  "action.save": "저장",

  "upload.forceDesc": "의미 및 아티클 지문 중복 정책에도 이 제출을 수락합니다. 멱등성 충돌은 계속 적용됩니다.",

  // System info
  "systemInfo.diagnosticsFailed": "진단 패키지를 만들 수 없습니다.",
  ...duplicateLocaleEntries.kor,
  ...nextKo,
};

export default ko;
