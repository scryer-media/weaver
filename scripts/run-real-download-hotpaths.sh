#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: run-real-download-hotpaths.sh --env /path/to/nntp.env --nzb /path/to/file.nzb [options]

Options:
  --env PATH              NNTP credential env file
  --nzb PATH              NZB file to submit
  --out-dir PATH          Output/artifact directory
  --weaver PATH           Weaver binary (default: target/release/weaver)
  --port N                Weaver HTTP port (default: 19090)
  --duration-sec N        Max benchmark duration before cancel (default: 180, 0 = until terminal)
  --sample-ms N           Poll interval in ms (default: 500)
  --category NAME         Submit category (default: movies)
  --job-password VALUE    Archive password for the job

Environment toggles:
  PERF_STAT=1             Run `sudo perf stat -p <pid>` during the benchmark window
  PERF_RECORD=1           Run `sudo perf record -p <pid>` during the benchmark window
  PERF_FREQ=999           Sample frequency for PERF_RECORD
  RUST_LOG=warn           RUST_LOG for the managed weaver process

Required env-file variables:
  NNTP_HOST
  NNTP_PORT
  NNTP_TLS=true|false
  NNTP_USERNAME
  NNTP_PASSWORD
  NNTP_CONNECTIONS
EOF
}

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "${SCRIPT_DIR}/.." && pwd)

ENV_FILE=""
NZB_FILE=""
OUT_DIR=""
WEAVER_BIN="${REPO_ROOT}/target/release/weaver"
PORT=19090
DURATION_SEC=180
SAMPLE_MS=500
CATEGORY="movies"
JOB_PASSWORD=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --env)
      ENV_FILE="$2"
      shift 2
      ;;
    --nzb)
      NZB_FILE="$2"
      shift 2
      ;;
    --out-dir)
      OUT_DIR="$2"
      shift 2
      ;;
    --weaver)
      WEAVER_BIN="$2"
      shift 2
      ;;
    --port)
      PORT="$2"
      shift 2
      ;;
    --duration-sec)
      DURATION_SEC="$2"
      shift 2
      ;;
    --sample-ms)
      SAMPLE_MS="$2"
      shift 2
      ;;
    --category)
      CATEGORY="$2"
      shift 2
      ;;
    --job-password)
      JOB_PASSWORD="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ -z "${ENV_FILE}" || -z "${NZB_FILE}" ]]; then
  usage >&2
  exit 1
fi

if [[ ! -f "${ENV_FILE}" ]]; then
  echo "env file not found: ${ENV_FILE}" >&2
  exit 1
fi

if [[ ! -f "${NZB_FILE}" ]]; then
  echo "nzb file not found: ${NZB_FILE}" >&2
  exit 1
fi

if [[ ! -x "${WEAVER_BIN}" ]]; then
  echo "weaver binary not executable: ${WEAVER_BIN}" >&2
  exit 1
fi

set -a
source "${ENV_FILE}"
set +a

: "${NNTP_HOST:?NNTP_HOST missing in env file}"
: "${NNTP_PORT:?NNTP_PORT missing in env file}"
: "${NNTP_TLS:?NNTP_TLS missing in env file}"
: "${NNTP_USERNAME:?NNTP_USERNAME missing in env file}"
: "${NNTP_PASSWORD:?NNTP_PASSWORD missing in env file}"
: "${NNTP_CONNECTIONS:?NNTP_CONNECTIONS missing in env file}"

if [[ -z "${OUT_DIR}" ]]; then
  OUT_DIR="$(pwd)/tmp/real-download-$(date +%Y%m%d-%H%M%S)"
fi

mkdir -p "${OUT_DIR}/data/intermediate" "${OUT_DIR}/data/complete"

CONFIG_PATH="${OUT_DIR}/weaver.toml"
LOG_PATH="${OUT_DIR}/weaver.log"
PID_PATH="${OUT_DIR}/weaver.pid"
SAMPLES_PATH="${OUT_DIR}/samples.jsonl"
SUMMARY_PATH="${OUT_DIR}/summary.json"
PERF_STAT_PATH="${OUT_DIR}/perf-stat.txt"
PERF_DATA_PATH="${OUT_DIR}/perf.data"
PERF_REPORT_PATH="${OUT_DIR}/perf-report.txt"
SUBMIT_PAYLOAD_PATH="${OUT_DIR}/submit-payload.json"
QUERY_PAYLOAD_PATH="${OUT_DIR}/query-payload.json"
CANCEL_PAYLOAD_PATH="${OUT_DIR}/cancel-payload.json"

python3 - "${CONFIG_PATH}" <<'PY'
import os
import pathlib
import sys

config_path = pathlib.Path(sys.argv[1])
data_dir = config_path.parent / "data"

def esc(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')

text = f'''data_dir = "{esc(str(data_dir))}"
intermediate_dir = "{esc(str(data_dir / "intermediate"))}"
complete_dir = "{esc(str(data_dir / "complete"))}"
cleanup_after_extract = true
max_retries = 3

[[servers]]
id = 1
host = "{esc(os.environ["NNTP_HOST"])}"
port = {int(os.environ["NNTP_PORT"])}
tls = {os.environ["NNTP_TLS"].strip().lower()}
username = "{esc(os.environ["NNTP_USERNAME"])}"
password = "{esc(os.environ["NNTP_PASSWORD"])}"
connections = {int(os.environ["NNTP_CONNECTIONS"])}
active = true
priority = 0

[[categories]]
id = 1
name = "movies"

[[categories]]
id = 2
name = "tv"
'''

config_path.write_text(text, encoding="utf-8")
PY

WEAVER_PID=""
PERF_PID=""

cleanup() {
  if [[ -n "${PERF_PID}" ]]; then
    kill "${PERF_PID}" >/dev/null 2>&1 || true
    wait "${PERF_PID}" >/dev/null 2>&1 || true
  fi
  if [[ -n "${WEAVER_PID}" ]]; then
    kill "${WEAVER_PID}" >/dev/null 2>&1 || true
    wait "${WEAVER_PID}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

RUST_LOG="${RUST_LOG:-warn}" "${WEAVER_BIN}" --config "${CONFIG_PATH}" serve --port "${PORT}" >"${LOG_PATH}" 2>&1 &
WEAVER_PID=$!
echo "${WEAVER_PID}" > "${PID_PATH}"

GRAPHQL_URL="http://127.0.0.1:${PORT}/graphql"

python3 - "${GRAPHQL_URL}" <<'PY'
import json
import sys
import time
import urllib.request

url = sys.argv[1]
payload = json.dumps({"query": "{ version }"}).encode()
deadline = time.time() + 30
last_error = None
while time.time() < deadline:
    try:
        req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=3) as resp:
            if resp.status == 200:
                sys.exit(0)
    except Exception as exc:  # noqa: BLE001
        last_error = exc
    time.sleep(0.5)
raise SystemExit(f"timed out waiting for weaver graphql at {url}: {last_error}")
PY

if [[ "${PERF_STAT:-0}" == "1" ]]; then
  sudo -n perf stat -d -p "${WEAVER_PID}" -o "${PERF_STAT_PATH}" -- sleep "${DURATION_SEC}" &
  PERF_PID=$!
elif [[ "${PERF_RECORD:-0}" == "1" ]]; then
  sudo -n perf record -F "${PERF_FREQ:-999}" -g -p "${WEAVER_PID}" -o "${PERF_DATA_PATH}" -- sleep "${DURATION_SEC}" &
  PERF_PID=$!
fi

python3 - "${NZB_FILE}" "${CATEGORY}" "${JOB_PASSWORD}" "${SUBMIT_PAYLOAD_PATH}" <<'PY'
import base64
import json
import pathlib
import sys

nzb_path = pathlib.Path(sys.argv[1])
category = sys.argv[2]
job_password = sys.argv[3]
output_path = pathlib.Path(sys.argv[4])

payload = {
    "query": """
mutation($source: NzbSourceInput!, $filename: String, $password: String, $category: String) {
  submitNzb(source: $source, filename: $filename, password: $password, category: $category) {
    id
    status
    name
  }
}
""",
    "variables": {
        "source": {"nzbBase64": base64.b64encode(nzb_path.read_bytes()).decode()},
        "filename": nzb_path.name,
        "category": category,
    },
}
if job_password:
    payload["variables"]["password"] = job_password
output_path.write_text(json.dumps(payload), encoding="utf-8")
PY

SUBMIT_RESPONSE="$(curl -fsS -H 'Content-Type: application/json' --data-binary @"${SUBMIT_PAYLOAD_PATH}" "${GRAPHQL_URL}")"
JOB_ID="$(jq -r '.data.submitNzb.id // empty' <<<"${SUBMIT_RESPONSE}")"

if [[ -z "${JOB_ID}" || "${JOB_ID}" == "null" ]]; then
  echo "submit failed: ${SUBMIT_RESPONSE}" >&2
  exit 1
fi

START_TS="$(date +%s.%N)"
LAST_PRINT_MS=0
STATUS=""

while :; do
  NOW_TS="$(date +%s.%N)"
  ELAPSED_MS="$(python3 - "${START_TS}" "${NOW_TS}" <<'PY'
import sys
start = float(sys.argv[1])
now = float(sys.argv[2])
print(int((now - start) * 1000))
PY
)"

  jq -nc --argjson id "${JOB_ID}" '{
    query: "query($id: Int!) { job(id: $id) { id status progress health totalBytes downloadedBytes optionalRecoveryBytes optionalRecoveryDownloadedBytes failedBytes error } metrics { currentDownloadSpeed articlesPerSec decodeRateMbps bytesDownloaded bytesDecoded bytesCommitted segmentsDownloaded segmentsDecoded segmentsCommitted } }",
    variables: { id: $id }
  }' > "${QUERY_PAYLOAD_PATH}"
  SNAPSHOT="$(curl -fsS -H 'Content-Type: application/json' --data-binary @"${QUERY_PAYLOAD_PATH}" "${GRAPHQL_URL}")"
  jq -c --argjson elapsed_ms "${ELAPSED_MS}" '.data + {elapsed_ms: $elapsed_ms}' <<<"${SNAPSHOT}" >> "${SAMPLES_PATH}"

  STATUS="$(jq -r '.data.job.status' <<<"${SNAPSHOT}")"
  if (( ELAPSED_MS - LAST_PRINT_MS >= 5000 )); then
    LAST_PRINT_MS="${ELAPSED_MS}"
    PROGRESS="$(jq -r '.data.job.progress * 100' <<<"${SNAPSHOT}")"
    SPEED="$(jq -r '.data.metrics.currentDownloadSpeed' <<<"${SNAPSHOT}")"
    HEALTH="$(jq -r '.data.job.health / 10' <<<"${SNAPSHOT}")"
    echo "status=${STATUS} elapsed_ms=${ELAPSED_MS} progress=${PROGRESS} speed_bps=${SPEED} health=${HEALTH}"
  fi

  if [[ "${STATUS}" == "COMPLETE" || "${STATUS}" == "FAILED" ]]; then
    break
  fi

  if [[ "${DURATION_SEC}" != "0" ]] && (( ELAPSED_MS >= DURATION_SEC * 1000 )); then
    jq -nc --argjson id "${JOB_ID}" '{query: "mutation($id: Int!) { cancelJob(id: $id) }", variables: { id: $id }}' > "${CANCEL_PAYLOAD_PATH}"
    curl -fsS -H 'Content-Type: application/json' --data-binary @"${CANCEL_PAYLOAD_PATH}" "${GRAPHQL_URL}" >/dev/null || true
    STATUS="CANCELLED"
    break
  fi

  python3 - "${SAMPLE_MS}" <<'PY'
import sys
import time
time.sleep(int(sys.argv[1]) / 1000.0)
PY
done

if [[ -n "${PERF_PID}" ]]; then
  wait "${PERF_PID}" || true
  if [[ -f "${PERF_DATA_PATH}" ]]; then
    sudo -n perf report --stdio -i "${PERF_DATA_PATH}" > "${PERF_REPORT_PATH}" || true
  fi
fi

python3 - "${SAMPLES_PATH}" "${SUMMARY_PATH}" "${STATUS}" "${JOB_ID}" <<'PY'
import json
import pathlib
import sys

samples_path = pathlib.Path(sys.argv[1])
summary_path = pathlib.Path(sys.argv[2])
terminal_status = sys.argv[3]
job_id = int(sys.argv[4])

samples = [json.loads(line) for line in samples_path.read_text().splitlines() if line.strip()]
if not samples:
    raise SystemExit("no samples captured")

first = samples[0]
last = samples[-1]
first_byte_ms = None
for sample in samples:
    if int(sample["job"]["downloadedBytes"]) + int(sample["job"]["failedBytes"]) > 0:
        first_byte_ms = int(sample["elapsed_ms"])
        break

summary = {
    "job_id": job_id,
    "status": terminal_status,
    "duration_ms": int(last["elapsed_ms"]),
    "time_to_first_byte_ms": first_byte_ms,
    "peak_download_speed": max(int(sample["metrics"]["currentDownloadSpeed"]) for sample in samples),
    "peak_articles_per_sec": max(float(sample["metrics"]["articlesPerSec"]) for sample in samples),
    "peak_decode_rate_mbps": max(float(sample["metrics"]["decodeRateMbps"]) for sample in samples),
    "lowest_health": min(int(sample["job"]["health"]) for sample in samples),
    "final_total_bytes": int(last["job"]["totalBytes"]),
    "final_downloaded_bytes": int(last["job"]["downloadedBytes"]),
    "final_failed_bytes": int(last["job"]["failedBytes"]),
    "sample_count": len(samples),
}

summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
print(json.dumps(summary, indent=2))
PY

echo "artifacts: ${OUT_DIR}"
