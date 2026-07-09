#!/bin/bash
# avx512-aws-userdata.sh — EC2 cloud-init user-data. Runs on boot as root on a
# fresh Ubuntu 24.04 Intel AVX-512 (c6i/c7i) or AMD Zen4 (c7a) instance and drives
# the whole VBMI2 weaver-vs-rapidyenc validation end to end, unattended:
#   1. fetch the weaver working tree (S3 tarball — includes uncommitted WIP, i.e.
#      the new decode_kernel_avx512_raw — or a git clone as fallback)
#   2. run ci/bench/avx512-profile.sh (installs deps, builds weaver + rapidyenc,
#      runs cargo test on real AVX-512, then the same-run VBMI2 ratio)
#   3. push the results tarball to S3 AND echo summary to the serial console
#   4. self-terminate (if enabled)
#
# The launcher (avx2-aws-run.sh) substitutes the __PLACEHOLDERS__ below.
set -uxo pipefail

# ── config (launcher fills these) ────────────────────────────────────────────
S3_CODE="__S3_CODE__"
S3_RESULTS="__S3_RESULTS__"
GIT_URL="__GIT_URL__"
GIT_REF="__GIT_REF__"
TERMINATE="__TERMINATE__"
RUN_USER="ubuntu"
HOME_DIR="$(getent passwd "$RUN_USER" | cut -d: -f6)"
[ -n "$HOME_DIR" ] || HOME_DIR="$(eval echo "~$RUN_USER")"
WEAVER_DIR="$HOME_DIR/weaver"

echo "=== avx512-aws-userdata starting $(date -u +%FT%TZ) on $(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2-) ==="

export DEBIAN_FRONTEND=noninteractive
apt-get update -y
apt-get install -y git curl tar unzip ca-certificates || true
if ! command -v aws >/dev/null 2>&1; then
  curl -fsSL "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o /tmp/awscliv2.zip \
    && unzip -q /tmp/awscliv2.zip -d /tmp && /tmp/aws/install && hash -r
fi
command -v aws >/dev/null 2>&1 || { echo "FATAL: aws cli install failed — cannot fetch code"; exit 1; }

# ── 1. fetch the weaver tree ─────────────────────────────────────────────────
rm -rf "$WEAVER_DIR"; mkdir -p "$WEAVER_DIR"
if [ -n "$S3_CODE" ]; then
  echo "=== fetching code tarball $S3_CODE ==="
  aws s3 cp "$S3_CODE" /tmp/weaver-src.tar.gz
  tar xzf /tmp/weaver-src.tar.gz -C "$WEAVER_DIR" --strip-components=0
elif [ -n "$GIT_URL" ]; then
  echo "=== git clone $GIT_URL @ $GIT_REF ==="
  git clone "$GIT_URL" "$WEAVER_DIR"
  ( cd "$WEAVER_DIR" && [ -n "$GIT_REF" ] && git checkout "$GIT_REF" || true )
else
  echo "FATAL: no S3_CODE and no GIT_URL — nothing to run"; exit 1
fi
chown -R "$RUN_USER:$RUN_USER" "$HOME_DIR"

echo 1 > /proc/sys/kernel/perf_event_paranoid || true
echo 0 > /proc/sys/kernel/kptr_restrict || true

# ── 2. run the profile as the normal user ────────────────────────────────────
echo "=== running avx512-profile.sh as $RUN_USER ==="
sudo -u "$RUN_USER" -H bash -lc "cd '$WEAVER_DIR' && chmod +x ci/bench/avx512-profile.sh && ./ci/bench/avx512-profile.sh" \
  || echo "WARN: avx512-profile.sh returned non-zero (results may still be partial)"

# ── 3. collect + publish results ─────────────────────────────────────────────
RES_DIR="$(ls -1dt "$WEAVER_DIR"/ci/bench/results/avx512-* 2>/dev/null | head -1)"
if [ -n "$RES_DIR" ] && [ -d "$RES_DIR" ]; then
  TARBALL="/tmp/avx512-results-$(basename "$RES_DIR").tar.gz"
  tar czf "$TARBALL" -C "$(dirname "$RES_DIR")" "$(basename "$RES_DIR")"
  cp "$TARBALL" "$HOME_DIR/" && chown "$RUN_USER:$RUN_USER" "$HOME_DIR/$(basename "$TARBALL")"
  if [ -n "$S3_RESULTS" ]; then
    echo "=== uploading results to $S3_RESULTS ==="
    aws s3 cp "$TARBALL" "${S3_RESULTS%/}/$(basename "$TARBALL")" || echo "WARN: S3 upload failed (check instance IAM role)"
  fi
  echo "########################## SUMMARY ##########################"
  cat "$RES_DIR/summary.txt" 2>/dev/null || true
  echo "###################### decode_timing ########################"
  cat "$RES_DIR/decode_timing.txt" 2>/dev/null || true
  echo "###################### cargo test ###########################"
  grep -E 'test result|FAILED' "$RES_DIR/cargo-test.txt" 2>/dev/null | tail -5 || true
  echo "#############################################################"
else
  echo "FATAL: no results dir produced under $WEAVER_DIR/ci/bench/results/"
fi

echo "=== avx512-aws-userdata DONE $(date -u +%FT%TZ) ==="
sync
if [ "$TERMINATE" = "1" ]; then
  echo "=== self-terminating in 20s (set TERMINATE=0 to keep the box) ==="
  sleep 20
  shutdown -h now
fi
