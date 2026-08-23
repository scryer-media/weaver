#!/usr/bin/env bash
# avx2-aws-run.sh — launch a self-contained, repeatable AVX2 weaver-vs-rapidyenc
# perf profile on a fresh AWS AMD Zen2/Zen3 (c5a/c6a) instance. One command:
# bundles the current commit, uploads it to S3, boots
# an instance that runs ci/bench/avx2-profile.sh unattended via cloud-init,
# publishes results back to S3, and self-terminates. Re-run = run this again.
#
# Prereqs (one-time):
#   - awscli v2 configured (`aws configure`) with EC2 + S3 + SSM read perms
#   - an S3 bucket you own                          (export S3_BUCKET=...)
#   - an EC2 instance profile restricted to `s3:GetObject` and `s3:PutObject`
#     for `arn:aws:s3:::<bucket>/<prefix>/*`. Create and review that role outside
#     this script, then export INSTANCE_PROFILE=<profile-name>.
#
# Usage:
#   S3_BUCKET=my-bucket INSTANCE_PROFILE=weaver-avx2 ./ci/bench/avx2-aws-run.sh
#   (optional) AWS_REGION=us-east-1 INSTANCE_TYPE=c5a.2xlarge TERMINATE=1 KEY_NAME=mykey
set -euo pipefail

# ── config ───────────────────────────────────────────────────────────────────
S3_BUCKET="${S3_BUCKET:?set S3_BUCKET to an S3 bucket you own}"
S3_PREFIX="${S3_PREFIX:-weaver-avx2}"
INSTANCE_PROFILE="${INSTANCE_PROFILE:?set INSTANCE_PROFILE to an EC2 instance profile that can access the bucket (see header)}"
INSTANCE_TYPE="${INSTANCE_TYPE:-c5a.2xlarge}"     # AMD Zen 2 = Windows AVX2 target uarch
AWS_REGION="${AWS_REGION:-$(aws configure get region 2>/dev/null || echo us-east-1)}"
KEY_NAME="${KEY_NAME:-}"                          # optional, only for SSH debugging
SECURITY_GROUP_ID="${SECURITY_GROUP_ID:-}"       # optional; default VPC SG if empty
SUBNET_ID="${SUBNET_ID:-}"                        # optional; default subnet if empty
TERMINATE="${TERMINATE:-1}"
VOLUME_GB="${VOLUME_GB:-30}"
DRY_RUN="${DRY_RUN:-0}"
INCLUDE_TRACKED_CHANGES="${INCLUDE_TRACKED_CHANGES:-0}"
BENCH_TAG="${BENCH_TAG:-avx2}"       # instance Name suffix + weaver-bench tag (avx2 | avx512)

log(){ printf '\033[1;34m[aws-run]\033[0m %s\n' "$*"; }
die(){ printf '\033[1;31m[aws-run:FAIL]\033[0m %s\n' "$*" >&2; exit 1; }

command -v aws >/dev/null 2>&1 || die "awscli not found — install it and run: aws configure"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WEAVER_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
[ -f "$WEAVER_ROOT/Cargo.toml" ] || die "expected weaver workspace root at $WEAVER_ROOT (no Cargo.toml)"
USERDATA="${USERDATA:-$SCRIPT_DIR/avx2-aws-userdata.sh}"
[ -f "$USERDATA" ] || die "missing user-data template: $USERDATA (set USERDATA=/abs/path)"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"

# ── 1. bundle source ─────────────────────────────────────────────────────────
TARBALL="/tmp/weaver-src-$STAMP.tar.gz"
command -v git >/dev/null 2>&1 || die "git not found (needed to select the files to bundle)"
git -C "$WEAVER_ROOT" rev-parse --is-inside-work-tree >/dev/null 2>&1 \
  || die "$WEAVER_ROOT is not a git work tree"
if [ "$INCLUDE_TRACKED_CHANGES" = "1" ]; then
  log "bundling tracked working-tree files (explicit INCLUDE_TRACKED_CHANGES=1; untracked files excluded)…"
  git -C "$WEAVER_ROOT" ls-files -z --cached -- . \
      ':(exclude)ci/bench/results/**' \
      ':(exclude)**/tests/fixtures/**' \
      ':(exclude)ci/vendor/**' \
      ':(exclude)apps/weaver-web/public/**' \
    | tar czf "$TARBALL" -C "$WEAVER_ROOT" --null -T -
else
  log "bundling committed HEAD at $WEAVER_ROOT…"
  git -C "$WEAVER_ROOT" archive --format=tar.gz --output="$TARBALL" HEAD -- . \
      ':(exclude)ci/bench/results/**' \
      ':(exclude)**/tests/fixtures/**' \
      ':(exclude)ci/vendor/**' \
      ':(exclude)apps/weaver-web/public/**'
fi
log "tarball: $TARBALL ($(du -h "$TARBALL" | cut -f1))"

S3_CODE="s3://$S3_BUCKET/$S3_PREFIX/weaver-src-$STAMP.tar.gz"
S3_RESULTS="s3://$S3_BUCKET/$S3_PREFIX/results/"
log "uploading code -> $S3_CODE"
[ "$DRY_RUN" = "1" ] || aws s3 cp "$TARBALL" "$S3_CODE" --region "$AWS_REGION"

# ── 2. resolve latest Ubuntu 24.04 amd64 AMI for the region (via SSM) ─────────
log "resolving Ubuntu 24.04 AMI (region $AWS_REGION)…"
AMI_ID="$(aws ssm get-parameter --region "$AWS_REGION" \
  --name /aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id \
  --query Parameter.Value --output text 2>/dev/null || true)"
[ -n "$AMI_ID" ] && [ "$AMI_ID" != "None" ] || die "could not resolve Ubuntu 24.04 AMI in $AWS_REGION"
log "AMI: $AMI_ID"

# ── 3. render user-data from the template ────────────────────────────────────
UD="/tmp/avx2-userdata-$STAMP.sh"
sed -e "s|__S3_CODE__|$S3_CODE|g" \
    -e "s|__S3_RESULTS__|$S3_RESULTS|g" \
    -e "s|__GIT_URL__||g" -e "s|__GIT_REF__||g" \
    -e "s|__TERMINATE__|$TERMINATE|g" \
    "$USERDATA" > "$UD"

# ── 4. launch ────────────────────────────────────────────────────────────────
RUN_ARGS=(
  --region "$AWS_REGION"
  --image-id "$AMI_ID"
  --instance-type "$INSTANCE_TYPE"
  --iam-instance-profile "Name=$INSTANCE_PROFILE"
  --user-data "file://$UD"
  --instance-initiated-shutdown-behavior terminate
  --block-device-mappings "DeviceName=/dev/sda1,Ebs={VolumeSize=$VOLUME_GB,VolumeType=gp3}"
  --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=weaver-$BENCH_TAG-profile},{Key=weaver-bench,Value=$BENCH_TAG}]"
  --count 1
)
[ -n "$KEY_NAME" ]          && RUN_ARGS+=( --key-name "$KEY_NAME" )
[ -n "$SECURITY_GROUP_ID" ] && RUN_ARGS+=( --security-group-ids "$SECURITY_GROUP_ID" )
[ -n "$SUBNET_ID" ]         && RUN_ARGS+=( --subnet-id "$SUBNET_ID" )

if [ "$DRY_RUN" = "1" ]; then
  log "DRY_RUN=1 — would run: aws ec2 run-instances ${RUN_ARGS[*]}"
  exit 0
fi

log "launching $INSTANCE_TYPE …"
IID="$(aws ec2 run-instances "${RUN_ARGS[@]}" --query 'Instances[0].InstanceId' --output text)"
[ -n "$IID" ] || die "run-instances returned no instance id"

TERM_MSG="$([ "$TERMINATE" = 1 ] && echo self-terminate || echo 'stay up (TERMINATE=0)')"
cat <<EOF

============================================================
 launched: $IID  ($INSTANCE_TYPE, $AWS_REGION)
 It will: install deps -> build weaver+rapidyenc -> perf profile ->
          push results to S3 -> $TERM_MSG.
 Expect ~8-15 min.

 Results (tarball) will appear at:
   ${S3_RESULTS}avx2-results-<stamp>.tar.gz
   fetch:  aws s3 cp ${S3_RESULTS} . --recursive --region $AWS_REGION

 Watch progress / read summary+perf-stat on the console:
   aws ec2 get-console-output --region $AWS_REGION --instance-id $IID --output text | tail -120

 Status:
   aws ec2 describe-instances --region $AWS_REGION --instance-ids $IID \\
     --query 'Reservations[0].Instances[0].State.Name' --output text
============================================================
EOF
