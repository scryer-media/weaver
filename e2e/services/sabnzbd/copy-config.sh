#!/usr/bin/with-contenv sh
set -eu

tmp_config="/tmp/sabnzbd.ini"

awk \
  -v nntp_host="${SABNZBD_NNTP_HOST:-nntp}" \
  -v nntp_port="${SABNZBD_NNTP_PORT:-119}" '
  /^\[\[e2e\]\]$/ { in_e2e = 1; print; next }
  /^\[\[/ && in_e2e { in_e2e = 0 }
  in_e2e && /^host = / { print "host = " nntp_host; next }
  in_e2e && /^port = / { print "port = " nntp_port; next }
  { print }
' /config-src/sabnzbd.ini > "${tmp_config}"
if [ ! -f /config/sabnzbd.ini ] || ! cmp -s "${tmp_config}" /config/sabnzbd.ini; then
  install -o abc -g abc -m 0644 "${tmp_config}" /config/sabnzbd.ini
fi

mkdir -p /sabnzbd-downloads/complete /sabnzbd-downloads/incomplete
chown -R "${PUID:-1000}:${PGID:-1000}" /sabnzbd-downloads
chmod 0775 /sabnzbd-downloads /sabnzbd-downloads/complete /sabnzbd-downloads/incomplete
