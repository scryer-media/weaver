#!/usr/bin/with-contenv sh
set -eu

tmp_config="/tmp/nzbget.conf"

cp /config-src/nzbget.conf "${tmp_config}"

sed -i \
  -e "s/^Server1.Host=.*/Server1.Host=${NZBGET_NNTP_HOST:-nntp}/" \
  -e "s/^Server1.Port=.*/Server1.Port=${NZBGET_NNTP_PORT:-119}/" \
  "${tmp_config}"

case "${NZBGET_CONFIG_VARIANT:-default}" in
  default)
    ;;
  manual-import)
    sed -i 's/^RenameAfterUnpack=yes$/RenameAfterUnpack=no/' "${tmp_config}"
    ;;
  *)
    echo "unknown NZBGet config variant: ${NZBGET_CONFIG_VARIANT}" >&2
    exit 1
    ;;
esac

if [ ! -f /config/nzbget.conf ] || ! cmp -s "${tmp_config}" /config/nzbget.conf; then
  install -o abc -g abc -m 0644 "${tmp_config}" /config/nzbget.conf
fi

mkdir -p /nzbget-downloads/completed /nzbget-downloads/intermediate
chown -R "${PUID:-1000}:${PGID:-1000}" /nzbget-downloads
chmod 0775 /nzbget-downloads /nzbget-downloads/completed /nzbget-downloads/intermediate
