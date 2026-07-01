#!/bin/sh
# nfpm deb pre-remove: stop + disable only on real removal, not on upgrade.
# deb passes "remove"/"purge" on uninstall and "upgrade" on an upgrade.
set -e

case "$1" in
  remove|purge)
    systemctl stop cs-agent.service 2>/dev/null || true
    systemctl disable cs-agent.service 2>/dev/null || true
    ;;
esac

exit 0
