#!/bin/sh
# nfpm deb post-install: reload units, enable for boot, and (re)start.
# The unit's ConditionPathExists=/etc/computestacks/agent.yml gates a fresh, unconfigured
# install from actually running — `restart` is a no-op-skip in that case, not a failure.
set -e

systemctl daemon-reload 2>/dev/null || true
systemctl enable cs-agent.service 2>/dev/null || true
# Upgrade: restart to pick up the new binary. Fresh install: start if configured (condition gates it).
systemctl restart cs-agent.service 2>/dev/null || true

exit 0
