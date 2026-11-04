#!/bin/bash
# Complete uninstaller for AstraG (and legacy astra-clone) on Ubuntu 22.04 / 24.04
set -euo pipefail

if [ "$(id -u)" -ne 0 ]; then
    echo "Run as root: sudo bash uninstall.sh" >&2
    exit 1
fi

echo "==> Stopping and disabling AstraG services..."
systemctl stop astrag.service 2>/dev/null || true
systemctl disable astrag.service 2>/dev/null || true
systemctl stop astra-clone.service 2>/dev/null || true
systemctl disable astra-clone.service 2>/dev/null || true

pkill -f "astra_clone_backend.py" 2>/dev/null || true
pkill -f "astrag_backend.py" 2>/dev/null || true

echo "==> Removing systemd units and CLI launchers..."
rm -f /etc/systemd/system/astrag.service /etc/systemd/system/astra-clone.service
systemctl daemon-reload || true
systemctl reset-failed 2>/dev/null || true
rm -f /usr/bin/astrag /usr/bin/astra-clone

echo "==> Removing application, config, and state directories..."
rm -rf /opt/astrag /opt/astra-clone /etc/astrag /etc/astra-clone /var/lib/astrag /var/lib/astra-clone

echo "==> Removing system user and group..."
if id "astrag" >/dev/null 2>&1; then userdel "astrag" 2>/dev/null || true; fi
if getent group "astrag" >/dev/null 2>&1; then groupdel "astrag" 2>/dev/null || true; fi
if id "astra-clone" >/dev/null 2>&1; then userdel "astra-clone" 2>/dev/null || true; fi
if getent group "astra-clone" >/dev/null 2>&1; then groupdel "astra-clone" 2>/dev/null || true; fi

echo "==> AstraG has been completely uninstalled."
