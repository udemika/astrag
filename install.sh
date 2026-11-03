#!/bin/bash
# Ubuntu 22.04 / 24.04 systemd installer and updater for AstraG.
set -euo pipefail
START=false
CUSTOM_PORT="${ASTRA_PORT:-}"
while [ "$#" -gt 0 ]; do
    case "$1" in
        --start|--restart|--update) START=true; shift ;;
        --port)
            if [ "$#" -lt 2 ]; then echo "Missing port number after --port" >&2; exit 2; fi
            CUSTOM_PORT="$2"; shift 2 ;;
        *) echo "Usage: sudo bash install.sh [--start|--update] [--port PORT]" >&2; exit 2 ;;
    esac
done
if [ "$(id -u)" -ne 0 ]; then echo "Run as root (sudo)." >&2; exit 1; fi
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_DIR=/opt/astrag
STATIC_DIR="$APP_DIR/www"
CONFIG_DIR=/etc/astrag
STATE_DIR=/var/lib/astrag
SERVICE_USER=astrag
for file in astra_clone_backend.py stream_engine.py account_security.py create_admin.py requirements.txt www/index.html www/account.html www/mod.js; do
    if [ ! -f "$SCRIPT_DIR/$file" ]; then echo "Missing $SCRIPT_DIR/$file" >&2; exit 1; fi
done
apt-get update
DEBIAN_FRONTEND=noninteractive apt-get install -y python3 python3-venv
python3 -c 'import sys; assert sys.version_info >= (3, 10), "Python 3.10+ required"'
if ! getent group "$SERVICE_USER" >/dev/null; then groupadd --system "$SERVICE_USER"; fi
if ! id "$SERVICE_USER" >/dev/null 2>&1; then
    useradd --system --gid "$SERVICE_USER" --home-dir "$STATE_DIR" --no-create-home --shell /usr/sbin/nologin "$SERVICE_USER"
fi
if [ "$(id -u "$SERVICE_USER")" -eq 0 ]; then echo "Refusing root service user" >&2; exit 1; fi
install -d -o root -g root -m 0755 "$APP_DIR" "$STATIC_DIR"
install -d -o root -g "$SERVICE_USER" -m 0750 "$CONFIG_DIR"
install -d -o "$SERVICE_USER" -g "$SERVICE_USER" -m 0750 "$STATE_DIR"

# Seamless migration from legacy /var/lib/astra-clone and /etc/astra-clone if present
if [ ! -f "$STATE_DIR/state.json" ] && [ -f /var/lib/astra-clone/state.json ]; then
    cp -a /var/lib/astra-clone/state.json "$STATE_DIR/state.json"
    chown "$SERVICE_USER:$SERVICE_USER" "$STATE_DIR/state.json"
    chmod 0640 "$STATE_DIR/state.json"
fi
if systemctl is-active --quiet astra-clone.service 2>/dev/null; then
    systemctl stop astra-clone.service || true
    systemctl disable astra-clone.service || true
fi

for file in astra_clone_backend.py stream_engine.py account_security.py create_admin.py requirements.txt; do
    install -o root -g root -m 0644 "$SCRIPT_DIR/$file" "$APP_DIR/$file"
done
if [ -f "$SCRIPT_DIR/astrag_backend.py" ]; then
    install -o root -g root -m 0644 "$SCRIPT_DIR/astrag_backend.py" "$APP_DIR/astrag_backend.py"
fi
# Preserve installed static files before updating, never recursively delete them.
if [ -f "$STATIC_DIR/index.html" ]; then
    install -d -o root -g root -m 0700 "$APP_DIR/backups"
    saved="$(mktemp -d "$APP_DIR/backups/www.XXXXXXXX")"
    cp -a "$STATIC_DIR/." "$saved/"
fi
cp -R --no-preserve=ownership "$SCRIPT_DIR/www/." "$STATIC_DIR/"
chown -R root:root "$STATIC_DIR"
find "$STATIC_DIR" -type d -exec chmod 0755 {} +
find "$STATIC_DIR" -type f -exec chmod 0644 {} +
python3 -m venv "$APP_DIR/venv"
"$APP_DIR/venv/bin/python" -m pip install -r "$APP_DIR/requirements.txt"
chown -R root:root "$APP_DIR/venv"
chmod -R go-w "$APP_DIR/venv"
# This is a systemd EnvironmentFile, not executable shell or mutable state.
if [ ! -e "$CONFIG_DIR/astrag.conf" ]; then
    (umask 027; cat > "$CONFIG_DIR/astrag.conf" <<EOF
ASTRA_HOST=0.0.0.0
ASTRA_PORT=${CUSTOM_PORT:-8000}
ASTRA_STATIC_DIR=/opt/astrag/www
ASTRA_STATE_FILE=/var/lib/astrag/state.json
ASTRA_CONFIG_FILE=/etc/astrag/astrag.conf
EOF
    )
elif [ -n "$CUSTOM_PORT" ]; then
    if grep -q "^ASTRA_PORT=" "$CONFIG_DIR/astrag.conf"; then
        sed -i "s/^ASTRA_PORT=.*/ASTRA_PORT=${CUSTOM_PORT}/" "$CONFIG_DIR/astrag.conf"
    else
        echo "ASTRA_PORT=${CUSTOM_PORT}" >> "$CONFIG_DIR/astrag.conf"
    fi
fi
if ! grep -q "^ASTRA_HOST=" "$CONFIG_DIR/astrag.conf"; then
    echo "ASTRA_HOST=0.0.0.0" >> "$CONFIG_DIR/astrag.conf"
fi
chown root:"$SERVICE_USER" "$CONFIG_DIR/astrag.conf"
chmod 0640 "$CONFIG_DIR/astrag.conf"
SERVICE_SRC="$SCRIPT_DIR/astrag.service"
if [ ! -f "$SERVICE_SRC" ]; then SERVICE_SRC="$SCRIPT_DIR/astra-clone.service"; fi
install -o root -g root -m 0644 "$SERVICE_SRC" /etc/systemd/system/astrag.service
install -o root -g root -m 0644 "$SERVICE_SRC" /etc/systemd/system/astra-clone.service
cat > /usr/bin/astrag <<'EOF'
#!/bin/sh
exec /opt/astrag/venv/bin/python /opt/astrag/astra_clone_backend.py "$@"
EOF
chown root:root /usr/bin/astrag
chmod 0755 /usr/bin/astrag
ln -sf /usr/bin/astrag /usr/bin/astra-clone
systemctl daemon-reload
if "$START"; then
    systemctl enable astrag.service
    systemctl restart astrag.service
    echo "Service astrag enabled and restarted (default login: admin / admin)."
else
    echo "Installed AstraG into /opt/astrag. Service was NOT enabled, started, or restarted."
    echo "Review /etc/astrag/astrag.conf, then:"
    echo "  sudo systemctl enable --now astrag.service"
    echo "For an already running service, restart it explicitly to load changes."
fi
echo "Logs: journalctl -u astrag.service -f"
