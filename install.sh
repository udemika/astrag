#!/bin/bash
# Ubuntu 22.04 / 24.04 one-command systemd installer, updater, and uninstaller for AstraG.
# Supports two installation modes without persistent venv:
#   1) --apt    (default) : System Ubuntu APT packages only, zero pip / zero venv
#   2) --binary           : Single standalone compiled binary (/opt/astrag/astrag) like Cesbo Astra
set -euo pipefail

START=true
UNINSTALL=false
INSTALL_MODE=""
CUSTOM_PORT="${ASTRA_PORT:-8000}"
REPO_URL="${ASTRA_REPO_URL:-https://github.com/udemika/astrag.git}"

while [ "$#" -gt 0 ]; do
    case "$1" in
        --apt|--system) INSTALL_MODE="apt"; shift ;;
        --binary|--bin|--single-binary) INSTALL_MODE="binary"; shift ;;
        --mode)
            if [ "$#" -lt 2 ]; then echo "Missing mode ('apt' or 'binary') after --mode" >&2; exit 2; fi
            INSTALL_MODE="$2"; shift 2 ;;
        --start|--restart|--update) START=true; shift ;;
        --no-start) START=false; shift ;;
        --uninstall|--remove|--purge) UNINSTALL=true; shift ;;
        --port)
            if [ "$#" -lt 2 ]; then echo "Missing port number after --port" >&2; exit 2; fi
            CUSTOM_PORT="$2"; shift 2 ;;
        --repo)
            if [ "$#" -lt 2 ]; then echo "Missing repository URL after --repo" >&2; exit 2; fi
            REPO_URL="$2"; shift 2 ;;
        *)
            echo "Usage: sudo bash install.sh [--apt|--binary] [--start|--update|--no-start|--uninstall] [--port PORT] [--repo GIT_URL]" >&2
            exit 2
            ;;
    esac
done

if [ "$(id -u)" -ne 0 ]; then
    echo "Run as root (sudo)." >&2
    exit 1
fi

APP_DIR=/opt/astrag
STATIC_DIR="$APP_DIR/www"
CONFIG_DIR=/etc/astrag
STATE_DIR=/var/lib/astrag
SERVICE_USER=astrag

if "$UNINSTALL"; then
    echo "==> Stopping and removing AstraG services..."
    systemctl stop astrag.service 2>/dev/null || true
    systemctl disable astrag.service 2>/dev/null || true
    systemctl stop astra-clone.service 2>/dev/null || true
    systemctl disable astra-clone.service 2>/dev/null || true
    pkill -f "astra_clone_backend.py" 2>/dev/null || true
    pkill -f "astrag_backend.py" 2>/dev/null || true
    pkill -f "/opt/astrag/astrag" 2>/dev/null || true
    rm -f /etc/systemd/system/astrag.service /etc/systemd/system/astra-clone.service
    systemctl daemon-reload || true
    systemctl reset-failed 2>/dev/null || true
    rm -f /usr/bin/astrag /usr/bin/astra-clone
    rm -rf /opt/astrag /opt/astra-clone /etc/astrag /etc/astra-clone /var/lib/astrag /var/lib/astra-clone
    if id "$SERVICE_USER" >/dev/null 2>&1; then userdel "$SERVICE_USER" 2>/dev/null || true; fi
    if getent group "$SERVICE_USER" >/dev/null 2>&1; then groupdel "$SERVICE_USER" 2>/dev/null || true; fi
    if id "astra-clone" >/dev/null 2>&1; then userdel "astra-clone" 2>/dev/null || true; fi
    if getent group "astra-clone" >/dev/null 2>&1; then groupdel "astra-clone" 2>/dev/null || true; fi
    echo "==> AstraG has been completely removed from this server."
    exit 0
fi

# Auto-detect previous install mode on --update if --apt/--binary was not explicitly specified
if [ -z "$INSTALL_MODE" ]; then
    if [ -x "$APP_DIR/astrag" ] && [ ! -d "$APP_DIR/venv" ]; then
        INSTALL_MODE="binary"
    else
        INSTALL_MODE="apt"
    fi
fi

if [ "$INSTALL_MODE" != "apt" ] && [ "$INSTALL_MODE" != "binary" ]; then
    echo "Invalid install mode '$INSTALL_MODE' (expected 'apt' or 'binary')." >&2
    exit 2
fi

SCRIPT_DIR=""
if [ -n "${BASH_SOURCE[0]:-}" ] && [ -f "${BASH_SOURCE[0]}" ]; then
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
fi

TMP_CLONE=""
TMP_BUILD=""
cleanup_tmp() {
    if [ -n "$TMP_CLONE" ] && [ -d "$TMP_CLONE" ]; then
        rm -rf "$TMP_CLONE"
    fi
    if [ -n "$TMP_BUILD" ] && [ -d "$TMP_BUILD" ]; then
        rm -rf "$TMP_BUILD"
    fi
}
trap cleanup_tmp EXIT

if [ -z "$SCRIPT_DIR" ] || [ ! -f "$SCRIPT_DIR/astra_clone_backend.py" ]; then
    echo "==> Downloading AstraG source from GitHub ($REPO_URL)..."
    apt-get update -qq
    DEBIAN_FRONTEND=noninteractive apt-get install -y -qq git curl ca-certificates
    TMP_CLONE="$(mktemp -d /tmp/astrag-src.XXXXXX)"
    git clone --depth 1 "$REPO_URL" "$TMP_CLONE"
    SCRIPT_DIR="$TMP_CLONE"
fi

for file in astra_clone_backend.py stream_engine.py account_security.py create_admin.py www/index.html www/account.html www/mod.js; do
    if [ ! -f "$SCRIPT_DIR/$file" ]; then echo "Missing $SCRIPT_DIR/$file" >&2; exit 1; fi
done

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

# Remove any legacy /opt/astrag/venv so neither mode uses a persistent virtualenv
rm -rf "$APP_DIR/venv"

if [ "$INSTALL_MODE" = "apt" ]; then
    echo "==> [Mode 1: APT-Only] Installing Ubuntu system APT packages (zero pip, zero venv)..."
    apt-get update
    DEBIAN_FRONTEND=noninteractive apt-get install -y \
        python3 \
        python3-fastapi \
        python3-uvicorn \
        python3-pydantic \
        python3-starlette \
        python3-anyio \
        python3-h11 \
        python3-click \
        curl \
        ca-certificates
    # Optional WebSocket / multipart / HTTPX system packages across Ubuntu 22.04 and 24.04
    DEBIAN_FRONTEND=noninteractive apt-get install -y \
        python3-websockets python3-wsproto python3-multipart python3-httpx uvicorn 2>/dev/null || true

    python3 -c 'import sys, fastapi, uvicorn, pydantic; assert sys.version_info >= (3, 10), "Python 3.10+ required"'

    rm -f "$APP_DIR/astrag"
    for file in astra_clone_backend.py stream_engine.py account_security.py create_admin.py; do
        install -o root -g root -m 0644 "$SCRIPT_DIR/$file" "$APP_DIR/$file"
    done
    if [ -f "$SCRIPT_DIR/astrag_backend.py" ]; then
        install -o root -g root -m 0644 "$SCRIPT_DIR/astrag_backend.py" "$APP_DIR/astrag_backend.py"
    else
        install -o root -g root -m 0644 "$SCRIPT_DIR/astra_clone_backend.py" "$APP_DIR/astrag_backend.py"
    fi

    EXEC_START_CMD="/usr/bin/python3 /opt/astrag/astra_clone_backend.py"
    cat > /usr/bin/astrag <<'EOF'
#!/bin/sh
export MALLOC_MMAP_THRESHOLD_=131072
export MALLOC_TRIM_THRESHOLD_=131072
export MALLOC_ARENA_MAX=2
exec /usr/bin/python3 /opt/astrag/astra_clone_backend.py "$@"
EOF

else
    echo "==> [Mode 2: Single Binary] Compiling AstraG into a standalone binary (/opt/astrag/astrag) like Cesbo Astra..."
    apt-get update
    DEBIAN_FRONTEND=noninteractive apt-get install -y \
        python3 python3-dev python3-venv python3-pip binutils build-essential patchelf curl ca-certificates
    python3 -c 'import sys; assert sys.version_info >= (3, 10), "Python 3.10+ required"'

    TMP_BUILD="$(mktemp -d /tmp/astrag-build.XXXXXX)"
    python3 -m venv "$TMP_BUILD/venv"
    "$TMP_BUILD/venv/bin/python" -m pip install --upgrade pip
    "$TMP_BUILD/venv/bin/python" -m pip install -r "$SCRIPT_DIR/requirements.txt" pyinstaller

    "$TMP_BUILD/venv/bin/pyinstaller" \
        --onefile --clean --noconfirm \
        --name astrag \
        --distpath "$TMP_BUILD/dist" \
        --workpath "$TMP_BUILD/work" \
        --specpath "$TMP_BUILD" \
        --paths "$SCRIPT_DIR" \
        --add-data "$SCRIPT_DIR/www:www" \
        --hidden-import stream_engine \
        --hidden-import account_security \
        --hidden-import astra_clone_backend \
        --hidden-import uvicorn.logging \
        --hidden-import uvicorn.loops \
        --hidden-import uvicorn.loops.auto \
        --hidden-import uvicorn.loops.asyncio \
        --hidden-import uvicorn.protocols \
        --hidden-import uvicorn.protocols.http \
        --hidden-import uvicorn.protocols.http.auto \
        --hidden-import uvicorn.protocols.http.h11_impl \
        --hidden-import uvicorn.protocols.websockets \
        --hidden-import uvicorn.protocols.websockets.auto \
        --hidden-import uvicorn.protocols.websockets.websockets_impl \
        --hidden-import uvicorn.protocols.websockets.wsproto_impl \
        --hidden-import uvicorn.lifespan \
        --hidden-import uvicorn.lifespan.on \
        "$SCRIPT_DIR/astra_clone_backend.py"

    systemctl stop astrag.service 2>/dev/null || true
    install -o root -g root -m 0755 "$TMP_BUILD/dist/astrag" "$APP_DIR/astrag"
    install -o root -g root -m 0644 "$SCRIPT_DIR/create_admin.py" "$APP_DIR/create_admin.py"
    install -o root -g root -m 0644 "$SCRIPT_DIR/account_security.py" "$APP_DIR/account_security.py"
    rm -f "$APP_DIR/astra_clone_backend.py" "$APP_DIR/astrag_backend.py" "$APP_DIR/stream_engine.py" "$APP_DIR/requirements.txt"
    rm -rf "$TMP_BUILD"
    TMP_BUILD=""

    EXEC_START_CMD="/opt/astrag/astrag"
    cat > /usr/bin/astrag <<'EOF'
#!/bin/sh
export MALLOC_MMAP_THRESHOLD_=131072
export MALLOC_TRIM_THRESHOLD_=131072
export MALLOC_ARENA_MAX=2
exec /opt/astrag/astrag "$@"
EOF
fi

chown root:root /usr/bin/astrag
chmod 0755 /usr/bin/astrag
ln -sf /usr/bin/astrag /usr/bin/astra-clone

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
install -o root -g root -m 0644 "$SERVICE_SRC" /etc/systemd/system/astrag.service
sed -i "s|^ExecStart=.*|ExecStart=${EXEC_START_CMD}|" /etc/systemd/system/astrag.service

if command -v ufw >/dev/null 2>&1 && ufw status 2>/dev/null | grep -q "Status: active"; then
    ufw allow "${CUSTOM_PORT:-8000}/tcp" >/dev/null 2>&1 || true
fi

systemctl daemon-reload
if "$START"; then
    systemctl enable astrag.service
    systemctl restart astrag.service
    sleep 1
    if ! systemctl is-active --quiet astrag.service; then
        echo "ERROR: astrag.service failed to start. Recent logs:" >&2
        journalctl -u astrag.service -n 30 --no-pager >&2 || true
        exit 1
    fi
    SERVER_IP="$(hostname -I 2>/dev/null | awk '{print $1}' || echo '127.0.0.1')"
    echo "================================================================"
    echo " AstraG successfully installed and started! (Mode: ${INSTALL_MODE})"
    echo " Web Control Panel : http://${SERVER_IP}:${CUSTOM_PORT:-8000}"
    echo " Default Login     : admin"
    echo " Default Password  : admin"
    echo " Playlist M3U8     : http://${SERVER_IP}:${CUSTOM_PORT:-8000}/playlist.m3u8"
    echo "================================================================"
else
    echo "Installed AstraG (${INSTALL_MODE} mode) into /opt/astrag. Start with: sudo systemctl enable --now astrag.service"
fi
echo "Logs: sudo journalctl -u astrag.service -f"


