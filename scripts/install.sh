#!/usr/bin/env bash
set -Eeuo pipefail

APP="baqs"
APP_NAME="Broadcast Archive Query Service"
INSTALL_DIR="/opt/baqs"
CONFIG_DIR="/etc/baqs"
DATA_DIR="/var/lib/baqs"
SERVICE_NAME="baqs.service"
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}"

SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

OLAF_VERSION="${OLAF_VERSION:-v3.3.3}"
OLAF_REPO="https://github.com/JorenSix/Olaf.git"
OLAF_URL="https://github.com/JorenSix/Olaf/releases/download/${OLAF_VERSION}"
ZIG_VERSION="${ZIG_VERSION:-0.16.0}"
ZIG_INSTALL_DIR="/opt/zig-${ZIG_VERSION}"
RUST_HOME="/root/.rustup"
CARGO_HOME="/root/.cargo"

export RUSTUP_HOME="$RUST_HOME"
export CARGO_HOME="$CARGO_HOME"
export PATH="$CARGO_HOME/bin:/usr/local/bin:/usr/bin:/bin:$PATH"

CLI_ONLY=0
for arg in "$@"; do
    case "$arg" in
        --cli) CLI_ONLY=1 ;;
        -h|--help)
            echo "Usage: sudo bash scripts/install.sh [--cli]"
            exit 0
            ;;
        *) echo "Unknown argument: $arg" >&2; exit 2 ;;
    esac
done

log() { echo; echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }
fail() { echo; echo "ERROR: $*" >&2; exit 1; }
command_exists() { command -v "$1" >/dev/null 2>&1; }
cleanup() { [[ -n "${TMP:-}" && -d "$TMP" ]] && rm -rf "$TMP"; return 0; }

olaf_is_usable() { command_exists olaf && olaf help >/dev/null 2>&1; }
zig_is_usable() {
    command_exists zig \
        && [[ "$(zig version 2>/dev/null || true)" == "$ZIG_VERSION" ]] \
        && zig env >/dev/null 2>&1
}

echo "======================================"
echo " BAQS Installer"
echo " $APP_NAME"
echo "======================================"

[[ "$EUID" -eq 0 ]] || fail "Run this installer as root: sudo bash scripts/install.sh"

if [[ "$CLI_ONLY" -eq 0 && -f "$SERVICE_FILE" && "${BAQS_FORCE:-0}" != "1" ]]; then
    echo
    echo "BAQS is already installed ($SERVICE_FILE exists)."
    echo "Use scripts/update.sh to upgrade, or BAQS_FORCE=1 to reinstall."
    exit 0
fi

if [[ ! -f /etc/os-release ]]; then
    fail "Cannot identify operating system."
fi
. /etc/os-release
if [[ "$ID" != "ubuntu" ]] || [[ "$VERSION_ID" != "22.04" && "$VERSION_ID" != "24.04" && "$VERSION_ID" != "26.04" ]]; then
    fail "Only Ubuntu 22.04, 24.04, and 26.04 are supported."
fi

ARCH="$(uname -m)"
case "$ARCH" in
    x86_64) OLAF_ARCH="x86_64"; ZIG_ARCH="x86_64" ;;
    aarch64|arm64) OLAF_ARCH="aarch64"; ZIG_ARCH="aarch64" ;;
    *) fail "Unsupported architecture: $ARCH" ;;
esac

TMP="$(mktemp -d)"
trap cleanup EXIT

log "[1/8] Installing system dependencies..."
export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y \
    build-essential git make curl wget xz-utils ca-certificates \
    pkg-config ffmpeg libsqlite3-dev liblmdb-dev openssl python3

for binary in ffmpeg ffprobe; do
    command_exists "$binary" || fail "$binary installation failed."
done

install_zig() {
    if zig_is_usable; then
        return 0
    fi
    log "Installing Zig ${ZIG_VERSION}..."
    local archive="zig-${ZIG_ARCH}-linux-${ZIG_VERSION}.tar.xz"
    curl -fL "https://ziglang.org/download/${ZIG_VERSION}/${archive}" -o "$TMP/zig.tar.xz"
    tar -xf "$TMP/zig.tar.xz" -C "$TMP"
    local extracted="$TMP/zig-${ZIG_ARCH}-linux-${ZIG_VERSION}"
    [[ -x "$extracted/zig" && -d "$extracted/lib" ]] || fail "Invalid Zig archive."
    rm -rf "$ZIG_INSTALL_DIR"
    mkdir -p "$(dirname "$ZIG_INSTALL_DIR")"
    mv "$extracted" "$ZIG_INSTALL_DIR"
    rm -f /usr/local/bin/zig
    ln -s "$ZIG_INSTALL_DIR/zig" /usr/local/bin/zig
    zig_is_usable || fail "Zig installation failed."
}

olaf_release_digest() {
    if [[ -n "${OLAF_SHA256:-}" ]]; then
        printf '%s\n' "$OLAF_SHA256"
        return 0
    fi
    local asset="olaf_cli-linux-${OLAF_ARCH}.tar.gz"
    curl -fsSL "https://api.github.com/repos/JorenSix/Olaf/releases/tags/${OLAF_VERSION}" 2>/dev/null \
        | python3 -c '
import json, sys
asset = sys.argv[1]
try:
    release = json.load(sys.stdin)
except Exception:
    raise SystemExit(0)
for item in release.get("assets", []):
    if item.get("name") == asset and item.get("digest"):
        print(item["digest"].split(":", 1)[-1])
        raise SystemExit(0)
' "$asset" 2>/dev/null || true
}

install_olaf_release() {
    local asset="olaf_cli-linux-${OLAF_ARCH}.tar.gz"
    local url="${OLAF_URL}/${asset}"
    local digest
    digest="$(olaf_release_digest)"
    if [[ -z "$digest" ]]; then
        echo "No published SHA-256 digest found for ${asset}."
        return 1
    fi
    log "Downloading OLAF ${OLAF_VERSION} (${OLAF_ARCH})..."
    curl -fL "$url" -o "$TMP/olaf.tar.gz" || return 1
    echo "${digest}  $TMP/olaf.tar.gz" | sha256sum -c - || return 1
    mkdir -p "$TMP/olaf-release"
    tar -xzf "$TMP/olaf.tar.gz" -C "$TMP/olaf-release"
    local binary
    binary="$(find "$TMP/olaf-release" -type f -name olaf -perm -u+x | head -n1)"
    [[ -n "$binary" ]] || return 1
    install -m 0755 "$binary" /usr/local/bin/olaf
}

install_olaf_source() {
    log "Building OLAF ${OLAF_VERSION} from source..."
    install_zig
    git clone --depth 1 --branch "$OLAF_VERSION" "$OLAF_REPO" "$TMP/Olaf"
    make -C "$TMP/Olaf"
    make -C "$TMP/Olaf" install
}

log "[2/8] Installing OLAF..."
if olaf_is_usable; then
    echo "OLAF already usable. Skipping."
elif ! install_olaf_release; then
    install_olaf_source
fi
olaf_is_usable || fail "OLAF installation failed (olaf help did not succeed)."
echo "OLAF: $(command -v olaf)"

log "[3/8] Installing stable Rust..."
if ! command_exists rustup; then
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs -o "$TMP/rustup-init.sh"
    sh "$TMP/rustup-init.sh" -y --default-toolchain stable --profile minimal
fi
export PATH="$CARGO_HOME/bin:$PATH"
command_exists cargo || fail "Cargo installation failed."

log "[4/8] Building BAQS..."
cd "$SOURCE_DIR"
[[ -f Cargo.toml ]] || fail "Cargo.toml not found in $SOURCE_DIR."
cargo build --release
BINARY="$SOURCE_DIR/target/release/baqs"
[[ -x "$BINARY" ]] || fail "BAQS binary was not generated."

if [[ "$CLI_ONLY" -eq 1 ]]; then
    install -m 0755 "$BINARY" /usr/local/bin/baqs
    log "CLI-only installation completed."
    echo "Binary: /usr/local/bin/baqs"
    echo "Create a config with: baqs init-config --output ~/.config/baqs/config.toml"
    exit 0
fi

log "[5/8] Creating service account and directories..."
if ! id "$APP" >/dev/null 2>&1; then
    useradd --system --home-dir "$DATA_DIR" --shell /usr/sbin/nologin "$APP"
fi
install -d -o "$APP" -g "$APP" -m 0750 "$DATA_DIR"
install -d -o root -g "$APP" -m 0750 "$CONFIG_DIR"
install -d -o root -g root -m 0755 "$INSTALL_DIR"
install -m 0755 "$BINARY" "$INSTALL_DIR/baqs"

log "[6/8] Installing configuration..."
if [[ ! -f "$CONFIG_DIR/config.toml" ]]; then
    TOKEN="$(openssl rand -hex 32)"
    cat > "$CONFIG_DIR/baqs.env" <<EOF
BAQS_API_TOKEN=${TOKEN}
BAQS_S3_ENDPOINT=
BAQS_S3_BUCKET=
BAQS_S3_REGION=auto
BAQS_S3_ACCESS_KEY=
BAQS_S3_SECRET_KEY=
EOF
    chown root:root "$CONFIG_DIR/baqs.env"
    chmod 0600 "$CONFIG_DIR/baqs.env"
    BAQS_API_TOKEN="$TOKEN" "$INSTALL_DIR/baqs" init-config \
        --output "$CONFIG_DIR/config.toml" \
        --data-dir "$DATA_DIR" \
        --olaf-binary "$(command -v olaf)"
    echo "Generated API token and storage placeholders in $CONFIG_DIR/baqs.env."
    echo "Provision BAQS_S3_* values, then restart baqs."
else
    echo "Existing configuration found. Preserving it."
fi
chown root:"$APP" "$CONFIG_DIR/config.toml"
chmod 0640 "$CONFIG_DIR/config.toml"

log "[7/8] Installing systemd service..."
cat > "$SERVICE_FILE" <<'UNIT'
[Unit]
Description=Broadcast Archive Query Service
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=baqs
Group=baqs
EnvironmentFile=-/etc/baqs/baqs.env
ExecStart=/opt/baqs/baqs serve --config /etc/baqs/config.toml
Restart=always
RestartSec=5
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/var/lib/baqs
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
UNIT
chmod 0644 "$SERVICE_FILE"
systemctl daemon-reload
systemctl enable baqs

log "[8/8] Starting BAQS..."
systemctl restart baqs
for _ in $(seq 1 30); do
    if curl -fsS http://127.0.0.1:8080/healthz >/dev/null 2>&1; then
        break
    fi
    sleep 1
done
if ! curl -fsS http://127.0.0.1:8080/healthz >/dev/null 2>&1; then
    journalctl -u baqs --no-pager -n 40 || true
    fail "BAQS did not become healthy."
fi

log "BAQS installation completed successfully."
echo
echo "Binary:        $INSTALL_DIR/baqs"
echo "Configuration: $CONFIG_DIR/config.toml"
echo "Environment:   $CONFIG_DIR/baqs.env"
echo "Data:          $DATA_DIR"
echo "Service:       $SERVICE_NAME"
echo "Health:        http://127.0.0.1:8080/healthz"
echo "Swagger:       http://127.0.0.1:8080/docs"
echo
echo "Provision BAQS_S3_* values in $CONFIG_DIR/baqs.env before running queries."
echo "  sudo systemctl status baqs"
echo "  sudo journalctl -u baqs -f"
