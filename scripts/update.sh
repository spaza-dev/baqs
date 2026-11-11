#!/usr/bin/env bash
set -Eeuo pipefail

APP="baqs"
INSTALL_DIR="/opt/baqs"
DATA_DIR="/var/lib/baqs"
CONFIG_DIR="/etc/baqs"
SERVICE_NAME="baqs.service"
HEALTH_URL="${BAQS_HEALTH_URL:-http://127.0.0.1:8080/healthz}"
SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUST_HOME="/root/.rustup"
CARGO_HOME="/root/.cargo"
STAMP="$(date '+%Y%m%d-%H%M%S')"
BACKUP_DIR="/var/backups/baqs/${STAMP}"

export RUSTUP_HOME="$RUST_HOME"
export CARGO_HOME="$CARGO_HOME"
export PATH="$CARGO_HOME/bin:/usr/local/bin:/usr/bin:/bin:$PATH"

log() { echo; echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }
fail() { echo; echo "ERROR: $*" >&2; exit 1; }
command_exists() { command -v "$1" >/dev/null 2>&1; }
build_jobs() {
    if [[ -n "${BAQS_BUILD_JOBS:-}" ]]; then
        printf '%s\n' "$BAQS_BUILD_JOBS"
        return 0
    fi
    local mem_gb
    mem_gb="$(awk '/MemTotal/ { printf "%d", $2 / 1024 / 1024 }' /proc/meminfo 2>/dev/null || echo 0)"
    if [[ "$mem_gb" -le 2 ]]; then
        printf '1\n'
    elif [[ "$mem_gb" -le 4 ]]; then
        printf '2\n'
    else
        nproc 2>/dev/null || printf '1\n'
    fi
}

echo "======================================"
echo " BAQS Updater"
echo "======================================"

[[ "$EUID" -eq 0 ]] || fail "Run this updater as root: sudo bash scripts/update.sh"
[[ -f "$INSTALL_DIR/baqs" ]] || fail "BAQS is not installed. Run scripts/install.sh first."
[[ -f "$CONFIG_DIR/config.toml" ]] || fail "Missing $CONFIG_DIR/config.toml."

log "[1/6] Installing/updating Rust..."
if ! command_exists rustup; then
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs -o /tmp/baqs-rustup-init.sh
    sh /tmp/baqs-rustup-init.sh -y --default-toolchain stable --profile minimal
fi
export PATH="$CARGO_HOME/bin:$PATH"
rustup toolchain install stable --profile minimal
rustup default stable
command_exists cargo || fail "Cargo installation failed."

log "[2/6] Building BAQS..."
cd "$SOURCE_DIR"
[[ -f Cargo.toml ]] || fail "Cargo.toml not found in $SOURCE_DIR."
JOBS="$(build_jobs)"
log "Building with $JOBS parallel job(s)..."
cargo build --release --jobs "$JOBS"
BINARY="$SOURCE_DIR/target/release/baqs"
[[ -x "$BINARY" ]] || fail "BAQS binary was not generated."

log "[3/6] Backing up current installation..."
install -d -m 0750 "$BACKUP_DIR"
cp -a "$INSTALL_DIR/baqs" "$BACKUP_DIR/baqs"
cp -a "$CONFIG_DIR/config.toml" "$BACKUP_DIR/config.toml"
if [[ -f "$CONFIG_DIR/baqs.env" ]]; then
    cp -a "$CONFIG_DIR/baqs.env" "$BACKUP_DIR/baqs.env"
fi
ln -sfn "$BACKUP_DIR" /var/backups/baqs/previous

rollback() {
    echo "Rolling back to $BACKUP_DIR..." >&2
    install -m 0755 "$BACKUP_DIR/baqs" "$INSTALL_DIR/baqs" || true
    systemctl restart baqs || true
}

log "[4/6] Installing new binary..."
install -m 0755 "$BINARY" "$INSTALL_DIR/baqs"

log "[5/6] Restarting service..."
systemctl daemon-reload
if ! systemctl restart baqs; then
    journalctl -u baqs --no-pager -n 40 || true
    rollback
    fail "Service restart failed; rolled back."
fi

log "[6/6] Checking health..."
healthy=0
for _ in $(seq 1 30); do
    if curl -fsS "$HEALTH_URL" >/dev/null 2>&1; then
        healthy=1
        break
    fi
    sleep 1
done
if [[ "$healthy" -ne 1 ]]; then
    journalctl -u baqs --no-pager -n 40 || true
    rollback
    fail "BAQS health check failed; rolled back."
fi

log "BAQS update completed successfully."
echo "Backup: $BACKUP_DIR"
echo "Health: $HEALTH_URL"
