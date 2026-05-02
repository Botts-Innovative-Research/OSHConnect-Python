#!/usr/bin/env bash
# =============================================================================
# publish-local.sh — Build oshconnect and publish to the local PyPI server
#
# This is the one-command dev loop: edit code → run this → downstream picks
# up the new version via `pip install --index-url http://localhost:8090/simple/`.
#
# The local pypiserver container must be running (`docker compose up -d`).
# The --overwrite flag on the server allows re-uploading the same version,
# so you don't need to bump the version for every dev iteration.
#
# Usage:
#   ./scripts/publish-local.sh           # build + upload
#   ./scripts/publish-local.sh --no-build # upload existing wheel(s) in dist/
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

PYPI_URL="${LOCAL_PYPI_URL:-http://localhost:8090}"

RED='\033[0;31m'
GREEN='\033[0;32m'
CYAN='\033[0;36m'
NC='\033[0m'

info()  { echo -e "${CYAN}[INFO]${NC}  $*"; }
ok()    { echo -e "${GREEN}[OK]${NC}    $*"; }
fail()  { echo -e "${RED}[FAIL]${NC}  $*"; exit 1; }

info "Project root: ${PROJECT_ROOT}"

# ── Parse args ──────────────────────────────────────────────────────────────
SKIP_BUILD=false
for arg in "$@"; do
    case "$arg" in
        --no-build) SKIP_BUILD=true ;;
        --help|-h)
            echo "Usage: $0 [--no-build]"
            echo "  --no-build  Skip wheel build, upload whatever is in dist/"
            exit 0
            ;;
    esac
done

# ── Ensure local PyPI is running ────────────────────────────────────────────
pypi_ready() {
    # Just check for any HTTP response (2xx or 3xx) — an empty index still returns 200
    local code
    code=$(curl -s -o /dev/null -w "%{http_code}" --max-time 3 "${PYPI_URL}/" 2>/dev/null) || return 1
    [ "$code" -ge 200 ] && [ "$code" -lt 400 ]
}

info "Checking local PyPI at ${PYPI_URL}"
if pypi_ready; then
    ok "Local PyPI is already running"
else
    info "Local PyPI not running — starting container..."
    cd "${PROJECT_ROOT}"
    docker compose up -d pypi

    READY=false
    for i in $(seq 1 10); do
        sleep 1
        if pypi_ready; then
            READY=true
            break
        fi
        info "  waiting... (${i}/10)"
    done

    if [ "$READY" = false ]; then
        fail "Could not start local PyPI"
    fi
    ok "Local PyPI started"
fi

# ── Build ───────────────────────────────────────────────────────────────────
cd "${PROJECT_ROOT}"
info "Working directory: $(pwd)"

if [ "$SKIP_BUILD" = false ]; then
    info "Building wheel..."
    rm -rf dist/ build/ src/*.egg-info

    uv build || fail "uv build failed"
fi

WHEELS=(dist/*.whl)
if [ ${#WHEELS[@]} -eq 0 ] || [ ! -f "${WHEELS[0]}" ]; then
    fail "No wheel found in ${PROJECT_ROOT}/dist/. Build first or remove --no-build."
fi

ok "Wheel(s): ${WHEELS[*]}"

# ── Upload ──────────────────────────────────────────────────────────────────
# pypiserver runs with `-a . -P .` (auth disabled), but `uv publish` still
# prompts for credentials when none are configured. Pass empty values via
# flags to skip the prompt and run non-interactively.
info "Uploading to ${PYPI_URL}"
uv publish --publish-url "${PYPI_URL}" --username "" --password "" dist/*.whl \
    || fail "uv publish failed"

ok "Published to local PyPI"
echo ""
echo "  Browse:    ${PYPI_URL}/simple/"
echo "  Install:   pip install --index-url ${PYPI_URL}/simple/ oshconnect"
echo "  uv:        uv pip install --index-url ${PYPI_URL}/simple/ oshconnect"
echo "  uv sync:   uv sync  (if pyproject.toml has [[tool.uv.index]] configured)"
