#!/usr/bin/env bash
# generate_dags.sh — parse a Control-M XML export and generate Airflow DAG .py files
#
# Usage:
#   ./generate_dags.sh <input.xml> [output_dir] [company] [env] [log_level] [--dump-config]
#
# Examples:
#   ./generate_dags.sh dataset/nonprod/source_move.xml
#   ./generate_dags.sh dataset/production/export_xml_260612.xml output/prod mycompany prod
#   ./generate_dags.sh dataset/nonprod/source_move.xml output/test mycompany dev info --dump-config
#
# Config overrides (optional):
#   After running with --dump-config, edit the generated config files:
#     <output_dir>/config/<env>/<job_id>.json
#   Then re-run without --dump-config to apply your overrides.
#   Only keys present in the config file override IR-derived defaults.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV="${SCRIPT_DIR}/.venv"
PYTHON="${VENV}/bin/python"
PYFLAKES="${VENV}/bin/pyflakes"

# ── args ──────────────────────────────────────────────────────────────────────
INPUT="${1:-}"
if [ -z "$INPUT" ]; then
    echo "Usage: $0 <input.xml> [output_dir] [company] [env] [log_level] [--dump-config]"
    echo ""
    echo "  input.xml     — Control-M XML export file"
    echo "  output_dir    — where to write IR + DAGs (default: output/<stem>)"
    echo "  company       — company prefix in DAG ID (default: mycompany)"
    echo "  env           — deployment environment: dev|sit|uat|prod (default: dev)"
    echo "  log_level     — debug|info|warn|error (default: info)"
    echo "  --dump-config — write default config JSONs to config/<env>/ for manual editing"
    exit 1
fi

STEM=$(basename "$INPUT" .xml)
OUTPUT="${2:-${SCRIPT_DIR}/output/${STEM}}"
COMPANY="${3:-mycompany}"
ENV="${4:-dev}"
LOG_LEVEL="${5:-info}"
DUMP_CONFIG="${6:-}"
TEMPLATES_DIR="${SCRIPT_DIR}/templates"
BINARY="${SCRIPT_DIR}/target/release/ctm-parser"

# ── ensure venv exists and packages are installed ────────────────────────────
if [ ! -f "$PYTHON" ]; then
    echo "[venv] creating .venv..."
    python3 -m venv "$VENV"
fi

if [ ! -f "$PYFLAKES" ] || ! "$PYTHON" -c "import airflow" 2>/dev/null; then
    echo "[venv] installing dependencies from requirements.txt..."
    "$VENV/bin/pip" install --quiet -r "${SCRIPT_DIR}/requirements.txt"
fi

# ── build Rust binary if needed ───────────────────────────────────────────────
if [ ! -f "$BINARY" ] || find "${SCRIPT_DIR}/src" -name "*.rs" -newer "$BINARY" | grep -q .; then
    echo "[build] compiling ctm-parser..."
    cargo build --release --manifest-path "${SCRIPT_DIR}/Cargo.toml"
fi

# ── print run config ──────────────────────────────────────────────────────────
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Input     : $INPUT"
echo "  Output    : $OUTPUT"
echo "  Company   : $COMPANY"
echo "  Env       : $ENV"
echo "  Templates : $TEMPLATES_DIR"
echo "  Python    : $("$PYTHON" --version 2>&1)"
if [ -n "$DUMP_CONFIG" ]; then
    echo "  Mode      : parse + dump config + generate DAGs"
else
    CONFIG_DIR="$OUTPUT/config/$ENV"
    if [ -d "$CONFIG_DIR" ] && [ "$(ls "$CONFIG_DIR" 2>/dev/null | wc -l | tr -d ' ')" -gt 0 ]; then
        echo "  Mode      : parse + generate DAGs  [config overrides from $CONFIG_DIR]"
    else
        echo "  Mode      : parse + generate DAGs  [no config overrides]"
    fi
fi
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# ── preserve config/ across output wipe ──────────────────────────────────────
CONFIG_BACKUP=""
if [ -d "$OUTPUT/config" ]; then
    CONFIG_BACKUP=$(mktemp -d)
    cp -r "$OUTPUT/config" "$CONFIG_BACKUP/"
fi

rm -rf "$OUTPUT"
mkdir -p "$OUTPUT"

if [ -n "$CONFIG_BACKUP" ]; then
    cp -r "$CONFIG_BACKUP/config" "$OUTPUT/"
    rm -rf "$CONFIG_BACKUP"
fi

# ── generate ──────────────────────────────────────────────────────────────────
EXTRA_FLAGS=""
[ -n "$DUMP_CONFIG" ] && EXTRA_FLAGS="--dump-config"

"$BINARY" \
    --input         "$INPUT" \
    --output        "$OUTPUT" \
    --format        json \
    --log-level     "$LOG_LEVEL" \
    --company       "$COMPANY" \
    --env           "$ENV" \
    --templates-dir "$TEMPLATES_DIR" \
    --generate-dags \
    $EXTRA_FLAGS

# ── verify generated DAG .py files ───────────────────────────────────────────
DAGS_DIR="${OUTPUT}/auto_converted/dags"
DAG_COUNT=$(ls "$DAGS_DIR" 2>/dev/null | wc -l | tr -d ' ')

VERIFY_PASS=0
VERIFY_FAIL=0

if [ "$DAG_COUNT" -gt 0 ]; then
    echo ""
    echo "[verify] checking $DAG_COUNT generated DAG file(s)..."
    for DAG_FILE in "$DAGS_DIR"/*.py; do
        BASENAME=$(basename "$DAG_FILE")
        ERRORS=""

        # Check 1: unreplaced placeholders
        REMAINING=$(grep -oE '##[A-Z_]+##' "$DAG_FILE" | sort -u || true)
        if [ -n "$REMAINING" ]; then
            ERRORS="unreplaced placeholders: $(echo "$REMAINING" | tr '\n' ' ')"
        fi

        # Check 2: Python syntax (ast parse)
        if [ -z "$ERRORS" ]; then
            if ! "$PYTHON" -c "import ast; ast.parse(open('${DAG_FILE}').read())" 2>/dev/null; then
                ERRORS="Python syntax error"
            fi
        fi

        # Check 3: pyflakes lint
        if [ -z "$ERRORS" ]; then
            LINT=$("$PYFLAKES" "$DAG_FILE" 2>&1 || true)
            if [ -n "$LINT" ]; then
                ERRORS="pyflakes: $LINT"
            fi
        fi

        if [ -z "$ERRORS" ]; then
            echo "  [PASS] $BASENAME"
            VERIFY_PASS=$(( VERIFY_PASS + 1 ))
        else
            echo "  [FAIL] $BASENAME — $ERRORS"
            VERIFY_FAIL=$(( VERIFY_FAIL + 1 ))
        fi
    done
fi

# ── summary ───────────────────────────────────────────────────────────────────
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Results"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "  auto_converted/"
echo "    jobs/                $(ls "${OUTPUT}/auto_converted/jobs/"                2>/dev/null | wc -l | tr -d ' ') IR files"
echo "    dag_groups/          $(ls "${OUTPUT}/auto_converted/dag_groups/"          2>/dev/null | wc -l | tr -d ' ') self-contained DAG manifests"
echo "    dag_groups_external/ $(ls "${OUTPUT}/auto_converted/dag_groups_external/" 2>/dev/null | wc -l | tr -d ' ') DAG manifests (needs ExternalTaskSensor)"
echo "    dag_singles/         $(ls "${OUTPUT}/auto_converted/dag_singles/"         2>/dev/null | wc -l | tr -d ' ') single-task DAG manifests"
echo "    dags/                $DAG_COUNT generated DAG .py files  ← deploy these"
echo ""
echo "  manual_review/"
echo "    jobs/                $(ls "${OUTPUT}/manual_review/jobs/" 2>/dev/null | wc -l | tr -d ' ') IR files (require human action)"
echo ""

if [ "$DAG_COUNT" -gt 0 ]; then
    echo "  Verification: $VERIFY_PASS passed, $VERIFY_FAIL failed"
    echo ""
    echo "  Generated DAG files:"
    ls "$DAGS_DIR" | sed 's/^/    /'
    echo ""
    echo "  Deploy path: $DAGS_DIR"
    echo ""
fi

CONFIG_COUNT=$(ls "${OUTPUT}/config/${ENV}/" 2>/dev/null | wc -l | tr -d ' ')
if [ "$CONFIG_COUNT" -gt 0 ]; then
    echo "  Config override files (${OUTPUT}/config/${ENV}/):"
    ls "${OUTPUT}/config/${ENV}/" | sed 's/^/    /'
    echo ""
    echo "  Edit any config file, then re-run without --dump-config to apply overrides."
fi

# Exit non-zero if any DAG failed verification
if [ "$VERIFY_FAIL" -gt 0 ]; then
    echo ""
    echo "  ERROR: $VERIFY_FAIL DAG file(s) failed verification — fix before deploying"
    exit 1
fi
