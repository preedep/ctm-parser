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

# ── args ──────────────────────────────────────────────────────────────────────
INPUT="${1:-}"
if [ -z "$INPUT" ]; then
    echo "Usage: $0 <input.xml> [output_dir] [company] [env] [log_level] [--dump-config]"
    echo ""
    echo "  input.xml    — Control-M XML export file"
    echo "  output_dir   — where to write IR + DAGs (default: output/<stem>)"
    echo "  company      — company prefix in DAG ID (default: mycompany)"
    echo "  env          — deployment environment: dev|sit|uat|prod (default: dev)"
    echo "  log_level    — debug|info|warn|error (default: info)"
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

# ── build if binary is missing or source is newer ────────────────────────────
if [ ! -f "$BINARY" ] || find "${SCRIPT_DIR}/src" -name "*.rs" -newer "$BINARY" | grep -q .; then
    echo "[build] compiling ctm-parser..."
    cargo build --release --manifest-path "${SCRIPT_DIR}/Cargo.toml"
fi

# ── run ───────────────────────────────────────────────────────────────────────
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Input     : $INPUT"
echo "  Output    : $OUTPUT"
echo "  Company   : $COMPANY"
echo "  Env       : $ENV"
echo "  Templates : $TEMPLATES_DIR"
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

# Preserve existing config/ dir across runs so overrides survive the wipe
CONFIG_BACKUP=""
if [ -d "$OUTPUT/config" ]; then
    CONFIG_BACKUP=$(mktemp -d)
    cp -r "$OUTPUT/config" "$CONFIG_BACKUP/"
fi

rm -rf "$OUTPUT"
mkdir -p "$OUTPUT"

# Restore config overrides
if [ -n "$CONFIG_BACKUP" ]; then
    cp -r "$CONFIG_BACKUP/config" "$OUTPUT/"
    rm -rf "$CONFIG_BACKUP"
fi

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
echo "    dags/                $(ls "${OUTPUT}/auto_converted/dags/"               2>/dev/null | wc -l | tr -d ' ') generated DAG .py files  ← deploy these"
echo ""
echo "  manual_review/"
echo "    jobs/                $(ls "${OUTPUT}/manual_review/jobs/" 2>/dev/null | wc -l | tr -d ' ') IR files (require human action)"
echo ""

DAG_COUNT=$(ls "${OUTPUT}/auto_converted/dags/" 2>/dev/null | wc -l | tr -d ' ')
if [ "$DAG_COUNT" -gt 0 ]; then
    echo "  Generated DAG files:"
    ls "${OUTPUT}/auto_converted/dags/" | sed 's/^/    /'
    echo ""
    echo "  Deploy path: ${OUTPUT}/auto_converted/dags/"
    echo ""
fi

CONFIG_COUNT=$(ls "${OUTPUT}/config/${ENV}/" 2>/dev/null | wc -l | tr -d ' ')
if [ "$CONFIG_COUNT" -gt 0 ]; then
    echo "  Config override files (${OUTPUT}/config/${ENV}/):"
    ls "${OUTPUT}/config/${ENV}/" | sed 's/^/    /'
    echo ""
    echo "  Edit any config file, then re-run without --dump-config to apply overrides."
fi
