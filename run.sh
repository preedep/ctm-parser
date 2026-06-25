#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FORMAT="${1:-json}"
LOG_LEVEL="${2:-info}"
TARGET="${3:-nonprod}"   # nonprod | production | path/to/file.xml
COMPANY="${4:-mycompany}"
ENV="${5:-dev}"

case "$TARGET" in
    nonprod)
        NONPROD_DIR="${SCRIPT_DIR}/dataset/nonprod"
        OUTPUT="${SCRIPT_DIR}/output/nonprod"
        INPUT_FILES=("${NONPROD_DIR}"/*.xml)
        ;;
    production)
        OUTPUT="${SCRIPT_DIR}/output/production"
        INPUT_FILES=("${SCRIPT_DIR}/dataset/production/export_xml_260612.xml")
        ;;
    *.xml)
        OUTPUT="${SCRIPT_DIR}/output/custom"
        INPUT_FILES=("$TARGET")
        ;;
    *)
        echo "Usage: $0 [json|yaml] [debug|info|warn] [nonprod|production|path/to/file.xml]"
        exit 1
        ;;
esac

cargo build --release --manifest-path "${SCRIPT_DIR}/Cargo.toml"

rm -rf "${OUTPUT}"
mkdir -p "${OUTPUT}"

run_one() {
    local INPUT="$1"
    local OUT_DIR="$2"
    local STEM
    STEM=$(basename "$INPUT" .xml)

    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "  Input : $(basename "$INPUT")"
    echo "  Output: $OUT_DIR"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    rm -rf "$OUT_DIR"
    mkdir -p "$OUT_DIR"

    "${SCRIPT_DIR}/target/release/ctm-parser" \
        --input         "$INPUT" \
        --output        "$OUT_DIR" \
        --format        "$FORMAT" \
        --log-level     "$LOG_LEVEL" \
        --company       "$COMPANY" \
        --env           "$ENV" \
        --templates-dir "${SCRIPT_DIR}/templates" \
        --config-dir    "${SCRIPT_DIR}/config/${STEM}" \
        --generate-dags

    echo ""
    echo "  dags/          $(ls "${OUT_DIR}/dags/"          2>/dev/null | wc -l | tr -d ' ') generated DAG .py files  ← deploy these"
    echo "  ir/            $(ls "${OUT_DIR}/ir/"            2>/dev/null | wc -l | tr -d ' ') job IR files"
    echo "  manifests/     $(ls "${OUT_DIR}/manifests/"     2>/dev/null | wc -l | tr -d ' ') DAG manifests"
    echo "  manual_review/ $(ls "${OUT_DIR}/manual_review/" 2>/dev/null | wc -l | tr -d ' ') jobs requiring human action"
    echo "  migration_summary.json"
}

if [ "$TARGET" = "nonprod" ]; then
    for XML in "${INPUT_FILES[@]}"; do
        STEM=$(basename "$XML" .xml)
        run_one "$XML" "${OUTPUT}/${STEM}"
    done
    echo ""
    echo "All nonprod runs complete. Results under: ${OUTPUT}/"
else
    run_one "${INPUT_FILES[0]}" "${OUTPUT}"
    echo ""
    echo "Output written to: ${OUTPUT}/"
fi
