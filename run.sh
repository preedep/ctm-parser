#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INPUT="${SCRIPT_DIR}/dataset/export_xml_260612.xml"
OUTPUT="${SCRIPT_DIR}/output"
FORMAT="${1:-json}"
LOG_LEVEL="${2:-info}"

cargo build --release --manifest-path "${SCRIPT_DIR}/Cargo.toml"

rm -rf "${OUTPUT}"
mkdir -p "${OUTPUT}"

"${SCRIPT_DIR}/target/release/ctm-parser" \
  --input "${INPUT}" \
  --output "${OUTPUT}" \
  --format "${FORMAT}" \
  --log-level "${LOG_LEVEL}"

echo ""
echo "Output written to: ${OUTPUT}/"
echo "  jobs/                  $(ls "${OUTPUT}/jobs/" | wc -l | tr -d ' ') job IR files"
echo "  dag_groups/            $(ls "${OUTPUT}/dag_groups/" | wc -l | tr -d ' ') self-contained DAGs (intra-folder deps only)"
echo "  dag_groups_external/   $(ls "${OUTPUT}/dag_groups_external/" | wc -l | tr -d ' ') DAGs with ExternalTaskSensor dependencies"
echo "  dag_singles/           $(ls "${OUTPUT}/dag_singles/" | wc -l | tr -d ' ') single-task DAGs (no dependencies)"
echo "  migration_summary.json"
