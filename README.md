# ctm-parser

Rust CLI tool that parses Control-M Enterprise Manager XML exports and converts them to an intermediate JSON/YAML representation (Job IR). The IR is consumed by a downstream DAG generator that produces Apache Airflow 3.x DAG `.py` files.

**Migration scale:** ~30,000 Control-M jobs → Airflow DAGs

---

## Prerequisites

| Tool | Version | Purpose |
|---|---|---|
| Rust | stable (2021 edition) | Build the parser |
| Python | 3.10–3.14 | DAG verification in `.venv` |

```bash
# One-time venv setup (generate_dags.sh auto-creates this if missing)
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

---

## Quick start

```bash
# Parse + generate DAGs for a single scenario
./generate_dags.sh dataset/nonprod/source_move.xml

# Parse all nonprod scenarios
./run.sh

# Parse production dataset
./run.sh json info production
```

---

## Output structure

Each run produces a flat directory per input XML:

```
output/<scenario>/
├── dags/                  ← generated DAG .py files  (deploy these)
├── ir/                    ← job IR JSON (auto-converted jobs)
├── manifests/             ← DAG grouper output (groups, singles)
├── manual_review/         ← IR files requiring human action
└── migration_summary.json
```

- `dags/` is the deploy target — copy to Airflow's DAG folder
- `manual_review/` is the migration engineer's backlog; `ls | wc -l` = remaining work
- `ir/` and `manifests/` are inputs to the downstream DAG generator

---

## Pipeline

```
XML  →  reader  →  model  →  classifier  →  mapper  →  ir  →  grouper  →  codegen  →  output
         Stage 2   Stage 3    Stage 4        Stage 5    Stage 6  Stage 7     Stage 8
```

| Stage | Module | Output |
|---|---|---|
| 2 | `reader/` | Raw attribute maps per job |
| 3 | `model/` | Typed `ControlMJob` structs |
| 4 | `classifier/` | `JobPattern` enum |
| 5 | `mapper/` | `DagConfig` per job |
| 6 | `ir/` | `JobIR` JSON/YAML files |
| 7 | `grouper/` | DAG group / single manifests |
| 8 | `codegen/` | Airflow DAG `.py` files from templates |

---

## DAG code generation

`generate_dags.sh` runs the full pipeline and verifies each generated `.py` file:

```bash
# Generate with IR-derived defaults
./generate_dags.sh dataset/nonprod/source_move.xml

# Dump config defaults for manual editing
./generate_dags.sh dataset/nonprod/source_move.xml output/sm mycompany dev info --dump-config
# → writes config/source_move/dev/source_move.json

# Edit the config, then regenerate — only changed keys are overridden
./generate_dags.sh dataset/nonprod/source_move.xml
```

### Config overrides

Config files live at `config/<scenario>/<env>/<job_id>.json` (gitignored — not committed).
Only the keys you want to change need to be present:

```json
{
  "DEST_PORT": "991",
  "DEST_USER": "airflow",
  "PASSWORD_VAR": "airflow-alld-secret",
  "SSH_CONN_ID": "ssh-benseni-ctlm"
}
```

### Verification

Each generated `.py` file is checked automatically:
1. No unreplaced `##KEY##` placeholders
2. Python syntax (`ast.parse`)
3. `pyflakes` lint

Exit code 1 if any file fails — safe to use as a CI gate.

---

## Dataset profile

Real export: `dataset/production/export_xml_260612.xml`

| Metric | Count |
|---|---|
| Folders processed | 6,164 |
| Total jobs | 30,297 |
| Auto-converted | 26,265 (86.7%) |
| Requires manual review | 4,032 (13.3%) |
| Parse errors | 0 |

---

## Migration result breakdown

### Auto-converted jobs — 26,265 (86.7%)

All jobs execute remotely on agent nodes — the worker pod only dispatches.
Agent OS determines the operator: Linux/Unix → `SSHOperator`, Windows → `PsrpOperator`.

| Pattern | Count | Operator |
|---|---|---|
| `BashJob` | ~11,958 | `SSHOperator` / `PsrpOperator` |
| `CyclicJob` | ~2,334 | same as underlying `APPL_TYPE` + `schedule=timedelta(N)` |
| `FileTransfer` | ~10,667 | `SSHOperator` / `PsrpOperator` — agent runs `lftp`/PowerShell |
| `FileWatcher (LOCAL)` | ~144 | `SSHOperator` / `PsrpOperator` — polling loop on agent |
| `FileWatcher (SFTP/FTP/S3/BLOB)` | — | `SFTPSensor` / `FTPSensor` / `S3KeySensor` / `WasbBlobSensor` |
| `AwsJob` | ~556 | `StepFunctionStartExecutionOperator` / `LambdaInvokeFunctionOperator` |
| `AlreadyAirflow` | ~51 | `TriggerDagRunOperator` |
| `DependencyGate` | ~80 | `EmptyOperator` |

### Manual review jobs — 4,032 (13.3%)

| Reason | Count |
|---|---|
| `complex_cyclic_sequence` | 1,401 |
| `named_calendar:DAYSCAL` | 1,113 |
| `complex_cyclic_times` | 743 |
| `unknown_transfer_protocol:S3` | 522 |
| `confirm_required` | 242 |
| `named_calendar:WEEKSCAL` | 133 |
| `bim_sla_checkpoint` | 68 |
| `incond_next_date` | 9 |
| `incond_static_condition` | 8 |
| `outcond_delete` | 2 |
| `output_pattern_match` | 2 |
| `exclusive_resource_lock` | 1 |
| `date_shift` | 1 |
| `named_calendar:CONFCAL` | 1 |

---

## DAG grouping model

**1 FOLDER = 1 DAG** — the authoritative mapping rule.

| Control-M | Airflow |
|---|---|
| `FOLDER` | DAG |
| `JOB` | Task (one operator) |
| `INCOND`/`OUTCOND` within folder | `>>` task dependency |
| `INCOND`/`OUTCOND` across folders | `ExternalTaskSensor` |

Manifests are split by dependency complexity:
- **groups** — self-contained; no cross-DAG wiring needed
- **groups_external** — requires `ExternalTaskSensor` for cross-folder dependencies
- **singles** — one task, no edges

---

## Reference docs

| Document | Contents |
|---|---|
| `docs/ctm-to-airflow-mapping.md` | Primary DAG generator reference: operator mapping, dependency wiring, token substitution, generation algorithm |
| `docs/attribute-mapping.md` | Control-M attribute → Airflow field mapping table |
| `docs/patterns.md` | `JobPattern` classification decision tree + ManualReview trigger list |
| `docs/ir-schema.md` | `JobIR` JSON schema with `plugin_config` shapes per pattern |
| `docs/xml-examples.md` | XML snippets for every job type |
| `docs/schema-reference.md` | XSD type breakdown + full dataset statistics |

---

## Commands

```bash
cargo build --release
cargo test
cargo test <test_name>
cargo clippy -- -D warnings
cargo fmt
cargo check
```
