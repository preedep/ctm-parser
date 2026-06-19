# ctm-parser

Rust CLI tool that parses Control-M Enterprise Manager XML exports and converts them to an intermediate JSON/YAML representation (Job IR). The IR is consumed by a downstream DAG generator that produces Apache Airflow DAG `.py` files.

**Migration scale:** ~30,000 Control-M jobs → Airflow DAGs

---

## Quick start

```bash
cargo build --release

./run.sh
# or manually:
cargo run --release -- --input dataset/export_xml_260612.xml --output ./output/ --format json
```

```
output/
├── auto_converted/
│   ├── jobs/                IR files for auto-converted jobs
│   ├── dag_groups/          self-contained DAGs (intra-folder edges only)
│   ├── dag_groups_external/ DAGs requiring ExternalTaskSensor (cross-folder)
│   └── dag_singles/         isolated single-task DAGs
├── manual_review/
│   └── jobs/                IR files for jobs requiring human action
└── migration_summary.json
```

---

## Pipeline

```
XML export  →  reader  →  model  →  classifier  →  mapper  →  ir  →  grouper  →  output
               Stage 2    Stage 3    Stage 4        Stage 5    Stage 6  Stage 7
```

| Stage | Module | Output |
|---|---|---|
| 2 | `reader/` | Raw attribute maps per job |
| 3 | `model/` | Typed `ControlMJob` structs |
| 4 | `classifier/` | `JobPattern` enum |
| 5 | `mapper/` | `DagConfig` per job |
| 6 | `ir/` | `JobIR` JSON/YAML files |
| 7 | `grouper/` | DAG group / single manifests |

---

## Dataset profile

Real export: `export_xml_260612.xml`

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

These jobs produce a DAG file without human intervention.

All jobs execute remotely on agent nodes — the worker pod never runs commands locally or transfers files directly. Agent OS determines the remote execution operator: Linux/Unix → `SSHOperator`, Windows → `PsrpOperator`.

| Pattern | Count | Operator |
|---|---|---|
| `BashJob` | ~11,958 | `SSHOperator` (linux) / `PsrpOperator` (windows) |
| `CyclicJob` | ~2,334 | same as underlying `APPL_TYPE` + DAG `schedule=timedelta(N)` |
| `FileTransfer (FTP/FTP-SSL/SFTP/LOCAL)` | ~10,667 | `SSHOperator` or `PsrpOperator` — agent executes `lftp`/PowerShell |
| `FileWatcher (LOCAL)` | ~144 | `SSHOperator` or `PsrpOperator` — polling loop on agent node |
| `FileWatcher (SFTP)` | — | `SFTPSensor` — Airflow worker connects to SFTP server directly |
| `FileWatcher (FTP/FTP-SSL)` | — | `FTPSensor` — Airflow worker connects to FTP server directly |
| `FileWatcher (S3)` | — | `S3KeySensor` |
| `FileWatcher (BLOB)` | — | `WasbBlobSensor` |
| `AwsJob` | ~556 | `StepFunctionStartExecutionOperator` / `LambdaInvokeFunctionOperator` |
| `AlreadyAirflow` | ~51 | `TriggerDagRunOperator` |
| `DependencyGate` | ~80 | `EmptyOperator` |

**FileWatcher operator selection rule:** If `%%FileWatch-CONNTYPE` (or file path prefix) indicates SFTP/FTP/S3/BLOB, use the corresponding Airflow sensor — the worker pod connects directly to that service. If `LOCAL` or absent, the file lives on the agent node; poll it via `SSHOperator`/`PsrpOperator` depending on agent OS.

### Manual review jobs — 4,032 (13.3%)

These jobs cannot be auto-converted. They are excluded from DAG generation and listed in `migration_summary.json` with a reason code for the migration engineer.

| Reason | Count | Root cause | Recommended action |
|---|---|---|---|
| `complex_cyclic_sequence` | 1,401 | `CYCLIC=1` with a variable interval sequence, not a fixed interval | Build custom Airflow timetable or redesign as event-driven |
| `named_calendar:DAYSCAL` | 1,113 | Schedule driven by a named calendar — cannot derive a cron expression | Translate calendar to cron or implement a custom `Timetable` |
| `complex_cyclic_times` | 743 | `CYCLIC=1` with multiple specific run times, not a single interval | Same as `complex_cyclic_sequence` |
| `unknown_transfer_protocol:S3` | 522 | FILE_TRANS jobs targeting S3 — needs `S3Hook` / `S3ToLocalFilesystemOperator` | Map to Amazon provider S3 operators |
| `confirm_required` | 242 | `CONFIRM=1` — job requires a manual approval gate before execution | Implement with `BranchPythonOperator` or an external trigger |
| `named_calendar:WEEKSCAL` | 133 | Schedule driven by a named week calendar | Same as `DAYSCAL` |
| `bim_sla_checkpoint` | 68 | `APPL_TYPE=BIM` — SLA checkpoint job, no Airflow equivalent | Replace with Airflow SLA callbacks on surrounding tasks |
| `incond_next_date` | 9 | `INCOND ODATE=NEXT` — forward dependency on tomorrow's run | Redesign with ops team — forward dependencies have no Airflow equivalent |
| `incond_static_condition` | 8 | `INCOND ODATE=STAT` — waits on a persistent static flag | Replace with an Airflow Variable check (`ShortCircuitOperator`) |
| `outcond_delete` | 2 | `OUTCOND SIGN="-"` — removes a condition; no Airflow equivalent | Redesign dependency model |
| `output_pattern_match` | 2 | `ON` block matches on job output content | Implement with a custom sensor or post-processing task |
| `exclusive_resource_lock` | 1 | `CONTROL` block — exclusive resource lock | Use Airflow pool with `pool_slots=MAX` to serialize |
| `date_shift` | 1 | `SHIFT` attribute — execution date shifted by N days | Translate to `timedelta` offset in `ExternalTaskSensor` |
| `named_calendar:CONFCAL` | 1 | Schedule driven by confirmation calendar | Same as `DAYSCAL` |

---

## DAG grouping model

**1 FOLDER = 1 DAG** — the authoritative mapping rule.

| Control-M | Airflow |
|---|---|
| `FOLDER` | DAG |
| `JOB` | Task (one operator) |
| `INCOND`/`OUTCOND` within folder | `>>` task dependency |
| `INCOND`/`OUTCOND` across folders | `ExternalTaskSensor` |
| Unlinked jobs in same folder | Independent parallel tasks |

Output is split into two top-level buckets, then three tiers within `auto_converted/`:

- **`auto_converted/`** — DAG generator reads only from here
- **`manual_review/`** — migration engineer's backlog; `ls jobs/ | wc -l` = remaining work

Within `auto_converted/`, three tiers by complexity:

1. **`dag_groups/`** — fully self-contained; no cross-DAG wiring needed
2. **`dag_groups_external/`** — requires `ExternalTaskSensor` for cross-folder dependencies
3. **`dag_singles/`** — one task, no edges, simplest to generate

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
