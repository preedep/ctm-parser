# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

Rust CLI tool that parses Control-M EM XML exports and converts them to an intermediate JSON/YAML representation (job IR), which a downstream DAG generator uses to produce Airflow DAG `.py` files.

Migration scale: ~2,000+ Control-M jobs → Airflow DAGs within 17 months.

## Commands

```bash
cargo build --release
cargo test
cargo test <test_name>                                          # single test
cargo run -- --input jobs.xml --output ./output/ --format json
cargo run -- --input jobs.xml --output ./output/ --format yaml --log-level debug
cargo clippy -- -D warnings
cargo fmt
cargo check
```

## Repository layout

```
ctm-parser/
├── src/
│   ├── main.rs              # CLI entry point (clap)
│   ├── reader/mod.rs        # Stage 2: XML tokenizer (quick-xml, SAX-style)
│   ├── model/mod.rs         # Stage 3: ControlMJob / ControlMCondition / ControlMSchedule structs
│   ├── classifier/mod.rs    # Stage 4: classify_job() → JobPattern enum
│   ├── mapper/mod.rs        # Stage 5: attribute mapping → DagConfig
│   ├── ir/mod.rs            # Stage 6: JobIR struct + serde_json / serde_yaml serialization
│   └── error.rs             # Unified ParseError enum
├── tests/
│   ├── fixtures/            # Sample Control-M XML snippets (one per job type)
│   └── integration/         # End-to-end: XML → job_ir.json assertions
└── docs/
    ├── attribute-mapping.md # Control-M attr → Airflow field mapping table
    └── patterns.md          # JobPattern enum decision rules
```

## Tech stack

- **Rust edition**: 2021
- **XML parsing**: `quick-xml` (SAX streaming, zero-copy `&[u8]` where possible)
- **Serde**: `serde`, `serde_json`, `serde_yaml`
- **CLI**: `clap` v4 (derive macros)
- **Error handling**: `thiserror` for library errors, `anyhow` for CLI top-level
- **Logging**: `tracing` + `tracing-subscriber`
- **Testing**: built-in `#[test]`, `rstest` for parametric tests

## Pipeline stages

| Stage | Module | Responsibility |
|---|---|---|
| 1 | `main.rs` | CLI arg parsing, orchestration |
| 2 | `reader/` | SAX-stream XML → raw attribute maps |
| 3 | `model/` | Raw maps → typed `ControlMJob` structs |
| 4 | `classifier/` | `classify_job()` → `JobPattern` enum |
| 5 | `mapper/` | `JobPattern` + `ControlMJob` → `DagConfig` |
| 6 | `ir/` | `DagConfig` → `JobIR` → JSON/YAML file per job |

## Core types

```rust
// model/mod.rs — all unknown attributes MUST go into `extra`, never dropped
pub struct ControlMJob {
    pub jobname: String,
    pub application: Option<String>,
    pub sub_application: Option<String>,
    pub cmdline: Option<String>,
    pub maxwait: Option<u32>,
    pub cyclic: Option<CyclicConfig>,
    pub incond: Vec<InCondition>,
    pub outcond: Vec<OutCondition>,
    pub shout: Vec<ShoutConfig>,
    pub variables: HashMap<String, String>,  // VARIABLE + AUTOEDIT2 NAME→VALUE
    pub on_events: Vec<OnEvent>,
    pub quantitative: Vec<QuantitativeResource>,
    pub extra: HashMap<String, serde_json::Value>, // unmapped attrs land here
}

// classifier/mod.rs — grounded in real dataset (30,304 jobs)
pub enum JobPattern {
    BashJob,                                       // APPL_TYPE=OS, has CMDLINE
    FileTransfer { protocol: TransferProtocol },   // APPL_TYPE=FILE_TRANS (13,946 jobs)
    FileWatcher,                                   // APPL_TYPE=FileWatch (144 jobs)
    AwsJob { service: AwsServiceType },            // APPL_TYPE=AWS (604 jobs)
    CyclicJob { interval_secs: u64 },              // CYCLIC=1 (3,602 jobs) — overrides above
    AlreadyAirflow,                                // APPL_TYPE=AIRFLOWV2 (51 jobs)
    DependencyGate,                                // TASKTYPE=Dummy, no CMDLINE
    ManualReview { reason: String },               // BIM, named calendars, CONFIRM, etc.
}

// error.rs
pub enum ParseError {
    Xml { offset: u64, source: quick_xml::Error },
    MissingAttr { attr: &'static str, job: String },
    UnknownJobType { job_type: String }, // routes to ManualReview — not a hard failure
    Io(#[from] std::io::Error),
}
```

## Real dataset profile (export_xml_260612.xml)

| Metric | Value |
|---|---|
| Total folders | 6,164 (6,163 FOLDER + 1 SMART_FOLDER) |
| Total jobs | 30,304 |
| TASKTYPE=Job | 17,726 |
| TASKTYPE=Command | 12,078 |
| TASKTYPE=Dummy | 500 |
| APPL_TYPE=OS | 15,239 |
| APPL_TYPE=FILE_TRANS | 13,946 |
| APPL_TYPE=AWS | 604 |
| APPL_TYPE=FileWatch | 144 |
| APPL_TYPE=BIM | 68 |
| APPL_TYPE=AIRFLOWV2 | 51 |
| CYCLIC=1 | 3,602 |
| Jobs with INCOND | 23,306 (76.9%) |
| Jobs with OUTCOND | 30,163 (99.5%) |
| Jobs with ON blocks | 25,183 (83.1%) |
| Jobs with QUANTITATIVE | 2,276 (7.5%) |

## Attribute mapping (Stage 5)

| Control-M attribute | Airflow / DAG config field | Notes |
|---|---|---|
| `CMDLINE` | `BashOperator.bash_command` | `%%VAR%%` → `{{ var.value.VAR }}`, `%%$ODATE` → `{{ ds_nodash }}` |
| `MAXWAIT` | `execution_timeout_sec` | minutes × 60; 0 = no timeout |
| `MAXRERUN` | `retries` | copy as int |
| `RETRO=1` | `catchup=True` | |
| `INCOND ODATE=ODAT` | intra/cross-folder dependency | edge `>>` (same folder) or `ExternalTaskSensor` (cross-folder) |
| `INCOND ODATE=PREV` | `ExternalTaskSensor` | `execution_delta=timedelta(days=1)` — auto-converted |
| `INCOND ODATE=NEXT` | ManualReview | forward dependency, no Airflow equivalent — 31 cases |
| `INCOND ODATE=STAT` | ManualReview | persistent manual flag, no Airflow equivalent — 14 cases |
| `INCOND ODATE=****` | ManualReview | wildcard date, no Airflow equivalent — 6 cases |
| `INCOND AND_OR=O` | `TriggerRule.ONE_SUCCESS` | native Airflow OR logic — 331 cases; record `trigger_rule` in IR |
| `OUTCOND SIGN="+"` | dependency graph edge | recorded in IR `dependencies.downstream` |
| `OUTCOND ODATE=STAT` | note in IR only | job still auto-converts; emit `outcond_static: true` — 18 cases |
| `CYCLIC=1` + `INTERVAL` | `schedule=timedelta(seconds=N)` | `00015M` → 900 sec |
| `DAYS`/`WEEKDAYS`/months | cron expression | `DAYSCAL`/`CONFCAL` present → `ManualReview` |
| `QUANTITATIVE.NAME` | `pool` | normalize to `snake_case` |
| `SHOUT WHEN=EXECTIME` | `sla_sec` | `TIME=>060` → 3600 sec |
| `SHOUT WHEN=NOTOK` | `on_failure_callback` | |
| `ON CODE=*failed*` | `on_failure_callback` | |
| `ON CODE=*success*` | `on_success_callback` | |
| `%%FileWatch-FILE_PATH` | `FileSensor.filepath` | |
| `%%FTP-CONNTYPE2` | `FTPOperator`/`SFTPOperator` | |
| `%%AWS-SERVICE_TYPE=STEP` | `StepFunctionStartExecutionOperator` | |
| `%%UCM-DAGID` | `TriggerDagRunOperator.trigger_dag_id` | AIRFLOWV2 jobs |

## Condition patterns (from real dataset — 32,994 INCOND / 30,467 OUTCOND)

### INCOND ODATE values

| Value | Count | Airflow mapping | Auto-convert? |
|---|---|---|---|
| `ODAT` | 32,747 | same-day `>>` edge or `ExternalTaskSensor` | yes |
| `PREV` | 196 | `ExternalTaskSensor(execution_delta=timedelta(days=1))` | yes |
| `NEXT` | 31 | forward dependency — no clean mapping | ManualReview |
| `STAT` | 14 | persistent manual flag — no Airflow equivalent | ManualReview |
| `****` | 6 | wildcard date — no Airflow equivalent | ManualReview |

### INCOND AND_OR values

| Value | Count | Airflow mapping |
|---|---|---|
| `A` | 32,663 | default `TriggerRule.ALL_SUCCESS` (implicit) |
| `O` | 331 | `TriggerRule.ONE_SUCCESS` on the downstream task — record `trigger_rule` in IR |

### OUTCOND SIGN values

| Value | Count | Handling |
|---|---|---|
| `+` | 30,441 | normal condition add — dependency graph edge |
| `-` | 26 | delete condition — ManualReview: `outcond_delete` |

### OUTCOND ODATE values

| Value | Count | Handling |
|---|---|---|
| `ODAT` | 30,449 | normal |
| `STAT` | 18 | persistent flag — job still auto-converts; emit `outcond_static: true` in IR |

### Condition NAME suffix patterns

All names follow `{JOBNAME}-{SUFFIX}`. Common suffixes: `-ENDED-OK` (32,378), `-ENDED` (104), `-END-OK` (37 — typo variant), `-M2F` (8 — manual-to-force), `-ALERT-*-ENDED-OK` (alert jobs).

## DAG grouping model

**1 FOLDER = 1 DAG.** This is the authoritative mapping decision.

| Control-M concept | Airflow concept |
|---|---|
| `FOLDER` | DAG |
| `JOB` | Task (one operator per job) |
| `INCOND`/`OUTCOND` within same folder | Task dependency (`>>`) |
| `INCOND`/`OUTCOND` referencing another folder | `ExternalTaskSensor` task |
| Jobs in same folder with no condition links | Independent parallel tasks (no edges) |

**Rationale:** Folders are an intentional administrative grouping — same schedule window, same datacenter, same owning team. Even unrelated jobs in the same folder belong in the same DAG operationally. Connected-component splitting would create DAGs that don't map to anything the ops team recognizes, making validation harder.

**Downstream DAG generator must:**
1. Group IR files by `source_folder`
2. Build task list from all jobs in folder
3. Resolve intra-folder conditions → `>>` edges (match `downstream[].condition_name` to `upstream[].condition_name` within the same folder)
4. Resolve cross-folder conditions → `ExternalTaskSensor` tasks (condition name found in a different folder's jobs)
5. Emit one `.py` DAG file per folder

## Output behavior

```
output/
├── jobs/                    one IR file per job: job_{JOBNAME}.json (or .yaml)
├── dag_groups/              self-contained DAGs: intra-folder edges only
├── dag_groups_external/     DAGs with ExternalTaskSensor cross-folder dependencies
├── dag_singles/             single-task DAGs: isolated jobs with no dependencies
└── migration_summary.json
```

- `jobs/` — raw job IR; consumed by anything that needs per-job detail
- `dag_groups/` — fully self-contained; DAG generator only needs this folder's jobs
- `dag_groups_external/` — requires cross-DAG wiring; more complex to generate
- `dag_singles/` — simplest case; one task, no edges, no sensors
- `unmapped_attrs` field must always be emitted (even as `[]`) — never suppressed
- Exit code 0 when jobs route to `ManualReview`; exit code 1 only on fatal IO/XML errors

### Isolation rule (grouper stage)

A job is isolated (→ `dag_singles/`) if it appears in **neither** side of any edge AND is not referenced by any external sensor within its folder's resolved graph. Connected jobs (→ `dag_groups/`) are any job touched by at least one edge or sensor.

### dag_groups manifest shape

```json
{
  "dag_id": "FOLDER_NAME",
  "datacenter": "neutron",
  "schedule": "0 2 * * *",
  "timezone": null,
  "jobs": [{ "job_id": "...", "pattern": "BashJob", "operator": "BashOperator" }],
  "edges": [{ "from": "JOB_A", "to": "JOB_B", "condition": "JOB_A-ENDED-OK" }],
  "external_sensors": [
    { "in_job": "JOB_C", "condition": "OTHER-ENDED-OK", "source_folder": "OTHER_FOLDER", "source_job": "JOB_X" },
    { "in_job": "JOB_D", "condition": "ORPHAN-ENDED-OK", "source_folder": null, "source_job": null }
  ]
}
```

### dag_singles manifest shape

```json
{
  "dag_id": "JOB_NAME",
  "source_folder": "FOLDER_NAME",
  "datacenter": "neutron",
  "schedule": "0 2 * * *",
  "timezone": null,
  "job": { "job_id": "JOB_NAME", "pattern": "BashJob", "operator": "BashOperator" }
}
```

`source_folder: null` on an external sensor means the upstream condition was not found in any folder — producing job is ManualReview, outside the export scope, or from a legacy system.

## Reference documents

| Document | Contents |
|---|---|
| `docs/schema-reference.md` | Full XSD type breakdown + dataset statistics |
| `docs/attribute-mapping.md` | Control-M → Airflow field-by-field mapping, plugin variable namespaces |
| `docs/patterns.md` | `JobPattern` classification decision tree, ManualReview trigger list |
| `docs/ir-schema.md` | JobIR JSON schema with `plugin_config` shapes per pattern |
| `docs/xml-examples.md` | Real XML snippets for every job type (fixture reference) |

## Coding conventions

- `Result<T, ParseError>` throughout library code — no `.unwrap()`
- Unknown XML attributes → `extra` HashMap + `tracing::warn!` per attribute; never panic or skip silently
- Attribute name comparisons must be case-insensitive (`to_ascii_lowercase()`)
- Classifier logic lives only in `classifier/mod.rs`; mapper logic only in `mapper/mod.rs` — no cross-module business logic
- Attribute name constants defined in `model/mod.rs` — no string literals scattered across modules
- `ManualReview` is a valid output state, not an error
- Do not use `std::fs::read_to_string` for XML — use `quick-xml::Reader` streaming
- One fixture XML file per job pattern in `tests/fixtures/`
- Integration tests must assert both `pattern` and `unmapped_attrs` in output IR

## Constraints and Rules

- All documents must not contain company names or other sensitive data
- No real usernames, service account names, or OS user identifiers in docs or fixtures
- No real hostnames, server names, agent node names, or datacenter names beyond the generic placeholder `neutron`
- No real file paths that reveal internal directory structures or storage layout
- No real AWS resource names, Step Function ARNs, Lambda function names, or account identifiers
- No real DAG IDs, Airflow connection names, or internal system identifiers
- No real FTP/SFTP account names, remote hosts, or credentials
- Use generic placeholders: `agent01`, `agent02` for nodes; `svc_batch`, `svcaccount` for run-as; `MY_CONN`, `FTP_CONN_01`, `AWS_CONN_01`, `AIRFLOW_CONN_01` for connection names; `my-step-function-prod` for AWS resources; `my-cloud-dag-prod` for DAG IDs
- Test fixtures in `tests/fixtures/` must follow the same rules — use `RT_` prefix job names with generic identifiers only
- Before committing any new doc or fixture, grep for real internal names from the dataset
