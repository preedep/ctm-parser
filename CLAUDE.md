# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

Rust CLI tool that parses Control-M EM XML exports and converts them to an intermediate JSON/YAML representation (job IR), which a downstream DAG generator uses to produce Airflow DAG `.py` files.

Migration scale: ~2,000+ Control-M jobs → Airflow DAGs within 17 months.

## Prerequisites

| Tool | Version | Purpose |
|---|---|---|
| Rust | stable (2021 edition) | Build the parser |
| Python | 3.10–3.14 | DAG verification in `.venv` |

**Python venv setup** (one-time, after cloning):
```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```
`generate_dags.sh` auto-creates the venv if absent — manual setup is only needed when running verification outside the script (e.g. `pyflakes` in CI).

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
│   ├── main.rs              # CLI entry point (clap), orchestration
│   ├── lib.rs               # Exposes all modules for integration tests
│   ├── reader/mod.rs        # Stage 2: XML tokenizer (quick-xml, SAX-style)
│   ├── model/mod.rs         # Stage 3: ControlMJob + child structs
│   ├── classifier/mod.rs    # Stage 4: classify_job() → JobPattern enum
│   ├── mapper/mod.rs        # Stage 5: JobPattern + ControlMJob → DagConfig
│   ├── ir/mod.rs            # Stage 6: DagConfig → JobIR → JSON/YAML per job
│   ├── grouper/mod.rs       # Stage 7: JobIR[] → dag_groups / dag_singles manifests
│   └── error.rs             # ParseError enum (thiserror)
├── tests/
│   ├── fixtures/            # One XML snippet per JobPattern (bash, cyclic, filewatcher, manual_review)
│   └── parse_test.rs        # Integration tests: XML → IR assertions (pattern + unmapped_attrs)
├── docs/
│   ├── ctm-to-airflow-mapping.md  # Primary DAG generator reference (START HERE)
│   ├── attribute-mapping.md       # Control-M attr → Airflow field mapping table
│   ├── patterns.md                # JobPattern decision tree + ManualReview trigger list
│   ├── ir-schema.md               # JobIR JSON schema with plugin_config shapes
│   ├── xml-examples.md            # Real XML snippets for every job type
│   └── schema-reference.md        # XSD type breakdown + dataset statistics
└── run.sh                   # Run against dataset/export_xml_260612.xml → output/
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
| 7 | `grouper/` | `JobIR[]` → dag_groups / dag_groups_external / dag_singles manifests |

## Core types

```rust
// model/mod.rs — all unknown attributes MUST go into `extra`, never dropped
pub struct ControlMJob {
    pub jobname: String,
    pub parent_folder: String,
    pub tasktype: String,
    pub appl_type: String,
    pub cmdline: Option<String>,
    pub cyclic: bool,
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

// grouper/mod.rs — DAG grouping: 1 FOLDER = 1 DAG (authoritative rule)
// Three-pass resolution: OUTCOND index → cross-folder producers → per-folder split
// Output: dag_groups/ (self-contained), dag_groups_external/ (needs ExternalTaskSensor),
//         dag_singles/ (isolated jobs, no edges, no sensors)
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

## Mapping and output rules

Full detail lives in `docs/ctm-to-airflow-mapping.md`. Key invariants to keep in mind when modifying code:

- **1 FOLDER = 1 DAG** — never split a folder by connected components
- **Execution model** — all jobs run remotely on agent nodes; worker pod only dispatches
  - Linux/Unix agent (`NODEID`) → `SSHOperator`
  - Windows agent (`NODEID`) → `PsrpOperator` (`apache-airflow-providers-microsoft-psrp`)
  - `NODEID` maps to an Airflow connection ID; agent OS determined from connection registry
- `unmapped_attrs` must always be emitted (even as `[]`) — never suppressed
- Exit code 0 when jobs route to `ManualReview`; exit code 1 only on fatal IO/XML errors
- `INCOND ODATE=PREV` → `execution_delta_days=1` in IR; `NEXT/STAT/****` → ManualReview
- `INCOND AND_OR=O` → `trigger_rule="ONE_SUCCESS"` in IR
- `OUTCOND ODATE=STAT` → `outcond_static=true` in IR (job still auto-converts)
- `OUTCOND SIGN="-"` → ManualReview: `outcond_delete`
- Condition suffix pattern: `{JOBNAME}-ENDED-OK` (32,378 cases); suffix never modified by parser
- `CyclicJob` affects DAG `schedule=timedelta(N)` only — operator still derived from `APPL_TYPE` + `NODEID`
- FileTransfer `transfer_type`: `onprem_to_onprem` / `onprem_to_cloud` / `cloud_to_cloud` — all execute via SSH/PSRP on agent/relay node

### Output directories

```
output/
├── auto_converted/
│   ├── jobs/                job IR files for auto-converted jobs only
│   ├── dag_groups/          self-contained DAGs (intra-folder edges only)
│   ├── dag_groups_external/ DAGs requiring ExternalTaskSensor
│   └── dag_singles/         isolated single-task DAGs
├── manual_review/
│   └── jobs/                job IR files for ManualReview jobs (human audit)
└── migration_summary.json
```

- DAG generator reads from `auto_converted/` only — never touches `manual_review/`
- `manual_review/jobs/` is the migration engineer's backlog; `ls | wc -l` = remaining work
- Grouper resolution uses all IRs (including ManualReview) to build the OUTCOND index correctly, but only writes connected/isolated DAG manifests for auto-converted jobs
- A job is isolated (→ `dag_singles/`) if it has no intra-folder edges, is not referenced by any external sensor, and is not a cross-folder producer (its OUTCOND consumed by a job in a different folder)

## Reference documents

| Document | Contents |
|---|---|
| `docs/schema-reference.md` | Full XSD type breakdown + dataset statistics |
| `docs/attribute-mapping.md` | Control-M → Airflow field-by-field mapping, plugin variable namespaces |
| `docs/patterns.md` | `JobPattern` classification decision tree, ManualReview trigger list |
| `docs/ir-schema.md` | JobIR JSON schema with `plugin_config` shapes per pattern |
| `docs/xml-examples.md` | Real XML snippets for every job type (fixture reference) |
| `docs/ctm-to-airflow-mapping.md` | Primary DAG generator reference: concept mapping, operator selection, dependency wiring, token substitution, generation algorithm, ManualReview action guide |

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
