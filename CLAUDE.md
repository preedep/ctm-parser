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
    pub incond: Vec<Condition>,
    pub outcond: Vec<Condition>,
    pub shout: Vec<ShoutConfig>,
    pub variables: HashMap<String, String>,
    pub extra: HashMap<String, serde_json::Value>, // unmapped attrs land here
}

// classifier/mod.rs
pub enum JobPattern {
    BashJob,
    FileWatcher,
    DatabaseJob { db_type: DbType },
    CyclicJob { interval: Duration },
    CrossFolderDep,
    ManualReview { reason: String }, // non-empty unmapped_attrs or unknown type
}

// error.rs
pub enum ParseError {
    Xml { offset: u64, source: quick_xml::Error },
    MissingAttr { attr: &'static str, job: String },
    UnknownJobType { job_type: String }, // routes to ManualReview — not a hard failure
    Io(#[from] std::io::Error),
}
```

## Attribute mapping (Stage 5)

| Control-M attribute | Airflow / DAG config field | Notes |
|---|---|---|
| `CMDLINE` | `BashOperator.bash_command` | strip `%%VAR%%` → `{{ var.value.VAR }}` |
| `MAXWAIT` | `execution_timeout` (seconds) | minutes × 60 |
| `INCOND` | `ExternalTaskSensor` | one sensor task per condition |
| `OUTCOND` | `TriggerDagRunOperator` | emit after task success |
| `CYCLIC` + interval | `schedule_interval` timedelta | parse `INTERVAL` field |
| `DAYS` / `MONTHS` | cron expression | croniter logic ported to Rust |
| `QUANTITATIVE` resource | `pool` name | normalize to snake_case |
| `SHOUT` on-ok / on-fail | `on_success_callback` / `on_failure_callback` | |
| `RETRO` = YES | `catchup = True` | |

## Output behavior

- One IR file per job: `job_{JOBNAME}.json` (or `.yaml`)
- Summary report `migration_summary.json` written after every run
- `unmapped_attrs` field must always be emitted (even as `[]`) — never suppressed
- Exit code 0 when jobs route to `ManualReview`; exit code 1 only on fatal IO/XML errors

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
