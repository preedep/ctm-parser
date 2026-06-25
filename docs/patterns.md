# JobPattern Classification Rules

Defines how `classify_job()` in `classifier/mod.rs` maps a `ControlMJob` to a `JobPattern` variant.
All counts from `export_xml_260612.xml` (30,304 jobs).

---

## JobPattern enum

```rust
pub enum JobPattern {
    BashJob,                                  // OS command/script
    FileTransfer { protocol: TransferProtocol },  // FTP/SFTP file movement
    FileWatcher,                              // file arrival sensor
    AwsJob { service: AwsServiceType },       // AWS Step/Lambda/Batch
    CyclicJob { interval_secs: u64 },         // repeating job
    AlreadyAirflow,                           // existing Airflow DAG (AIRFLOWV2 plugin)
    DependencyGate,                           // Dummy job used only for condition fan-in/fan-out
    ManualReview { reason: String },          // anything that cannot be auto-converted
}
```

---

## Classification decision tree

```
classify_job(job: &ControlMJob) -> JobPattern
│
├─ CYCLIC="1"
│   └─ AND CYCLIC_INTERVAL_SEQUENCE is empty AND IND_CYCLIC != "I"
│       ├─ YES → CyclicJob { interval_secs: parse_interval(INTERVAL) }
│       └─ NO  → ManualReview("complex_cyclic")
│
├─ APPL_TYPE = "AIRFLOWV2"
│   └─ AlreadyAirflow
│
├─ APPL_TYPE = "BIM"
│   └─ ManualReview("bim_sla_checkpoint")
│
├─ APPL_TYPE = "FileWatch"
│   └─ FileWatcher
│
├─ APPL_TYPE = "FILE_TRANS"
│   └─ FileTransfer { protocol: derive_protocol(variables) }
│       %%FTP-CONNTYPE2 = "FTP"          → TransferProtocol::Ftp
│       %%FTP-CONNTYPE2 = "FTP-SSL"      → TransferProtocol::FtpSsl
│       %%FTP-CONNTYPE2 = "SFTP"         → TransferProtocol::Sftp
│       %%FTP-CONNTYPE2 = "LOCAL"        → TransferProtocol::Local
│       %%FTP-CONNTYPE2 = "S3"           → TransferProtocol::S3
│       %%FTP-CONNTYPE2 = "AZURE"/"BLOB" → TransferProtocol::Azure
│       otherwise                        → TransferProtocol::Unknown → ManualReview
│
├─ APPL_TYPE = "AWS"
│   └─ AwsJob { service: derive_aws_service(variables) }
│       %%AWS-SERVICE_TYPE = "STEP"   → AwsServiceType::StepFunctions
│       %%AWS-SERVICE_TYPE = "LAMBDA" → AwsServiceType::Lambda
│       %%AWS-SERVICE_TYPE = "BATCH"  → AwsServiceType::Batch
│       otherwise                     → ManualReview("unknown_aws_service")
│
├─ TASKTYPE = "Dummy" AND APPL_TYPE ∈ {"OS", ""} AND no CMDLINE
│   └─ DependencyGate
│       (no execution — only INCOND/OUTCOND wiring)
│
├─ APPL_TYPE ∈ {"OS", ""} AND (CMDLINE present OR TASKTYPE = "Command")
│   ├─ has ManualReview triggers (see below)?
│   │   └─ YES → ManualReview(first_trigger_reason)
│   └─ NO  → BashJob
│
└─ anything else
    └─ ManualReview("unrecognized_appl_type:{APPL_TYPE}")
```

---

## ManualReview triggers (checked before BashJob classification)

Any of these conditions forces `ManualReview`. Collect ALL reasons into a `Vec<String>` and join for the reason field.

| Condition | reason string |
|---|---|
| `CONFIRM = "1"` | `"confirm_required"` |
| `SHIFT` ≠ `"Ignore Job"` and ≠ `"+00"` equivalent | `"date_shift"` |
| `DAYSCAL` non-empty | `"named_calendar:DAYSCAL"` |
| `CONFCAL` non-empty | `"named_calendar:CONFCAL"` |
| `WEEKSCAL` non-empty | `"named_calendar:WEEKSCAL"` |
| `CYCLIC_INTERVAL_SEQUENCE` non-empty | `"complex_cyclic_sequence"` |
| `CYCLIC_TIMES_SEQUENCE` non-empty | `"complex_cyclic_times"` |
| `IND_CYCLIC = "I"` | `"independent_cyclic"` |
| any `OUTCOND` has `SIGN = "-"` | `"outcond_delete"` |
| any `INCOND` has `ODATE ∈ {"STAT", "****"}` | `"incond_static_date"` |
| any `CONTROL` element present | `"exclusive_resource_lock"` |
| any `ON` where `STMT ≠ "*"` | `"output_pattern_match"` |
| any `DO_REMEDY` element in any `ON` block | `"itsm_remedy_action"` |
| `extra` HashMap non-empty after known attrs stripped | `"unknown_attrs:{keys}"` |

---

## Supporting enums

```rust
pub enum TransferProtocol {
    Ftp,
    FtpSsl,
    Sftp,
    Local,
    S3,    // FTP-CONNTYPE2=S3 → aws s3 cp on agent node
    Azure, // FTP-CONNTYPE2=AZURE or BLOB → azcopy on agent node
    Unknown(String),
}

pub enum AwsServiceType {
    StepFunctions,
    Lambda,
    Batch,
    Unknown(String),
}
```

---

## INTERVAL parsing

Format: `NNNNNu` where u ∈ `{M, H, D}`.

```
"00015M" → 15 * 60       = 900 seconds
"00001H" → 1 * 3600      = 3600 seconds
"00001D" → 1 * 86400     = 86400 seconds
"00000M" → 0 (no repeat; treat CYCLIC=0 equivalent)
```

If INTERVAL is missing or malformed when CYCLIC=1 → `ManualReview("invalid_interval")`.

---

## Dataset breakdown by expected pattern

| JobPattern | Approximate count | Derivation |
|---|---|---|
| `BashJob` | ~14,000 | APPL_TYPE=OS, TASKTYPE=Command/Job, no ManualReview triggers |
| `FileTransfer` | ~13,946 | APPL_TYPE=FILE_TRANS |
| `CyclicJob` | ~3,602 | CYCLIC=1 (may overlap other patterns) |
| `FileWatcher` | ~144 | APPL_TYPE=FileWatch |
| `AwsJob` | ~604 | APPL_TYPE=AWS |
| `AlreadyAirflow` | ~51 | APPL_TYPE=AIRFLOWV2 |
| `DependencyGate` | ~300 (est.) | TASKTYPE=Dummy, APPL_TYPE=OS, no CMDLINE |
| `ManualReview` | ~680 (target) | BIM + complex scheduling + confirm + other triggers |

Note: `CyclicJob` is an orthogonal property — a cyclic FileTransfer job is classified as `CyclicJob` because cyclic takes precedence (it affects the DAG schedule fundamentally). The operator is still derived from `APPL_TYPE` (e.g. FILE_TRANS → FTPOperator). `CyclicJob` changes `schedule=timedelta(seconds=N)` on the DAG; it does NOT use a sensor. INCOND-derived `ExternalTaskSensor` tasks are generated independently if the job has cross-folder dependencies.

---

## classifier/mod.rs implementation notes

- Check ManualReview triggers first before committing to any positive pattern — collect all reasons, don't short-circuit.
- All string comparisons must use `to_ascii_lowercase()`.
- APPL_TYPE absent or empty → treat as `"os"`.
- A job can have `CYCLIC=1` and `APPL_TYPE=FILE_TRANS` → `CyclicJob` wins (the cyclic schedule is the primary structural concern; the file transfer operator detail is still derived from `APPL_TYPE` and stored in `dag_config.plugin_config`).
- `CyclicJob` → `schedule=timedelta(seconds=N)` on the DAG. No sensor. The Airflow scheduler fires it on the interval automatically. Do not confuse with `FileSensor` (FileWatcher) or `ExternalTaskSensor` (cross-folder INCOND) — those are dependency constructs, not schedule constructs.
- `DependencyGate` jobs produce minimal IR — `pattern: "DependencyGate"`, `dag_config: null`, `dependencies` wired normally.
- Log `tracing::warn!` for every ManualReview trigger found.
