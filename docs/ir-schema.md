# Job IR Schema Reference

Defines the intermediate representation (IR) written by `ir/mod.rs` for each job.
One file per job: `job_{JOBNAME}.json` (or `.yaml`).

---

## Top-level structure

```json
{
  "job_id": "RT_JOB001",
  "source_folder": "MY_FOLDER",
  "datacenter": "neutron",
  "pattern": "BashJob",
  "tasktype": "Command",
  "appl_type": "OS",
  "dag_config": { ... },
  "dependencies": { ... },
  "callbacks": { ... },
  "variables": { ... },
  "unmapped_attrs": ["CONFCAL", "SHIFT"]
}
```

| Field | Type | Always present | Notes |
|---|---|---|---|
| `job_id` | string | yes | = JOBNAME |
| `source_folder` | string | yes | = PARENT_FOLDER |
| `datacenter` | string | yes | = DATACENTER from enclosing FOLDER |
| `pattern` | string | yes | `JobPattern` variant name |
| `tasktype` | string | yes | original TASKTYPE value |
| `appl_type` | string | yes | original APPL_TYPE value (empty string if absent) |
| `dag_config` | object\|null | yes | null for DependencyGate and ManualReview |
| `dependencies` | object | yes | always present; may be empty arrays |
| `callbacks` | object | yes | always present; may be empty |
| `variables` | object | yes | job params dict; may be empty |
| `unmapped_attrs` | array | yes | **never omitted** — empty array `[]` when none |

---

## dag_config object

```json
{
  "schedule": "0 2 * * *",
  "start_date": "2024-01-01",
  "end_date": null,
  "timezone": "Asia/Bangkok",
  "catchup": false,
  "retries": 0,
  "execution_timeout_sec": 3600,
  "priority_weight": 100,
  "pool": "am_load_1",
  "pool_slots": 1,
  "owner": "svc_batch",
  "tags": ["MY_FOLDER", "APP_NAME", "SUB_APP"],
  "sla_sec": null,
  "nodeid": "agent01",
  "run_as": "svc_batch",
  "agent_os": "linux",
  "operator": "SSHOperator",
  "command": "/batch/run.sh {{ ds_nodash }}",
  "plugin_config": { ... }
}
```

| Field | Type | Source | Notes |
|---|---|---|---|
| `schedule` | string | derived from DAYS/WEEKDAYS/months or INTERVAL | cron string or `"timedelta:900"` for cyclic |
| `start_date` | string\|null | ACTIVE_FROM (YYYYMMDD → YYYY-MM-DD) | |
| `end_date` | string\|null | ACTIVE_TILL | |
| `timezone` | string\|null | TIMEZONE | |
| `catchup` | bool | RETRO=1 → true | |
| `retries` | int | MAXRERUN | |
| `execution_timeout_sec` | int\|null | MAXWAIT × 60; 0 → null | |
| `priority_weight` | int\|null | derived from PRIORITY | AA→100, A→90…Z→10 |
| `pool` | string\|null | QUANTITATIVE.NAME (snake_case) | |
| `pool_slots` | int | QUANTITATIVE.QUANT | |
| `owner` | string\|null | OWNER | |
| `tags` | array | APPLICATION, SUB_APPLICATION, PARENT_FOLDER | |
| `sla_sec` | int\|null | from SHOUT WHEN=EXECTIME, TIME field | |
| `nodeid` | string | NODEID | agent node name — maps to Airflow connection ID |
| `run_as` | string\|null | RUN_AS | SSH/PSRP username — stored in connection, referenced here for audit |
| `agent_os` | string | derived from connection registry by NODEID | `"linux"` → `SSHOperator`; `"windows"` → `PsrpOperator` |
| `operator` | string | derived from pattern + agent_os | `SSHOperator`, `PsrpOperator`, `StepFunctionStartExecutionOperator`, etc. |
| `command` | string\|null | CMDLINE after token substitution | for SSHOperator (`command=`) and PsrpOperator (`powershell=`) |
| `plugin_config` | object\|null | plugin-specific fields | see below |

### plugin_config per pattern

**FileTransfer:**
```json
{
  "protocol": "SFTP",
  "transfer_type": "onprem_to_onprem",
  "account": "FTP_CONN_01",
  "local_host": "agent01",
  "remote_host": "remote.host.example",
  "transfers": [
    { "local_path": "/data/*.DAT", "remote_path": "./", "direction": "upload", "type": "I" }
  ]
}
```

`transfer_type` values:
- `"onprem_to_onprem"` — both source and destination are on-premise hosts; command runs on agent node via `SSHOperator` / `PsrpOperator`
- `"onprem_to_cloud"` — source is on-premise, destination is S3 / Azure Blob; command runs on relay server via `SSHOperator` / `PsrpOperator`
- `"cloud_to_cloud"` — both endpoints are cloud storage; command runs on relay server via `SSHOperator` / `PsrpOperator`

The DAG generator builds the shell command (`lftp`, `aws s3 cp`, `azcopy`, PowerShell) from `protocol`, `transfer_type`, and the `transfers[]` entries, then passes it as `command` to `SSHOperator` or `powershell` to `PsrpOperator`.

**FileWatcher:**
```json
{
  "file_path": "/data/input/FILE_{{ ds_nodash }}.CTL",
  "mode": "CREATE",
  "timeout_hours": 5,
  "poke_interval_sec": 60,
  "min_size_bytes": 0
}
```

FileWatcher runs a polling loop script on the agent node via `SSHOperator` / `PsrpOperator` — it does NOT use Airflow's `FileSensor` (which would require the file to be accessible from the worker pod).

**AwsJob (StepFunctions):**
```json
{
  "service_type": "STEP",
  "account": "MY_AWS_CONN",
  "region": "ap-southeast-1",
  "state_machine_name": "my-step-function-prod",
  "execution_name": "my-execution",
  "input_json": "{\"key\": \"value\"}"
}
```

**AwsJob (Lambda):**
```json
{
  "service_type": "LAMBDA",
  "account": "MY_AWS_CONN",
  "function_name": "my-lambda-function",
  "payload": "{}"
}
```

**AlreadyAirflow:**
```json
{
  "trigger_dag_id": "my-cloud-crm-batch-prod",
  "account": "AIRFLOW_CONN_01",
  "run_date": null
}
```

---

## dependencies object

```json
{
  "upstream": [
    {
      "condition_name": "RT_JOB000-ENDED-OK",
      "odate": "ODAT",
      "and_or": "A"
    }
  ],
  "downstream": [
    {
      "condition_name": "RT_JOB001-ENDED-OK",
      "odate": "ODAT",
      "sign": "+"
    }
  ]
}
```

Both `upstream` and `downstream` arrays are always present (empty array `[]` when none).

---

## callbacks object

```json
{
  "on_failure": [
    { "type": "shout", "dest": "EM", "urgency": "V", "message": "%%JOBNAME FAILED" }
  ],
  "on_success": [],
  "sla_alerts": [
    { "type": "shout", "dest": "EM", "urgency": "V", "message": "%%JOBNAME >60 min", "threshold_min": 60 }
  ]
}
```

---

## variables object

Job-level variables (non-plugin-namespaced) emitted as a flat dict:

```json
{
  "%%TDAY": "%%SUBSTR %%DATE 1 6",
  "%%TYEAR": "%%SUBSTR %%DATE 7 4"
}
```

---

## unmapped_attrs array

Always emitted. Contains attribute/element names that could not be automatically mapped:

```json
["CONFCAL", "SHIFT", "CONTROL:LOCK_RESOURCE_X"]
```

Format: plain attribute name, or `ELEMENT:VALUE` for child elements.

---

## migration_summary.json

Written once per run to the output directory root.

```json
{
  "run_timestamp": "2026-06-12T20:00:00Z",
  "input_file": "export_xml_260612.xml",
  "total": 30304,
  "auto_converted": 29000,
  "manual_review": 1304,
  "patterns": {
    "BashJob": 14000,
    "FileTransfer": 13946,
    "CyclicJob": 3602,
    "FileWatcher": 144,
    "AwsJob": 604,
    "AlreadyAirflow": 51,
    "DependencyGate": 300,
    "ManualReview": 1304
  },
  "manual_review_reasons": {
    "bim_sla_checkpoint": 68,
    "named_calendar:DAYSCAL": 400,
    "named_calendar:CONFCAL": 200,
    "confirm_required": 50,
    "unknown_attrs": 30
  },
  "folders_processed": 6164,
  "errors": []
}
```

`auto_converted` = `total` − `manual_review`. `patterns` counts may overlap (a CyclicJob BashJob counts in both `CyclicJob` and the total). `errors` lists fatal parse errors (XML structural failures); jobs routed to ManualReview are NOT errors.

---

## File naming

```
output/
├── job__FOLDER_NAME__JOBNAME.json
├── job__FOLDER_NAME__RT_JOB001.json
├── ...
└── migration_summary.json
```

Job filename: `job__{FOLDER_NAME}__{JOBNAME}.{format}`. Both components are taken verbatim from the XML (case-preserved); `/`, `\`, `:` are replaced with `_`. The double-underscore separator ensures uniqueness across folders — the same JOBNAME can appear in multiple folders.
