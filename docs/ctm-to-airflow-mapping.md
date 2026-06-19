# Control-M to Airflow Mapping Guide

This document is the primary reference for the DAG generator. It defines how every
Control-M concept, attribute, and pattern maps to an Airflow equivalent.

---

## 1. Concept Mapping

| Control-M Concept | Control-M Term | Airflow Equivalent | Airflow Term |
|---|---|---|---|
| Scheduling unit | `FOLDER` | Workflow definition | `DAG` |
| Executable unit | `JOB` | Unit of work | `Task` / `Operator` |
| Job type | `APPL_TYPE` | Operator class | `BashOperator`, `FileSensor`, etc. |
| Upstream dependency | `INCOND` | Task dependency or cross-DAG wait | `>>` edge or `ExternalTaskSensor` |
| Downstream signal | `OUTCOND` | Task completion signal | implicit on task success |
| Execution schedule | `DAYS` + `TIMEFROM` | Cron expression | `schedule="0 2 * * *"` |
| Repeating interval | `CYCLIC=1` + `INTERVAL` | Timedelta schedule | `schedule=timedelta(seconds=N)` |
| Retry count | `MAXRERUN` | Retry count | `retries=N` |
| Execution time limit | `MAXWAIT` (minutes) | Execution timeout | `execution_timeout=timedelta(seconds=N*60)` |
| Backfill | `RETRO=1` | Catch-up runs | `catchup=True` |
| Resource limit | `QUANTITATIVE NAME+QUANT` | Pool + slots | `pool="name"`, `pool_slots=N` |
| Failure handler | `ON CODE=*failed*` / `SHOUT WHEN=NOTOK` | Failure callback | `on_failure_callback` |
| Success handler | `ON CODE=*success*` | Success callback | `on_success_callback` |
| SLA alert | `SHOUT WHEN=EXECTIME` | SLA miss callback | `sla=timedelta(seconds=N)` |
| Named calendar | `DAYSCAL` / `CONFCAL` / `WEEKSCAL` | — | ManualReview (no equivalent) |
| SLA checkpoint | `APPL_TYPE=BIM` | — | ManualReview (no equivalent) |
| Manual gate | `CONFIRM=1` | — | ManualReview (no equivalent) |
| Variable substitution | `%%VAR` / `%%$ODATE` | Jinja2 template | `{{ var.value.VAR }}` / `{{ ds_nodash }}` |

---

## 2. JobPattern → Airflow Operator

| JobPattern | Airflow Operator | Import | Notes |
|---|---|---|---|
| `BashJob` | `BashOperator` | `airflow.operators.bash` | `bash_command` from CMDLINE after token substitution |
| `FileWatcher` | `FileSensor` | `airflow.sensors.filesystem` | `filepath`, `poke_interval`, `timeout` from plugin vars |
| `FileTransfer (FTP)` | `FTPOperator` | `airflow.providers.ftp.operators.ftp` | local/remote path, connection from `%%FTP-ACCOUNT` |
| `FileTransfer (SFTP)` | `SFTPOperator` | `airflow.providers.sftp.operators.sftp` | same as FTP but SFTP provider |
| `FileTransfer (LOCAL)` | `BashOperator` | `airflow.operators.bash` | `cp` / `mv` command |
| `AwsJob (STEP)` | `StepFunctionStartExecutionOperator` | `airflow.providers.amazon.aws.operators.step_function` | state machine name, input JSON from payload vars |
| `AwsJob (LAMBDA)` | `LambdaInvokeFunctionOperator` | `airflow.providers.amazon.aws.operators.lambda_function` | function name, payload |
| `AwsJob (BATCH)` | `BatchOperator` | `airflow.providers.amazon.aws.operators.batch` | job name, queue, overrides |
| `AlreadyAirflow` | `TriggerDagRunOperator` | `airflow.operators.trigger_dagrun` | `trigger_dag_id` from `%%UCM-DAGID` |
| `DependencyGate` | `EmptyOperator` | `airflow.operators.empty` | `trigger_rule=TriggerRule.ALL_SUCCESS` |
| `CyclicJob` | same as underlying `APPL_TYPE` | — | **Affects DAG schedule only** — not the operator. See note below. |
| `ManualReview` | — | — | skip; emit to manual migration report |

> **CyclicJob note:** `CyclicJob` is an orthogonal classification — it changes the DAG's `schedule` argument to `timedelta(seconds=N)` but does **not** determine the operator. The operator is derived from the job's `APPL_TYPE` exactly as for non-cyclic jobs (e.g. `APPL_TYPE=OS` → `BashOperator`, `APPL_TYPE=FILE_TRANS` → `FTPOperator`). A CyclicJob does **not** use a sensor — the Airflow scheduler fires it automatically on the interval. INCOND-derived `ExternalTaskSensor` tasks are still generated normally if the job has cross-folder dependencies.

---

## 3. DAG-Level Attributes

| Control-M | Source | Airflow DAG argument | Example |
|---|---|---|---|
| Folder name | `FOLDER_NAME` | `dag_id` | `dag_id="PAYMENT_FOLDER"` |
| Schedule | `DAYS` + `TIMEFROM` | `schedule` | `schedule="0 2 * * *"` |
| Cyclic interval | `CYCLIC=1` + `INTERVAL` | `schedule` | `schedule=timedelta(seconds=900)` |
| Catchup | `RETRO=1` | `catchup` | `catchup=True` |
| Start date | `ACTIVE_FROM` | `start_date` | `start_date=datetime(2024, 1, 1)` |
| End date | `ACTIVE_TILL` | `end_date` | `end_date=datetime(2025, 12, 31)` |
| Timezone | `TIMEZONE` | `timezone` | `timezone="Asia/Bangkok"` |
| Tags | `APPLICATION`, `SUB_APPLICATION` | `tags` | `tags=["APP_NAME", "SUB_APP"]` |
| Owner | `OWNER` | `owner` (default_args) | `"owner": "svc_batch"` |

### Schedule derivation rules

```
CYCLIC=1 + INTERVAL=00015M   →  schedule=timedelta(seconds=900)
                                 (Airflow scheduler fires on this interval — no sensor needed)
DAYSCAL / CONFCAL present    →  ManualReview (cannot derive)
DAYS=ALL + WEEKDAYS=ALL      →  schedule="<minute> <hour> * * *"
DAYS=1,15 + WEEKDAYS=ALL     →  schedule="<minute> <hour> 1,15 * *"
DAYS=ALL + WEEKDAYS=1,2,3    →  schedule="<minute> <hour> * * 1,2,3"
DAYS=1,15 + WEEKDAYS=1 + DAYS_AND_OR=A  →  schedule="<minute> <hour> 1,15 * 1"
TIMEFROM=0200                →  hour=2, minute=0
TIMEFROM absent              →  hour=0, minute=0
```

> **When is a sensor used vs a schedule?**
> - `schedule=timedelta(...)` — the DAG recurs automatically; no sensor involved. Used for `CyclicJob`.
> - `ExternalTaskSensor` — a task inside the DAG that waits for a specific upstream task in another DAG to finish. Comes from INCOND cross-folder dependencies, independent of whether the job is cyclic.
> - `FileSensor` — a task that waits for a file to appear on disk. Only for `FileWatcher` (`APPL_TYPE=FileWatch`).

---

## 4. Task-Level Attributes

| Control-M attribute | Airflow Task argument | Notes |
|---|---|---|
| `MAXRERUN` | `retries` | direct integer copy |
| `MAXWAIT` | `execution_timeout` | `timedelta(seconds=MAXWAIT*60)`; 0 → None |
| `RETRO` | `catchup` | `True` when RETRO=1 |
| `PRIORITY` | `priority_weight` | AA→100, A→90, B→80 … Z→10 |
| `QUANTITATIVE NAME` | `pool` | normalize to snake_case |
| `QUANTITATIVE QUANT` | `pool_slots` | direct integer |
| `SHOUT WHEN=EXECTIME TIME=>060` | `sla` | `timedelta(seconds=3600)` |
| `SHOUT WHEN=NOTOK` | `on_failure_callback` | emit shout details to callback |
| `ON CODE=*failed*` | `on_failure_callback` | |
| `ON CODE=*success*` | `on_success_callback` | |

---

## 5. Dependency Wiring

### 5.1 INCOND resolution rules

Every `INCOND` on a job is resolved by looking up its `condition_name` in the global
OUTCOND index (built from all jobs' `downstream[].condition_name`):

```
INCOND condition_name found in SAME folder
    + ODATE=ODAT   →  intra-DAG edge:  producer_task >> consumer_task

INCOND condition_name found in DIFFERENT folder
    + ODATE=ODAT   →  ExternalTaskSensor(
                           external_dag_id=source_folder,
                           external_task_id=source_job,
                           execution_delta=None,
                       )
    + ODATE=PREV   →  ExternalTaskSensor(
                           execution_delta=timedelta(days=1),
                       )

INCOND condition_name NOT FOUND anywhere
                   →  ExternalTaskSensor(
                           external_dag_id=None,   # orphan — log warning
                           external_task_id=None,
                       )
```

### 5.2 AND_OR logic

| `AND_OR` | Applies to | Airflow implementation |
|---|---|---|
| `A` (AND, default) | downstream task | `trigger_rule=TriggerRule.ALL_SUCCESS` (Airflow default — omit) |
| `O` (OR) | downstream task | `trigger_rule=TriggerRule.ONE_SUCCESS` — set explicitly |

When a task has multiple INCOND and **any** of them has `AND_OR=O`, set
`trigger_rule=TriggerRule.ONE_SUCCESS` on that task.

### 5.3 OUTCOND handling

| OUTCOND `SIGN` | OUTCOND `ODATE` | Handling |
|---|---|---|
| `+` | `ODAT` | normal — task completion signals downstream |
| `+` | `STAT` | task still runs; add comment `# emits persistent condition` |
| `-` | any | ManualReview: `outcond_delete` — cannot auto-convert |

### 5.4 Conditions to skip (ManualReview triggers)

| INCOND `ODATE` | Reason | Action |
|---|---|---|
| `NEXT` | forward dependency on tomorrow's run | ManualReview: `incond_next_date` |
| `STAT` | persistent static condition, never auto-clears | ManualReview: `incond_static_condition` |
| `****` | wildcard — any date | ManualReview: `incond_wildcard_date` |

---

## 6. Variable / Token Substitution

Control-M uses `%%` tokens in CMDLINE and variable values. These map to Jinja2 in Airflow.

| Control-M token | Airflow Jinja2 | Notes |
|---|---|---|
| `%%$ODATE` | `{{ ds_nodash }}` | execution date YYYYMMDD |
| `%%ODATE` | `{{ ds_nodash }}` | same |
| `%%YYYY` | `{{ execution_date.year }}` | 4-digit year |
| `%%MM` | `{{ execution_date.strftime('%m') }}` | 2-digit month |
| `%%DD` | `{{ execution_date.strftime('%d') }}` | 2-digit day |
| `%%$DATE` | `{{ ds }}` | execution date YYYY-MM-DD |
| `%%TIME` | `{{ execution_date.strftime('%H%M') }}` | HHMM |
| `%%JOBNAME` | `{{ task.task_id }}` | |
| `%%SCHEDTABLE` | `{{ dag.dag_id }}` | |
| `%%VAR%%` | `{{ var.value.VAR }}` | Airflow variable lookup |
| `%4E` in AWS payload | `\n` (newline) | URL-encoded — decode before writing IR |

---

## 7. Plugin Variable Namespaces

Control-M stores plugin config as `VARIABLE` child elements with namespaced names.
The DAG generator reads these from `dag_config.plugin_config` in the IR.

### FileWatch (`%%FileWatch-*`)

| Variable | IR field | Airflow FileSensor arg |
|---|---|---|
| `%%FileWatch-FILE_PATH` | `plugin_config.file_path` | `filepath` |
| `%%FileWatch-MODE` | `plugin_config.mode` | — (CREATE = wait for new file) |
| `%%FileWatch-INT_FILE_SEARCHES` | `plugin_config.poke_interval_sec` | `poke_interval` |
| `%%FileWatch-TIME_LIMIT` | `plugin_config.timeout_hours` | `timeout` |
| `%%FileWatch-MIN_DET_SIZE` | `plugin_config.min_size_bytes` | custom check |

### FILE_TRANS (`%%FTP-*`)

| Variable | IR field | Notes |
|---|---|---|
| `%%FTP-CONNTYPE2` | `plugin_config.protocol` | FTP / SFTP / LOCAL |
| `%%FTP-ACCOUNT` | `plugin_config.account` | Airflow connection ID |
| `%%FTP-LHOST` | `plugin_config.local_host` | source agent |
| `%%FTP-RHOST` | `plugin_config.remote_host` | destination host |
| `%%FTP-LPATH{N}` | `plugin_config.transfers[N].local_path` | |
| `%%FTP-RPATH{N}` | `plugin_config.transfers[N].remote_path` | |
| `%%FTP-UPLOAD{N}` | `plugin_config.transfers[N].direction` | 1=upload, 0=download |
| `%%FTP-TYPE{N}` | `plugin_config.transfers[N].type` | A=ASCII, I=binary |

### AWS (`%%AWS-*`)

| Variable | IR field | Notes |
|---|---|---|
| `%%AWS-SERVICE_TYPE` | `plugin_config.service_type` | STEP / LAMBDA / BATCH |
| `%%AWS-ACCOUNT` | `plugin_config.account` | Airflow AWS connection ID |
| `%%AWS-STEP_NAME` | `plugin_config.state_machine_name` | Step Functions ARN or name |
| `%%AWS-STEP_EXECUTION_NAME` | `plugin_config.execution_name` | |
| `%%AWS-STEP_PAYLOAD_JSON-*` | `plugin_config.input_json` | URL-decode `%4E` → `\n` |
| `%%AWS-LAMBDA_FUNCTION_NAME` | `plugin_config.function_name` | Lambda only |
| `%%AWS-BATCH_JOB_NAME` | `plugin_config.job_name` | Batch only |

### AIRFLOWV2 (`%%UCM-*`)

| Variable | IR field | Notes |
|---|---|---|
| `%%UCM-DAGID` | `plugin_config.trigger_dag_id` | target DAG to trigger |
| `%%UCM-ACCOUNT` | `plugin_config.account` | Airflow connection ID |

---

## 8. Callbacks and Alerts

### on_failure_callback sources (in priority order)

1. `ON CODE=*failed*` block with `DOACTION ACTION=NOTOK`
2. `SHOUT WHEN=NOTOK` — emit shout details
3. `ON CODE=NOTOK` blocks

### on_success_callback sources

1. `ON CODE=*success*` block with `DOACTION ACTION=OK`

### SLA (sla_miss_callback)

- `SHOUT WHEN=EXECTIME TIME=>NNN` → `sla=timedelta(seconds=NNN*60)`
- Multiple SHOUT EXECTIME → use the smallest threshold as the SLA

---

## 9. DAG Generator Input → Output

The DAG generator reads from `output/auto_converted/dag_groups/` and
`output/auto_converted/dag_groups_external/` (each a `{FOLDER_NAME}.json`) and
produces one `{FOLDER_NAME}.py` per manifest. Never read from `manual_review/`.

### Input manifest structure

```json
{
  "dag_id": "PAYMENT_FOLDER",
  "datacenter": "neutron",
  "schedule": "0 2 * * *",
  "timezone": "Asia/Bangkok",
  "jobs": [
    { "job_id": "RT_PAY_001", "pattern": "BashJob", "operator": "BashOperator" },
    { "job_id": "RT_PAY_002", "pattern": "BashJob", "operator": "BashOperator" }
  ],
  "edges": [
    { "from": "RT_PAY_001", "to": "RT_PAY_002", "condition": "RT_PAY_001-ENDED-OK" }
  ],
  "external_sensors": [
    {
      "in_job": "RT_PAY_001",
      "condition": "RT_UPSTREAM-ENDED-OK",
      "odate": "ODAT",
      "source_folder": "UPSTREAM_FOLDER",
      "source_job": "RT_UPSTREAM",
      "execution_delta_days": null,
      "trigger_rule": null
    }
  ]
}
```

### Output DAG skeleton

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.trigger_rule import TriggerRule

with DAG(
    dag_id="PAYMENT_FOLDER",
    schedule="0 2 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["APP_NAME"],
) as dag:

    # External sensors (cross-folder dependencies)
    wait_RT_UPSTREAM = ExternalTaskSensor(
        task_id="wait_RT_UPSTREAM",
        external_dag_id="UPSTREAM_FOLDER",
        external_task_id="RT_UPSTREAM",
        execution_delta=None,           # timedelta(days=1) when odate=PREV
    )

    # Tasks
    RT_PAY_001 = BashOperator(
        task_id="RT_PAY_001",
        bash_command="/batch/pay_001.sh {{ ds_nodash }}",
        retries=2,
        execution_timeout=timedelta(seconds=3600),
        pool="batch_pool",
        pool_slots=1,
    )

    RT_PAY_002 = BashOperator(
        task_id="RT_PAY_002",
        bash_command="/batch/pay_002.sh {{ ds_nodash }}",
        retries=0,
        # trigger_rule=TriggerRule.ONE_SUCCESS  ← set when AND_OR=O
    )

    # Dependency wiring
    wait_RT_UPSTREAM >> RT_PAY_001   # external sensor feeds into first task
    RT_PAY_001 >> RT_PAY_002         # intra-DAG edge from condition
```

### Generation algorithm

```
for each manifest in auto_converted/dag_groups/ and auto_converted/dag_groups_external/:

  1. Read job IR files from auto_converted/jobs/ for each job in manifest.jobs
  2. Emit DAG header (dag_id, schedule, start_date, catchup, timezone, tags)
  3. For each external_sensor:
       emit ExternalTaskSensor(
           task_id = "wait_{source_job}",
           external_dag_id = source_folder,
           external_task_id = source_job,
           execution_delta = timedelta(days=execution_delta_days) if set else None,
       )
  4. For each job in manifest.jobs:
       look up job IR from jobs/job_{job_id}.json
       emit operator matching job.pattern (see section 2)
       apply all task-level attributes (section 4)
  5. For each edge:
       emit: {from_task} >> {to_task}
       if trigger_rule set: add trigger_rule=TriggerRule.{trigger_rule} to to_task
  6. Wire external sensors → their consumer tasks:
       for each external_sensor, find jobs that have it in their upstream
       emit: wait_{source_job} >> {in_job}

for each manifest in auto_converted/dag_singles/:
  emit single-task DAG (no edges, no sensors)
```

---

## 10. ManualReview Handling

Jobs routed to `ManualReview` are **excluded** from DAG generation. The generator must:

1. Skip the job silently (do not emit operator or raise error)
2. If ALL jobs in a folder are ManualReview → skip the entire DAG file
3. If SOME jobs in a folder are ManualReview → emit remaining connected jobs,
   log a warning for each skipped job

The `migration_summary.json` and `manual_review_reasons` field provide the full list
of skipped jobs grouped by reason for the human migration engineer.

### ManualReview reason → recommended human action

| Reason | Action |
|---|---|
| `bim_sla_checkpoint` | Replace with Airflow SLA callbacks manually |
| `named_calendar:DAYSCAL` | Translate calendar to cron or custom timetable |
| `named_calendar:CONFCAL` | Same as DAYSCAL |
| `named_calendar:WEEKSCAL` | Same as DAYSCAL |
| `confirm_required` | Add `BranchPythonOperator` for manual approval gate |
| `outcond_delete` | Redesign dependency — removal pattern not needed in Airflow |
| `incond_static_condition` | Replace with a persistent Airflow Variable check |
| `incond_wildcard_date` | Clarify intent with ops team — likely always-true gate |
| `incond_next_date` | Redesign — forward dependency not supported in Airflow |
| `complex_cyclic_sequence` | Build custom timetable or use sensors |
| `complex_cyclic_times` | Same as above |
| `independent_cyclic` | Redesign as separate DAG with its own schedule |
| `output_pattern_match` | Implement with `ShortCircuitOperator` or custom sensor |
| `itsm_remedy_action` | Implement via `on_failure_callback` calling ITSM API |
| `unknown_transfer_protocol:FTP-SSL` | Add FTP-SSL support to classifier or handle manually |
| `unknown_transfer_protocol:S3` | Map to `S3Hook` / `S3ToLocalFilesystemOperator` |
| `exclusive_resource_lock` | Use Airflow pool with `pool_slots=MAX` to serialize |
| `date_shift` | Translate shift offset to `timedelta` in schedule or sensor |
