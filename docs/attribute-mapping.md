# Control-M → Airflow Attribute Mapping

Maps Control-M XML attributes and child elements to Airflow DAG/operator fields.
All mappings verified against `export_xml_260612.xml` (30,304 jobs).

---

## JobData attributes → DagConfig

### Identity / metadata

| Control-M | Airflow field | Transformation |
|---|---|---|
| `JOBNAME` | `dag_id` (base) | used as-is for job IR `job_id` |
| `DESCRIPTION` | `dag.doc_md` | copy as-is |
| `APPLICATION` | `dag.tags` | add as tag |
| `SUB_APPLICATION` | `dag.tags` | add as tag |
| `PARENT_FOLDER` | `dag.tags` | add as tag; also used to group DAGs |
| `RUN_AS` | (dropped) | OS user — Airflow handles identity via connections |
| `NODEID` | (dropped) | agent host — replaced by Airflow executor target |
| `OWNER` | `dag.owner` | copy as-is |
| `APPL_TYPE` | used for classification only | see `patterns.md` |

### Scheduling — time window

| Control-M | Airflow field | Transformation |
|---|---|---|
| `TIMEFROM` | `dag.start_date` (time component) | `HHMM` → `HH:MM` |
| `TIMETO` | (informational) | stored in `dag_config.time_to`; not directly mapped |
| `TIMEZONE` | `dag.timezone` | IANA name, copy as-is |
| `ACTIVE_FROM` | `dag.start_date` | `YYYYMMDD` → `datetime` |
| `ACTIVE_TILL` | `dag.end_date` | `YYYYMMDD` → `datetime` |

### Scheduling — day/month

| Control-M | Airflow field | Transformation |
|---|---|---|
| `DAYS` + `WEEKDAYS` + `DAYS_AND_OR` + month flags | `dag.schedule` (cron) | see cron derivation rules below |
| `DAYSCAL` / `CONFCAL` / `WEEKSCAL` | `unmapped_attrs` + `ManualReview` | named calendars cannot be auto-converted |
| `RETRO` = `1` | `dag.catchup = True` | `0` → `catchup = False` |
| `SHIFT` ≠ `Ignore Job` | `unmapped_attrs` | date shifting → `ManualReview` |

#### Cron derivation rules

```
DAYS="ALL"  + WEEKDAYS="ALL"  + all months=1  → "0 0 * * *"  (adjust hour from TIMEFROM)
DAYS="ALL"  + WEEKDAYS="1-5"  + all months=1  → "0 H * * 1-5"
DAYS="1,15" + WEEKDAYS="ALL"  + all months=1  → "0 H 1,15 * *"
DAYS_AND_OR="A" → AND (both day AND weekday must match)
DAYS_AND_OR="O" → OR  (either matches)
months not all 1  → add month field to cron
DAYSCAL/CONFCAL present          → cannot derive cron; route to ManualReview
```

When `CYCLIC=1`, ignore this cron derivation — see cyclic mapping below.

### Cyclic / repeat

| Control-M | Airflow field | Transformation |
|---|---|---|
| `CYCLIC` = `1` | `dag.schedule` | use `timedelta` instead of cron |
| `INTERVAL` | `dag.schedule_interval` | `00015M` → `timedelta(minutes=15)` |
| `CYCLIC_INTERVAL_SEQUENCE` | `unmapped_attrs` | multi-interval sequences → `ManualReview` |
| `CYCLIC_TIMES_SEQUENCE` | `unmapped_attrs` | explicit run times → `ManualReview` |
| `CYCLIC_TOLERANCE` | (dropped) | no Airflow equivalent |
| `IND_CYCLIC` = `S` | (default) | sequential — no change |
| `IND_CYCLIC` = `I` | `unmapped_attrs` | independent cyclic → `ManualReview` |

INTERVAL format: `NNNNNu` where u = `M`(minutes) / `H`(hours) / `D`(days).

### Execution control

| Control-M | Airflow field | Transformation |
|---|---|---|
| `MAXWAIT` | `dag_config.execution_timeout_sec` | minutes × 60; 0 = no timeout |
| `MAXRERUN` | `dag_config.retries` | copy as int |
| `PRIORITY` | `dag_config.priority_weight` | `AA`=100, `A`=90…`Z`=10 |
| `CRITICAL` = `1` | `dag_config.sla` | flag for SLA monitoring |
| `CONFIRM` = `1` | `unmapped_attrs` | manual confirmation → `ManualReview` |
| `MAXDAYS` | (dropped) | retention, not Airflow concern |
| `MAXRUNS` | (dropped) | concurrency handled by Airflow executor |

### CMDLINE transformation (OS jobs)

Raw `CMDLINE` → `BashOperator.bash_command`:

1. Replace `%%VARIABLE%%` syntax with Jinja: `%%VAR%%` → `{{ var.value.VAR }}`
2. Replace date tokens:
   - `%%$ODATE` → `{{ ds_nodash }}`
   - `%%$DATE` → `{{ ds }}`
   - `%%$YEAR` → `{{ execution_date.year }}`
   - `%%MONTH` → `{{ execution_date.month }}`
   - `%%DAY` → `{{ execution_date.day }}`
3. Any remaining `%%` tokens → add name to `unmapped_attrs`; emit as-is in template

---

## Child element mappings

### INCOND → ExternalTaskSensor

Each `INCOND` becomes one `ExternalTaskSensor` task.

| INCOND attribute | Airflow field | Notes |
|---|---|---|
| `NAME` | used to derive `external_dag_id` | strip `-ENDED-OK` suffix; look up job by condition name |
| `ODATE="ODAT"` | `execution_delta=timedelta(0)` | same execution date |
| `ODATE="PREV"` | `execution_delta=timedelta(days=1)` | previous day's run |
| `ODATE="NEXT"` | `execution_delta=timedelta(days=-1)` | next day's run |
| `ODATE="STAT"` / `"****"` | `unmapped_attrs` | static/wildcard → `ManualReview` |
| `AND_OR="A"` | all sensors must pass | default — tasks in sequence |
| `AND_OR="O"` | any sensor suffices | emit as parallel sensors + `ShortCircuitOperator` |

### OUTCOND → TriggerDagRunOperator (when SIGN="+")

| OUTCOND attribute | Airflow field | Notes |
|---|---|---|
| `NAME` | `trigger_dag_id` (informational) | condition name records the downstream link |
| `SIGN="+"` | `TriggerDagRunOperator` | standard — post condition |
| `SIGN="-"` | `unmapped_attrs` | condition deletion → `ManualReview` |
| `ODATE` | same as INCOND mapping | |

In practice, OUTCOND is used to record the dependency graph; the DAG generator uses it to wire `ExternalTaskSensor` on the downstream side rather than adding explicit trigger operators.

### VARIABLE / AUTOEDIT2

Variables fall into two categories:

**Plugin config variables** (namespaced `%%PLUGIN-KEY`): extracted and mapped per plugin — see plugin sections below.

**Job variables** (non-namespaced or `%%VARNAME`): emitted to `dag_config.params` dict for Jinja rendering.

**AUTOEDIT** (legacy `EXP="%%VAR=value"`): parse `EXP` by splitting on first `=`.

### SHOUT → callbacks

| SHOUT.WHEN | Airflow field | Notes |
|---|---|---|
| `NOTOK` | `on_failure_callback` | |
| `OK` | `on_success_callback` | |
| `EXECTIME` | `sla` (SLA miss callback) | TIME attribute sets the threshold |
| `LATETIME` | `sla` | late start detection |
| `LATESUB` | `sla` | late submission |

DEST and MESSAGE are stored in `dag_config.callbacks` for the callback handler to use.

### QUANTITATIVE → Airflow pool

```
QUANTITATIVE.NAME → pool (normalize: replace spaces/hyphens with _, lowercase)
QUANTITATIVE.QUANT → pool_slots
```

### CONTROL (resource lock)

No direct Airflow equivalent. Emit `NAME` + `TYPE` to `unmapped_attrs`.

### ON blocks → callbacks / branching

| ON.CODE pattern | Interpretation | Airflow mapping |
|---|---|---|
| `*failed*` / `NOTOK` | failure handler | `on_failure_callback` |
| `*success*` / `OK` / `Job:Complete` | success handler | `on_success_callback` |
| `*INCOMPLETED*` | incomplete state | `on_retry_callback` |
| specific exit code (integer) | conditional branch | `BranchPythonOperator` |
| output pattern (`STMT` not `*`) | output parsing | `unmapped_attrs` → `ManualReview` |

ON child action mapping:

| DO element | Airflow equivalent |
|---|---|
| `DOACTION ACTION="OK"` | mark task success |
| `DOACTION ACTION="NOTOK"` | mark task failure |
| `DOACTION ACTION="RERUN"` | retry |
| `DOFORCEJOB` | `TriggerDagRunOperator` |
| `DOMAIL` | email callback (use Airflow `email_on_failure`) |
| `DOSHOUT` | alert callback (Slack/PagerDuty hook) |
| `DOCOND` | post/delete condition → store in `dag_config` |
| `DOVARIABLE` / `DOAUTOEDIT2` | `Variable.set()` in callback |
| `DOREMEDY` | `unmapped_attrs` → ITSM integration |

---

## Plugin-specific mappings

### OS jobs (APPL_TYPE=OS)

`TASKTYPE=Command` or `Job`, `CMDLINE` present.

```
CMDLINE  → BashOperator.bash_command  (after %%VAR substitution)
NODEID   → (dropped; Airflow executor handles placement)
RUN_AS   → (dropped; use Airflow connection or k8s serviceAccount)
```

### FILE_TRANS jobs (APPL_TYPE=FILE_TRANS, APPL_FORM=AFT)

All config is in `VARIABLE` children with `%%FTP-` prefix.

| VARIABLE NAME | Airflow / config | Notes |
|---|---|---|
| `%%FTP-ACCOUNT` | connection id | look up in Airflow Connections |
| `%%FTP-LHOST` / `%%FTP-RHOST` | connection host | |
| `%%FTP-LPATH1`..`%%FTP-LPATH5` | source paths | may contain `%%VAR` tokens |
| `%%FTP-RPATH1`..`%%FTP-RPATH5` | destination paths | |
| `%%FTP-CONNTYPE1/2` | `FTP`/`SFTP`/`LOCAL` | determines operator type |
| `%%FTP-TYPE1`..5 | `A`(ASCII)/`I`(binary) | |
| `%%FTP-UPLOAD1`..5 | `0`/`1` | direction flag |
| `%%FTP-TRANSFER_NUM` | count of transfer pairs | |
| `%%FTP-LOSTYPE`/`%%FTP-ROSTYPE` | `Unix`/`Windows` | OS type of local/remote |

Mapped to: `SFTPOperator` or `FTPOperator` per `CONNTYPE`. Multiple paths → one operator per path pair.

### FileWatch jobs (APPL_TYPE=FileWatch, APPL_FORM=File Watcher)

| VARIABLE NAME | Airflow field | Notes |
|---|---|---|
| `%%FileWatch-FILE_PATH` | `FileSensor.filepath` | path may contain `%%$ODATE` etc. |
| `%%FileWatch-MODE` | sensor mode | `CREATE` = file exists; `SIZE` = stable size |
| `%%FileWatch-TIME_LIMIT` | `FileSensor.timeout` | hours |
| `%%FileWatch-INT_FILE_SEARCHES` | `FileSensor.poke_interval` | seconds |
| `%%FileWatch-MIN_DET_SIZE` | size threshold | 0 = any size |
| `%%FileWatch-NUM_OF_ITERATIONS` | (dropped) | |
| `%%FileWatch-INT_FILESIZE_COMPARISON` | (dropped) | |
| `%%FileWatch-START_TIME` | `NOW` = no delay | |
| `%%FileWatch-MIN_AGE` / `%%FileWatch-MAX_AGE` | `unmapped_attrs` | age filtering → `ManualReview` if not `NO_MIN_AGE`/`NO_MAX_AGE` |

Mapped to: `FileSensor` (or `S3KeySensor` if path is s3://).

### AWS jobs (APPL_TYPE=AWS, APPL_FORM=AWS)

| VARIABLE NAME | Key | Airflow operator |
|---|---|---|
| `%%AWS-SERVICE_TYPE=STEP` | Step Functions | `StepFunctionStartExecutionOperator` |
| `%%AWS-SERVICE_TYPE=LAMBDA` | Lambda | `LambdaInvokeFunctionOperator` |
| `%%AWS-SERVICE_TYPE=BATCH` | AWS Batch | `BatchOperator` |
| `%%AWS-ACCOUNT` | connection id | Airflow AWS connection |
| `%%AWS-REGION` | `region_name` | |
| `%%AWS-STEP_NAME` | `state_machine_arn` | |
| `%%AWS-STEP_PAYLOAD_JSON-N001-VALUE` | `input` (JSON string) | URL-decode `%4E` → `\n` |
| `%%AWS-LAMBDA_NAME` | `function_name` | |
| `%%AWS-BATCH_JOB_DEFINITION` | `job_definition` | |
| `%%AWS-BATCH_JOB_QUEUE` | `job_queue` | |
| `%%AWS-BATCH_JOB_NAME` | `job_name` | |

`%%AWS-APPEND_LOG=Y` → set `awslogs_group` on operator.

### BIM jobs (APPL_TYPE=BIM, APPL_FORM=CONTROL-M BIM)

Business Impact Management — SLA checkpoint jobs. These are `TASKTYPE=Dummy` and do not execute commands.

| VARIABLE NAME | Purpose | Airflow equivalent |
|---|---|---|
| `%%BIM-SERVICE_NAME` | SLA service name | `dag.sla_miss_callback` label |
| `%%BIM-DUE_TIME` | expected completion `HH:MM,tolerance` | `dag.sla` |
| `%%BIM-SERVICE_PRIORITY` | priority level 1-5 | `dag_config.priority_weight` |
| `%%BIM-SENSITIVITY` | tolerance percentage | informational |
| `%%BIM-EVENTS` | event list | `unmapped_attrs` |

BIM jobs map to `ManualReview` with reason `BIM_SLA_CHECKPOINT` — they have no execution logic; their equivalent is Airflow's native SLA mechanism configured on the DAG.

### AIRFLOWV2 jobs (APPL_TYPE=AIRFLOWV2)

Jobs that trigger an existing Airflow DAG from Control-M. These represent jobs already migrated to Airflow that Control-M is calling via the UCM plugin.

| VARIABLE NAME | Purpose | Airflow field |
|---|---|---|
| `%%UCM-DAGID` | target DAG id | `TriggerDagRunOperator.trigger_dag_id` |
| `%%UCM-ACCOUNT` | Airflow connection | `http_conn_id` |
| `%%UCM-APP_NAME` | plugin name (`AIRFLOWV2`) | used for classification |
| `%%UCM-RUNDATE` | execution date override | `execution_date` |
| `%%UCM-RUNMODE` | run mode | `conf` param |
| `%%UCM-GETTASKS` | task fetch flag | `conf` param |

AIRFLOWV2 jobs → `JobPattern::AlreadyAirflow` (already migrated; emit as `TriggerDagRunOperator` stub or skip with note in summary).

---

## %%Variable token substitution

All `%%TOKEN%%` patterns in `CMDLINE`, `VARIABLE.VALUE`, `FileWatch-FILE_PATH` etc.:

| Control-M token | Jinja equivalent | Notes |
|---|---|---|
| `%%$ODATE` | `{{ ds_nodash }}` | order date YYYYMMDD |
| `%%$DATE` | `{{ ds }}` | YYYY-MM-DD |
| `%%$YEAR` / `%%YYYY` | `{{ execution_date.year }}` | |
| `%%MONTH` / `%%MM` | `{{ execution_date.month:02d }}` | |
| `%%DAY` / `%%DD` | `{{ execution_date.day:02d }}` | |
| `%%TIME` | `{{ execution_date.strftime('%H%M') }}` | HHMM |
| `%%HH` | `{{ execution_date.strftime('%H') }}` | |
| `%%JOBNAME` | `{{ task.task_id }}` | |
| `%%VARNAME%%` (double percent, user-defined) | `{{ var.value.VARNAME }}` | lookup in Airflow Variables |
| `%%$VARNAME` (single percent, system) | `{{ var.value.VARNAME }}` | |

URL-encoded characters in values (`%4E` = `\n`) must be decoded before writing to IR.

---

## Unmapped attributes → ManualReview triggers

The following always route a job to `JobPattern::ManualReview`:

| Attribute / condition | Reason |
|---|---|
| `CONFIRM=1` | requires human gate |
| `SHIFT` ≠ `Ignore Job` | date shifting logic |
| `DAYSCAL` / `CONFCAL` / `WEEKSCAL` present and non-empty | named calendar dependency |
| `CYCLIC_INTERVAL_SEQUENCE` or `CYCLIC_TIMES_SEQUENCE` present | complex repeat patterns |
| `IND_CYCLIC=I` | independent cyclic |
| `OUTCOND SIGN="-"` | condition deletion |
| `INCOND ODATE="STAT"` or `"****"` | static/wildcard date |
| `APPL_TYPE=BIM` | SLA checkpoint, no execution |
| `CONTROL` element present | exclusive resource lock |
| `ON` with output pattern (`STMT` ≠ `*`) | output parsing logic |
| `DOREMEDY` in ON block | ITSM integration |
| Unknown `APPL_TYPE` | unrecognized plugin |
| `extra` HashMap non-empty after known attrs stripped | truly unknown attributes |
