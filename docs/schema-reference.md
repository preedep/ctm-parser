# Control-M EM XML Schema Reference

Source: `ctmem_Folder.xsd` + verified against `export_xml_260612.xml` (30,304 jobs, 6,164 folders).

---

## Document structure

```
DEFTABLE
├── FOLDER          (SimpleFolder)   ← 6,163 in dataset
├── SMART_FOLDER    (SmartFolder)    ← 1 in dataset
├── SCHED_TABLE     (SimpleFolder)   ← alias
├── TABLE           (SimpleFolder)   ← alias
├── SMART_TABLE     (SmartTable)     ← alias for SmartFolder
└── SCHED_GROUP     (SmartTable)     ← alias
```

The export is flat: all folders live directly under `<DEFTABLE>`. `SMART_FOLDER` differs from `FOLDER` only in that it carries job-level default attributes on the folder element itself (inherited by child jobs) and allows `SUB_FOLDER` nesting.

---

## FOLDER / SimpleFolder

XML element: `<FOLDER>` (or `<SCHED_TABLE>` / `<TABLE>`)

| Attribute | Type | Notes |
|---|---|---|
| `FOLDER_NAME` | string | primary key for a folder |
| `TABLE_NAME` | string | alias used in older exports |
| `DATACENTER` | string | Control-M server / agent host |
| `PLATFORM` | string | `UNIX`, `Windows`, etc. |
| `VERSION` | string | CM EM version number |
| `LAST_UPLOAD` | string | `YYYYMMDDHHMMSStz` |
| `FOLDER_ORDER_METHOD` | string | ordering rule |
| `TABLE_USERDAILY` | string | |
| `REAL_FOLDER_ID` | int | internal DB id |
| `REAL_TABLEID` | int | |
| `TYPE` | int | |
| `USED_BY` / `USED_BY_CODE` | string/int | |
| `ENFORCE_VALIDATION` | `N`\|`Y` | |
| `SITE_STANDARD_NAME` | string | |
| `MODIFIED` | emBoolean | |
| `FOLDER_DSN` / `TABLE_DSN` | string | |

Children: `<JOB>` (0..*), `<ADDITIONAL_FOLDER_DETAILS>` (FolderClientData, 0..*)

### SMART_FOLDER / SmartFolder

Same folder attributes plus all `JobData` attributes as folder-level defaults (APPLICATION, RUN_AS, NODEID, scheduling etc.) plus:

| Extra attribute | Notes |
|---|---|
| `REMOVEATONCE` | |
| `DAYSKEEPINNOTOK` (int) | |

Additional children: `<SUB_FOLDER>` (recursive), all `JobData` child elements at folder scope.

---

## JOB / JobData

XML element: `<JOB>` (child of any folder type)

### Identity

| Attribute | Type | Notes |
|---|---|---|
| `JOBNAME` | string | **primary key** — unique within folder |
| `JOBISN` | int | internal sequence number |
| `MEMNAME` | string | script/member name |
| `APPLICATION` | string | application group |
| `SUB_APPLICATION` | string | sub-application |
| `GROUP` | string | job group |
| `DESCRIPTION` | string | free-text description |
| `PARENT_FOLDER` | string | owning folder name |
| `PARENT_TABLE` | string | owning table (legacy) |
| `END_FOLDER` | string | |

### Execution

| Attribute | Type | Notes |
|---|---|---|
| `TASKTYPE` | string | **`Job`**, **`Command`**, **`Dummy`**, `SMART Table` |
| `APPL_TYPE` | string | plugin type: `OS`, `FILE_TRANS`, `FileWatch`, `AWS`, `BIM`, `AIRFLOWV2` |
| `APPL_FORM` | string | plugin form: `AFT`, `File Watcher`, `AWS`, `CONTROL-M BIM`, `AIRFLOWV2` |
| `APPL_VER` | string | plugin version |
| `CMDLINE` | string | command to execute (OS jobs) |
| `NODEID` | string | agent hostname to run on |
| `RUN_AS` | string | OS user |
| `OWNER` | string | CM owner |
| `PRIORITY` | string | `AA`, `A`..`Z` |
| `CRITICAL` | string | `0`/`1` |
| `CONFIRM` | string | `0`/`1` — require manual confirmation |
| `MEMLIB` / `OVERLIB` / `OVERRIDE_PATH` | string | script library paths |
| `DOCLIB` / `DOCMEM` | string | documentation reference |
| `SYSDB` | string | `0`/`1` |
| `MULTY_AGENT` | string | `N`/`Y` |
| `SYSTEM_AFFINITY` | string | |
| `CM_VER` | string | CM server version |
| `INSTREAM_JCL` / `USE_INSTREAM_JCL` | string | JCL inline (mainframe) |

### Scheduling — time window

| Attribute | Type | Notes |
|---|---|---|
| `TIMEFROM` | string | `HHMM` — earliest start time |
| `TIMETO` | string | `HHMM` or `>` (open-ended) |
| `FROM` | string | alias for TIMEFROM in some versions |
| `DUE_OUT` | string | expected completion time |
| `FROM_DAYSOFFSET` / `TO_DAYSOFFSET` / `DUE_OUT_DAYSOFFSET` | string | day offset modifiers |
| `TIMEZONE` | string | IANA timezone |
| `ACTIVE_FROM` / `ACTIVE_TILL` | string | `YYYYMMDD` validity range |
| `ODATE` | string | original order date override |
| `SCHEDULING_ENVIRONMENT` | string | |

### Scheduling — day/month selection

| Attribute | Notes |
|---|---|
| `DAYS` | day-of-month mask (`ALL`, `1,15`, etc.) |
| `WEEKDAYS` | weekday mask (`ALL`, `1-5`, etc.) |
| `DAYS_AND_OR` | `A`(AND) / `O`(OR) — combine DAYS with WEEKDAYS |
| `JAN`…`DEC` | per-month flag `0`/`1` — all 12 present |
| `DATE` | specific calendar date |
| `DAYSCAL` / `WEEKSCAL` | named calendar references |
| `CONFCAL` | confirmation calendar |
| `SHIFT` | `Ignore Job`, `Next`, `Prev` |
| `SHIFTNUM` | `+00`, `+01` etc. |
| `RETRO` | `0`/`1` — catchup on missed runs |
| `STAT_CAL` | statistical calendar |
| `PREV_DAY` | |
| `ADJUST_COND` | |
| `RULE_BASED_CALENDAR_RELATIONSHIP` | `O`(OR) / `A`(AND) |
| `TAG_RELATIONSHIP` | `O`(OR) / `A`(AND) |

### Scheduling — cyclic / repeat

| Attribute | Type | Notes |
|---|---|---|
| `CYCLIC` | string | `0`/`1` — enables repeat |
| `INTERVAL` | string | `00015M` format (minutes) |
| `IND_CYCLIC` | string | `S`(sequential) / `I`(independent) |
| `CYCLIC_TYPE` | string | `C`(cyclic) |
| `CYCLIC_TOLERANCE` | int | tolerance in minutes |
| `CYCLIC_INTERVAL_SEQUENCE` | string | comma-separated intervals |
| `CYCLIC_TIMES_SEQUENCE` | string | comma-separated run times |

Real example (15-minute cyclic job):
```
CYCLIC="1" INTERVAL="00015M" CYCLIC_TYPE="C" IND_CYCLIC="S" CYCLIC_TOLERANCE="0"
```

### Execution control

| Attribute | Type | Notes |
|---|---|---|
| `MAXWAIT` | int | max wait time in minutes |
| `MAXRERUN` | int | max automatic rerun attempts |
| `MAXDAYS` | int | max days to keep in active network |
| `MAXRUNS` | int | max concurrent instances |
| `AUTOARCH` | string | auto-archive flag |
| `RERUNMEM` | string | script to use on rerun |
| `RETEN_DAYS` / `RETEN_GEN` | string | output retention |
| `REQUEST_NJE_NODE` | string | |
| `TASK_CLASS` / `CATEGORY` | string | |
| `LARGE_SIZE` / `PREVENTNCT2` | string | |
| `OPTION` / `PAR` / `MINIMUM` | string | platform-specific |
| `PDSNAME` | string | PDS name (mainframe) |
| `JOBS_IN_GROUP` | string | |
| `FPROCS` / `TPGMS` / `TPROCS` | string | step range (mainframe) |

### Audit / versioning

| Attribute | Notes |
|---|---|
| `CREATED_BY` / `AUTHOR` | |
| `CREATION_USER` / `CREATION_DATE` / `CREATION_TIME` | |
| `CHANGE_USERID` / `CHANGE_DATE` / `CHANGE_TIME` | |
| `JOB_VERSION` | |
| `VERSION_OPCODE` / `IS_CURRENT_VERSION` / `VERSION_SERIAL` / `VERSION_HOST` | versioning system |

---

## JOB child elements

### INCOND — upstream dependency (prerequisite)

```xml
<INCOND AND_OR="A" NAME="RT_JOB001-ENDED-OK" ODATE="ODAT"/>
```

| Attribute | Values | Notes |
|---|---|---|
| `NAME` | string | condition token (shared with upstream OUTCOND) |
| `ODATE` | `ODAT`, `PREV`, `NEXT`, `STAT`, `****` | date qualifier — `ODAT` = same day (96.5% of data) |
| `AND_OR` | `A`(AND) / `O`(OR) | combine multiple INCONDs |
| `OP` | string | operation modifier |

Dataset: 23,306 jobs have at least one INCOND (76.9%). ODATE distribution: `ODAT`=32,747, `PREV`=196, `NEXT`=31, `STAT`=14, `****`=6.

### OUTCOND — downstream condition signal

```xml
<OUTCOND NAME="RT_JOB001-ENDED-OK" ODATE="ODAT" SIGN="+"/>
```

| Attribute | Values | Notes |
|---|---|---|
| `NAME` | string | condition token posted to the network |
| `ODATE` | `ODAT`, `PREV`, `NEXT` | date qualifier |
| `SIGN` | `+` (add) / `-` (delete) | `+` = post condition (99.9% of data) |

Dataset: 30,163 jobs have at least one OUTCOND (99.5%).

### VARIABLE / AUTOEDIT2 — job variables

```xml
<VARIABLE NAME="%%FileWatch-FILE_PATH" VALUE="/data/input/*.csv"/>
<AUTOEDIT2 NAME="%%MY_VAR" VALUE="some_value"/>
```

Both use `NAME` + `VALUE`. Plugin jobs encode all their configuration as `VARIABLE` elements with namespaced names (`%%FTP-*`, `%%FileWatch-*`, `%%AWS-*`, `%%UCM-*`, `%%BIM-*`).

Dataset: 21,100 jobs have VARIABLE/AUTOEDIT2 elements (69.6%).

### AUTOEDIT — legacy variable (old syntax)

```xml
<AUTOEDIT EXP="%%VAR=value"/>
```

Single `EXP` attribute containing `%%NAME=VALUE` expression.

### SHOUT — alert / notification

```xml
<SHOUT WHEN="EXECTIME" TIME=">060" URGENCY="V" DEST="EM"
       MESSAGE="%%JOBNAME execution > 60 min"/>
```

| Attribute | Values | Notes |
|---|---|---|
| `WHEN` | `EXECTIME`, `LATETIME`, `LATESUB`, `NOTOK`, `OK` | trigger event |
| `TIME` | `>NNN` | threshold in minutes |
| `URGENCY` | `R`(regular), `U`(urgent), `V`(very urgent) | |
| `DEST` | `EM`, email address | destination |
| `MESSAGE` | string | may contain `%%JOBNAME` etc. |
| `DAYSOFFSET` | string | |

Dataset: 497 jobs have SHOUT (1.6%). WHEN distribution: `EXECTIME`=313, `LATETIME`=221, `LATESUB`=80.

### QUANTITATIVE — resource pool (semaphore)

```xml
<QUANTITATIVE NAME="AM_LOAD_1" QUANT="1"/>
```

| Attribute | Notes |
|---|---|
| `NAME` | pool/resource name → maps to Airflow `pool` (snake_case) |
| `QUANT` (int) | slots to acquire |
| `ONFAIL` / `ONOK` | resource action on job state |

Dataset: 2,276 jobs use QUANTITATIVE (7.5%).

### CONTROL — exclusive/shared resource lock

```xml
<CONTROL NAME="RESOURCE_X" TYPE="E" ONFAIL="CONT"/>
```

| Attribute | Values | Notes |
|---|---|---|
| `NAME` | string | resource name |
| `TYPE` | `E`(exclusive) / `S`(shared) | |
| `ONFAIL` | `CONT`, `STOP` | |

### ON — event handler

```xml
<ON CODE="*failed*" STMT="*">
    <DOACTION ACTION="NOTOK"/>
</ON>
<ON CODE="*success*" STMT="*">
    <DOACTION ACTION="OK"/>
</ON>
```

| Attribute | Notes |
|---|---|
| `CODE` | exit code / pattern to match (`*`, `*failed*`, `*success*`, `*INCOMPLETED*`, `Job:Complete`, specific code) |
| `STMT` | output statement match |
| `PATTERN` | regex pattern |
| `AND_OR` | combine conditions |
| `FROM_COLUMN` / `TO_COLUMN` | column range for pattern match |

Dataset: 25,183 jobs have ON blocks (83.1%).

ON child actions:

| Element | Key attributes | Airflow equivalent |
|---|---|---|
| `DOACTION` | `ACTION`: `OK`, `NOTOK`, `RERUN` | task state |
| `DOCOND` | `NAME`, `ODATE`, `SIGN` | post condition |
| `DOFORCEJOB` | `NAME`, `TABLE_NAME`, `ODATE` | trigger downstream DAG |
| `DOMAIL` | `DEST`, `SUBJECT`, `MESSAGE` | email callback |
| `DOSHOUT` | `URGENCY`, `MESSAGE`, `DEST` | alert callback |
| `DOOUTPUT` | `OPTION`, `PAR` | sysout action |
| `DOVARIABLE` / `DOAUTOEDIT2` | `NAME`, `VALUE` | set variable |
| `DOIFRERUN` | `CONFIRM`, step range | conditional rerun |
| `DOREMEDY` | `URGENCY`, `DESCRIPTION`, `SUMMARY` | ITSM ticket |

### STEP_RANGE — step/proc range (mainframe only)

```xml
<STEP_RANGE NAME="S1" FPGMS="PGM1" TPGMS="PGM2"/>
```

### RULE_BASED_CALENDAR / TAG — per-job calendar override

Has a full copy of the scheduling attributes (DAYS, months, CONFCAL etc.) scoped to this tag only.

### CAPTURE — output variable capture

```xml
<CAPTURE SEARCH_STRING="Total:" SKIP_COLUMNS="7" COUNT_TO_TAKE="1" VARIABLE="%%TOTAL"/>
```

---

## Plugin types (APPL_TYPE)

| APPL_TYPE | APPL_FORM | TASKTYPE | Count | Description |
|---|---|---|---|---|
| `OS` | (none) | `Command`/`Job` | 15,239 | Standard OS command / shell script |
| `FILE_TRANS` | `AFT` | `Job`/`Command`/`Dummy` | 13,946 | Advanced File Transfer (FTP/SFTP) |
| `AWS` | `AWS` | `Job`/`Dummy` | 604 | AWS Step Functions / Lambda / Batch |
| `FileWatch` | `File Watcher` | `Job`/`Dummy` | 144 | File arrival detection |
| `BIM` | `CONTROL-M BIM` | `Dummy` | 68 | Business flow SLA tracking |
| `AIRFLOWV2` | `AIRFLOWV2` | `Job` | 51 | Trigger an existing Airflow DAG |
| (none) | (none) | `Command`/`Job`/`Dummy` | 252 | Legacy / no plugin |

### Plugin variable namespaces

Each plugin encodes its config as `VARIABLE` child elements:

| Plugin | Variable prefix | Key variables |
|---|---|---|
| `FILE_TRANS` | `%%FTP-` | `ACCOUNT`, `LHOST`, `RHOST`, `LPATH1`, `RPATH1`, `CONNTYPE1/2`, `TRANSFER_NUM`, `TYPE1..5` |
| `FileWatch` | `%%FileWatch-` | `FILE_PATH`, `MODE` (`CREATE`/`SIZE`), `TIME_LIMIT`, `INT_FILE_SEARCHES`, `MIN_DET_SIZE`, `NUM_OF_ITERATIONS` |
| `AWS` | `%%AWS-` | `SERVICE_TYPE` (`STEP`/`LAMBDA`/`BATCH`), `ACCOUNT`, `REGION`, `STEP_NAME`, `LAMBDA_NAME`, `BATCH_JOB_DEFINITION` |
| `BIM` | `%%BIM-` | `SERVICE_NAME`, `SERVICE_PRIORITY`, `DUE_TIME`, `SENSITIVITY`, `EVENTS` |
| `AIRFLOWV2` | `%%UCM-` | `APP_NAME`, `DAGID`, `ACCOUNT`, `RUNDATE`, `RUNMODE`, `GETTASKS` |

---

## TASKTYPE semantics

| TASKTYPE | Count | Meaning |
|---|---|---|
| `Job` | 17,726 | Full job definition — has script/command execution |
| `Command` | 12,078 | Inline command via `CMDLINE` attribute |
| `Dummy` | 500 | No execution — used for dependency aggregation, BIM SLA tracking, or plugin stubs |
| `SMART Table` | 1 | Smart folder / table job definition |

`Dummy` jobs with `APPL_TYPE=BIM` are SLA checkpoints. `Dummy` jobs with `APPL_TYPE=FileWatch`/`FILE_TRANS`/`AWS` use `VARIABLE` children to configure the plugin (the `TASKTYPE=Dummy` means the scheduling agent itself does not launch a process; the plugin agent handles execution).

---

## Condition naming convention

The dataset uses a consistent pattern:

```
{JOBNAME}-ENDED-OK     ← success condition (posted via OUTCOND SIGN="+")
{JOBNAME}-ENDED-NOTOK  ← failure condition (less common)
```

Cross-folder dependencies reference conditions from jobs in other folders via `INCOND` with `ODATE="ODAT"`.

---

## emBoolean type

Used in `MODIFIED`, `INSTRUCTION_READ`, `COMMENT_READ`:

```
"0" | "1" | "True" | "true" | "False" | "false"
```

---

## Dataset statistics (export_xml_260612.xml)

| Metric | Value |
|---|---|
| Export date | 2026-06-12 20:00:06 UTC |
| Server | neutron |
| Total FOLDER elements | 6,163 |
| Total SMART_FOLDER elements | 1 |
| Total JOB elements | 30,304 |
| Jobs with INCOND | 23,306 (76.9%) |
| Jobs with OUTCOND | 30,163 (99.5%) |
| Jobs with VARIABLE/AUTOEDIT2 | 21,100 (69.6%) |
| Jobs with ON blocks | 25,183 (83.1%) |
| Jobs with SHOUT | 497 (1.6%) |
| Jobs with QUANTITATIVE | 2,276 (7.5%) |
| CYCLIC=1 jobs | 3,602 (11.9%) |
