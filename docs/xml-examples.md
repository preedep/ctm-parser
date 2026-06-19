# Control-M XML Examples

Illustrative examples based on the dataset structure. All usernames, hostnames, file paths, and system identifiers have been replaced with generic placeholders.

---

## Document root

```xml
<?xml version='1.0' encoding='UTF-8' ?>
<DEFTABLE xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
          xsi:noNamespaceSchemaLocation="Folder.xsd">

    <FOLDER DATACENTER="neutron" FOLDER_NAME="MY_FOLDER"
            LAST_UPLOAD="20160329074251UTC" PLATFORM="UNIX"
            REAL_FOLDER_ID="0" TYPE="1" VERSION="919">
        <JOB .../>
        <JOB .../>
    </FOLDER>

    <SMART_FOLDER DATACENTER="neutron" FOLDER_NAME="SMART_EXAMPLE"
                  APPLICATION="MY_APP" RUN_AS="ctmsrv" NODEID="neutron">
        <SUB_FOLDER JOBNAME="GROUP_A" ...>
            <JOB .../>
        </SUB_FOLDER>
    </SMART_FOLDER>

</DEFTABLE>
```

---

## OS / BashJob (TASKTYPE=Command, APPL_TYPE=OS)

```xml
<JOB APPLICATION="CLEAR_CONDITION"
     APPL_TYPE="OS"
     APR="1" AUG="1" DEC="1" FEB="1" JAN="1" JUL="1" JUN="1"
     MAR="1" MAY="1" NOV="1" OCT="1" SEP="1"
     AUTOARCH="0"
     CMDLINE="ctmcontb -DELETEFROM *ENDED-OK* 0404 0410"
     CONFIRM="0"
     CREATED_BY="controlm"
     CREATION_DATE="20160329" CREATION_TIME="144234" CREATION_USER="svcuser"
     CRITICAL="0"
     CYCLIC="0" CYCLIC_TOLERANCE="0" CYCLIC_TYPE="C"
     DAYS="ALL" DAYS_AND_OR="A"
     IND_CYCLIC="S" INTERVAL="00000M"
     JOBISN="3" JOBNAME="Clear_Condition"
     MAXDAYS="0" MAXRERUN="0" MAXRUNS="0" MAXWAIT="0"
     MULTY_AGENT="N"
     NODEID="neutron"
     PARENT_FOLDER="CLEAR_CONDITION"
     PRIORITY="AA"
     RETRO="0" RULE_BASED_CALENDAR_RELATIONSHIP="O"
     RUN_AS="ctmsrv"
     SHIFT="Ignore Job" SHIFTNUM="+00"
     SUB_APPLICATION="CLEAR_CONDITION"
     SYSDB="0"
     TASKTYPE="Command"
     USE_INSTREAM_JCL="N"
     WEEKDAYS="ALL">
    <OUTCOND NAME="CLEAR_CONDITION-ENDED-OK" ODATE="ODAT" SIGN="+"/>
</JOB>
```

**Classification:** `BashJob`
**Key fields:** CMDLINE → `BashOperator.bash_command`, DAYS/WEEKDAYS → cron, OUTCOND → dependency graph.

---

## OS / BashJob with INCOND, VARIABLE, and SHOUT

```xml
<JOB APPLICATION="EDW_DP"
     APPL_TYPE="OS"
     CMDLINE="/batch/edw/run_export.sh %%$ODATE %%YYYY %%MM %%DD"
     JOBNAME="RT_EDW_EXPORT_D01"
     MAXWAIT="90"
     MAXRERUN="2"
     PARENT_FOLDER="EDW_DAILY"
     RETRO="0"
     TASKTYPE="Job"
     WEEKDAYS="ALL" DAYS="ALL">
    <INCOND AND_OR="A" NAME="RT_EDW_LOAD_D99-ENDED-OK" ODATE="ODAT"/>
    <OUTCOND NAME="RT_EDW_EXPORT_D01-ENDED-OK" ODATE="ODAT" SIGN="+"/>
    <VARIABLE NAME="%%OUT_DIR" VALUE="/data/edw/export/%%$ODATE"/>
    <SHOUT WHEN="EXECTIME" TIME=">060"
           URGENCY="V" DEST="EM"
           MESSAGE="RT_EDW_EXPORT_D01 execution greater than 60 minutes"/>
    <SHOUT WHEN="EXECTIME" TIME=">090"
           URGENCY="V" DEST="EM"
           MESSAGE="Call team EDW: RT_EDW_EXPORT_D01 greater than 90 minutes"/>
    <ON CODE="*" STMT="*">
        <DOACTION ACTION="OK"/>
    </ON>
</JOB>
```

**Classification:** `BashJob`
**Key fields:** MAXWAIT=90 → `execution_timeout_sec=5400`, MAXRERUN=2 → `retries=2`, SHOUT EXECTIME → `sla_sec`, INCOND → `ExternalTaskSensor`.

---

## Cyclic job (CYCLIC=1, INTERVAL=15 min)

```xml
<JOB APPLICATION="MONITOR"
     APPL_TYPE="OS"
     CMDLINE="/batch/monitor/check_queue.sh"
     CYCLIC="1"
     CYCLIC_TOLERANCE="0" CYCLIC_TYPE="C"
     IND_CYCLIC="S"
     INTERVAL="00015M"
     JOBNAME="RT_QUEUE_MONITOR"
     PARENT_FOLDER="MONITOR_FOLDER"
     TASKTYPE="Command"
     WEEKDAYS="ALL" DAYS="ALL">
    <OUTCOND NAME="RT_QUEUE_MONITOR-ENDED-OK" ODATE="ODAT" SIGN="+"/>
</JOB>
```

**Classification:** `CyclicJob { interval_secs: 900 }`
**Key fields:** `dag.schedule = timedelta(seconds=900)`, `dag.catchup = False`.

---

## FileWatch job (APPL_TYPE=FileWatch)

```xml
<JOB APPLICATION="EDW_BI"
     APPL_FORM="File Watcher"
     APPL_TYPE="FileWatch"
     APPL_VER="W2K/XP"
     DAYSCAL="Calendar"
     JOBNAME="BI_D_WATCHER_005"
     MEMNAME="BI_EDW_EXT_OPG_PMS_CREDIT_CARD"
     MEMLIB="Not in use for File Watcher jobs"
     NODEID="agent01"
     PARENT_FOLDER="EDW_BI_WATCHER"
     RUN_AS="svcaccount"
     TASKTYPE="Job"
     TIMEFROM="1930">
    <OUTCOND NAME="BI_D_WATCHER_005-ENDED-OK" ODATE="ODAT" SIGN="+"/>
    <VARIABLE NAME="%%FileWatch-FILE_PATH"
              VALUE="\\fileserver\data\EDW\LOADS\FILE_D%%$ODATE..CTL"/>
    <VARIABLE NAME="%%FileWatch-MIN_DET_SIZE"     VALUE="0"/>
    <VARIABLE NAME="%%FileWatch-INT_FILE_SEARCHES" VALUE="60"/>
    <VARIABLE NAME="%%FileWatch-INT_FILESIZE_COMPARISON" VALUE="10"/>
    <VARIABLE NAME="%%FileWatch-NUM_OF_ITERATIONS" VALUE="3"/>
    <VARIABLE NAME="%%FileWatch-TIME_LIMIT"        VALUE="5"/>
    <VARIABLE NAME="%%FileWatch-START_TIME"        VALUE="NOW"/>
    <VARIABLE NAME="%%FileWatch-STOP_TIME"         VALUE="0"/>
    <VARIABLE NAME="%%FileWatch-MIN_AGE"           VALUE="NO_MIN_AGE"/>
    <VARIABLE NAME="%%FileWatch-MAX_AGE"           VALUE="NO_MAX_AGE"/>
    <VARIABLE NAME="%%FileWatch-PATH"              VALUE="Not in use for File Watcher jobs"/>
    <VARIABLE NAME="%%FileWatch-MODE"              VALUE="CREATE"/>
    <VARIABLE NAME="%%FileWatch-FILESIZE_WILDCARD" VALUE="N"/>
</JOB>
```

**Classification:** `FileWatcher`
**Note:** `DAYSCAL="Calendar"` → add `"named_calendar:DAYSCAL"` to `unmapped_attrs`.
**plugin_config:** `file_path` with `%%$ODATE` → `{{ ds_nodash }}`, `mode=CREATE`, `timeout_hours=5`, `poke_interval_sec=60`.

---

## FILE_TRANS / FTP job (APPL_TYPE=FILE_TRANS)

```xml
<JOB APPLICATION="APP_EXPINV_DAILY"
     APPL_FORM="AFT"
     APPL_TYPE="FILE_TRANS"
     APPL_VER="6.1.01"
     DESCRIPTION="FTP File *.DAT INV To EDW"
     JOBNAME="RT_EXPINVAFT1"
     MAXWAIT="3"
     NODEID="agent01"
     PARENT_FOLDER="EXPINV_DAILY_TABLE"
     RUN_AS="svc_ftp_user"
     TASKTYPE="Job"
     DAYS="ALL" DAYS_AND_OR="O">
    <INCOND AND_OR="A" NAME="RT_EXPINV0002-ENDED-OK" ODATE="ODAT"/>
    <OUTCOND NAME="RT_EXPINVAFT1-ENDED-OK" ODATE="ODAT" SIGN="+"/>
    <VARIABLE NAME="%%FTP-ACCOUNT"      VALUE="FTP_CONN_01"/>
    <VARIABLE NAME="%%FTP-LOSTYPE"      VALUE="Unix"/>
    <VARIABLE NAME="%%FTP-LUSER"        VALUE="svc_local"/>
    <VARIABLE NAME="%%FTP-ROSTYPE"      VALUE="Windows"/>
    <VARIABLE NAME="%%FTP-RUSER"        VALUE="svc_remote"/>
    <VARIABLE NAME="%%FTP-LPATH1"       VALUE="/data/export/*.DAT"/>
    <VARIABLE NAME="%%FTP-RPATH1"       VALUE=".\"/>
    <VARIABLE NAME="%%FTP-CONNTYPE1"    VALUE="LOCAL"/>
    <VARIABLE NAME="%%FTP-CONNTYPE2"    VALUE="FTP"/>
    <VARIABLE NAME="%%FTP-LHOST"        VALUE="agent01"/>
    <VARIABLE NAME="%%FTP-RHOST"        VALUE="remote.transfer.host"/>
    <VARIABLE NAME="%%FTP-LPASSIVE"     VALUE="0"/>
    <VARIABLE NAME="%%FTP-RPASSIVE"     VALUE="0"/>
    <VARIABLE NAME="%%FTP-UPLOAD1"      VALUE="1"/>
    <VARIABLE NAME="%%FTP-TRANSFER_NUM" VALUE="1"/>
    <VARIABLE NAME="%%FTP-TYPE1"        VALUE="A"/>
    <VARIABLE NAME="%%FTP-MINSIZE1"     VALUE="0"/>
    <VARIABLE NAME="%%FTP-TIMELIMIT1"   VALUE="0"/>
    <VARIABLE NAME="%%FTP-RPF"          VALUE="1"/>
    <VARIABLE NAME="%%FTP-USE_DEF_NUMRETRIES" VALUE="1"/>
    <!-- ... more transfer pairs for indices 2-5 ... -->
</JOB>
```

**Classification:** `FileTransfer { protocol: Ftp }`
**plugin_config:** 1 transfer pair: `LOCAL:/backup_db2/EDW/data/*.DAT` → `FTP:./` on `remote.transfer.host`.

---

## AWS Step Functions job (APPL_TYPE=AWS)

```xml
<JOB APPLICATION="AFT_NSS"
     APPL_FORM="AWS"
     APPL_TYPE="AWS"
     APPL_VER="9.0.19.100"
     CMDLINE="/NSS/bin/BatchAlertTrigger.sh /NSS/data/LEADS/file-%%$YEAR.-%%MONTH.-%%DAY..txt"
     JOBNAME="RT_AFT_NSS_LEAD_UNS_AWS_D05"
     MAXWAIT="10"
     NODEID="agent02"
     PARENT_FOLDER="AFT_NSS_LEADS_DAILY"
     RUN_AS="svc_batch"
     TASKTYPE="Job"
     DAYS="ALL">
    <INCOND AND_OR="A" NAME="RT_AFT_NSS_LEAD_UNS_AWS_D01-ENDED-OK" ODATE="ODAT"/>
    <OUTCOND NAME="RT_AFT_NSS_LEAD_UNS_AWS_D05-ENDED-OK" ODATE="ODAT" SIGN="+"/>
    <VARIABLE NAME="%%AWS-SERVICE_TYPE"        VALUE="STEP"/>
    <VARIABLE NAME="%%AWS-STEP_NAME"           VALUE="my-step-function-batch-import-prod"/>
    <VARIABLE NAME="%%AWS-STEP_EXECUTION_NAME" VALUE="batch-import-prod"/>
    <VARIABLE NAME="%%AWS-STEP_PAYLOAD_TYPE"   VALUE="JSON"/>
    <VARIABLE NAME="%%AWS-STEP_PAYLOAD_JSON-N001-VALUE"
              VALUE='{"filename":"/data/file-%%$YEAR.-%%MONTH.-%%DAY..txt",%4E"skipheader":"false"}'/>
    <VARIABLE NAME="%%AWS-ACCOUNT"             VALUE="AWS_CONN_01"/>
    <VARIABLE NAME="%%AWS-APPEND_LOG"          VALUE="Y"/>
    <VARIABLE NAME="%%AWS-LAMBDA_PAYLOAD_TYPE" VALUE="JSON"/>
    <VARIABLE NAME="%%AWS-BATCH_JOB_TYPE"      VALUE="SINGLE"/>
    <!-- ... other AWS-BATCH_* defaults ... -->
    <ON CODE="*success*" STMT="*"><DOACTION ACTION="OK"/></ON>
    <ON CODE="*failed*"  STMT="*"><DOACTION ACTION="NOTOK"/></ON>
</JOB>
```

**Classification:** `AwsJob { service: StepFunctions }`
**Note:** `%4E` in payload value = URL-encoded newline `\n` — decode before writing to IR.

---

## BIM job (APPL_TYPE=BIM) — ManualReview

```xml
<JOB APPLICATION="APP_MSS"
     APPL_FORM="CONTROL-M BIM"
     APPL_TYPE="BIM"
     APPL_VER="1"
     JOBNAME="RT_MSSD999999"
     MEMNAME="BIM"
     NODEID="agent03"
     PARENT_FOLDER="MSS_DAILY"
     RUN_AS="ctmagent"
     TASKTYPE="Dummy"
     WEEKDAYS="ALL">
    <INCOND AND_OR="O" NAME="RT_MSSD000081-ENDED-OK" ODATE="ODAT"/>
    <INCOND AND_OR="O" NAME="RT_MSSW000081-ENDED-OK" ODATE="ODAT"/>
    <OUTCOND NAME="RT_MSSD999999-ENDED-OK" ODATE="ODAT" SIGN="+"/>
    <VARIABLE NAME="%%BIM-SERVICE_NAME"      VALUE="MSS__GROUP"/>
    <VARIABLE NAME="%%BIM-SERVICE_PRIORITY"  VALUE="3"/>
    <VARIABLE NAME="%%BIM-DUE_TIME"          VALUE="08:00,2"/>
    <VARIABLE NAME="%%BIM-SENSITIVITY"       VALUE="10%"/>
</JOB>
```

**Classification:** `ManualReview { reason: "bim_sla_checkpoint" }`

---

## AIRFLOWV2 job (APPL_TYPE=AIRFLOWV2) — AlreadyAirflow

```xml
<JOB APPLICATION="APP_CLOUD_CRM"
     APPL_FORM="AIRFLOWV2"
     APPL_TYPE="AIRFLOWV2"
     JOBNAME="RT_CLOUD_CRM_D0040"
     NODEID="agent02-az"
     PARENT_FOLDER="APP_CLOUD_CRM"
     RUN_AS="svc_airflow"
     TASKTYPE="Job"
     DAYS="ALL">
    <INCOND AND_OR="A" NAME="RT_CLOUD_CRM_D0030-ENDED-OK" ODATE="ODAT"/>
    <OUTCOND NAME="RT_CLOUD_CRM_D0040-ENDED-OK" ODATE="ODAT" SIGN="+"/>
    <VARIABLE NAME="%%UCM-APP_NAME"   VALUE="AIRFLOWV2"/>
    <VARIABLE NAME="%%UCM-DAGID"      VALUE="my-cloud-crm-batch-prod"/>
    <VARIABLE NAME="%%UCM-GETTASKS"   VALUE="unchecked"/>
    <VARIABLE NAME="%%UCM-ACCOUNT"    VALUE="AIRFLOW_CONN_01"/>
    <VARIABLE NAME="%%HH"             VALUE="%%SUBSTR %%TIME 1 4"/>
    <ON CODE="*INCOMPLETED*" STMT="*"/>
    <ON CODE="*target file does not exist in the path*" STMT="*"/>
    <ON CODE="*" STMT="*"/>
</JOB>
```

**Classification:** `AlreadyAirflow`

---

## Dependency gate (TASKTYPE=Dummy, no execution)

```xml
<JOB APPLICATION="MSS_DAILY"
     APPL_TYPE="OS"
     JOBNAME="RT_MSSD_GATE_ALL"
     PARENT_FOLDER="MSS_DAILY"
     TASKTYPE="Dummy"
     WEEKDAYS="ALL">
    <INCOND AND_OR="A" NAME="RT_MSSD000001-ENDED-OK" ODATE="ODAT"/>
    <INCOND AND_OR="A" NAME="RT_MSSD000002-ENDED-OK" ODATE="ODAT"/>
    <INCOND AND_OR="A" NAME="RT_MSSD000003-ENDED-OK" ODATE="ODAT"/>
    <OUTCOND NAME="RT_MSSD_GATE_ALL-ENDED-OK" ODATE="ODAT" SIGN="+"/>
</JOB>
```

**Classification:** `DependencyGate`
**Airflow equivalent:** `EmptyOperator` (or `PythonOperator` with `pass`) with `trigger_rule=ALL_SUCCESS`.

---

## QUANTITATIVE resource pool

```xml
<JOB JOBNAME="RT_AM_LOAD_BATCH_01"
     APPL_TYPE="OS"
     CMDLINE="/batch/am_load.sh"
     TASKTYPE="Command"
     PARENT_FOLDER="AM_LOAD_FOLDER">
    <INCOND AND_OR="A" NAME="RT_AM_LOAD_PRE-ENDED-OK" ODATE="ODAT"/>
    <OUTCOND NAME="RT_AM_LOAD_BATCH_01-ENDED-OK" ODATE="ODAT" SIGN="+"/>
    <QUANTITATIVE NAME="AM_LOAD_1" QUANT="1"/>
</JOB>
```

**Classification:** `BashJob`
**dag_config:** `pool="am_load_1"`, `pool_slots=1`

---

## INCOND with ODATE=PREV (cross-day dependency)

```xml
<INCOND AND_OR="A" NAME="RT_EOD_JOB-ENDED-OK" ODATE="PREV"/>
```

**Meaning:** depends on yesterday's run of `RT_EOD_JOB`.
**Airflow:** `ExternalTaskSensor(execution_delta=timedelta(days=1))`.

---

## ON block variants

```xml
<!-- Standard success/failure handlers -->
<ON CODE="*success*" STMT="*"><DOACTION ACTION="OK"/></ON>
<ON CODE="*failed*"  STMT="*"><DOACTION ACTION="NOTOK"/></ON>

<!-- Force another job on failure -->
<ON CODE="NOTOK" STMT="*">
    <DOFORCEJOB TABLE_NAME="RECOVERY_FOLDER" NAME="RT_RECOVERY_JOB" ODATE="ODAT"/>
</ON>

<!-- Send mail on failure -->
<ON CODE="NOTOK" STMT="*">
    <DOMAIL URGENCY="U" DEST="ops-team@example.com"
            SUBJECT="%%JOBNAME FAILED"
            MESSAGE="Job %%JOBNAME ended NOTOK at %%TIME"/>
</ON>

<!-- Specific exit code handling -->
<ON CODE="Job:Complete" STMT="*"><DOACTION ACTION="OK"/></ON>

<!-- Pattern match in output (→ ManualReview) -->
<ON CODE="*" STMT="ERROR: file not found">
    <DOACTION ACTION="NOTOK"/>
</ON>
```
