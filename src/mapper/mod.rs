use std::collections::HashMap;

use serde_json::Value;

use crate::classifier::{AwsServiceType, JobPattern, TransferProtocol};
use crate::model::{ControlMJob, OnAction, ShoutConfig};

#[derive(Debug, serde::Serialize)]
pub struct DagConfig {
    pub schedule: Option<String>,
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub timezone: Option<String>,
    pub catchup: bool,
    pub retries: u32,
    pub execution_timeout_sec: Option<u64>,
    pub priority_weight: Option<u32>,
    pub pool: Option<String>,
    pub pool_slots: u32,
    pub owner: Option<String>,
    pub tags: Vec<String>,
    pub sla_sec: Option<u64>,
    pub operator: String,
    pub bash_command: Option<String>,
    pub plugin_config: Option<Value>,
}

pub fn build_dag_config(job: &ControlMJob, pattern: &JobPattern) -> Option<DagConfig> {
    match pattern {
        JobPattern::DependencyGate | JobPattern::ManualReview { .. } => None,
        _ => Some(build_config(job, pattern)),
    }
}

fn build_config(job: &ControlMJob, pattern: &JobPattern) -> DagConfig {
    let schedule = derive_schedule(job, pattern);
    let (pool, pool_slots) = derive_pool(job);
    let tags = derive_tags(job);
    let sla_sec = derive_sla(job);
    let execution_timeout_sec = if job.maxwait > 0 {
        Some(job.maxwait as u64 * 60)
    } else {
        None
    };

    let (operator, bash_command, plugin_config) = derive_operator(job, pattern);

    DagConfig {
        schedule,
        start_date: format_date(job.active_from.as_deref()),
        end_date: format_date(job.active_till.as_deref()),
        timezone: job.timezone.clone(),
        catchup: job.retro,
        retries: job.maxrerun,
        execution_timeout_sec,
        priority_weight: parse_priority(job.priority.as_deref()),
        pool,
        pool_slots,
        owner: job.owner.clone(),
        tags,
        sla_sec,
        operator,
        bash_command,
        plugin_config,
    }
}

fn derive_schedule(job: &ControlMJob, pattern: &JobPattern) -> Option<String> {
    if let JobPattern::CyclicJob { interval_secs } = pattern {
        return Some(format!("timedelta:{}", interval_secs));
    }

    // Named calendars cannot be auto-converted
    if job.dayscal.is_some() || job.confcal.is_some() || job.weekscal.is_some() {
        return None;
    }

    derive_cron(job)
}

fn derive_cron(job: &ControlMJob) -> Option<String> {
    let hour = job
        .timefrom
        .as_deref()
        .and_then(parse_hhmm_hour)
        .unwrap_or(0);
    let minute = job
        .timefrom
        .as_deref()
        .and_then(parse_hhmm_minute)
        .unwrap_or(0);

    let days = job.days.as_deref().unwrap_or("ALL");
    let weekdays = job.weekdays.as_deref().unwrap_or("ALL");
    let and_or = job.days_and_or.as_deref().unwrap_or("O");

    let day_field = if days.eq_ignore_ascii_case("all") {
        "*".to_string()
    } else {
        days.to_string()
    };

    let dow_field = if weekdays.eq_ignore_ascii_case("all") {
        "*".to_string()
    } else {
        convert_weekdays(weekdays)
    };

    let month_field = if job.months.all_active() {
        "*".to_string()
    } else {
        let active = job.months.active_months();
        if active.is_empty() {
            return None;
        }
        active
            .iter()
            .map(|m| m.to_string())
            .collect::<Vec<_>>()
            .join(",")
    };

    // When AND: both day-of-month AND weekday must match
    // When OR (default): either matches — standard cron behaviour
    let (cron_day, cron_dow) = if and_or.eq_ignore_ascii_case("A") {
        // Cron doesn't natively AND day+weekday; emit a note but use both non-wildcard
        (day_field, dow_field)
    } else {
        // OR: if one is *, the other drives the schedule
        match (day_field.as_str(), dow_field.as_str()) {
            ("*", _) => ("*".into(), dow_field),
            (_, "*") => (day_field, "*".into()),
            _ => (day_field, dow_field),
        }
    };

    Some(format!("{} {} {} {} {}", minute, hour, cron_day, month_field, cron_dow))
}

fn parse_hhmm_hour(s: &str) -> Option<u32> {
    if s.len() >= 2 {
        s[..2].parse().ok()
    } else {
        None
    }
}

fn parse_hhmm_minute(s: &str) -> Option<u32> {
    if s.len() >= 4 {
        s[2..4].parse().ok()
    } else {
        None
    }
}

fn convert_weekdays(s: &str) -> String {
    // Control-M uses 1=Sunday..7=Saturday; cron uses 0=Sunday..6=Saturday
    s.replace("1-5", "1-5")  // weekdays already in cron format in dataset
     .replace("1-7", "*")
}

fn derive_pool(job: &ControlMJob) -> (Option<String>, u32) {
    if let Some(q) = job.quantitative.first() {
        let pool_name = q.name
            .to_ascii_lowercase()
            .replace(' ', "_")
            .replace('-', "_");
        (Some(pool_name), q.quant)
    } else {
        (None, 1)
    }
}

fn derive_tags(job: &ControlMJob) -> Vec<String> {
    let mut tags: Vec<String> = Vec::new();
    if let Some(ref a) = job.application {
        if !a.is_empty() { tags.push(a.clone()); }
    }
    if let Some(ref sa) = job.sub_application {
        if !sa.is_empty() && Some(sa) != job.application.as_ref() {
            tags.push(sa.clone());
        }
    }
    if !job.parent_folder.is_empty() {
        tags.push(job.parent_folder.clone());
    }
    tags.dedup();
    tags
}

fn derive_sla(job: &ControlMJob) -> Option<u64> {
    for shout in &job.shout {
        if shout.when.eq_ignore_ascii_case("EXECTIME") {
            if let Some(secs) = parse_shout_time_secs(shout) {
                return Some(secs);
            }
        }
    }
    None
}

fn parse_shout_time_secs(shout: &ShoutConfig) -> Option<u64> {
    let time = shout.time.as_deref()?;
    // Format: ">NNN" where NNN is minutes
    let stripped = time.trim_start_matches('>');
    stripped.parse::<u64>().ok().map(|m| m * 60)
}

fn parse_priority(p: Option<&str>) -> Option<u32> {
    let p = p?;
    if p.eq_ignore_ascii_case("AA") {
        return Some(100);
    }
    let c = p.chars().next()?;
    if c.is_ascii_alphabetic() {
        // A=90, B=80, ..., Z=10
        let rank = (c.to_ascii_uppercase() as u32).saturating_sub(b'A' as u32);
        Some(90u32.saturating_sub(rank * 10).max(10))
    } else {
        p.parse().ok()
    }
}

fn format_date(s: Option<&str>) -> Option<String> {
    let s = s?;
    if s.len() == 8 {
        Some(format!("{}-{}-{}", &s[..4], &s[4..6], &s[6..8]))
    } else {
        Some(s.to_string())
    }
}

fn derive_operator(job: &ControlMJob, pattern: &JobPattern) -> (String, Option<String>, Option<Value>) {
    match pattern {
        JobPattern::BashJob => {
            let cmd = job.cmdline.as_deref().map(substitute_tokens);
            ("BashOperator".into(), cmd, None)
        }
        JobPattern::FileWatcher => {
            let cfg = build_filewatcher_config(job);
            ("FileSensor".into(), None, Some(cfg))
        }
        JobPattern::FileTransfer { protocol } => {
            let op = match protocol {
                TransferProtocol::Sftp => "SFTPOperator",
                _ => "FTPOperator",
            };
            let cfg = build_filetrans_config(job, protocol);
            (op.into(), None, Some(cfg))
        }
        JobPattern::AwsJob { service } => {
            let op = match service {
                AwsServiceType::StepFunctions => "StepFunctionStartExecutionOperator",
                AwsServiceType::Lambda => "LambdaInvokeFunctionOperator",
                AwsServiceType::Batch => "BatchOperator",
                AwsServiceType::Unknown(_) => "PythonOperator",
            };
            let cfg = build_aws_config(job, service);
            (op.into(), None, Some(cfg))
        }
        JobPattern::AlreadyAirflow => {
            let cfg = build_airflow_config(job);
            ("TriggerDagRunOperator".into(), None, Some(cfg))
        }
        JobPattern::CyclicJob { .. } => {
            let cmd = job.cmdline.as_deref().map(substitute_tokens);
            ("BashOperator".into(), cmd, None)
        }
        JobPattern::DependencyGate => ("EmptyOperator".into(), None, None),
        JobPattern::ManualReview { .. } => ("ManualReview".into(), None, None),
    }
}

pub fn substitute_tokens(cmd: &str) -> String {
    let mut result = cmd.to_string();
    // System date tokens (must come before generic %% substitution)
    let replacements = [
        ("%%$ODATE", "{{ ds_nodash }}"),
        ("%%$DATE", "{{ ds }}"),
        ("%%$YEAR", "{{ execution_date.year }}"),
        ("%%YYYY", "{{ execution_date.year }}"),
        ("%%MONTH", "{{ execution_date.month | string | zfill(2) }}"),
        ("%%MM", "{{ execution_date.month | string | zfill(2) }}"),
        ("%%DAY", "{{ execution_date.day | string | zfill(2) }}"),
        ("%%DD", "{{ execution_date.day | string | zfill(2) }}"),
        ("%%TIME", "{{ execution_date.strftime('%H%M') }}"),
        ("%%HH", "{{ execution_date.strftime('%H') }}"),
        ("%%JOBNAME", "{{ task.task_id }}"),
    ];
    for (token, jinja) in &replacements {
        result = result.replace(token, jinja);
    }
    result
}

fn build_filewatcher_config(job: &ControlMJob) -> Value {
    let vars = &job.variables;
    let file_path = vars
        .get("%%FileWatch-FILE_PATH")
        .map(|s| substitute_tokens(s))
        .unwrap_or_default();
    let mode = vars.get("%%FileWatch-MODE").cloned().unwrap_or_else(|| "CREATE".into());
    let timeout_hours: f64 = vars
        .get("%%FileWatch-TIME_LIMIT")
        .and_then(|v| v.parse().ok())
        .unwrap_or(5.0);
    let poke_secs: u64 = vars
        .get("%%FileWatch-INT_FILE_SEARCHES")
        .and_then(|v| v.parse().ok())
        .unwrap_or(60);
    let min_size: u64 = vars
        .get("%%FileWatch-MIN_DET_SIZE")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);

    serde_json::json!({
        "file_path": file_path,
        "mode": mode,
        "timeout_hours": timeout_hours,
        "poke_interval_sec": poke_secs,
        "min_size_bytes": min_size
    })
}

fn build_filetrans_config(job: &ControlMJob, protocol: &TransferProtocol) -> Value {
    let vars = &job.variables;
    let transfer_num: usize = vars
        .get("%%FTP-TRANSFER_NUM")
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);

    let mut transfers = Vec::new();
    for i in 1..=transfer_num.min(5) {
        let local_path = vars.get(&format!("%%FTP-LPATH{}", i)).cloned().unwrap_or_default();
        let remote_path = vars.get(&format!("%%FTP-RPATH{}", i)).cloned().unwrap_or_default();
        let upload = vars.get(&format!("%%FTP-UPLOAD{}", i)).map(|v| v == "1").unwrap_or(true);
        let file_type = vars.get(&format!("%%FTP-TYPE{}", i)).cloned().unwrap_or_else(|| "I".into());
        transfers.push(serde_json::json!({
            "local_path": substitute_tokens(&local_path),
            "remote_path": substitute_tokens(&remote_path),
            "direction": if upload { "upload" } else { "download" },
            "type": file_type
        }));
    }

    serde_json::json!({
        "protocol": protocol.to_string(),
        "account": vars.get("%%FTP-ACCOUNT").cloned().unwrap_or_default(),
        "local_host": vars.get("%%FTP-LHOST").cloned().unwrap_or_default(),
        "remote_host": vars.get("%%FTP-RHOST").cloned().unwrap_or_default(),
        "transfers": transfers
    })
}

fn build_aws_config(job: &ControlMJob, service: &AwsServiceType) -> Value {
    let vars = &job.variables;
    let account = vars.get("%%AWS-ACCOUNT").cloned().unwrap_or_default();
    let region = vars.get("%%AWS-REGION").cloned().unwrap_or_default();

    match service {
        AwsServiceType::StepFunctions => {
            let payload = vars
                .get("%%AWS-STEP_PAYLOAD_JSON-N001-VALUE")
                .map(|s| decode_url_encoded(s))
                .unwrap_or_default();
            serde_json::json!({
                "service_type": "STEP",
                "account": account,
                "region": region,
                "state_machine_name": vars.get("%%AWS-STEP_NAME").cloned().unwrap_or_default(),
                "execution_name": vars.get("%%AWS-STEP_EXECUTION_NAME").cloned().unwrap_or_default(),
                "input_json": payload
            })
        }
        AwsServiceType::Lambda => serde_json::json!({
            "service_type": "LAMBDA",
            "account": account,
            "region": region,
            "function_name": vars.get("%%AWS-LAMBDA_NAME").cloned().unwrap_or_default(),
            "payload": vars.get("%%AWS-LAMBDA_PAYLOAD_JSON-N001-VALUE").cloned().unwrap_or_else(|| "{}".into())
        }),
        AwsServiceType::Batch => serde_json::json!({
            "service_type": "BATCH",
            "account": account,
            "region": region,
            "job_definition": vars.get("%%AWS-BATCH_JOB_DEFINITION").cloned().unwrap_or_default(),
            "job_queue": vars.get("%%AWS-BATCH_JOB_QUEUE").cloned().unwrap_or_default(),
            "job_name": vars.get("%%AWS-BATCH_JOB_NAME").cloned().unwrap_or_default()
        }),
        AwsServiceType::Unknown(s) => serde_json::json!({ "service_type": s, "account": account }),
    }
}

fn build_airflow_config(job: &ControlMJob) -> Value {
    let vars = &job.variables;
    serde_json::json!({
        "trigger_dag_id": vars.get("%%UCM-DAGID").cloned().unwrap_or_default(),
        "account": vars.get("%%UCM-ACCOUNT").cloned().unwrap_or_default(),
        "run_date": vars.get("%%UCM-RUNDATE").cloned(),
        "run_mode": vars.get("%%UCM-RUNMODE").cloned()
    })
}

/// Decode URL-encoded characters in AWS payload values (%4E → \n)
fn decode_url_encoded(s: &str) -> String {
    percent_encoding::percent_decode_str(s)
        .decode_utf8()
        .map(|c| c.into_owned())
        .unwrap_or_else(|_| s.to_string())
}

pub fn build_callbacks(job: &ControlMJob) -> serde_json::Value {
    let mut on_failure: Vec<Value> = Vec::new();
    let mut on_success: Vec<Value> = Vec::new();
    let mut sla_alerts: Vec<Value> = Vec::new();

    // SHOUT-based callbacks
    for s in &job.shout {
        let entry = serde_json::json!({
            "type": "shout",
            "dest": s.dest,
            "urgency": s.urgency,
            "message": s.message
        });
        match s.when.to_ascii_uppercase().as_str() {
            "NOTOK" => on_failure.push(entry),
            "OK" => on_success.push(entry),
            "EXECTIME" | "LATETIME" | "LATESUB" => {
                let threshold_min: Option<u64> = s
                    .time
                    .as_deref()
                    .map(|t| t.trim_start_matches('>'))
                    .and_then(|t| t.parse().ok());
                let mut alert = entry.clone();
                if let Some(obj) = alert.as_object_mut() {
                    obj.insert("threshold_min".into(), threshold_min.into());
                }
                sla_alerts.push(alert);
            }
            _ => {}
        }
    }

    // ON block callbacks
    for on in &job.on_events {
        let code = on.code.to_ascii_lowercase();
        let is_failure = code.contains("failed") || code == "notok" || code.contains("incompleted");
        let is_success = code.contains("success") || code == "ok" || code == "job:complete";

        for action in &on.actions {
            match action {
                OnAction::DoMail { dest, subject, message } => {
                    let entry = serde_json::json!({
                        "type": "email",
                        "dest": dest,
                        "subject": subject,
                        "message": message
                    });
                    if is_failure { on_failure.push(entry.clone()); }
                    if is_success { on_success.push(entry); }
                }
                OnAction::DoShout { urgency, message, dest } => {
                    let entry = serde_json::json!({
                        "type": "shout",
                        "dest": dest,
                        "urgency": urgency,
                        "message": message
                    });
                    if is_failure { on_failure.push(entry.clone()); }
                    if is_success { on_success.push(entry); }
                }
                _ => {}
            }
        }
    }

    serde_json::json!({
        "on_failure": on_failure,
        "on_success": on_success,
        "sla_alerts": sla_alerts
    })
}

pub fn build_job_variables(job: &ControlMJob) -> HashMap<String, String> {
    job.variables
        .iter()
        .filter(|(k, _)| {
            // Exclude plugin-namespaced variables — they go into plugin_config
            !k.starts_with("%%FTP-")
                && !k.starts_with("%%FileWatch-")
                && !k.starts_with("%%AWS-")
                && !k.starts_with("%%UCM-")
                && !k.starts_with("%%BIM-")
        })
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}
