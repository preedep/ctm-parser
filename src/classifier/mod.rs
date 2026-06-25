use tracing::warn;

use crate::model::ControlMJob;

#[derive(Debug, Clone, PartialEq)]
pub enum TransferProtocol {
    Ftp,
    FtpSsl,
    Sftp,
    Local,
    /// FTP-CONNTYPE2=S3 — transfer to/from AWS S3 via `aws s3 cp` on agent node
    S3,
    /// FTP-CONNTYPE2=AZURE or BLOB — transfer to/from Azure Blob Storage via `azcopy` on agent node
    Azure,
    Unknown(String),
}

impl std::fmt::Display for TransferProtocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ftp => write!(f, "FTP"),
            Self::FtpSsl => write!(f, "FTP-SSL"),
            Self::Sftp => write!(f, "SFTP"),
            Self::Local => write!(f, "LOCAL"),
            Self::S3 => write!(f, "S3"),
            Self::Azure => write!(f, "AZURE"),
            Self::Unknown(s) => write!(f, "{}", s),
        }
    }
}

/// How a FileWatch job locates its target file.
#[derive(Debug, Clone, PartialEq)]
pub enum FileWatchMode {
    /// File lives on the agent node's local filesystem — poll via SSHOperator/PsrpOperator
    Local,
    /// File on an SFTP server — use SFTPSensor (Airflow worker connects directly)
    Sftp,
    /// File on an FTP server — use FTPSensor (covers plain FTP and FTP-SSL via FTPHook TLS)
    Ftp,
    /// File in AWS S3 — use S3KeySensor
    S3,
    /// File in Azure Blob Storage — use WasbBlobSensor
    Blob,
}

impl std::fmt::Display for FileWatchMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Local => write!(f, "LOCAL"),
            Self::Sftp  => write!(f, "SFTP"),
            Self::Ftp   => write!(f, "FTP"),
            Self::S3    => write!(f, "S3"),
            Self::Blob  => write!(f, "BLOB"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum AwsServiceType {
    StepFunctions,
    Lambda,
    Batch,
    Unknown(String),
}

impl std::fmt::Display for AwsServiceType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::StepFunctions => write!(f, "STEP"),
            Self::Lambda => write!(f, "LAMBDA"),
            Self::Batch => write!(f, "BATCH"),
            Self::Unknown(s) => write!(f, "{}", s),
        }
    }
}

#[derive(Debug, Clone)]
pub enum JobPattern {
    BashJob,
    FileTransfer { protocol: TransferProtocol },
    FileWatcher { mode: FileWatchMode },
    AwsJob { service: AwsServiceType },
    CyclicJob { interval_secs: u64 },
    AlreadyAirflow,
    DependencyGate,
    ManualReview { reason: String },
}

impl JobPattern {
    pub fn name(&self) -> &'static str {
        match self {
            Self::BashJob => "BashJob",
            Self::FileTransfer { .. } => "FileTransfer",
            Self::FileWatcher { .. } => "FileWatcher",
            Self::AwsJob { .. } => "AwsJob",
            Self::CyclicJob { .. } => "CyclicJob",
            Self::AlreadyAirflow => "AlreadyAirflow",
            Self::DependencyGate => "DependencyGate",
            Self::ManualReview { .. } => "ManualReview",
        }
    }

    pub fn is_auto_converted(&self) -> bool {
        !matches!(self, Self::ManualReview { .. })
    }

    pub fn filewatch_mode(&self) -> Option<&FileWatchMode> {
        if let Self::FileWatcher { mode } = self { Some(mode) } else { None }
    }
}

pub fn classify_job(job: &ControlMJob) -> JobPattern {
    let appl_type = job.appl_type.to_ascii_lowercase();
    let tasktype = job.tasktype.to_ascii_lowercase();

    // CyclicJob takes highest precedence — affects DAG schedule fundamentally
    if job.cyclic {
        let has_complex_sequence = job.cyclic_interval_sequence.is_some()
            || job.cyclic_times_sequence.is_some();
        let is_independent = job.ind_cyclic.as_deref().map(|s| s.eq_ignore_ascii_case("I")).unwrap_or(false);

        if has_complex_sequence || is_independent {
            let reason = if has_complex_sequence {
                "complex_cyclic_sequence".to_string()
            } else {
                "independent_cyclic".to_string()
            };
            warn!(job = %job.jobname, reason = %reason, "ManualReview: cyclic complexity");
            return JobPattern::ManualReview { reason };
        }

        match parse_interval(job.interval.as_deref()) {
            Ok(secs) => return JobPattern::CyclicJob { interval_secs: secs },
            Err(reason) => {
                warn!(job = %job.jobname, reason = %reason, "ManualReview: invalid interval");
                return JobPattern::ManualReview { reason };
            }
        }
    }

    // Plugin-specific classification
    match appl_type.as_str() {
        "airflowv2" => return JobPattern::AlreadyAirflow,
        "bim" => {
            warn!(job = %job.jobname, "ManualReview: bim_sla_checkpoint");
            return JobPattern::ManualReview { reason: "bim_sla_checkpoint".into() };
        }
        "filewatch" => {
            let mode = derive_filewatch_mode(job);
            return JobPattern::FileWatcher { mode };
        }
        "file_trans" => {
            let protocol = derive_transfer_protocol(job);
            if let TransferProtocol::Unknown(ref p) = protocol {
                let reason = format!("unknown_transfer_protocol:{}", p);
                warn!(job = %job.jobname, reason = %reason, "ManualReview");
                return JobPattern::ManualReview { reason };
            }
            return JobPattern::FileTransfer { protocol };
        }
        "aws" => {
            let service = derive_aws_service(job);
            if let AwsServiceType::Unknown(ref s) = service {
                let reason = format!("unknown_aws_service:{}", s);
                warn!(job = %job.jobname, reason = %reason, "ManualReview");
                return JobPattern::ManualReview { reason };
            }
            return JobPattern::AwsJob { service };
        }
        _ => {}
    }

    // Unknown plugin
    if !appl_type.is_empty() && appl_type != "os" {
        let reason = format!("unrecognized_appl_type:{}", job.appl_type);
        warn!(job = %job.jobname, reason = %reason, "ManualReview");
        return JobPattern::ManualReview { reason };
    }

    // DependencyGate: Dummy with no command and no plugin
    if tasktype == "dummy" && job.cmdline.is_none() {
        return JobPattern::DependencyGate;
    }

    // OS / Command jobs — check ManualReview triggers
    let reasons = collect_manual_review_reasons(job);
    if !reasons.is_empty() {
        let reason = reasons.join(";");
        warn!(job = %job.jobname, reason = %reason, "ManualReview: trigger(s) found");
        return JobPattern::ManualReview { reason };
    }

    JobPattern::BashJob
}

fn collect_manual_review_reasons(job: &ControlMJob) -> Vec<String> {
    let mut reasons = Vec::new();

    if job.confirm {
        reasons.push("confirm_required".into());
    }

    let shift = job.shift.as_deref().unwrap_or("Ignore Job");
    if !shift.eq_ignore_ascii_case("ignore job") && !shift.starts_with('+') {
        reasons.push("date_shift".into());
    }

    if job.dayscal.is_some() {
        reasons.push("named_calendar:DAYSCAL".into());
    }
    if job.confcal.is_some() {
        reasons.push("named_calendar:CONFCAL".into());
    }
    if job.weekscal.is_some() {
        reasons.push("named_calendar:WEEKSCAL".into());
    }

    if job.cyclic_interval_sequence.is_some() {
        reasons.push("complex_cyclic_sequence".into());
    }
    if job.cyclic_times_sequence.is_some() {
        reasons.push("complex_cyclic_times".into());
    }

    if job.ind_cyclic.as_deref().map(|s| s.eq_ignore_ascii_case("I")).unwrap_or(false) {
        reasons.push("independent_cyclic".into());
    }

    if job.outcond.iter().any(|c| c.sign == "-") {
        reasons.push("outcond_delete".into());
    }

    if job.incond.iter().any(|c| c.odate.eq_ignore_ascii_case("STAT")) {
        reasons.push("incond_static_condition".into());
    }
    if job.incond.iter().any(|c| c.odate == "****") {
        reasons.push("incond_wildcard_date".into());
    }
    if job.incond.iter().any(|c| c.odate.eq_ignore_ascii_case("NEXT")) {
        reasons.push("incond_next_date".into());
    }

    if !job.controls.is_empty() {
        reasons.push("exclusive_resource_lock".into());
    }

    if job.on_events.iter().any(|on| on.has_output_pattern) {
        reasons.push("output_pattern_match".into());
    }

    if job.on_events.iter().any(|on| on.has_remedy) {
        reasons.push("itsm_remedy_action".into());
    }

    if !job.extra.is_empty() {
        let keys: Vec<_> = job.extra.keys().cloned().collect();
        reasons.push(format!("unknown_attrs:{}", keys.join(",")));
    }

    reasons
}

fn derive_transfer_protocol(job: &ControlMJob) -> TransferProtocol {
    let conntype = job
        .variables
        .get("%%FTP-CONNTYPE2")
        .map(|s| s.to_ascii_uppercase());

    match conntype.as_deref() {
        Some("FTP")             => TransferProtocol::Ftp,
        Some("FTP-SSL")         => TransferProtocol::FtpSsl,
        Some("SFTP")            => TransferProtocol::Sftp,
        Some("LOCAL")           => TransferProtocol::Local,
        Some("S3")              => TransferProtocol::S3,
        Some("AZURE") | Some("BLOB") => TransferProtocol::Azure,
        Some(other)             => TransferProtocol::Unknown(other.to_string()),
        None => {
            let conn1 = job.variables.get("%%FTP-CONNTYPE1").map(|s| s.to_ascii_uppercase());
            match conn1.as_deref() {
                Some("FTP")             => TransferProtocol::Ftp,
                Some("FTP-SSL")         => TransferProtocol::FtpSsl,
                Some("SFTP")            => TransferProtocol::Sftp,
                Some("LOCAL")           => TransferProtocol::Local,
                Some("S3")              => TransferProtocol::S3,
                Some("AZURE") | Some("BLOB") => TransferProtocol::Azure,
                _                       => TransferProtocol::Ftp,
            }
        }
    }
}

fn derive_filewatch_mode(job: &ControlMJob) -> FileWatchMode {
    let vars = &job.variables;

    // Check explicit connection type variable
    let conntype = vars
        .get("%%FileWatch-CONNTYPE")
        .or_else(|| vars.get("%%FileWatch-CONNECTION_TYPE"))
        .map(|s| s.to_ascii_uppercase());

    if let Some(ref ct) = conntype {
        match ct.as_str() {
            "SFTP"              => return FileWatchMode::Sftp,
            "FTP" | "FTP-SSL"  => return FileWatchMode::Ftp,
            "S3"               => return FileWatchMode::S3,
            "BLOB" | "AZURE"   => return FileWatchMode::Blob,
            "LOCAL"            => return FileWatchMode::Local,
            _ => {}
        }
    }

    // Infer from file path prefix
    let path = vars
        .get("%%FileWatch-FILE_PATH")
        .map(|s| s.to_ascii_lowercase())
        .unwrap_or_default();

    if path.starts_with("sftp://")       { return FileWatchMode::Sftp; }
    if path.starts_with("ftp://")
    || path.starts_with("ftps://")       { return FileWatchMode::Ftp; }
    if path.starts_with("s3://")         { return FileWatchMode::S3; }
    if path.contains(".blob.core")       { return FileWatchMode::Blob; }

    // Default: local file on agent node
    FileWatchMode::Local
}

fn derive_aws_service(job: &ControlMJob) -> AwsServiceType {
    let service = job
        .variables
        .get("%%AWS-SERVICE_TYPE")
        .map(|s| s.to_ascii_uppercase());

    match service.as_deref() {
        Some("STEP") => AwsServiceType::StepFunctions,
        Some("LAMBDA") => AwsServiceType::Lambda,
        Some("BATCH") => AwsServiceType::Batch,
        Some(other) => AwsServiceType::Unknown(other.to_string()),
        None => AwsServiceType::Unknown("(missing)".into()),
    }
}

/// Parse Control-M INTERVAL format: `NNNNNu` where u ∈ {M, H, D}
pub fn parse_interval(interval: Option<&str>) -> Result<u64, String> {
    let raw = match interval {
        Some(s) if !s.is_empty() => s,
        _ => return Err("invalid_interval:(missing)".into()),
    };

    let (digits, unit) = raw.split_at(raw.len().saturating_sub(1));
    let n: u64 = digits
        .parse()
        .map_err(|_| format!("invalid_interval:{}", raw))?;

    let secs = match unit.to_ascii_uppercase().as_str() {
        "M" => n * 60,
        "H" => n * 3600,
        "D" => n * 86400,
        _ => return Err(format!("invalid_interval:{}", raw)),
    };

    Ok(secs)
}
