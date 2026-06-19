use std::collections::HashMap;
use std::path::Path;

use serde::Serialize;
use serde_json::Value;

use crate::classifier::JobPattern;
use crate::error::ParseError;
use crate::mapper::{build_callbacks, build_dag_config, build_job_variables, DagConfig};
use crate::model::{ControlMJob, InCondition, OutCondition};

#[derive(Debug, Serialize)]
pub struct JobIr {
    pub job_id: String,
    pub source_folder: String,
    pub datacenter: String,
    pub pattern: String,
    pub tasktype: String,
    pub appl_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dag_config: Option<DagConfig>,
    pub dependencies: Dependencies,
    pub callbacks: Value,
    pub variables: HashMap<String, String>,
    pub unmapped_attrs: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct Dependencies {
    pub upstream: Vec<UpstreamDep>,
    pub downstream: Vec<DownstreamDep>,
}

#[derive(Debug, Serialize)]
pub struct UpstreamDep {
    pub condition_name: String,
    pub odate: String,
    pub and_or: String,
    /// Set when AND_OR=O — downstream task needs TriggerRule.ONE_SUCCESS
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trigger_rule: Option<String>,
    /// Set when ODATE=PREV/NEXT — ExternalTaskSensor needs execution_delta
    #[serde(skip_serializing_if = "Option::is_none")]
    pub execution_delta_days: Option<i32>,
}

#[derive(Debug, Serialize)]
pub struct DownstreamDep {
    pub condition_name: String,
    pub odate: String,
    pub sign: String,
    /// True when ODATE=STAT — condition persists and never auto-clears
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub outcond_static: bool,
}

#[derive(Debug, Serialize)]
pub struct MigrationSummary {
    pub input_file: String,
    pub total: usize,
    pub auto_converted: usize,
    pub manual_review: usize,
    pub patterns: HashMap<String, usize>,
    pub manual_review_reasons: HashMap<String, usize>,
    pub folders_processed: usize,
    pub errors: Vec<String>,
}

pub fn build_ir(
    job: &ControlMJob,
    datacenter: &str,
    pattern: &JobPattern,
) -> JobIr {
    let dag_config = build_dag_config(job, pattern);
    let callbacks = build_callbacks(job);
    let variables = build_job_variables(job);
    let unmapped_attrs = collect_unmapped(job, pattern);

    JobIr {
        job_id: job.jobname.clone(),
        source_folder: job.parent_folder.clone(),
        datacenter: datacenter.to_string(),
        pattern: pattern.name().to_string(),
        tasktype: job.tasktype.clone(),
        appl_type: job.appl_type.clone(),
        dag_config,
        dependencies: build_dependencies(job),
        callbacks,
        variables,
        unmapped_attrs,
    }
}

fn build_dependencies(job: &ControlMJob) -> Dependencies {
    let upstream = job
        .incond
        .iter()
        .map(|c: &InCondition| {
            let trigger_rule = if c.and_or.eq_ignore_ascii_case("O") {
                Some("ONE_SUCCESS".to_string())
            } else {
                None
            };
            let execution_delta_days = match c.odate.to_ascii_uppercase().as_str() {
                "PREV" => Some(1),
                "NEXT" => Some(-1),
                _ => None,
            };
            UpstreamDep {
                condition_name: c.name.clone(),
                odate: c.odate.clone(),
                and_or: c.and_or.clone(),
                trigger_rule,
                execution_delta_days,
            }
        })
        .collect();

    let downstream = job
        .outcond
        .iter()
        .map(|c: &OutCondition| DownstreamDep {
            condition_name: c.name.clone(),
            odate: c.odate.clone(),
            sign: c.sign.clone(),
            outcond_static: c.odate.eq_ignore_ascii_case("STAT"),
        })
        .collect();

    Dependencies { upstream, downstream }
}

fn collect_unmapped(job: &ControlMJob, pattern: &JobPattern) -> Vec<String> {
    let mut attrs: Vec<String> = Vec::new();

    if job.dayscal.is_some() {
        attrs.push("DAYSCAL".into());
    }
    if job.confcal.is_some() {
        attrs.push("CONFCAL".into());
    }
    if job.weekscal.is_some() {
        attrs.push("WEEKSCAL".into());
    }

    let shift = job.shift.as_deref().unwrap_or("Ignore Job");
    if !shift.eq_ignore_ascii_case("ignore job") && !shift.starts_with('+') {
        attrs.push(format!("SHIFT:{}", shift));
    }

    for c in &job.controls {
        attrs.push(format!("CONTROL:{}", c.name));
    }

    if job.outcond.iter().any(|c| c.sign == "-") {
        attrs.push("OUTCOND_DELETE".into());
    }

    for on in &job.on_events {
        if on.has_output_pattern {
            attrs.push(format!("ON_PATTERN:{}", on.stmt));
        }
        if on.has_remedy {
            attrs.push("DOREMEDY".into());
        }
    }

    for k in job.extra.keys() {
        attrs.push(format!("ATTR:{}", k));
    }

    // For ManualReview, include the reason as an unmapped attr
    if let JobPattern::ManualReview { reason } = pattern {
        attrs.push(format!("MANUAL_REVIEW:{}", reason));
    }

    attrs
}

pub fn write_ir_json(ir: &JobIr, output_dir: &Path) -> Result<(), ParseError> {
    let safe_name = ir.job_id.replace(['/', '\\', ':'], "_");
    let path = output_dir.join(format!("job_{}.json", safe_name));
    let json = serde_json::to_string_pretty(ir)?;
    std::fs::write(path, json)?;
    Ok(())
}

pub fn write_ir_yaml(ir: &JobIr, output_dir: &Path) -> Result<(), ParseError> {
    let safe_name = ir.job_id.replace(['/', '\\', ':'], "_");
    let path = output_dir.join(format!("job_{}.yaml", safe_name));
    let yaml = serde_yaml::to_string(ir)?;
    std::fs::write(path, yaml)?;
    Ok(())
}

pub fn write_summary(
    summary: &MigrationSummary,
    output_dir: &Path,
) -> Result<(), ParseError> {
    let path = output_dir.join("migration_summary.json");
    let json = serde_json::to_string_pretty(summary)?;
    std::fs::write(path, json)?;
    Ok(())
}

pub fn build_summary(
    input_file: &str,
    irs: &[JobIr],
    folders_processed: usize,
    errors: Vec<String>,
) -> MigrationSummary {
    let total = irs.len();
    let manual_review = irs.iter().filter(|ir| ir.pattern == "ManualReview").count();
    let auto_converted = total - manual_review;

    let mut patterns: HashMap<String, usize> = HashMap::new();
    let mut manual_review_reasons: HashMap<String, usize> = HashMap::new();

    for ir in irs {
        *patterns.entry(ir.pattern.clone()).or_insert(0) += 1;

        if ir.pattern == "ManualReview" {
            for attr in &ir.unmapped_attrs {
                if let Some(reason) = attr.strip_prefix("MANUAL_REVIEW:") {
                    for r in reason.split(';') {
                        *manual_review_reasons.entry(r.to_string()).or_insert(0) += 1;
                    }
                }
            }
        }
    }

    MigrationSummary {
        input_file: input_file.to_string(),
        total,
        auto_converted,
        manual_review,
        patterns,
        manual_review_reasons,
        folders_processed,
        errors,
    }
}
