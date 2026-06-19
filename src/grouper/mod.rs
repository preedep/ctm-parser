use std::collections::{HashMap, HashSet};
use std::path::Path;

use serde::Serialize;

use crate::error::ParseError;
use crate::ir::JobIr;

#[derive(Debug, Serialize)]
pub struct DagGroup {
    pub dag_id: String,
    pub datacenter: String,
    pub schedule: Option<String>,
    pub timezone: Option<String>,
    pub jobs: Vec<JobSummary>,
    pub edges: Vec<Edge>,
    pub external_sensors: Vec<ExternalSensor>,
}

/// A standalone job — no intra-folder edges and no external sensors touching it.
/// Maps to a single-task DAG in Airflow.
#[derive(Debug, Serialize)]
pub struct DagSingle {
    pub dag_id: String,
    pub source_folder: String,
    pub datacenter: String,
    pub schedule: Option<String>,
    pub timezone: Option<String>,
    pub job: JobSummary,
}

#[derive(Debug, Serialize)]
pub struct JobSummary {
    pub job_id: String,
    pub pattern: String,
    pub operator: String,
}

#[derive(Debug, Serialize)]
pub struct Edge {
    pub from: String,
    pub to: String,
    pub condition: String,
    /// Set when any upstream condition for `to` has AND_OR=O
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trigger_rule: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ExternalSensor {
    pub in_job: String,
    pub condition: String,
    pub odate: String,
    pub source_folder: Option<String>,
    pub source_job: Option<String>,
    /// Set when ODATE=PREV/NEXT — number of days offset for execution_delta
    #[serde(skip_serializing_if = "Option::is_none")]
    pub execution_delta_days: Option<i32>,
    /// Set when AND_OR=O for this condition
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trigger_rule: Option<String>,
}

pub struct GroupResult {
    /// Folders with intra-folder edges only — fully self-contained DAGs
    pub groups: Vec<DagGroup>,
    /// Folders with at least one ExternalTaskSensor (cross-folder dependency)
    pub groups_external: Vec<DagGroup>,
    /// Isolated jobs with no edges and no sensors — each becomes a 1-task DAG
    pub singles: Vec<DagSingle>,
}

/// Three-pass grouper:
///
/// Pass 1 — build global OUTCOND index: condition_name → (folder, job_id)
/// Pass 2 — build cross-folder producer set: job_ids that are consumed by a job
///           in a DIFFERENT folder (they must not be treated as isolated)
/// Pass 3 — per folder, resolve each INCOND; split jobs into:
///   - connected (has edge, external sensor, or is a cross-folder producer) → DagGroup
///   - isolated (truly no connections anywhere) → DagSingle
pub fn build_dag_groups(irs: &[JobIr]) -> GroupResult {
    // Pass 1 — global OUTCOND index (only "+" sign conditions create dependencies)
    let mut cond_index: HashMap<&str, (&str, &str)> = HashMap::new();
    for ir in irs {
        for dep in &ir.dependencies.downstream {
            if dep.sign == "+" {
                cond_index
                    .entry(dep.condition_name.as_str())
                    .or_insert((ir.source_folder.as_str(), ir.job_id.as_str()));
            }
        }
    }

    // Pass 2 — find jobs that are cross-folder producers
    // A job is a cross-folder producer if its OUTCOND condition is consumed by a job
    // in a different folder. These must land in dag_groups_external, not dag_singles.
    let mut cross_folder_producers: HashSet<&str> = HashSet::new();
    for ir in irs {
        for incond in &ir.dependencies.upstream {
            if let Some((src_folder, src_job)) = cond_index.get(incond.condition_name.as_str()) {
                if *src_folder != ir.source_folder.as_str() {
                    cross_folder_producers.insert(src_job);
                }
            }
        }
    }

    // Pass 3 — group by folder
    let mut folder_map: HashMap<&str, Vec<&JobIr>> = HashMap::new();
    for ir in irs {
        folder_map.entry(ir.source_folder.as_str()).or_default().push(ir);
    }

    let mut groups: Vec<DagGroup> = Vec::new();
    let mut groups_external: Vec<DagGroup> = Vec::new();
    let mut singles: Vec<DagSingle> = Vec::new();

    for (folder, jobs) in folder_map {
        split_folder(folder, &jobs, &cond_index, &cross_folder_producers, &mut groups, &mut groups_external, &mut singles);
    }

    groups.sort_by(|a, b| a.dag_id.cmp(&b.dag_id));
    groups_external.sort_by(|a, b| a.dag_id.cmp(&b.dag_id));
    singles.sort_by(|a, b| a.dag_id.cmp(&b.dag_id));

    GroupResult { groups, groups_external, singles }
}

fn split_folder(
    folder: &str,
    jobs: &[&JobIr],
    cond_index: &HashMap<&str, (&str, &str)>,
    cross_folder_producers: &HashSet<&str>,
    groups: &mut Vec<DagGroup>,
    groups_external: &mut Vec<DagGroup>,
    singles: &mut Vec<DagSingle>,
) {
    let datacenter = jobs.first().map(|j| j.datacenter.as_str()).unwrap_or("");

    // Resolve all edges and sensors for this folder
    let mut edges: Vec<Edge> = Vec::new();
    let mut external_sensors: Vec<ExternalSensor> = Vec::new();

    for job in jobs {
        for incond in &job.dependencies.upstream {
            let cond = incond.condition_name.as_str();
            match cond_index.get(cond) {
                Some((src_folder, src_job)) if *src_folder == folder => {
                    edges.push(Edge {
                        from: src_job.to_string(),
                        to: job.job_id.clone(),
                        condition: cond.to_string(),
                        trigger_rule: incond.trigger_rule.clone(),
                    });
                }
                Some((src_folder, src_job)) => {
                    external_sensors.push(ExternalSensor {
                        in_job: job.job_id.clone(),
                        condition: cond.to_string(),
                        odate: incond.odate.clone(),
                        source_folder: Some(src_folder.to_string()),
                        source_job: Some(src_job.to_string()),
                        execution_delta_days: incond.execution_delta_days,
                        trigger_rule: incond.trigger_rule.clone(),
                    });
                }
                None => {
                    external_sensors.push(ExternalSensor {
                        in_job: job.job_id.clone(),
                        condition: cond.to_string(),
                        odate: incond.odate.clone(),
                        source_folder: None,
                        source_job: None,
                        execution_delta_days: incond.execution_delta_days,
                        trigger_rule: incond.trigger_rule.clone(),
                    });
                }
            }
        }
    }

    // Build set of job_ids that are connected:
    // - appear on either side of an intra-folder edge
    // - are referenced by an external sensor (consumer side)
    // - are cross-folder producers (their OUTCOND is consumed by another folder)
    let mut connected: HashSet<&str> = HashSet::new();
    for edge in &edges {
        connected.insert(edge.from.as_str());
        connected.insert(edge.to.as_str());
    }
    for sensor in &external_sensors {
        connected.insert(sensor.in_job.as_str());
    }
    for job in jobs {
        if cross_folder_producers.contains(job.job_id.as_str()) {
            connected.insert(job.job_id.as_str());
        }
    }

    // Partition jobs
    let mut group_jobs: Vec<JobSummary> = Vec::new();
    for job in jobs {
        // ManualReview jobs are written to manual_review/jobs/ — never to dag_singles/
        if job.pattern == "ManualReview" {
            continue;
        }
        let summary = job_summary(job);
        if connected.contains(job.job_id.as_str()) {
            group_jobs.push(summary);
        } else {
            // Isolated auto-converted job — becomes its own single-task DAG
            let schedule = job.dag_config.as_ref().and_then(|c| c.schedule.clone());
            let timezone = job.dag_config.as_ref().and_then(|c| c.timezone.clone());
            singles.push(DagSingle {
                dag_id: job.job_id.clone(),
                source_folder: folder.to_string(),
                datacenter: datacenter.to_string(),
                schedule,
                timezone,
                job: summary,
            });
        }
    }

    // Only emit a DagGroup if there are connected jobs
    if !group_jobs.is_empty() {
        let schedule = jobs
            .iter()
            .filter(|j| connected.contains(j.job_id.as_str()))
            .find_map(|j| j.dag_config.as_ref()?.schedule.clone());
        let timezone = jobs
            .iter()
            .filter(|j| connected.contains(j.job_id.as_str()))
            .find_map(|j| j.dag_config.as_ref()?.timezone.clone());

        let has_external = !external_sensors.is_empty()
            || group_jobs.iter().any(|j| cross_folder_producers.contains(j.job_id.as_str()));
        let group = DagGroup {
            dag_id: folder.to_string(),
            datacenter: datacenter.to_string(),
            schedule,
            timezone,
            jobs: group_jobs,
            edges,
            external_sensors,
        };

        if has_external {
            groups_external.push(group);
        } else {
            groups.push(group);
        }
    }
}

fn job_summary(ir: &JobIr) -> JobSummary {
    JobSummary {
        job_id: ir.job_id.clone(),
        pattern: ir.pattern.clone(),
        operator: ir
            .dag_config
            .as_ref()
            .map(|c| c.operator.clone())
            .unwrap_or_else(|| "none".into()),
    }
}

pub fn write_dag_group(group: &DagGroup, output_dir: &Path) -> Result<(), ParseError> {
    let safe_name = group.dag_id.replace(['/', '\\', ':'], "_");
    let path = output_dir.join(format!("{}.json", safe_name));
    let json = serde_json::to_string_pretty(group)?;
    std::fs::write(path, json)?;
    Ok(())
}

pub fn write_dag_single(single: &DagSingle, output_dir: &Path) -> Result<(), ParseError> {
    let safe_name = single.dag_id.replace(['/', '\\', ':'], "_");
    let path = output_dir.join(format!("{}.json", safe_name));
    let json = serde_json::to_string_pretty(single)?;
    std::fs::write(path, json)?;
    Ok(())
}
