use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Parser, ValueEnum};
use tracing::info;

use ctm_parser::classifier::classify_job;
use ctm_parser::grouper::{build_dag_groups, write_dag_group, write_dag_single};
use ctm_parser::ir::{build_ir, build_summary, write_ir_json, write_ir_yaml, write_summary, JobIr};
use ctm_parser::reader;

#[derive(Debug, Clone, ValueEnum)]
enum OutputFormat {
    Json,
    Yaml,
}

#[derive(Parser, Debug)]
#[command(name = "ctm-parser", about = "Parse Control-M EM XML exports to Airflow job IR")]
struct Args {
    /// Path to Control-M XML export file
    #[arg(short, long)]
    input: PathBuf,

    /// Output root directory (jobs/ and dag_groups/ are created inside)
    #[arg(short, long)]
    output: PathBuf,

    /// Output format for job IR files
    #[arg(short, long, value_enum, default_value = "json")]
    format: OutputFormat,

    /// Log level (error, warn, info, debug, trace)
    #[arg(long, default_value = "info")]
    log_level: String,
}

fn main() -> Result<()> {
    let args = Args::parse();

    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(&args.log_level));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let ac_dir = args.output.join("auto_converted");
    let mr_dir = args.output.join("manual_review");

    let jobs_dir = ac_dir.join("jobs");
    let groups_dir = ac_dir.join("dag_groups");
    let groups_ext_dir = ac_dir.join("dag_groups_external");
    let singles_dir = ac_dir.join("dag_singles");
    let mr_jobs_dir = mr_dir.join("jobs");

    for dir in [&jobs_dir, &groups_dir, &groups_ext_dir, &singles_dir, &mr_jobs_dir] {
        std::fs::create_dir_all(dir)
            .with_context(|| format!("cannot create {:?}", dir))?;
    }

    info!(input = ?args.input, output = ?args.output, "starting parse");

    let file = File::open(&args.input)
        .with_context(|| format!("cannot open {:?}", args.input))?;
    let folders = reader::parse_xml(BufReader::new(file))
        .with_context(|| "XML parse failed")?;

    let folders_processed = folders.len();
    info!(folders = folders_processed, "folders parsed");

    // Stage 1 — classify + write job IR files
    let mut irs: Vec<JobIr> = Vec::new();
    let mut errors: Vec<String> = Vec::new();

    for folder in &folders {
        for job in &folder.jobs {
            let pattern = classify_job(job);
            let ir = build_ir(job, &folder.datacenter, &pattern);

            let dest = if ir.pattern == "ManualReview" { &mr_jobs_dir } else { &jobs_dir };
            let write_result = match args.format {
                OutputFormat::Json => write_ir_json(&ir, dest),
                OutputFormat::Yaml => write_ir_yaml(&ir, dest),
            };

            if let Err(e) = write_result {
                let msg = format!("failed to write IR for job {}: {}", job.jobname, e);
                tracing::error!("{}", msg);
                errors.push(msg);
            }

            irs.push(ir);
        }
    }

    let ac_count = irs.iter().filter(|ir| ir.pattern != "ManualReview").count();
    let mr_count = irs.len() - ac_count;
    info!(auto_converted = ac_count, manual_review = mr_count, "job IR files written");

    // Stage 2 — resolve dependencies, split into groups / groups_external / singles
    let result = build_dag_groups(&irs);

    for group in &result.groups {
        if let Err(e) = write_dag_group(group, &groups_dir) {
            let msg = format!("failed to write dag_group {}: {}", group.dag_id, e);
            tracing::error!("{}", msg);
            errors.push(msg);
        }
    }

    for group in &result.groups_external {
        if let Err(e) = write_dag_group(group, &groups_ext_dir) {
            let msg = format!("failed to write dag_group_external {}: {}", group.dag_id, e);
            tracing::error!("{}", msg);
            errors.push(msg);
        }
    }

    for single in &result.singles {
        if let Err(e) = write_dag_single(single, &singles_dir) {
            let msg = format!("failed to write dag_single {}: {}", single.dag_id, e);
            tracing::error!("{}", msg);
            errors.push(msg);
        }
    }

    info!(
        groups = result.groups.len(),
        groups_external = result.groups_external.len(),
        singles = result.singles.len(),
        "DAG manifests written"
    );

    // Summary
    let input_name = args
        .input
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("unknown");

    let summary = build_summary(input_name, &irs, folders_processed, errors);

    info!(
        total = summary.total,
        auto_converted = summary.auto_converted,
        manual_review = summary.manual_review,
        "processing complete"
    );

    write_summary(&summary, &args.output)
        .with_context(|| "failed to write migration_summary.json")?;

    Ok(())
}
