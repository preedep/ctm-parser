use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Parser, ValueEnum};
use tracing::info;

use ctm_parser::classifier::classify_job;
use ctm_parser::codegen::{dump_config, generate_dags};
use ctm_parser::grouper::{build_dag_groups, write_dag_group, write_dag_single};
use ctm_parser::ir::{build_ir, build_summary, write_ir_json, write_ir_yaml, write_summary, JobIr};
use ctm_parser::node_registry::NodeRegistry;
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

    /// Output root directory
    #[arg(short, long)]
    output: PathBuf,

    /// Output format for job IR files
    #[arg(short, long, value_enum, default_value = "json")]
    format: OutputFormat,

    /// Log level (error, warn, info, debug, trace)
    #[arg(long, default_value = "info")]
    log_level: String,

    /// Generate Airflow DAG .py files from templates
    #[arg(long, default_value_t = false)]
    generate_dags: bool,

    /// Company prefix used in DAG IDs and template COMPANY placeholder
    #[arg(long, default_value = "mycompany")]
    company: String,

    /// Deployment environment (dev, sit, uat, prod) — appended to DAG ID
    #[arg(long, default_value = "dev")]
    env: String,

    /// Path to templates directory (default: ./templates relative to cwd)
    #[arg(long, default_value = "templates")]
    templates_dir: PathBuf,

    /// Config override directory for this input scenario (default: ./config/<input_stem>)
    /// Override files are read from <config_dir>/<env>/<job_id>.json
    #[arg(long)]
    config_dir: Option<PathBuf>,

    /// Dump IR-derived substitution defaults to <config_dir>/<env>/<job_id>.json for manual editing
    #[arg(long, default_value_t = false)]
    dump_config: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();

    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(&args.log_level));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    // Flat output layout:
    //   dags/           — generated DAG .py files (deploy target)
    //   ir/             — auto-converted job IR JSON
    //   manifests/      — grouper output (groups, groups_external, singles merged)
    //   manual_review/  — IR files needing human action
    let ir_dir        = args.output.join("ir");
    let manifests_dir = args.output.join("manifests");
    let mr_dir        = args.output.join("manual_review");

    for dir in [&ir_dir, &manifests_dir, &mr_dir] {
        std::fs::create_dir_all(dir)
            .with_context(|| format!("cannot create {:?}", dir))?;
    }

    info!(input = ?args.input, output = ?args.output, "starting parse");

    // Load Windows node registry from skill raws — fall back to empty (all Linux) if missing
    let registry_path = std::path::Path::new(".claude/skills/controlm2airflow/raws/node_id_win.md");
    let registry = match NodeRegistry::load(registry_path) {
        Ok(r) => {
            info!(path = %registry_path.display(), "node registry loaded");
            r
        }
        Err(e) => {
            tracing::warn!(path = %registry_path.display(), error = %e, "node registry not found — defaulting all nodes to Linux");
            NodeRegistry::default()
        }
    };

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
            let ir = build_ir(job, &folder.datacenter, &pattern, &registry);

            let dest = if ir.pattern == "ManualReview" { &mr_dir } else { &ir_dir };
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

    // Stage 2 — resolve dependencies, write all manifests into manifests/
    let result = build_dag_groups(&irs);

    for group in &result.groups {
        if let Err(e) = write_dag_group(group, &manifests_dir) {
            let msg = format!("failed to write dag_group {}: {}", group.dag_id, e);
            tracing::error!("{}", msg);
            errors.push(msg);
        }
    }

    for group in &result.groups_external {
        if let Err(e) = write_dag_group(group, &manifests_dir) {
            let msg = format!("failed to write dag_group_external {}: {}", group.dag_id, e);
            tracing::error!("{}", msg);
            errors.push(msg);
        }
    }

    for single in &result.singles {
        if let Err(e) = write_dag_single(single, &manifests_dir) {
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

    // Stage 3 — optional DAG code generation from templates
    let input_stem = args
        .input
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("default");
    let config_dir = args
        .config_dir
        .clone()
        .unwrap_or_else(|| std::path::PathBuf::from("config").join(input_stem));

    if args.dump_config {
        let configs_written = dump_config(&irs, &config_dir, &args.company, &args.env)
            .with_context(|| "config dump failed")?;
        info!(configs_written = configs_written, config_dir = %config_dir.display(), "config files dumped");
    }

    if args.generate_dags {
        let dags_written = generate_dags(&irs, &args.output, &args.templates_dir, &config_dir, &args.company, &args.env)
            .with_context(|| "DAG code generation failed")?;
        info!(dags_written = dags_written, "DAG code generation complete");
    }

    Ok(())
}
