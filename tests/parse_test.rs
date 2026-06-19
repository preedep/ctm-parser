use std::io::BufReader;

use ctm_parser::classifier::{classify_job, JobPattern};
use ctm_parser::ir::{build_ir, build_summary};
use ctm_parser::reader::parse_xml;

fn parse_fixture(xml: &str) -> Vec<ctm_parser::model::ControlMFolder> {
    parse_xml(BufReader::new(xml.as_bytes())).expect("parse failed")
}

#[test]
fn bash_job_classified_correctly() {
    let xml = std::fs::read_to_string("tests/fixtures/bash_job.xml").unwrap();
    let folders = parse_fixture(&xml);

    assert_eq!(folders.len(), 1);
    let job = &folders[0].jobs[0];

    assert_eq!(job.jobname, "RT_BASH_JOB_001");
    assert_eq!(job.tasktype, "Command");
    assert_eq!(job.appl_type, "OS");

    let pattern = classify_job(job);
    assert!(matches!(pattern, JobPattern::BashJob), "expected BashJob, got {:?}", pattern.name());
}

#[test]
fn bash_job_ir_has_correct_fields() {
    let xml = std::fs::read_to_string("tests/fixtures/bash_job.xml").unwrap();
    let folders = parse_fixture(&xml);
    let job = &folders[0].jobs[0];
    let pattern = classify_job(job);
    let ir = build_ir(job, "neutron", &pattern);

    assert_eq!(ir.job_id, "RT_BASH_JOB_001");
    assert_eq!(ir.pattern, "BashJob");
    assert!(ir.unmapped_attrs.is_empty(), "unexpected unmapped_attrs: {:?}", ir.unmapped_attrs);

    let config = ir.dag_config.as_ref().expect("dag_config should be present");
    assert_eq!(config.retries, 2);
    assert_eq!(config.execution_timeout_sec, Some(5400)); // 90 min * 60
    assert_eq!(config.catchup, false);
    assert!(config.bash_command.as_deref().map(|c| c.contains("{{ ds_nodash }}")).unwrap_or(false),
        "CMDLINE token %%$ODATE should be substituted");

    assert_eq!(ir.dependencies.upstream.len(), 1);
    assert_eq!(ir.dependencies.upstream[0].condition_name, "RT_UPSTREAM_001-ENDED-OK");
    assert_eq!(ir.dependencies.downstream.len(), 1);
    assert_eq!(ir.dependencies.downstream[0].sign, "+");
}

#[test]
fn bash_job_sla_derived_from_shout() {
    let xml = std::fs::read_to_string("tests/fixtures/bash_job.xml").unwrap();
    let folders = parse_fixture(&xml);
    let job = &folders[0].jobs[0];
    let pattern = classify_job(job);
    let ir = build_ir(job, "neutron", &pattern);

    let config = ir.dag_config.as_ref().unwrap();
    assert_eq!(config.sla_sec, Some(3600)); // SHOUT TIME=">060" → 60 min * 60
}

#[test]
fn filewatcher_classified_correctly() {
    let xml = std::fs::read_to_string("tests/fixtures/filewatcher_job.xml").unwrap();
    let folders = parse_fixture(&xml);
    let job = &folders[0].jobs[0];

    let pattern = classify_job(job);
    assert!(matches!(pattern, JobPattern::FileWatcher), "expected FileWatcher, got {:?}", pattern.name());
}

#[test]
fn filewatcher_ir_plugin_config() {
    let xml = std::fs::read_to_string("tests/fixtures/filewatcher_job.xml").unwrap();
    let folders = parse_fixture(&xml);
    let job = &folders[0].jobs[0];
    let pattern = classify_job(job);
    let ir = build_ir(job, "neutron", &pattern);

    assert_eq!(ir.pattern, "FileWatcher");
    assert!(ir.unmapped_attrs.is_empty(), "unexpected unmapped_attrs: {:?}", ir.unmapped_attrs);

    let cfg = ir.dag_config.as_ref().unwrap().plugin_config.as_ref().unwrap();
    assert!(cfg["file_path"].as_str().unwrap().contains("{{ ds_nodash }}"),
        "FILE_PATH should have %%$ODATE substituted");
    assert_eq!(cfg["mode"].as_str().unwrap(), "CREATE");
    assert_eq!(cfg["poke_interval_sec"].as_u64().unwrap(), 60);
}

#[test]
fn cyclic_job_classified_correctly() {
    let xml = std::fs::read_to_string("tests/fixtures/cyclic_job.xml").unwrap();
    let folders = parse_fixture(&xml);
    let job = &folders[0].jobs[0];

    let pattern = classify_job(job);
    match &pattern {
        JobPattern::CyclicJob { interval_secs } => assert_eq!(*interval_secs, 900),
        other => panic!("expected CyclicJob, got {:?}", other.name()),
    }
}

#[test]
fn cyclic_job_ir_schedule() {
    let xml = std::fs::read_to_string("tests/fixtures/cyclic_job.xml").unwrap();
    let folders = parse_fixture(&xml);
    let job = &folders[0].jobs[0];
    let pattern = classify_job(job);
    let ir = build_ir(job, "neutron", &pattern);

    let config = ir.dag_config.as_ref().unwrap();
    assert_eq!(config.schedule.as_deref(), Some("timedelta:900"));
}

#[test]
fn manual_review_job_classified_with_reason() {
    let xml = std::fs::read_to_string("tests/fixtures/manual_review_job.xml").unwrap();
    let folders = parse_fixture(&xml);
    let job = &folders[0].jobs[0];

    let pattern = classify_job(job);
    match &pattern {
        JobPattern::ManualReview { reason } => {
            assert!(reason.contains("named_calendar:CONFCAL"),
                "reason should mention CONFCAL, got: {}", reason);
        }
        other => panic!("expected ManualReview, got {:?}", other.name()),
    }
}

#[test]
fn manual_review_ir_unmapped_attrs_always_present() {
    let xml = std::fs::read_to_string("tests/fixtures/manual_review_job.xml").unwrap();
    let folders = parse_fixture(&xml);
    let job = &folders[0].jobs[0];
    let pattern = classify_job(job);
    let ir = build_ir(job, "neutron", &pattern);

    // unmapped_attrs must always be emitted — never missing
    assert!(!ir.unmapped_attrs.is_empty(), "ManualReview job must have non-empty unmapped_attrs");
    assert!(ir.unmapped_attrs.iter().any(|a| a.contains("CONFCAL") || a.contains("MANUAL_REVIEW")));

    // dag_config must be null for ManualReview
    assert!(ir.dag_config.is_none(), "ManualReview should have null dag_config");
}

#[test]
fn summary_counts_correctly() {
    let xml = std::fs::read_to_string("tests/fixtures/bash_job.xml").unwrap();
    let folders = parse_fixture(&xml);

    let mut irs = Vec::new();
    for folder in &folders {
        for job in &folder.jobs {
            let pattern = classify_job(job);
            irs.push(build_ir(job, &folder.datacenter, &pattern));
        }
    }

    let summary = build_summary("bash_job.xml", &irs, folders.len(), vec![]);
    assert_eq!(summary.total, 1);
    assert_eq!(summary.manual_review, 0);
    assert_eq!(summary.auto_converted, 1);
    assert_eq!(summary.patterns.get("BashJob"), Some(&1));
}
