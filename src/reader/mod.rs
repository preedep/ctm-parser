use std::collections::HashMap;
use std::io::BufRead;

use quick_xml::events::Event;
use quick_xml::Reader;
use tracing::warn;

use crate::error::ParseError;
use crate::model::*;

pub fn parse_xml<R: BufRead>(reader: R) -> Result<Vec<ControlMFolder>, ParseError> {
    let mut xml = Reader::from_reader(reader);
    xml.config_mut().trim_text(true);

    let mut folders: Vec<ControlMFolder> = Vec::new();
    let mut buf = Vec::new();

    // Stack-based state: (element_name, folder_context, job_context)
    let mut current_folder: Option<ControlMFolder> = None;
    let mut current_job: Option<ControlMJob> = None;
    let mut current_on: Option<OnEvent> = None;
    let mut element_stack: Vec<String> = Vec::new();

    loop {
        match xml.read_event_into(&mut buf) {
            Ok(Event::Start(ref e)) | Ok(Event::Empty(ref e)) => {
                let tag = std::str::from_utf8(e.name().as_ref())
                    .unwrap_or("")
                    .to_string();
                let attrs = collect_attrs(e);

                match tag.as_str() {
                    "FOLDER" | "SCHED_TABLE" | "TABLE" | "SMART_FOLDER" | "SMART_TABLE"
                    | "SCHED_GROUP" => {
                        let folder_name = attrs
                            .get("FOLDER_NAME")
                            .or_else(|| attrs.get("TABLE_NAME"))
                            .cloned()
                            .unwrap_or_default();
                        let datacenter = attrs.get("DATACENTER").cloned().unwrap_or_default();
                        let platform = attrs.get("PLATFORM").cloned();
                        current_folder = Some(ControlMFolder {
                            folder_name,
                            datacenter,
                            platform,
                            jobs: Vec::new(),
                        });
                        element_stack.push(tag);
                    }
                    "JOB" => {
                        let job = build_job(attrs, &current_folder);
                        current_job = Some(job);
                        element_stack.push(tag);
                    }
                    "INCOND" => {
                        if let Some(job) = current_job.as_mut() {
                            job.incond.push(InCondition {
                                name: attrs.get("NAME").cloned().unwrap_or_default(),
                                odate: attrs.get("ODATE").cloned().unwrap_or_else(|| "ODAT".into()),
                                and_or: attrs.get("AND_OR").cloned().unwrap_or_else(|| "A".into()),
                            });
                        }
                    }
                    "OUTCOND" => {
                        if let Some(job) = current_job.as_mut() {
                            job.outcond.push(OutCondition {
                                name: attrs.get("NAME").cloned().unwrap_or_default(),
                                odate: attrs.get("ODATE").cloned().unwrap_or_else(|| "ODAT".into()),
                                sign: attrs.get("SIGN").cloned().unwrap_or_else(|| "+".into()),
                            });
                        }
                    }
                    "VARIABLE" | "AUTOEDIT2" => {
                        if let Some(job) = current_job.as_mut() {
                            if let (Some(name), Some(value)) =
                                (attrs.get("NAME"), attrs.get("VALUE"))
                            {
                                job.variables.insert(name.clone(), value.clone());
                            }
                        }
                    }
                    "AUTOEDIT" => {
                        if let Some(job) = current_job.as_mut() {
                            if let Some(exp) = attrs.get("EXP") {
                                if let Some(eq) = exp.find('=') {
                                    let name = exp[..eq].trim().to_string();
                                    let value = exp[eq + 1..].trim().to_string();
                                    job.variables.insert(name, value);
                                }
                            }
                        }
                    }
                    "SHOUT" => {
                        if let Some(job) = current_job.as_mut() {
                            job.shout.push(ShoutConfig {
                                when: attrs.get("WHEN").cloned().unwrap_or_default(),
                                time: attrs.get("TIME").cloned(),
                                urgency: attrs.get("URGENCY").cloned(),
                                dest: attrs.get("DEST").cloned(),
                                message: attrs.get("MESSAGE").cloned(),
                            });
                        }
                    }
                    "QUANTITATIVE" => {
                        if let Some(job) = current_job.as_mut() {
                            let quant = attrs
                                .get("QUANT")
                                .and_then(|v| v.parse().ok())
                                .unwrap_or(1);
                            if let Some(name) = attrs.get("NAME") {
                                job.quantitative.push(QuantitativeResource {
                                    name: name.clone(),
                                    quant,
                                });
                            }
                        }
                    }
                    "CONTROL" => {
                        if let Some(job) = current_job.as_mut() {
                            if let Some(name) = attrs.get("NAME") {
                                job.controls.push(ControlResource {
                                    name: name.clone(),
                                    resource_type: attrs
                                        .get("TYPE")
                                        .cloned()
                                        .unwrap_or_else(|| "E".into()),
                                });
                            }
                        }
                    }
                    "ON" | "ON_TABLE" | "ON_GROUP" => {
                        let stmt = attrs.get("STMT").cloned().unwrap_or_else(|| "*".into());
                        let has_output_pattern = stmt != "*";
                        let on = OnEvent {
                            code: attrs.get("CODE").cloned().unwrap_or_default(),
                            stmt,
                            has_remedy: false,
                            has_output_pattern,
                            actions: Vec::new(),
                        };
                        current_on = Some(on);
                        element_stack.push(tag);
                    }
                    "DOACTION" | "ACTION" => {
                        if let Some(on) = current_on.as_mut() {
                            if let Some(action) = attrs.get("ACTION") {
                                on.actions.push(OnAction::DoAction { action: action.clone() });
                            }
                        }
                    }
                    "DOCOND" => {
                        if let Some(on) = current_on.as_mut() {
                            on.actions.push(OnAction::DoCondition {
                                name: attrs.get("NAME").cloned().unwrap_or_default(),
                                odate: attrs.get("ODATE").cloned().unwrap_or_default(),
                                sign: attrs.get("SIGN").cloned().unwrap_or_default(),
                            });
                        }
                    }
                    "DOFORCEJOB" => {
                        if let Some(on) = current_on.as_mut() {
                            on.actions.push(OnAction::DoForceJob {
                                table_name: attrs.get("TABLE_NAME").cloned(),
                                name: attrs.get("NAME").cloned().unwrap_or_default(),
                            });
                        }
                    }
                    "DOMAIL" => {
                        if let Some(on) = current_on.as_mut() {
                            on.actions.push(OnAction::DoMail {
                                dest: attrs.get("DEST").cloned(),
                                subject: attrs.get("SUBJECT").cloned(),
                                message: attrs.get("MESSAGE").cloned(),
                            });
                        }
                    }
                    "DOSHOUT" => {
                        if let Some(on) = current_on.as_mut() {
                            on.actions.push(OnAction::DoShout {
                                urgency: attrs.get("URGENCY").cloned(),
                                message: attrs.get("MESSAGE").cloned(),
                                dest: attrs.get("DEST").cloned(),
                            });
                        }
                    }
                    "DOREMEDY" => {
                        if let Some(on) = current_on.as_mut() {
                            on.has_remedy = true;
                            on.actions.push(OnAction::DoRemedy {
                                description: attrs.get("DESCRIPTION").cloned(),
                                summary: attrs.get("SUMMARY").cloned(),
                            });
                        }
                    }
                    "DOVARIABLE" | "DOAUTOEDIT2" => {
                        if let Some(on) = current_on.as_mut() {
                            if let (Some(name), Some(value)) =
                                (attrs.get("NAME"), attrs.get("VALUE"))
                            {
                                on.actions.push(OnAction::DoVariable {
                                    name: name.clone(),
                                    value: value.clone(),
                                });
                            }
                        }
                    }
                    // Silently skip known metadata elements
                    "ADDITIONAL_JOB_DETAILS" | "ADDITIONAL_FOLDER_DETAILS"
                    | "WCM_NOTES_DATA" | "NOTE_HISTORY" | "BUSINESS_PARAMETER"
                    | "STEP_RANGE" | "RULE_BASED_CALENDAR" | "TAG"
                    | "RULE_BASED_CALENDARS" | "TAG_NAMES" | "CAPTURE"
                    | "DOIFRERUN" | "DOSYSOUT" | "DOOUTPUT" | "DOCTBRULE"
                    | "SUB_FOLDER" | "SUB_TABLE" | "WORKSPACE" | "DEFTABLE" => {}
                    other => {
                        if current_on.is_some() {
                            if let Some(on) = current_on.as_mut() {
                                on.actions.push(OnAction::Other { tag: other.to_string() });
                            }
                        }
                    }
                }

            }

            Ok(Event::End(ref e)) => {
                let tag = std::str::from_utf8(e.name().as_ref())
                    .unwrap_or("")
                    .to_string();

                match tag.as_str() {
                    "ON" | "ON_TABLE" | "ON_GROUP" => {
                        if let (Some(on), Some(job)) = (current_on.take(), current_job.as_mut()) {
                            job.on_events.push(on);
                        }
                        element_stack.pop();
                    }
                    "JOB" => {
                        if let (Some(job), Some(folder)) =
                            (current_job.take(), current_folder.as_mut())
                        {
                            folder.jobs.push(job);
                        }
                        element_stack.pop();
                    }
                    "FOLDER" | "SCHED_TABLE" | "TABLE" | "SMART_FOLDER" | "SMART_TABLE"
                    | "SCHED_GROUP" => {
                        if let Some(folder) = current_folder.take() {
                            folders.push(folder);
                        }
                        element_stack.pop();
                    }
                    _ => {}
                }
            }

            Ok(Event::Eof) => break,
            Err(e) => {
                let offset = xml.buffer_position() as u64;
                return Err(ParseError::Xml { offset, source: e });
            }
            _ => {}
        }
        buf.clear();
    }

    Ok(folders)
}

fn collect_attrs(e: &quick_xml::events::BytesStart) -> HashMap<String, String> {
    let mut map = HashMap::new();
    for attr_result in e.attributes() {
        let attr = match attr_result {
            Ok(a) => a,
            Err(_) => continue,
        };
        let key = std::str::from_utf8(attr.key.as_ref())
            .unwrap_or("")
            .to_string();
        let raw = String::from_utf8_lossy(&attr.value).into_owned();
        let val = quick_xml::escape::unescape(&raw)
            .map(|v| v.into_owned())
            .unwrap_or(raw);
        map.insert(key, val);
    }
    map
}

fn build_job(attrs: HashMap<String, String>, folder: &Option<ControlMFolder>) -> ControlMJob {
    let parent_folder = attrs
        .get(ATTR_PARENT_FOLDER)
        .cloned()
        .or_else(|| folder.as_ref().map(|f| f.folder_name.clone()))
        .unwrap_or_default();

    let jobname = attrs.get(ATTR_JOBNAME).cloned().unwrap_or_default();
    if jobname.is_empty() {
        warn!("JOB element missing JOBNAME in folder {:?}", folder.as_ref().map(|f| &f.folder_name));
    }

    let months = MonthFlags {
        jan: flag(&attrs, ATTR_JAN),
        feb: flag(&attrs, ATTR_FEB),
        mar: flag(&attrs, ATTR_MAR),
        apr: flag(&attrs, ATTR_APR),
        may: flag(&attrs, ATTR_MAY),
        jun: flag(&attrs, ATTR_JUN),
        jul: flag(&attrs, ATTR_JUL),
        aug: flag(&attrs, ATTR_AUG),
        sep: flag(&attrs, ATTR_SEP),
        oct: flag(&attrs, ATTR_OCT),
        nov: flag(&attrs, ATTR_NOV),
        dec: flag(&attrs, ATTR_DEC),
    };

    // Collect unknown attributes into extra
    let mut extra: HashMap<String, String> = HashMap::new();
    for (k, v) in &attrs {
        let upper = k.to_ascii_uppercase();
        if !KNOWN_JOB_ATTRS.iter().any(|known| known.eq_ignore_ascii_case(&upper)) {
            extra.insert(k.clone(), v.clone());
        }
    }
    if !extra.is_empty() {
        warn!(
            job = %jobname,
            attrs = ?extra.keys().collect::<Vec<_>>(),
            "unknown attributes on job"
        );
    }

    ControlMJob {
        jobname,
        tasktype: attrs.get(ATTR_TASKTYPE).cloned().unwrap_or_default(),
        appl_type: attrs.get(ATTR_APPL_TYPE).cloned().unwrap_or_default(),
        appl_form: attrs.get(ATTR_APPL_FORM).cloned(),
        cmdline: attrs.get(ATTR_CMDLINE).cloned().filter(|s| !s.is_empty()),
        nodeid: attrs.get(ATTR_NODEID).cloned(),
        run_as: attrs.get(ATTR_RUN_AS).cloned(),
        owner: attrs.get(ATTR_OWNER).cloned(),
        application: attrs.get(ATTR_APPLICATION).cloned(),
        sub_application: attrs.get(ATTR_SUB_APPLICATION).cloned(),
        description: attrs.get(ATTR_DESCRIPTION).cloned(),
        parent_folder,
        priority: attrs.get(ATTR_PRIORITY).cloned(),
        critical: flag(&attrs, ATTR_CRITICAL),
        confirm: flag(&attrs, ATTR_CONFIRM),
        cyclic: flag(&attrs, ATTR_CYCLIC),
        interval: attrs.get(ATTR_INTERVAL).cloned(),
        ind_cyclic: attrs.get(ATTR_IND_CYCLIC).cloned(),
        cyclic_interval_sequence: attrs.get(ATTR_CYCLIC_INTERVAL_SEQUENCE).cloned().filter(|s| !s.is_empty()),
        cyclic_times_sequence: attrs.get(ATTR_CYCLIC_TIMES_SEQUENCE).cloned().filter(|s| !s.is_empty()),
        maxwait: parse_u32(&attrs, ATTR_MAXWAIT),
        maxrerun: parse_u32(&attrs, ATTR_MAXRERUN),
        days: attrs.get(ATTR_DAYS).cloned(),
        weekdays: attrs.get(ATTR_WEEKDAYS).cloned(),
        days_and_or: attrs.get(ATTR_DAYS_AND_OR).cloned(),
        timefrom: attrs.get(ATTR_TIMEFROM).cloned(),
        timeto: attrs.get(ATTR_TIMETO).cloned(),
        timezone: attrs.get(ATTR_TIMEZONE).cloned().filter(|s| !s.is_empty()),
        active_from: attrs.get(ATTR_ACTIVE_FROM).cloned().filter(|s| !s.is_empty()),
        active_till: attrs.get(ATTR_ACTIVE_TILL).cloned().filter(|s| !s.is_empty()),
        retro: flag(&attrs, ATTR_RETRO),
        dayscal: attrs.get(ATTR_DAYSCAL).cloned().filter(|s| !s.is_empty()),
        weekscal: attrs.get(ATTR_WEEKSCAL).cloned().filter(|s| !s.is_empty()),
        confcal: attrs.get(ATTR_CONFCAL).cloned().filter(|s| !s.is_empty()),
        shift: attrs.get(ATTR_SHIFT).cloned(),
        months,
        incond: Vec::new(),
        outcond: Vec::new(),
        variables: HashMap::new(),
        shout: Vec::new(),
        on_events: Vec::new(),
        quantitative: Vec::new(),
        controls: Vec::new(),
        extra,
    }
}

fn flag(attrs: &HashMap<String, String>, key: &str) -> bool {
    match attrs.get(key).map(|s| s.as_str()) {
        Some("1") | Some("Y") | Some("True") | Some("true") => true,
        _ => false,
    }
}

fn parse_u32(attrs: &HashMap<String, String>, key: &str) -> u32 {
    attrs.get(key).and_then(|v| v.parse().ok()).unwrap_or(0)
}
