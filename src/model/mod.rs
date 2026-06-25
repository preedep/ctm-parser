use std::collections::HashMap;

// Attribute name constants — never scatter string literals across modules
pub const ATTR_JOBNAME: &str = "JOBNAME";
pub const ATTR_TASKTYPE: &str = "TASKTYPE";
pub const ATTR_APPL_TYPE: &str = "APPL_TYPE";
pub const ATTR_APPL_FORM: &str = "APPL_FORM";
pub const ATTR_APPL_VER: &str = "APPL_VER";
pub const ATTR_CMDLINE: &str = "CMDLINE";
pub const ATTR_NODEID: &str = "NODEID";
pub const ATTR_RUN_AS: &str = "RUN_AS";
pub const ATTR_OWNER: &str = "OWNER";
pub const ATTR_APPLICATION: &str = "APPLICATION";
pub const ATTR_SUB_APPLICATION: &str = "SUB_APPLICATION";
pub const ATTR_GROUP: &str = "GROUP";
pub const ATTR_MEMNAME: &str = "MEMNAME";
pub const ATTR_MEMLIB: &str = "MEMLIB";
pub const ATTR_DESCRIPTION: &str = "DESCRIPTION";
pub const ATTR_PARENT_FOLDER: &str = "PARENT_FOLDER";
pub const ATTR_PRIORITY: &str = "PRIORITY";
pub const ATTR_CRITICAL: &str = "CRITICAL";
pub const ATTR_CONFIRM: &str = "CONFIRM";
pub const ATTR_CYCLIC: &str = "CYCLIC";
pub const ATTR_INTERVAL: &str = "INTERVAL";
pub const ATTR_IND_CYCLIC: &str = "IND_CYCLIC";
pub const ATTR_CYCLIC_TYPE: &str = "CYCLIC_TYPE";
pub const ATTR_CYCLIC_TOLERANCE: &str = "CYCLIC_TOLERANCE";
pub const ATTR_CYCLIC_INTERVAL_SEQUENCE: &str = "CYCLIC_INTERVAL_SEQUENCE";
pub const ATTR_CYCLIC_TIMES_SEQUENCE: &str = "CYCLIC_TIMES_SEQUENCE";
pub const ATTR_MAXWAIT: &str = "MAXWAIT";
pub const ATTR_MAXRERUN: &str = "MAXRERUN";
pub const ATTR_MAXDAYS: &str = "MAXDAYS";
pub const ATTR_MAXRUNS: &str = "MAXRUNS";
pub const ATTR_DAYS: &str = "DAYS";
pub const ATTR_WEEKDAYS: &str = "WEEKDAYS";
pub const ATTR_DAYS_AND_OR: &str = "DAYS_AND_OR";
pub const ATTR_TIMEFROM: &str = "TIMEFROM";
pub const ATTR_TIMETO: &str = "TIMETO";
pub const ATTR_TIMEZONE: &str = "TIMEZONE";
pub const ATTR_ACTIVE_FROM: &str = "ACTIVE_FROM";
pub const ATTR_ACTIVE_TILL: &str = "ACTIVE_TILL";
pub const ATTR_RETRO: &str = "RETRO";
pub const ATTR_DAYSCAL: &str = "DAYSCAL";
pub const ATTR_WEEKSCAL: &str = "WEEKSCAL";
pub const ATTR_CONFCAL: &str = "CONFCAL";
pub const ATTR_SHIFT: &str = "SHIFT";
pub const ATTR_SHIFTNUM: &str = "SHIFTNUM";
pub const ATTR_DATE: &str = "DATE";
pub const ATTR_JAN: &str = "JAN";
pub const ATTR_FEB: &str = "FEB";
pub const ATTR_MAR: &str = "MAR";
pub const ATTR_APR: &str = "APR";
pub const ATTR_MAY: &str = "MAY";
pub const ATTR_JUN: &str = "JUN";
pub const ATTR_JUL: &str = "JUL";
pub const ATTR_AUG: &str = "AUG";
pub const ATTR_SEP: &str = "SEP";
pub const ATTR_OCT: &str = "OCT";
pub const ATTR_NOV: &str = "NOV";
pub const ATTR_DEC: &str = "DEC";

// Known attribute names consumed during parsing — anything not in this set → extra
pub const KNOWN_JOB_ATTRS: &[&str] = &[
    ATTR_JOBNAME, "JOBISN", ATTR_TASKTYPE, ATTR_APPL_TYPE, ATTR_APPL_FORM, ATTR_APPL_VER,
    ATTR_CMDLINE, ATTR_NODEID, ATTR_RUN_AS, ATTR_OWNER, ATTR_APPLICATION, ATTR_SUB_APPLICATION,
    ATTR_GROUP, ATTR_MEMNAME, "MEMLIB", "OVERLIB", "OVERRIDE_PATH", ATTR_DESCRIPTION,
    ATTR_PARENT_FOLDER, "PARENT_TABLE", "END_FOLDER",
    ATTR_PRIORITY, ATTR_CRITICAL, ATTR_CONFIRM,
    ATTR_CYCLIC, ATTR_INTERVAL, ATTR_IND_CYCLIC, ATTR_CYCLIC_TYPE, ATTR_CYCLIC_TOLERANCE,
    ATTR_CYCLIC_INTERVAL_SEQUENCE, ATTR_CYCLIC_TIMES_SEQUENCE,
    ATTR_MAXWAIT, ATTR_MAXRERUN, ATTR_MAXDAYS, ATTR_MAXRUNS,
    ATTR_DAYS, ATTR_WEEKDAYS, ATTR_DAYS_AND_OR,
    ATTR_TIMEFROM, ATTR_TIMETO, ATTR_TIMEZONE, ATTR_ACTIVE_FROM, ATTR_ACTIVE_TILL,
    ATTR_RETRO, ATTR_DAYSCAL, ATTR_WEEKSCAL, ATTR_CONFCAL, ATTR_SHIFT, ATTR_SHIFTNUM, ATTR_DATE,
    ATTR_JAN, ATTR_FEB, ATTR_MAR, ATTR_APR, ATTR_MAY, ATTR_JUN,
    ATTR_JUL, ATTR_AUG, ATTR_SEP, ATTR_OCT, ATTR_NOV, ATTR_DEC,
    "AUTOARCH", "RERUNMEM", "RETEN_DAYS", "RETEN_GEN", "DOCLIB", "DOCMEM",
    "SYSDB", "MULTY_AGENT", "SYSTEM_AFFINITY", "CM_VER", "INSTREAM_JCL", "USE_INSTREAM_JCL",
    "TASK_CLASS", "CATEGORY", "LARGE_SIZE", "PREVENTNCT2", "OPTION", "PAR", "MINIMUM",
    "PDSNAME", "JOBS_IN_GROUP", "FPROCS", "TPGMS", "TPROCS",
    "CREATED_BY", "AUTHOR", "CREATION_USER", "CREATION_DATE", "CREATION_TIME",
    "CHANGE_USERID", "CHANGE_DATE", "CHANGE_TIME",
    "JOB_VERSION", "VERSION_OPCODE", "IS_CURRENT_VERSION", "VERSION_SERIAL", "VERSION_HOST",
    "RULE_BASED_CALENDAR_RELATIONSHIP", "TAG_RELATIONSHIP",
    "STAT_CAL", "PREV_DAY", "ADJUST_COND", "DUE_OUT", "FROM", "ODATE",
    "FROM_DAYSOFFSET", "TO_DAYSOFFSET", "DUE_OUT_DAYSOFFSET",
    "REQUEST_NJE_NODE", "SCHEDULING_ENVIRONMENT",
    "DAYS_AND_OR", "SHIFT", "SHIFTNUM",
];

#[derive(Debug, Clone)]
pub struct ControlMFolder {
    pub folder_name: String,
    pub datacenter: String,
    pub platform: Option<String>,
    pub jobs: Vec<ControlMJob>,
}

#[derive(Debug, Clone)]
pub struct ControlMJob {
    pub jobname: String,
    pub tasktype: String,
    pub appl_type: String,
    pub appl_form: Option<String>,
    pub cmdline: Option<String>,
    /// Script filename for TASKTYPE=Job — combined with memlib to form the command when CMDLINE is absent
    pub memname: Option<String>,
    /// Directory on agent node containing the MEMNAME script
    pub memlib: Option<String>,
    pub nodeid: Option<String>,
    pub run_as: Option<String>,
    pub owner: Option<String>,
    pub application: Option<String>,
    pub sub_application: Option<String>,
    pub description: Option<String>,
    pub parent_folder: String,
    pub priority: Option<String>,
    pub critical: bool,
    pub confirm: bool,

    // Scheduling
    pub cyclic: bool,
    pub interval: Option<String>,
    pub ind_cyclic: Option<String>,
    pub cyclic_interval_sequence: Option<String>,
    pub cyclic_times_sequence: Option<String>,
    pub maxwait: u32,
    pub maxrerun: u32,
    pub days: Option<String>,
    pub weekdays: Option<String>,
    pub days_and_or: Option<String>,
    pub timefrom: Option<String>,
    pub timeto: Option<String>,
    pub timezone: Option<String>,
    pub active_from: Option<String>,
    pub active_till: Option<String>,
    pub retro: bool,
    pub dayscal: Option<String>,
    pub weekscal: Option<String>,
    pub confcal: Option<String>,
    pub shift: Option<String>,
    pub months: MonthFlags,

    // Child elements
    pub incond: Vec<InCondition>,
    pub outcond: Vec<OutCondition>,
    pub variables: HashMap<String, String>,
    pub shout: Vec<ShoutConfig>,
    pub on_events: Vec<OnEvent>,
    pub quantitative: Vec<QuantitativeResource>,
    pub controls: Vec<ControlResource>,

    // Unmapped attributes
    pub extra: HashMap<String, String>,
}

#[derive(Debug, Clone, Default)]
pub struct MonthFlags {
    pub jan: bool,
    pub feb: bool,
    pub mar: bool,
    pub apr: bool,
    pub may: bool,
    pub jun: bool,
    pub jul: bool,
    pub aug: bool,
    pub sep: bool,
    pub oct: bool,
    pub nov: bool,
    pub dec: bool,
}

impl MonthFlags {
    pub fn all_active(&self) -> bool {
        self.jan && self.feb && self.mar && self.apr && self.may && self.jun
            && self.jul && self.aug && self.sep && self.oct && self.nov && self.dec
    }

    pub fn active_months(&self) -> Vec<u8> {
        let flags = [
            (1u8, self.jan), (2, self.feb), (3, self.mar), (4, self.apr),
            (5, self.may), (6, self.jun), (7, self.jul), (8, self.aug),
            (9, self.sep), (10, self.oct), (11, self.nov), (12, self.dec),
        ];
        flags.iter().filter(|(_, v)| *v).map(|(m, _)| *m).collect()
    }
}

#[derive(Debug, Clone)]
pub struct InCondition {
    pub name: String,
    pub odate: String,
    pub and_or: String,
}

#[derive(Debug, Clone)]
pub struct OutCondition {
    pub name: String,
    pub odate: String,
    pub sign: String,
}

#[derive(Debug, Clone)]
pub struct ShoutConfig {
    pub when: String,
    pub time: Option<String>,
    pub urgency: Option<String>,
    pub dest: Option<String>,
    pub message: Option<String>,
}

#[derive(Debug, Clone)]
pub struct OnEvent {
    pub code: String,
    pub stmt: String,
    pub has_remedy: bool,
    pub has_output_pattern: bool,
    pub actions: Vec<OnAction>,
}

#[derive(Debug, Clone)]
pub enum OnAction {
    DoAction { action: String },
    DoCondition { name: String, odate: String, sign: String },
    DoForceJob { table_name: Option<String>, name: String },
    DoMail { dest: Option<String>, subject: Option<String>, message: Option<String> },
    DoShout { urgency: Option<String>, message: Option<String>, dest: Option<String> },
    DoRemedy { description: Option<String>, summary: Option<String> },
    DoVariable { name: String, value: String },
    Other { tag: String },
}

#[derive(Debug, Clone)]
pub struct QuantitativeResource {
    pub name: String,
    pub quant: u32,
}

#[derive(Debug, Clone)]
pub struct ControlResource {
    pub name: String,
    pub resource_type: String,
}
