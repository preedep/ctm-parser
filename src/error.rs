use thiserror::Error;

#[derive(Debug, Error)]
pub enum ParseError {
    #[error("XML parse error at byte {offset}: {source}")]
    Xml {
        offset: u64,
        source: quick_xml::Error,
    },

    #[error("Missing required attribute `{attr}` on job `{job}`")]
    MissingAttr { attr: &'static str, job: String },

    #[error("Unknown job type `{job_type}` — routed to ManualReview")]
    UnknownJobType { job_type: String },

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON serialization error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("YAML serialization error: {0}")]
    Yaml(#[from] serde_yaml::Error),

    #[error("template not found: {0}")]
    TemplateNotFound(String),

    #[error("codegen IO error writing {path}: {source}")]
    CodegenIo {
        path: String,
        #[source]
        source: std::io::Error,
    },
}
