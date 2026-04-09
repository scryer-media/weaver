use thiserror::Error;

#[derive(Debug, Error)]
pub enum NzbError {
    #[error("XML parse error: {0}")]
    Xml(String),

    #[error("missing required attribute '{attribute}' on <{element}>")]
    MissingAttribute { element: String, attribute: String },

    #[error("invalid value for '{attribute}' on <{element}>: {value}")]
    InvalidValue {
        element: String,
        attribute: String,
        value: String,
    },

    #[error("NZB contains no files")]
    EmptyNzb,
}
