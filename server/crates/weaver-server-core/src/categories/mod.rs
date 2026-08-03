pub mod model;
pub mod persistence;
pub mod queries;
pub mod record;
pub mod repository;
pub mod service;

pub use model::{
    CategoryConfig, CategoryValidationError, resolve_category, resolve_submission_category,
    validate_category_path_component,
};
