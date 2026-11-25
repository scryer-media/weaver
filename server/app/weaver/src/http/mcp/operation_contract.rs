// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Every operator-skill operation file must parse and validate against the
// schema this binary serves, under the same depth and complexity limits.
// The files are run by agents against live instances, and the MCP catalog
// embeds them, so a schema change that breaks one fails here instead of in
// the field. A tree with no skills has nothing to check.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_graphql::extensions::{Extension, ExtensionContext, ExtensionFactory, NextExecute};
use async_graphql::{Request, Response, Schema};
use weaver_server_api::{MutationRoot, QueryRoot, SubscriptionRoot, WeaverSchema};

// Stops every request after validation, so no resolver runs and only parse
// and validation errors can come back.
struct ValidateOnly;

impl ExtensionFactory for ValidateOnly {
    fn create(&self) -> Arc<dyn Extension> {
        Arc::new(ValidateOnlyExtension)
    }
}

struct ValidateOnlyExtension;

#[async_graphql::async_trait::async_trait]
impl Extension for ValidateOnlyExtension {
    async fn execute(
        &self,
        _ctx: &ExtensionContext<'_>,
        _operation_name: Option<&str>,
        _next: NextExecute<'_>,
    ) -> Response {
        Response::default()
    }
}

fn repository_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../..")
        .canonicalize()
        .expect("repository root")
}

fn operation_files(root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    let Ok(skills) = std::fs::read_dir(root.join("skills")) else {
        return files;
    };
    for skill in skills {
        let operations = skill.expect("skill entry").path().join("operations");
        let Ok(entries) = std::fs::read_dir(&operations) else {
            continue;
        };
        for entry in entries {
            let path = entry.expect("operation entry").path();
            if path
                .extension()
                .is_some_and(|extension| extension == "graphql")
            {
                files.push(path);
            }
        }
    }
    files.sort();
    files
}

fn validating_schema() -> WeaverSchema {
    weaver_server_api::context::apply_graphql_query_guards(Schema::build(
        QueryRoot::default(),
        MutationRoot::default(),
        SubscriptionRoot::default(),
    ))
    .extension(ValidateOnly)
    .finish()
}

async fn failures(schema: &WeaverSchema, request: Request) -> Option<String> {
    let response = schema.execute(request).await;
    (!response.errors.is_empty()).then(|| {
        response
            .errors
            .iter()
            .map(|error| error.message.as_str())
            .collect::<Vec<_>>()
            .join("; ")
    })
}

#[tokio::test]
async fn every_skill_operation_file_validates_against_the_served_schema() {
    let schema = validating_schema();
    let root = repository_root();
    let files = operation_files(&root);
    // Every file on disk is one the catalog embeds.
    assert_eq!(files.len(), super::catalog::SKILL_OPERATIONS.len());
    let mut failed = Vec::new();
    for path in &files {
        let document = std::fs::read_to_string(path).expect("read operation file");
        if let Some(messages) = failures(&schema, Request::new(document)).await {
            let relative = path.strip_prefix(&root).unwrap_or(path);
            failed.push(format!("{}: {messages}", relative.display()));
        }
    }
    assert!(
        failed.is_empty(),
        "{} of {} operation files do not validate:\n{}",
        failed.len(),
        files.len(),
        failed.join("\n")
    );
}

#[tokio::test]
async fn every_catalog_operation_validates_against_the_served_schema() {
    let schema = validating_schema();
    let catalog = &*super::catalog::CATALOG;
    for name in &catalog.varied {
        assert!(catalog.variants(name).len() > 1, "{name}");
    }
    let mut failed = Vec::new();
    for operation in catalog.operations() {
        let request = Request::new(operation.text).operation_name(operation.name.as_str());
        if let Some(messages) = failures(&schema, request).await {
            failed.push(format!("{}: {messages}", operation.name));
        }
    }
    assert!(
        failed.is_empty(),
        "{} of {} catalog operations do not validate:\n{}",
        failed.len(),
        catalog.operations().len(),
        failed.join("\n")
    );
}

#[tokio::test]
async fn the_check_passes_served_documents_and_fails_unknown_fields() {
    // Guards the check itself: a document the schema serves passes, and one
    // it does not serve must not pass because execution was skipped.
    let schema = validating_schema();
    assert_eq!(
        failures(&schema, Request::new("query Fixture { version }")).await,
        None
    );
    assert_eq!(
        failures(
            &schema,
            Request::new("mutation Fixture($id: Int!) { pauseJob(id: $id) }")
        )
        .await,
        None
    );
    assert!(
        failures(
            &schema,
            Request::new("query Fixture { notAFieldWeaverServes }")
        )
        .await
        .is_some()
    );
}
