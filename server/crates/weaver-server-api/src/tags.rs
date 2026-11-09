// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! Tags: free labels on a job, queued or in history, Usenet or torrent.
//! Nothing in Weaver reads a tag to decide anything. A tag is trimmed, and
//! one that is empty or holds a comma is left out (qBittorrent's rules).

use std::sync::Arc;

use async_graphql::{Context, Object, Result, SimpleObject};
use weaver_server_core::Database;
use weaver_server_core::jobs::ids::JobId;
use weaver_server_core::tags::TagSnapshot;

use crate::auth::{ControlGuard, ReadGuard, graphql_error};

/// A tag, with how many jobs carry it, queued or in history.
#[derive(Debug, Clone, PartialEq, Eq, SimpleObject)]
pub struct JobTag {
    pub name: String,
    pub count: u64,
}

fn internal(error: impl std::fmt::Display) -> async_graphql::Error {
    graphql_error("INTERNAL", error.to_string())
}

// The tags as stored: from memory when nothing changed them since the last
// read, else read off the async workers.
pub(crate) async fn tag_snapshot(db: &Database) -> Result<Arc<TagSnapshot>> {
    if let Some(snapshot) = db.cached_tag_snapshot() {
        return Ok(snapshot);
    }
    let db = db.clone();
    tokio::task::spawn_blocking(move || db.tag_snapshot())
        .await
        .map_err(internal)?
        .map_err(internal)
}

// The tags a tag filter reads: read only when a filter names a tag.
pub(crate) async fn tags_for(db: &Database, tag: Option<&str>) -> Result<Option<Arc<TagSnapshot>>> {
    match tag {
        Some(_) => Ok(Some(tag_snapshot(db).await?)),
        None => Ok(None),
    }
}

// A job's tags, for a `tags` field.
pub(crate) async fn tags_of(ctx: &Context<'_>, job_id: u64) -> Result<Vec<String>> {
    let snapshot = tag_snapshot(ctx.data::<Database>()?).await?;
    Ok(snapshot.of(JobId(job_id)).to_vec())
}

async fn job_tags(db: &Database) -> Result<Vec<JobTag>> {
    Ok(tag_snapshot(db)
        .await?
        .counts()
        .into_iter()
        .map(|count| JobTag {
            name: count.name,
            count: count.jobs,
        })
        .collect())
}

// Runs one change to the tags off the async workers, then answers every tag.
async fn change(
    ctx: &Context<'_>,
    op: impl FnOnce(&Database) -> Result<(), weaver_server_core::StateError> + Send + 'static,
) -> Result<Vec<JobTag>> {
    let db = ctx.data::<Database>()?.clone();
    let worker = db.clone();
    tokio::task::spawn_blocking(move || op(&worker))
        .await
        .map_err(internal)?
        .map_err(internal)?;
    job_tags(&db).await
}

fn job_ids(ids: &[u64]) -> Vec<JobId> {
    ids.iter().copied().map(JobId).collect()
}

#[derive(Default)]
pub struct TagsQuery;

#[Object]
impl TagsQuery {
    /// Every tag made, in tag order, with how many jobs carry it.
    #[graphql(guard = "ReadGuard")]
    async fn job_tags(&self, ctx: &Context<'_>) -> Result<Vec<JobTag>> {
        job_tags(ctx.data::<Database>()?).await
    }
}

#[derive(Default)]
pub struct TagsMutation;

#[Object]
impl TagsMutation {
    /// Make tags no job carries yet. Answers every tag.
    #[graphql(guard = "ControlGuard")]
    async fn create_tags(&self, ctx: &Context<'_>, tags: Vec<String>) -> Result<Vec<JobTag>> {
        change(ctx, move |db| {
            db.create_tags(tags.iter().map(String::as_str))
        })
        .await
    }

    /// Delete tags, taking them off every job. Answers every tag left.
    #[graphql(guard = "ControlGuard")]
    async fn delete_tags(&self, ctx: &Context<'_>, tags: Vec<String>) -> Result<Vec<JobTag>> {
        change(ctx, move |db| {
            db.delete_tags(tags.iter().map(String::as_str))
        })
        .await
    }

    /// Put tags on jobs, queued or in history, making any tag not made yet.
    /// An id of no job is passed over. Answers every tag.
    #[graphql(guard = "ControlGuard")]
    async fn add_job_tags(
        &self,
        ctx: &Context<'_>,
        ids: Vec<u64>,
        tags: Vec<String>,
    ) -> Result<Vec<JobTag>> {
        let ids = job_ids(&ids);
        change(ctx, move |db| {
            db.add_job_tags(&ids, tags.iter().map(String::as_str))
        })
        .await
    }

    /// Take tags off jobs; with no tags, take every tag off them. The tags
    /// stay made. Answers every tag.
    #[graphql(guard = "ControlGuard")]
    async fn remove_job_tags(
        &self,
        ctx: &Context<'_>,
        ids: Vec<u64>,
        #[graphql(default)] tags: Vec<String>,
    ) -> Result<Vec<JobTag>> {
        let ids = job_ids(&ids);
        change(ctx, move |db| {
            db.remove_job_tags(&ids, tags.iter().map(String::as_str))
        })
        .await
    }

    /// Give jobs exactly these tags, making any not made yet. Answers every
    /// tag.
    #[graphql(guard = "ControlGuard")]
    async fn set_job_tags(
        &self,
        ctx: &Context<'_>,
        ids: Vec<u64>,
        tags: Vec<String>,
    ) -> Result<Vec<JobTag>> {
        let ids = job_ids(&ids);
        change(ctx, move |db| {
            db.set_job_tags(&ids, tags.iter().map(String::as_str))
        })
        .await
    }
}
