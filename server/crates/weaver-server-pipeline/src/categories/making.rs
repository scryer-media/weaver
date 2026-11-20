// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The one owner of making a category: the qBittorrent facade's
// `createCategory`, the GraphQL `addCategory`, and a torrent add that names a
// category Weaver does not have (qBittorrent `SessionImpl::addTorrent_impl`
// makes it, sessionimpl.cpp:2853-2857) all make one here.

use crate::categories::persistence::CategoriesPersistenceQueries;
use crate::categories::queries::CategoriesQueries;
use std::sync::LazyLock;

use crate::StateError;
use crate::categories::CategoryConfig;
use crate::persistence::Database;
use crate::settings::SharedConfig;

// Held across every change to the categories, so two makers of one name
// make one category.
static WRITES: LazyLock<tokio::sync::Mutex<()>> = LazyLock::new(|| tokio::sync::Mutex::new(()));

/// The hold every change to the categories takes first.
pub struct CategoryWrites {
    _held: tokio::sync::MutexGuard<'static, ()>,
}

/// Waits for the hold on the categories.
pub async fn category_writes() -> CategoryWrites {
    CategoryWrites {
        _held: WRITES.lock().await,
    }
}

/// What a make came to.
#[derive(Debug, Clone)]
pub enum Made {
    /// Made now, under the id it was given.
    New(CategoryConfig),
    /// One of that name, in any letter case, was there already; nothing was
    /// made.
    There(CategoryConfig),
}

impl Made {
    pub fn category(&self) -> &CategoryConfig {
        match self {
            Self::New(category) | Self::There(category) => category,
        }
    }
}

/// Makes `category`, under the next free id, unless a category of its name
/// is there. The row is one write through the writer gate, off the async
/// runtime; the settings take it once it is written. Its name is the
/// caller's to check.
pub async fn make_category(
    _held: &CategoryWrites,
    config: &SharedConfig,
    db: &Database,
    category: CategoryConfig,
) -> Result<Made, StateError> {
    if let Some(there) = config
        .read()
        .await
        .categories
        .iter()
        .find(|there| there.name.eq_ignore_ascii_case(&category.name))
    {
        return Ok(Made::There(there.clone()));
    }
    let db = db.clone();
    let made = tokio::task::spawn_blocking(move || {
        let category = CategoryConfig {
            id: db.next_category_id()?,
            ..category
        };
        db.insert_category(&category).map(|()| category)
    })
    .await
    .map_err(|error| StateError::Database(format!("making a category ended early: {error}")))??;
    config.write().await.categories.push(made.clone());
    Ok(Made::New(made))
}
