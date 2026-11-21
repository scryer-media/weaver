// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::borrow::Borrow;
use std::ops::Deref;
use std::sync::Arc;

use weaver_server_db::persistence::connection::Database as SqlDatabase;
use weaver_server_rss::rss::queries::{ScheduleCache, ScheduleStore};

// What the network runtime needs of the composed database beyond its SQL
// connection: the RSS schedule cache a saved route change wakes, and the
// test counter of route policy reads.
pub trait NetDatabase: Borrow<SqlDatabase> + ScheduleStore + Send + Sync + 'static {
    // Counts one read of a saved route policy, for tests that assert a hot
    // path does not read the database.
    fn proxy_route_read(&self) {}
}

// The database handle the network runtimes hold: the composed database
// behind its `NetDatabase` seam.
#[derive(Clone)]
pub struct NetDb(Arc<dyn NetDatabase>);

impl NetDb {
    pub fn new(db: impl NetDatabase) -> Self {
        Self(Arc::new(db))
    }
}

impl Deref for NetDb {
    type Target = SqlDatabase;
    fn deref(&self) -> &SqlDatabase {
        (*self.0).borrow()
    }
}

impl Borrow<SqlDatabase> for NetDb {
    fn borrow(&self) -> &SqlDatabase {
        (*self.0).borrow()
    }
}

impl ScheduleStore for NetDb {
    fn schedule_cache(&self) -> &ScheduleCache {
        self.0.schedule_cache()
    }
}

impl NetDatabase for NetDb {
    fn proxy_route_read(&self) {
        self.0.proxy_route_read();
    }
}
