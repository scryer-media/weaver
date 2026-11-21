// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// One-time compatibility for installs upgrading from a pre-0.9.0 data
// directory.
//
// TRANSITIONAL: this module ships in 0.9.0 only and is scheduled for removal
// in 0.9.1. Removing it is three deletions — this file, its `mod` line in
// `lib.rs`, and the block that calls [`apply_pre_0_9_bind_compat_shim`] in the
// serve command — with nothing else to unpick.

use tracing::warn;
use weaver_server_settings::settings::persistence::SettingsPersistenceQueries;
use weaver_server_settings::settings::queries::SettingsQueries;

use weaver_server_base::security::{SETTING_ACCESS_MODE, SETTING_HTTP_BIND_ADDRESS};
use weaver_server_db::persistence::{StateError, connection::Database};

// First migration of the 0.9.0 line. A ledger that stops below this was last
// written by a pre-0.9.0 binary: 0.7.8 shipped through migration 0037 and
// 0.8.3 through 0039, while 0040-0043 are 0.9.0's own.
pub const FIRST_0_9_MIGRATION_VERSION: i64 = 40;

// The address a pre-0.9.0 install listened on without ever being told to:
// the default was `0.0.0.0` and nothing was stored, so the widening was
// invisible in the database and survives only if it is written down now.
const PRE_0_9_IMPLICIT_BIND_ADDRESS: &str = "0.0.0.0";

// Preserve a pre-0.9.0 install's network-wide HTTP bind by storing it, once.
//
// Before 0.9.0 the bind address defaulted to `0.0.0.0` and no setting recorded
// it; 0.9.0 defaults to loopback so that a fresh install is not exposed before
// its operator has answered a single question. Upgrading in place would
// therefore move a working remote-accessible install to loopback-only with
// nothing but a default change to explain it, and the clients that mattered —
// other machines, other containers, Sonarr-style integrations — would see
// connection refused. So an upgrade keeps what it had, and a fresh install
// keeps the safe default.
//
// Fires only when every one of these holds:
//
// 1. `pre_migration_schema_version` is `Some(v)` with `v < 40`: the ledger
//    proves a pre-0.9.0 binary last migrated this directory. `None` is a
//    fresh install (nothing to preserve) and `v >= 40` has already booted the
//    0.9.0 line at least once, where loopback is that install's status quo
//    rather than a regression.
// 2. `env_bind_address` is `None`. Any value of `WEAVER_HTTP_BIND_ADDRESS` —
//    including one this process would ignore as blank — is the operator
//    configuring the address in their deployment, and a stored value written
//    underneath it would surface as a surprise the day they remove it.
// 3. No bind address is stored yet, so nothing chosen can be overwritten.
//    This is also what makes the shim self-limiting: writing the setting is
//    what stops it ever firing again, on this boot or any later one.
// 4. No access mode is stored, i.e. the 0.9.0 setup wizard has never been
//    completed — the same marker the setup flow keys on. An operator who has
//    answered the access question has settled this install's exposure, and a
//    compatibility default must not reopen it.
//
// Deliberately NOT a condition: `WEAVER_STRICT_SECURITY`. A strict install
// with login disabled could not have been running network-wide in the first
// place (pre-0.9.0 refused to start in exactly that combination), so it either
// has login — where a wide bind is allowed — or pins the address in its
// environment, which condition 2 already defers to.
//
// The caller applies the stored value to the live config by settling the bind
// address from the settings table as it always does, which is why this runs
// before that step and does not touch the security config itself. It also does
// not mark security configured: reachability is preserved, the setup wizard
// still gets to ask.
//
// Returns whether the setting was written.
pub fn apply_pre_0_9_bind_compat_shim(
    db: &Database,
    pre_migration_schema_version: Option<i64>,
    env_bind_address: Option<&str>,
) -> Result<bool, StateError> {
    let Some(schema_version) = pre_migration_schema_version else {
        return Ok(false);
    };
    if schema_version >= FIRST_0_9_MIGRATION_VERSION || env_bind_address.is_some() {
        return Ok(false);
    }
    if db.get_setting(SETTING_HTTP_BIND_ADDRESS)?.is_some()
        || db.get_setting(SETTING_ACCESS_MODE)?.is_some()
    {
        return Ok(false);
    }

    db.set_setting(SETTING_HTTP_BIND_ADDRESS, PRE_0_9_IMPLICIT_BIND_ADDRESS)?;
    warn!(
        schema_version,
        setting = SETTING_HTTP_BIND_ADDRESS,
        bind_address = PRE_0_9_IMPLICIT_BIND_ADDRESS,
        "upgrade from a pre-0.9 install detected: 0.9 listens on loopback by default, so the \
         network-wide address this install was already serving on has been preserved as a stored \
         setting instead of narrowing it silently. Review it in Settings -> Security; this \
         one-time compatibility step is removed in 0.9.1"
    );
    Ok(true)
}
