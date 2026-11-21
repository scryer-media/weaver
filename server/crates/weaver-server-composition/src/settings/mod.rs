// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

pub mod archive_passwords;
pub use archive_passwords::SettingsArchivePasswordsQueries;
pub mod env_seed;
pub use weaver_server_settings::settings::levels;
pub use weaver_server_settings::settings::model;
pub use weaver_server_settings::settings::persistence;
pub use weaver_server_settings::settings::queries;
pub use weaver_server_settings::settings::record;
pub mod repository;
mod schedules;
pub use schedules::SettingsSchedulesQueries;
pub use weaver_server_net_runtime::settings::service;
pub use weaver_server_settings::settings::tracker_file;

pub use model::{
    BlocklistSource, Client, Config, DEFAULT_HISTORY_RETENTION_DAYS, DeliveryNamingOverrides,
    DirectStoreOverrides, Leaf, Level, Levels, MAX_HISTORY_RETENTION_DAYS, MetricsConfig,
    PeerTransport, PeerTransports, PerJobSeries, RetryOverrides, SeedTimeCounting, Seeding,
    SeedingAction, SeedingGoalMode, SeedingPolicy, SharedConfig, TorrentConfig, TorrentEncryption,
    TorrentLimit, TransportRule,
};
pub use service::HARDWARE_PROFILE_SETTING;
