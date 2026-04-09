use crate::StateError;
use crate::bandwidth::{IspBandwidthCapConfig, IspBandwidthCapPeriod, IspBandwidthCapWeekday};
use crate::persistence::Database;
use crate::settings::record::SettingRecord;
use crate::settings::{BufferPoolOverrides, Config, RetryOverrides, TunerOverrides};

impl Database {
    /// Load a full `Config` from the settings and servers tables.
    pub fn load_config(&self) -> Result<Config, StateError> {
        let settings: std::collections::HashMap<String, String> = self
            .list_setting_records()?
            .into_iter()
            .map(|SettingRecord { key, value }| (key, value))
            .collect();
        let servers = self.list_servers()?;
        let categories = self.list_categories()?;

        let data_dir = settings.get("data_dir").cloned().unwrap_or_default();
        let intermediate_dir = settings
            .get("intermediate_dir")
            .cloned()
            .or_else(|| settings.get("output_dir").cloned());
        let complete_dir = settings.get("complete_dir").cloned();
        let max_download_speed = settings
            .get("max_download_speed")
            .and_then(|v| v.parse().ok());
        let cleanup_after_extract = settings
            .get("cleanup_after_extract")
            .and_then(|v| v.parse().ok());
        let isp_bandwidth_cap = {
            let enabled = settings
                .get("bandwidth_cap.enabled")
                .and_then(|v| v.parse().ok())
                .unwrap_or(false);
            let limit_bytes = settings
                .get("bandwidth_cap.limit_bytes")
                .and_then(|v| v.parse().ok());
            let reset_time_minutes_local = settings
                .get("bandwidth_cap.reset_time_minutes_local")
                .and_then(|v| v.parse().ok());
            let period = settings
                .get("bandwidth_cap.period")
                .and_then(|v| parse_bandwidth_cap_period(v));
            let weekly_reset_weekday = settings
                .get("bandwidth_cap.weekly_reset_weekday")
                .and_then(|v| parse_bandwidth_cap_weekday(v));
            let monthly_reset_day = settings
                .get("bandwidth_cap.monthly_reset_day")
                .and_then(|v| v.parse().ok());

            match (
                period,
                limit_bytes,
                reset_time_minutes_local,
                weekly_reset_weekday,
                monthly_reset_day,
            ) {
                (
                    Some(period),
                    Some(limit_bytes),
                    Some(reset_time_minutes_local),
                    Some(weekly_reset_weekday),
                    Some(monthly_reset_day),
                ) => Some(IspBandwidthCapConfig {
                    enabled,
                    period,
                    limit_bytes,
                    reset_time_minutes_local,
                    weekly_reset_weekday,
                    monthly_reset_day,
                }),
                _ => None,
            }
        };

        let buffer_pool = {
            let small = settings
                .get("buffer_pool.small_count")
                .and_then(|v| v.parse().ok());
            let medium = settings
                .get("buffer_pool.medium_count")
                .and_then(|v| v.parse().ok());
            let large = settings
                .get("buffer_pool.large_count")
                .and_then(|v| v.parse().ok());
            if small.is_some() || medium.is_some() || large.is_some() {
                Some(BufferPoolOverrides {
                    small_count: small,
                    medium_count: medium,
                    large_count: large,
                })
            } else {
                None
            }
        };

        let tuner = {
            let max_dl = settings
                .get("tuner.max_concurrent_downloads")
                .and_then(|v| v.parse().ok());
            let max_dq = settings
                .get("tuner.max_decode_queue")
                .and_then(|v| v.parse().ok());
            let decode_threads = settings
                .get("tuner.decode_thread_count")
                .and_then(|v| v.parse().ok());
            let extract_threads = settings
                .get("tuner.extract_thread_count")
                .and_then(|v| v.parse().ok());
            if max_dl.is_some()
                || max_dq.is_some()
                || decode_threads.is_some()
                || extract_threads.is_some()
            {
                Some(TunerOverrides {
                    max_concurrent_downloads: max_dl,
                    max_decode_queue: max_dq,
                    decode_thread_count: decode_threads,
                    extract_thread_count: extract_threads,
                })
            } else {
                None
            }
        };

        let retry = {
            let max_retries = settings
                .get("retry.max_retries")
                .and_then(|v| v.parse().ok());
            let base_delay = settings
                .get("retry.base_delay_secs")
                .and_then(|v| v.parse().ok());
            let multiplier = settings
                .get("retry.multiplier")
                .and_then(|v| v.parse().ok());
            if max_retries.is_some() || base_delay.is_some() || multiplier.is_some() {
                Some(RetryOverrides {
                    max_retries,
                    base_delay_secs: base_delay,
                    multiplier,
                })
            } else {
                None
            }
        };

        Ok(Config {
            data_dir,
            intermediate_dir,
            complete_dir,
            buffer_pool,
            tuner,
            servers,
            categories,
            retry,
            max_download_speed,
            cleanup_after_extract,
            isp_bandwidth_cap,
            config_path: None,
        })
    }

    /// Save a full `Config` to the settings and servers tables.
    pub fn save_config(&self, config: &Config) -> Result<(), StateError> {
        self.set_setting("data_dir", &config.data_dir)?;
        if let Some(ref intermediate_dir) = config.intermediate_dir {
            self.set_setting("intermediate_dir", intermediate_dir)?;
        }
        if let Some(ref complete_dir) = config.complete_dir {
            self.set_setting("complete_dir", complete_dir)?;
        }
        if let Some(speed) = config.max_download_speed {
            self.set_setting("max_download_speed", &speed.to_string())?;
        }
        if let Some(cleanup) = config.cleanup_after_extract {
            self.set_setting("cleanup_after_extract", &cleanup.to_string())?;
        }
        if let Some(ref cap) = config.isp_bandwidth_cap {
            self.set_setting("bandwidth_cap.enabled", &cap.enabled.to_string())?;
            self.set_setting("bandwidth_cap.period", bandwidth_cap_period_str(cap.period))?;
            self.set_setting("bandwidth_cap.limit_bytes", &cap.limit_bytes.to_string())?;
            self.set_setting(
                "bandwidth_cap.reset_time_minutes_local",
                &cap.reset_time_minutes_local.to_string(),
            )?;
            self.set_setting(
                "bandwidth_cap.weekly_reset_weekday",
                bandwidth_cap_weekday_str(cap.weekly_reset_weekday),
            )?;
            self.set_setting(
                "bandwidth_cap.monthly_reset_day",
                &cap.monthly_reset_day.to_string(),
            )?;
        }

        if let Some(ref bp) = config.buffer_pool {
            if let Some(v) = bp.small_count {
                self.set_setting("buffer_pool.small_count", &v.to_string())?;
            }
            if let Some(v) = bp.medium_count {
                self.set_setting("buffer_pool.medium_count", &v.to_string())?;
            }
            if let Some(v) = bp.large_count {
                self.set_setting("buffer_pool.large_count", &v.to_string())?;
            }
        }

        if let Some(ref tuner) = config.tuner {
            if let Some(v) = tuner.max_concurrent_downloads {
                self.set_setting("tuner.max_concurrent_downloads", &v.to_string())?;
            }
            if let Some(v) = tuner.max_decode_queue {
                self.set_setting("tuner.max_decode_queue", &v.to_string())?;
            }
            if let Some(v) = tuner.decode_thread_count {
                self.set_setting("tuner.decode_thread_count", &v.to_string())?;
            }
        }

        if let Some(ref retry) = config.retry {
            if let Some(v) = retry.max_retries {
                self.set_setting("retry.max_retries", &v.to_string())?;
            }
            if let Some(v) = retry.base_delay_secs {
                self.set_setting("retry.base_delay_secs", &v.to_string())?;
            }
            if let Some(v) = retry.multiplier {
                self.set_setting("retry.multiplier", &v.to_string())?;
            }
        }

        self.replace_servers(&config.servers)?;
        self.replace_categories(&config.categories)?;

        Ok(())
    }
}

fn parse_bandwidth_cap_period(value: &str) -> Option<IspBandwidthCapPeriod> {
    match value {
        "daily" => Some(IspBandwidthCapPeriod::Daily),
        "weekly" => Some(IspBandwidthCapPeriod::Weekly),
        "monthly" => Some(IspBandwidthCapPeriod::Monthly),
        _ => None,
    }
}

fn bandwidth_cap_period_str(value: IspBandwidthCapPeriod) -> &'static str {
    match value {
        IspBandwidthCapPeriod::Daily => "daily",
        IspBandwidthCapPeriod::Weekly => "weekly",
        IspBandwidthCapPeriod::Monthly => "monthly",
    }
}

fn parse_bandwidth_cap_weekday(value: &str) -> Option<IspBandwidthCapWeekday> {
    match value {
        "mon" => Some(IspBandwidthCapWeekday::Mon),
        "tue" => Some(IspBandwidthCapWeekday::Tue),
        "wed" => Some(IspBandwidthCapWeekday::Wed),
        "thu" => Some(IspBandwidthCapWeekday::Thu),
        "fri" => Some(IspBandwidthCapWeekday::Fri),
        "sat" => Some(IspBandwidthCapWeekday::Sat),
        "sun" => Some(IspBandwidthCapWeekday::Sun),
        _ => None,
    }
}

fn bandwidth_cap_weekday_str(value: IspBandwidthCapWeekday) -> &'static str {
    match value {
        IspBandwidthCapWeekday::Mon => "mon",
        IspBandwidthCapWeekday::Tue => "tue",
        IspBandwidthCapWeekday::Wed => "wed",
        IspBandwidthCapWeekday::Thu => "thu",
        IspBandwidthCapWeekday::Fri => "fri",
        IspBandwidthCapWeekday::Sat => "sat",
        IspBandwidthCapWeekday::Sun => "sun",
    }
}
