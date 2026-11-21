// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use weaver_server_base::operations::instrumentation::VolumeSnapshot;

// A volume's row in the packet carries every figure the disk owner
// holds for it, and reads back as written.
#[test]
fn a_volume_is_listed_with_every_figure_held_for_it() {
    let listed = VolumeDiagnostics::from(VolumeSnapshot {
        label: "/mnt/pool".into(),
        kind: "platter",
        device: Some(7),
        written_bytes: 8_000_000,
        write_nanos: 2_500_000_000,
        write_calls: 120,
        write_seeks: 9,
        write_rate: 3_200_000,
        throughput: 0,
        buffered_bytes: 65_536,
        arrived_bytes: 0,
        counted_bytes: 70_000,
        carried_bytes: 0,
        peak_bytes: 90_000,
        buffer_soft_bytes: 0,
        buffer_limit_bytes: 0,
        idle_turns: 0,
        writer: Default::default(),
    });
    let json = serde_json::to_value(&listed).unwrap();
    assert_eq!(
        json,
        serde_json::json!({
            "label": "/mnt/pool",
            "storage_kind": "platter",
            "device": 7,
            "write_rate_bytes_per_second": 3_200_000,
            "buffered_bytes": 65_536,
            "counted_bytes": 70_000,
            "peak_counted_bytes": 90_000,
            "buffer_limit_bytes": 0,
            "written_bytes_total": 8_000_000,
            "write_seconds_total": 2.5,
            "write_calls_total": 120,
            "write_seeks_total": 9,
        })
    );
    let read: VolumeDiagnostics = serde_json::from_value(json).unwrap();
    assert_eq!(read.label, "/mnt/pool");
    assert_eq!(read.write_seeks_total, 9);
}
