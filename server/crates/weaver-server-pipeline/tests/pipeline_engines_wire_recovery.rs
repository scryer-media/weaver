// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

pub use weaver_server_pipeline::jobs;
mod pipeline {
    mod tests {
        use weaver_server_pipeline::pipeline::tests::{
            build_test_par2_index_for_files, direct_store,
        };
        mod engines {
            use weaver_server_pipeline::pipeline::tests::engines::{
                harness, restart_fixtures as restart, wire_recovery,
            };
            #[path = "recovery_audit_stream.rs"]
            mod recovery_audit;
            use weaver_server_pipeline::pipeline::tests::engines::{disk_archive, disk_reserve};
            mod disk_wait;
            mod lent_handed;
            mod recovery_damage_wire;
            mod recovery_duplicate_wire;
            mod recovery_encrypted_wire;
            mod recovery_geometry_wire;
            mod recovery_restart_wire;
            mod remaining_par2;
            mod remaining_stalls;
            mod repaired_source;
        }
    }
}
