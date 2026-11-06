// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

#[test]
fn concurrent_save_never_erases_a_committed_ssh_pin() {
    for id in 1..=8 {
        let db = Database::open_in_memory().unwrap();
        let mut profile = profile(id);
        profile.kind = ProxyKind::Ssh;
        profile.secrets.username = Some("fixture".into());
        profile.secrets.private_key = Some(weaver_tunnel::test_support::CLIENT_ED25519_PEM.into());
        db.save_proxy_profile(&profile).unwrap();
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let pin_db = db.clone();
        let pin_barrier = barrier.clone();
        let pin = std::thread::spawn(move || {
            pin_barrier.wait();
            pin_db.pin_proxy_host_key(id, 1, "SHA256:fixture")
        });
        profile.revision = 2;
        barrier.wait();
        db.save_proxy_profile(&profile).unwrap();
        if pin.join().unwrap().is_ok() {
            assert_eq!(
                db.proxy_host_key(id, 2).unwrap().as_deref(),
                Some("SHA256:fixture")
            );
        }
        assert!(db.pin_proxy_host_key(id, 1, "SHA256:stale").is_err());
    }
}
