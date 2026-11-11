// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

fn address(ip: &str) -> InterfaceAddress {
    InterfaceAddress {
        address: ip.parse().unwrap(),
        deprecated: false,
        tentative: false,
    }
}

fn snapshot() -> InterfaceSnapshot {
    InterfaceSnapshot {
        interfaces: vec![DiscoveredInterface {
            name: "wan0".into(),
            index: Some(1),
            up: true,
            addresses: vec![address("192.0.2.1"), address("2001:db8::1")],
        }],
        error: None,
    }
}

fn egress(binding: EgressBinding) -> EgressInterface {
    EgressInterface {
        id: 1,
        name: "Test egress".into(),
        binding,
        enabled: true,
        download_bps: None,
        upload_bps: None,
        quota: Default::default(),
    }
}

#[test]
fn health_distinguishes_missing_down_family_and_address_lifetime() {
    let mut sample = snapshot();
    let route = egress(EgressBinding::Interface {
        name: "wan0".into(),
    });
    assert_eq!(sample.health(&route, Some(false)), EgressHealth::Up);
    sample.interfaces[0].addresses[0].deprecated = true;
    assert!(matches!(
        sample.health(&route, Some(false)),
        EgressHealth::Down(_)
    ));
    assert_eq!(sample.health(&route, Some(true)), EgressHealth::Up);
    sample.interfaces[0].addresses[1].tentative = true;
    assert!(matches!(sample.health(&route, None), EgressHealth::Down(_)));
    sample.interfaces[0].up = false;
    assert_eq!(
        sample.health(&route, None),
        EgressHealth::Down("Interface is down".into())
    );
    sample.interfaces.clear();
    assert_eq!(
        sample.health(&route, None),
        EgressHealth::Down("Interface is missing".into())
    );
}

#[test]
fn source_must_be_present_and_usable_and_system_survives_enumeration_failure() {
    let mut sample = snapshot();
    let route = egress(EgressBinding::SourceAddress {
        address: "192.0.2.1".parse().unwrap(),
    });
    assert_eq!(sample.health(&route, Some(false)), EgressHealth::Up);
    assert!(matches!(
        sample.health(&route, Some(true)),
        EgressHealth::Down(_)
    ));
    sample.error = Some("enumeration failed".into());
    assert_eq!(sample.health(&route, None), EgressHealth::Unknown);
    assert_eq!(
        sample.health(&EgressInterface::system(), None),
        EgressHealth::Up
    );
    for ip in [
        "0.0.0.0",
        "224.0.0.1",
        "169.254.1.1",
        "::",
        "ff02::1",
        "fe80::1",
        "fd00::1",
    ] {
        assert!(!address(ip).usable(), "{ip}");
    }
}

#[tokio::test(start_paused = true)]
async fn injected_interface_changes_are_sampled_by_the_poll_clock() {
    struct Source {
        interfaces: RwLock<Vec<DiscoveredInterface>>,
        calls: tokio::sync::watch::Sender<u32>,
    }
    impl InterfaceSource for Source {
        fn interfaces(&self) -> io::Result<Vec<DiscoveredInterface>> {
            self.calls.send_modify(|n| *n += 1);
            Ok(self.interfaces.read().unwrap().clone())
        }
    }
    let (calls, mut observed) = tokio::sync::watch::channel(0);
    let source = Arc::new(Source {
        interfaces: RwLock::new(snapshot().interfaces),
        calls,
    });
    let monitor = InterfaceMonitor::start(source.clone(), &tokio::runtime::Handle::current());
    let route = egress(EgressBinding::Interface {
        name: "wan0".into(),
    });
    assert_eq!(monitor.snapshot().health(&route, None), EgressHealth::Up);
    observed.borrow_and_update();
    source.interfaces.write().unwrap().clear();
    // Await the new sample, allowing Tokio's paused clock to advance to the poll.
    observed.changed().await.unwrap();
    assert!(matches!(
        monitor.snapshot().health(&route, None),
        EgressHealth::Down(_)
    ));
    source
        .interfaces
        .write()
        .unwrap()
        .extend(snapshot().interfaces);
    observed.borrow_and_update();
    observed.changed().await.unwrap();
    assert_eq!(monitor.snapshot().health(&route, None), EgressHealth::Up);
}

// A source whose interfaces a test sets, that fails while told to, and
// counts its reads. A read is answered, compared and published in one turn
// of the monitor's task, so once the count passes a read, that read's
// sample is in.
struct SetSource {
    interfaces: RwLock<Vec<DiscoveredInterface>>,
    fail: std::sync::atomic::AtomicBool,
    calls: tokio::sync::watch::Sender<u32>,
}

impl InterfaceSource for SetSource {
    fn interfaces(&self) -> io::Result<Vec<DiscoveredInterface>> {
        self.calls.send_modify(|n| *n += 1);
        if self.fail.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(io::Error::other("enumeration failed"));
        }
        Ok(self.interfaces.read().unwrap().clone())
    }
}

fn set_source() -> (Arc<SetSource>, tokio::sync::watch::Receiver<u32>) {
    let (calls, observed) = tokio::sync::watch::channel(0);
    let source = Arc::new(SetSource {
        interfaces: RwLock::new(snapshot().interfaces),
        fail: std::sync::atomic::AtomicBool::new(false),
        calls,
    });
    (source, observed)
}

// Waits until a read begun after this call has been sampled whole: the
// read after the last one begun so far, which the source answered from
// what the test set before calling this.
async fn sampled(observed: &mut tokio::sync::watch::Receiver<u32>) {
    let begun = *observed.borrow();
    observed.wait_for(|n| *n >= begun + 2).await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn the_change_signal_rings_only_when_the_multicast_set_differs() {
    let (source, mut observed) = set_source();
    let monitor = InterfaceMonitor::start(source.clone(), &tokio::runtime::Handle::current());
    let mut changes = monitor.changes();
    assert_eq!(
        *changes.borrow_and_update(),
        multicast_view(&snapshot().interfaces)
    );

    // The same set again.
    sampled(&mut observed).await;
    assert!(!changes.has_changed().unwrap(), "the same set");

    // An address's lifetime flags are not where multicast joins.
    source.interfaces.write().unwrap()[0].addresses[1].tentative = true;
    sampled(&mut observed).await;
    assert!(!changes.has_changed().unwrap(), "a flag flipped");

    // A read that fails says nothing of the set; one that reads the same
    // set after it is no change either.
    source.fail.store(true, std::sync::atomic::Ordering::SeqCst);
    sampled(&mut observed).await;
    assert!(!changes.has_changed().unwrap(), "a failed read");
    source
        .fail
        .store(false, std::sync::atomic::Ordering::SeqCst);
    sampled(&mut observed).await;
    assert!(
        !changes.has_changed().unwrap(),
        "the same set after a failure"
    );

    // A new address is a change, and the signal holds the new set.
    source.interfaces.write().unwrap()[0]
        .addresses
        .push(address("192.0.2.7"));
    sampled(&mut observed).await;
    assert!(changes.has_changed().unwrap(), "an address came");
    let seen = changes.borrow_and_update().clone();
    assert_eq!(
        seen[0].addresses,
        ["192.0.2.1", "2001:db8::1", "192.0.2.7"]
            .map(|ip| ip.parse::<IpAddr>().unwrap())
            .to_vec()
    );

    // An interface going down, and one coming, are changes too.
    source.interfaces.write().unwrap()[0].up = false;
    sampled(&mut observed).await;
    assert!(changes.has_changed().unwrap(), "an interface went down");
    assert!(!changes.borrow_and_update()[0].up);
    source
        .interfaces
        .write()
        .unwrap()
        .push(DiscoveredInterface {
            name: "en1".into(),
            index: Some(2),
            up: true,
            addresses: vec![address("10.0.0.2")],
        });
    sampled(&mut observed).await;
    assert!(changes.has_changed().unwrap(), "an interface came");
}

// Changes found before the receiver looks ring it once, with the last set.
#[tokio::test(start_paused = true)]
async fn changes_before_the_receiver_looks_are_taken_together() {
    let (source, mut observed) = set_source();
    let monitor = InterfaceMonitor::start(source.clone(), &tokio::runtime::Handle::current());
    let mut changes = monitor.changes();
    changes.borrow_and_update();
    for ip in ["192.0.2.7", "192.0.2.8", "192.0.2.9"] {
        source.interfaces.write().unwrap()[0].addresses = vec![address(ip)];
        sampled(&mut observed).await;
    }
    changes.changed().await.unwrap();
    assert_eq!(
        changes.borrow_and_update()[0].addresses,
        ["192.0.2.9".parse::<IpAddr>().unwrap()]
    );
    assert!(!changes.has_changed().unwrap(), "one ring for the three");
}
